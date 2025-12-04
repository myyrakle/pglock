package main

import (
	"context"
	"fmt"
	"log"
	"sync"
	"time"

	"github.com/myyrakle/pglock"
)

func main() {
	lockClient := pglock.NewLockClient(pglock.LockClientOptions{
		DatabaseURL: "postgres://postgres@localhost:5432/postgres?sslmode=disable",
	})
	if err := lockClient.Initialize(); err != nil {
		log.Fatal(err)
	}

	ctx := context.Background()

	fmt.Println("=== Test 1: 기존 XLock 테스트 ===")
	testXLock(ctx, lockClient)

	fmt.Println("\n=== Test 2: 무제한 SLock 동시 획득 테스트 ===")
	testUnlimitedSLock(ctx, lockClient)

	fmt.Println("\n=== Test 3: 제한된 SLock 테스트 (최대 3개) ===")
	testLimitedSLock(ctx, lockClient)

	fmt.Println("\n=== Test 4: SLock 존재 시 XLock 차단 테스트 ===")
	testSLockBlocksXLock(ctx, lockClient)

	fmt.Println("\n=== Test 5: XLock 존재 시 SLock 차단 테스트 ===")
	testXLockBlocksSLock(ctx, lockClient)

	fmt.Println("\n모든 테스트 완료!")
}

func testXLock(ctx context.Context, lockClient pglock.LockClient) {
	wg := sync.WaitGroup{}
	for i := range 3 {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			fmt.Printf("[XLock] Try Lock %d\n", id)
			_, err := lockClient.XLock(ctx, pglock.XLockParams{
				Name:             "test_xlock",
				LockID:           fmt.Sprintf("xlock_%d", id),
				TTLSeconds:       60,
				IntervalDuration: 100 * time.Millisecond,
			})
			if err != nil {
				log.Printf("[XLock] Error %d: %v\n", id, err)
				return
			}
			defer lockClient.Unlock(ctx, pglock.UnlockParams{
				Name:   "test_xlock",
				LockID: fmt.Sprintf("xlock_%d", id),
			})

			fmt.Printf("[XLock] Locked %d\n", id)
			time.Sleep(500 * time.Millisecond)
			fmt.Printf("[XLock] Unlocked %d\n", id)
		}(i)
	}
	wg.Wait()
}

func testUnlimitedSLock(ctx context.Context, lockClient pglock.LockClient) {
	wg := sync.WaitGroup{}
	for i := range 5 {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			fmt.Printf("[UnlimitedSLock] Try Lock %d\n", id)
			_, err := lockClient.SLock(ctx, pglock.SLockParams{
				Name:             "test_unlimited_slock",
				LockID:           fmt.Sprintf("reader_%d", id),
				TTLSeconds:       30,
				MaxSharedLocks:   -1, // 무제한
				IntervalDuration: 100 * time.Millisecond,
			})
			if err != nil {
				log.Printf("[UnlimitedSLock] Error %d: %v\n", id, err)
				return
			}
			defer lockClient.Unlock(ctx, pglock.UnlockParams{
				Name:   "test_unlimited_slock",
				LockID: fmt.Sprintf("reader_%d", id),
			})

			fmt.Printf("[UnlimitedSLock] Locked %d\n", id)
			time.Sleep(1 * time.Second)
			fmt.Printf("[UnlimitedSLock] Unlocked %d\n", id)
		}(i)
	}
	wg.Wait()
}

func testLimitedSLock(ctx context.Context, lockClient pglock.LockClient) {
	wg := sync.WaitGroup{}
	for i := range 5 {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			fmt.Printf("[LimitedSLock] Try Lock %d\n", id)
			_, err := lockClient.SLock(ctx, pglock.SLockParams{
				Name:             "test_limited_slock",
				LockID:           fmt.Sprintf("reader_%d", id),
				TTLSeconds:       30,
				MaxSharedLocks:   3, // 최대 3개
				IntervalDuration: 100 * time.Millisecond,
			})
			if err != nil {
				log.Printf("[LimitedSLock] Error %d: %v\n", id, err)
				return
			}
			defer lockClient.Unlock(ctx, pglock.UnlockParams{
				Name:   "test_limited_slock",
				LockID: fmt.Sprintf("reader_%d", id),
			})

			fmt.Printf("[LimitedSLock] Locked %d (처음 3개는 즉시, 나머지는 대기 후 획득)\n", id)
			time.Sleep(1 * time.Second)
			fmt.Printf("[LimitedSLock] Unlocked %d\n", id)
		}(i)
	}
	wg.Wait()
}

func testSLockBlocksXLock(ctx context.Context, lockClient pglock.LockClient) {
	// 1. SLock 획득
	fmt.Println("[SLockBlocksXLock] SLock 획득 중...")
	_, err := lockClient.SLock(ctx, pglock.SLockParams{
		Name:             "test_slock_blocks_xlock",
		LockID:           "reader_1",
		TTLSeconds:       30,
		MaxSharedLocks:   -1,
		IntervalDuration: 100 * time.Millisecond,
	})
	if err != nil {
		log.Fatalf("[SLockBlocksXLock] SLock 획득 실패: %v", err)
	}
	fmt.Println("[SLockBlocksXLock] SLock 획득 완료")

	// 2. 다른 고루틴에서 XLock 시도 (실패해야 함)
	wg := sync.WaitGroup{}
	wg.Add(1)
	go func() {
		defer wg.Done()
		fmt.Println("[SLockBlocksXLock] XLock 시도 중... (SLock이 있으므로 대기)")
		ctx, cancel := context.WithTimeout(ctx, 2*time.Second)
		defer cancel()
		_, err := lockClient.XLock(ctx, pglock.XLockParams{
			Name:             "test_slock_blocks_xlock",
			LockID:           "writer_1",
			TTLSeconds:       30,
			IntervalDuration: 100 * time.Millisecond,
		})
		if err == context.DeadlineExceeded {
			fmt.Println("[SLockBlocksXLock] XLock 타임아웃 (예상된 동작)")
		} else if err != nil {
			fmt.Printf("[SLockBlocksXLock] XLock 에러: %v\n", err)
		} else {
			fmt.Println("[SLockBlocksXLock] XLock 획득 성공 (SLock 해제 후)")
		}
	}()

	// 3. 1초 후 SLock 해제
	time.Sleep(1 * time.Second)
	fmt.Println("[SLockBlocksXLock] SLock 해제 중...")
	lockClient.Unlock(ctx, pglock.UnlockParams{
		Name:   "test_slock_blocks_xlock",
		LockID: "reader_1",
	})
	fmt.Println("[SLockBlocksXLock] SLock 해제 완료")

	wg.Wait()
}

func testXLockBlocksSLock(ctx context.Context, lockClient pglock.LockClient) {
	// 1. XLock 획득
	fmt.Println("[XLockBlocksSLock] XLock 획득 중...")
	_, err := lockClient.XLock(ctx, pglock.XLockParams{
		Name:             "test_xlock_blocks_slock",
		LockID:           "writer_1",
		TTLSeconds:       30,
		IntervalDuration: 100 * time.Millisecond,
	})
	if err != nil {
		log.Fatalf("[XLockBlocksSLock] XLock 획득 실패: %v", err)
	}
	fmt.Println("[XLockBlocksSLock] XLock 획득 완료")

	// 2. 다른 고루틴에서 SLock 시도 (실패해야 함)
	wg := sync.WaitGroup{}
	wg.Add(1)
	go func() {
		defer wg.Done()
		fmt.Println("[XLockBlocksSLock] SLock 시도 중... (XLock이 있으므로 대기)")
		ctx, cancel := context.WithTimeout(ctx, 2*time.Second)
		defer cancel()
		_, err := lockClient.SLock(ctx, pglock.SLockParams{
			Name:             "test_xlock_blocks_slock",
			LockID:           "reader_1",
			TTLSeconds:       30,
			MaxSharedLocks:   -1,
			IntervalDuration: 100 * time.Millisecond,
		})
		if err == context.DeadlineExceeded {
			fmt.Println("[XLockBlocksSLock] SLock 타임아웃 (예상된 동작)")
		} else if err != nil {
			fmt.Printf("[XLockBlocksSLock] SLock 에러: %v\n", err)
		} else {
			fmt.Println("[XLockBlocksSLock] SLock 획득 성공 (XLock 해제 후)")
		}
	}()

	// 3. 1초 후 XLock 해제
	time.Sleep(1 * time.Second)
	fmt.Println("[XLockBlocksSLock] XLock 해제 중...")
	lockClient.Unlock(ctx, pglock.UnlockParams{
		Name:   "test_xlock_blocks_slock",
		LockID: "writer_1",
	})
	fmt.Println("[XLockBlocksSLock] XLock 해제 완료")

	wg.Wait()
}
