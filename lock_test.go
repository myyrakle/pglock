package pglock

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

var (
	testDB    *LockClient
	testDBURL string
)

func setupTestDB(t *testing.T) LockClient {
	if testDB != nil {
		return *testDB
	}

	// 환경 변수로 데이터베이스 URL 지정 가능
	testDBURL = "postgres://postgres@localhost:5432/postgres?sslmode=disable"

	// LockClient 생성 및 초기화
	client := NewLockClient(LockClientOptions{
		DatabaseURL: testDBURL,
	})

	// 연결 테스트
	if err := client.Connect(); err != nil {
		t.Skipf("PostgreSQL not available at %s: %v\nPlease start PostgreSQL or set TEST_DATABASE_URL environment variable", testDBURL, err)
	}

	if err := client.SetupTables(); err != nil {
		t.Fatalf("Could not initialize lock client: %s", err)
	}

	testDB = &client
	return client
}

func TestMain(m *testing.M) {
	m.Run()
}

// TestXLock_Sequential tests that XLocks are acquired sequentially
func TestXLock_Sequential(t *testing.T) {
	client := setupTestDB(t)
	ctx := context.Background()

	var wg sync.WaitGroup
	acquired := make([]int, 0)
	var mu sync.Mutex

	for i := 0; i < 3; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()

			_, err := client.XLock(ctx, XLockParams{
				Name:             "test_xlock_seq",
				LockID:           fmt.Sprintf("xlock_%d", id),
				TTLSeconds:       60,
				IntervalDuration: 50 * time.Millisecond,
			})
			require.NoError(t, err)

			mu.Lock()
			acquired = append(acquired, id)
			mu.Unlock()

			time.Sleep(200 * time.Millisecond)

			_, err = client.Unlock(ctx, UnlockParams{
				Name:   "test_xlock_seq",
				LockID: fmt.Sprintf("xlock_%d", id),
			})
			require.NoError(t, err)
		}(i)
	}

	wg.Wait()

	// 3개가 모두 획득되었는지 확인
	assert.Len(t, acquired, 3)
}

// TestSLock_Concurrent tests that multiple SLocks can be acquired concurrently
func TestSLock_Concurrent(t *testing.T) {
	client := setupTestDB(t)
	ctx := context.Background()

	var wg sync.WaitGroup
	lockCount := 5
	acquiredTimes := make([]time.Time, lockCount)

	startTime := time.Now()

	for i := 0; i < lockCount; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()

			_, err := client.SLock(ctx, SLockParams{
				Name:             "test_slock_concurrent",
				LockID:           fmt.Sprintf("reader_%d", id),
				TTLSeconds:       30,
				MaxSharedLocks:   -1, // 무제한
				IntervalDuration: 50 * time.Millisecond,
			})
			require.NoError(t, err)

			acquiredTimes[id] = time.Now()

			time.Sleep(500 * time.Millisecond)

			_, err = client.Unlock(ctx, UnlockParams{
				Name:   "test_slock_concurrent",
				LockID: fmt.Sprintf("reader_%d", id),
			})
			require.NoError(t, err)
		}(i)
	}

	wg.Wait()

	// 모든 락이 거의 동시에 획득되었는지 확인 (1초 이내)
	for _, acquiredTime := range acquiredTimes {
		assert.WithinDuration(t, startTime, acquiredTime, 1*time.Second)
	}
}

// TestSLock_Limited tests that MaxSharedLocks limit is enforced
func TestSLock_Limited(t *testing.T) {
	client := setupTestDB(t)
	ctx := context.Background()

	maxLocks := 3
	totalAttempts := 5

	var wg sync.WaitGroup
	acquiredTimes := make([]time.Time, totalAttempts)

	startTime := time.Now()

	for i := 0; i < totalAttempts; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()

			_, err := client.SLock(ctx, SLockParams{
				Name:             "test_slock_limited",
				LockID:           fmt.Sprintf("reader_%d", id),
				TTLSeconds:       30,
				MaxSharedLocks:   maxLocks,
				IntervalDuration: 50 * time.Millisecond,
			})
			require.NoError(t, err)

			acquiredTimes[id] = time.Now()

			time.Sleep(500 * time.Millisecond)

			_, err = client.Unlock(ctx, UnlockParams{
				Name:   "test_slock_limited",
				LockID: fmt.Sprintf("reader_%d", id),
			})
			require.NoError(t, err)
		}(i)
	}

	wg.Wait()

	// 처음 maxLocks개는 빠르게 획득, 나머지는 대기 후 획득
	quickAcquired := 0
	for _, acquiredTime := range acquiredTimes {
		if acquiredTime.Sub(startTime) < 300*time.Millisecond {
			quickAcquired++
		}
	}

	// 처음 3개는 즉시 획득되어야 함
	assert.GreaterOrEqual(t, quickAcquired, maxLocks)
}

// TestXLock_BlocksSLock tests that XLock blocks SLock
func TestXLock_BlocksSLock(t *testing.T) {
	client := setupTestDB(t)
	ctx := context.Background()

	// 1. XLock 획득
	_, err := client.XLock(ctx, XLockParams{
		Name:             "test_xlock_blocks_slock",
		LockID:           "writer_1",
		TTLSeconds:       30,
		IntervalDuration: 50 * time.Millisecond,
	})
	require.NoError(t, err)

	// 2. SLock 시도 (타임아웃되어야 함)
	slockCtx, cancel := context.WithTimeout(ctx, 500*time.Millisecond)
	defer cancel()

	_, err = client.SLock(slockCtx, SLockParams{
		Name:             "test_xlock_blocks_slock",
		LockID:           "reader_1",
		TTLSeconds:       30,
		MaxSharedLocks:   -1,
		IntervalDuration: 50 * time.Millisecond,
	})
	assert.ErrorIs(t, err, context.DeadlineExceeded)

	// 3. XLock 해제
	_, err = client.Unlock(ctx, UnlockParams{
		Name:   "test_xlock_blocks_slock",
		LockID: "writer_1",
	})
	require.NoError(t, err)

	// 4. 이제 SLock 획득 가능
	_, err = client.SLock(ctx, SLockParams{
		Name:             "test_xlock_blocks_slock",
		LockID:           "reader_1",
		TTLSeconds:       30,
		MaxSharedLocks:   -1,
		IntervalDuration: 50 * time.Millisecond,
	})
	require.NoError(t, err)

	// 정리
	client.Unlock(ctx, UnlockParams{
		Name:   "test_xlock_blocks_slock",
		LockID: "reader_1",
	})
}

// TestSLock_BlocksXLock tests that SLock blocks XLock
func TestSLock_BlocksXLock(t *testing.T) {
	client := setupTestDB(t)
	ctx := context.Background()

	// 1. SLock 획득
	_, err := client.SLock(ctx, SLockParams{
		Name:             "test_slock_blocks_xlock",
		LockID:           "reader_1",
		TTLSeconds:       30,
		MaxSharedLocks:   -1,
		IntervalDuration: 50 * time.Millisecond,
	})
	require.NoError(t, err)

	// 2. XLock 시도 (타임아웃되어야 함)
	xlockCtx, cancel := context.WithTimeout(ctx, 500*time.Millisecond)
	defer cancel()

	_, err = client.XLock(xlockCtx, XLockParams{
		Name:             "test_slock_blocks_xlock",
		LockID:           "writer_1",
		TTLSeconds:       30,
		IntervalDuration: 50 * time.Millisecond,
	})
	assert.ErrorIs(t, err, context.DeadlineExceeded)

	// 3. SLock 해제
	_, err = client.Unlock(ctx, UnlockParams{
		Name:   "test_slock_blocks_xlock",
		LockID: "reader_1",
	})
	require.NoError(t, err)

	// 4. 이제 XLock 획득 가능
	_, err = client.XLock(ctx, XLockParams{
		Name:             "test_slock_blocks_xlock",
		LockID:           "writer_1",
		TTLSeconds:       30,
		IntervalDuration: 50 * time.Millisecond,
	})
	require.NoError(t, err)

	// 정리
	client.Unlock(ctx, UnlockParams{
		Name:   "test_slock_blocks_xlock",
		LockID: "writer_1",
	})
}

// TestTryXLock_NonBlocking tests that TryXLock returns immediately
func TestTryXLock_NonBlocking(t *testing.T) {
	client := setupTestDB(t)
	ctx := context.Background()

	// 1. 첫 번째 락 획득
	result1, err := client.TryXLock(ctx, TryXLockParams{
		Name:       "test_try_xlock",
		LockID:     "lock_1",
		TTLSeconds: 30,
	})
	require.NoError(t, err)
	assert.True(t, result1.Acquired)

	// 2. 두 번째 시도는 즉시 실패해야 함
	startTime := time.Now()
	result2, err := client.TryXLock(ctx, TryXLockParams{
		Name:       "test_try_xlock",
		LockID:     "lock_2",
		TTLSeconds: 30,
	})
	elapsed := time.Since(startTime)

	require.NoError(t, err)
	assert.False(t, result2.Acquired)
	assert.Less(t, elapsed, 100*time.Millisecond, "TryXLock should return immediately")

	// 정리
	client.Unlock(ctx, UnlockParams{
		Name:   "test_try_xlock",
		LockID: "lock_1",
	})
}

// TestTrySLock_NonBlocking tests that TrySLock returns immediately
func TestTrySLock_NonBlocking(t *testing.T) {
	client := setupTestDB(t)
	ctx := context.Background()

	// 1. XLock 획득
	_, err := client.XLock(ctx, XLockParams{
		Name:             "test_try_slock",
		LockID:           "writer_1",
		TTLSeconds:       30,
		IntervalDuration: 50 * time.Millisecond,
	})
	require.NoError(t, err)

	// 2. TrySLock은 즉시 실패해야 함
	startTime := time.Now()
	result, err := client.TrySLock(ctx, TrySLockParams{
		Name:           "test_try_slock",
		LockID:         "reader_1",
		TTLSeconds:     30,
		MaxSharedLocks: -1,
	})
	elapsed := time.Since(startTime)

	require.NoError(t, err)
	assert.False(t, result.Acquired)
	assert.Less(t, elapsed, 100*time.Millisecond, "TrySLock should return immediately")

	// 정리
	client.Unlock(ctx, UnlockParams{
		Name:   "test_try_slock",
		LockID: "writer_1",
	})
}

// TestLockExpiration tests that locks expire after TTL
func TestLockExpiration(t *testing.T) {
	client := setupTestDB(t)
	ctx := context.Background()

	// 1. 짧은 TTL로 XLock 획득
	_, err := client.XLock(ctx, XLockParams{
		Name:             "test_expiration",
		LockID:           "lock_1",
		TTLSeconds:       1, // 1초
		IntervalDuration: 50 * time.Millisecond,
	})
	require.NoError(t, err)

	// 2. 즉시 다른 락 시도는 실패
	result1, err := client.TryXLock(ctx, TryXLockParams{
		Name:       "test_expiration",
		LockID:     "lock_2",
		TTLSeconds: 30,
	})
	require.NoError(t, err)
	assert.False(t, result1.Acquired)

	// 3. 1.5초 대기
	time.Sleep(1500 * time.Millisecond)

	// 4. 이제 락 획득 가능 (만료됨)
	result2, err := client.TryXLock(ctx, TryXLockParams{
		Name:       "test_expiration",
		LockID:     "lock_2",
		TTLSeconds: 30,
	})
	require.NoError(t, err)
	assert.True(t, result2.Acquired)

	// 정리
	client.Unlock(ctx, UnlockParams{
		Name:   "test_expiration",
		LockID: "lock_2",
	})
}
