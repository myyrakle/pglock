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

	wg := sync.WaitGroup{}

	for i := 0; i < 5; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			log.Printf("Goroutine %d: Attempting to acquire lock...", i)
			_, err := lockClient.SLock(ctx, pglock.SLockParams{
				Name:           "test_lock",
				LockID:         fmt.Sprintf("test_lock_%d", i),
				TTLSeconds:     60,
				MaxSharedLocks: 2,
			})
			if err != nil {
				log.Printf("Goroutine %d: Failed to acquire lock: %v", i, err)
				return
			}
			log.Printf("Goroutine %d: Lock acquired!", i)

			time.Sleep(1 * time.Second) // Simulate some work with the lock held

			if _, err := lockClient.Unlock(ctx, pglock.UnlockParams{
				Name:   "test_lock",
				LockID: fmt.Sprintf("test_lock_%d", i),
			}); err != nil {
				log.Printf("Goroutine %d: Failed to release lock: %v", i, err)
				return
			}
			log.Printf("Goroutine %d: Lock released!", i)
		}(i)
	}

	wg.Wait()
	log.Println("All goroutines have finished.")

}
