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
	for i := range 10 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			fmt.Println("Try Lock!", i)
			_, err := lockClient.XLock(ctx, pglock.XLockParams{
				Name:       "test_lock",
				Owner:      fmt.Sprintf("test_lock_%d", i),
				TTLSeconds: 60,
			})
			if err != nil {
				log.Fatal(err)
			}
			defer lockClient.Unlock(ctx, pglock.UnlockParams{
				Name:  "test_lock",
				Owner: fmt.Sprintf("test_lock_%d", i),
			})

			fmt.Println("Locked!", i)
			time.Sleep(time.Second * 2)
			fmt.Println("Unlocked!", i)
		}()
	}
	wg.Wait()

}
