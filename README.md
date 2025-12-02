# pglock
- Distributed Lock implementation using PostgreSQL

## Requirements
- PostgreSQL 13+
- Go 1.23+

## Usage

install
```bash
go get github.com/yourusername/pglock
```

configuration 
```go
	lockClient := pglock.NewLockClient(pglock.LockClientOptions{
		DatabaseURL: "postgres://postgres@localhost:5432/postgres?sslmode=disable",
	})
	if err := lockClient.Initialize(); err != nil {
		log.Fatal(err)
	}
```

using 
```go
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
```
