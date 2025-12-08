# pglock

![](https://img.shields.io/badge/language-Go-00ADD8) ![](https://img.shields.io/badge/version-0.2.0-brightgreen) [![GitHub license](https://img.shields.io/badge/license-MIT-blue.svg)](./LICENSE)

- Distributed Lock implementation using PostgreSQL

## Requirements

- PostgreSQL 13+
- Go 1.23+

## Usage

install

```bash
go get github.com/myyrakle/pglock@v0.1.0
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
		LockID:      fmt.Sprintf("test_lock_%d", i),
		TTLSeconds: 60,
	})
	if err != nil {
		log.Fatal(err)
	}
	defer lockClient.Unlock(ctx, pglock.UnlockParams{
		Name:  "test_lock",
		LockID: fmt.Sprintf("test_lock_%d", i),
	})
```

## Locks

1. Exclusive Lock (XLock)
2. Shared Lock (SLock)

- XLock
  XLock allows only one simultaneous access to the same name. All other accesses are blocked (similar to Mutex).

- SLock
  SLock allows N simultaneous accesses to the same name (similar to semaphore).
  If SLock is enabled, XLock access is blocked.

```go
	_, err := lockClient.SLock(ctx, pglock.SLockParams{
		Name:       "test_lock",
		LockID:      fmt.Sprintf("test_lock_%d", i),
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

## Internal

- SLock and XLock implement blocking through an internal try loop.
- The interval time can be adjusted to accommodate speed and database load.

```go
_, err := lockClient.XLock(ctx, pglock.XLockParams{
		Name:       "test_lock",
		LockID:      fmt.Sprintf("test_lock_%d", i),
		TTLSeconds: 60,
		IntervalDuration: time.Second * 1, // try loop interval
	})
	if err != nil {
		log.Fatal(err)
	}
```

- If you require precise optimization, you can use the non-blocking functions `TryXLock` and `TrySLock`.
