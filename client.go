package pglock

import (
	"context"
	"database/sql"
	"log"

	_ "github.com/lib/pq"
)

type LockClientOptions struct {
	DatabaseURL        string // [required] example: "postgres://user:password@localhost:5432/dbname"
	MaxOpenConnections int    // [optional] default: 10
	MaxIdleConnections int    // [optional] default: 5

	LockTableName              string // [optional] default: "lock"
	PriorityLockTableName      string // [optional] default: "priority_lock"
	PriorityLockQueueTableName string // [optional] default: "priority_lock_queue"
}

func (options *LockClientOptions) SetDefaults() {
	if options.LockTableName == "" {
		options.LockTableName = "lock"
	}
	if options.PriorityLockTableName == "" {
		options.PriorityLockTableName = "priority_lock"
	}
	if options.PriorityLockQueueTableName == "" {
		options.PriorityLockQueueTableName = "priority_lock_queue"
	}

	if options.MaxIdleConnections == 0 {
		options.MaxIdleConnections = 5
	}

	if options.MaxOpenConnections == 0 {
		options.MaxOpenConnections = 10
	}
}

func NewLockClient(options LockClientOptions) LockClient {
	options.SetDefaults()

	return &lockClient{
		options: options,
	}
}

type LockClient interface {
	Initialize() error
	TryXLock(ctx context.Context, params TryXLockParams) (TryXLockResult, error)
	XLock(ctx context.Context, params XLockParams) (XLockResult, error)
	Unlock(ctx context.Context, params UnlockParams) (UnlockResult, error)
}

type lockClient struct {
	options LockClientOptions
	db      *sql.DB
}

func (c *lockClient) connect() error {
	db, err := sql.Open("postgres", c.options.DatabaseURL)
	if err != nil {
		log.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	db.SetMaxOpenConns(c.options.MaxOpenConnections)
	db.SetMaxIdleConns(c.options.MaxIdleConnections)

	c.db = db

	return nil
}

func (c *lockClient) setupTables() error {
	if err := c.createLockTable(context.Background()); err != nil {
		return err
	}

	return nil
}

func (c *lockClient) Initialize() error {
	if err := c.connect(); err != nil {
		return err
	}

	if err := c.setupTables(); err != nil {
		return err
	}

	return nil
}
