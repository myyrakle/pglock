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
	// Connect Database and Setup Tables (Connect + SetupTables)
	Initialize() error
	// Connect to Database
	Connect() error
	// Setup necessary tables
	SetupTables() error

	// Try to acquire exclusive lock (non-blocking, returns immediately if lock is not available)
	TryXLock(ctx context.Context, params TryXLockParams) (TryXLockResult, error)
	// Acquire exclusive lock (blocking, waits until lock is available)
	XLock(ctx context.Context, params XLockParams) (XLockResult, error)

	// Try to acquire shared lock (non-blocking, returns immediately if lock is not available)
	TrySLock(ctx context.Context, params TrySLockParams) (TrySLockResult, error)

	// Acquire shared lock (blocking, waits until lock is available)
	SLock(ctx context.Context, params SLockParams) (SLockResult, error)

	// Release a lock (either exclusive or shared)
	Unlock(ctx context.Context, params UnlockParams) (UnlockResult, error)
}

type lockClient struct {
	options LockClientOptions
	db      *sql.DB
}

func (c *lockClient) Connect() error {
	if c.db != nil {
		return nil
	}

	db, err := sql.Open("postgres", c.options.DatabaseURL)
	if err != nil {
		log.Fatalf("Failed to open database: %v", err)
	}

	db.SetMaxOpenConns(c.options.MaxOpenConnections)
	db.SetMaxIdleConns(c.options.MaxIdleConnections)

	c.db = db

	return nil
}

func (c *lockClient) SetupTables() error {
	if err := c.createLockTable(context.Background()); err != nil {
		return err
	}

	return nil
}

func (c *lockClient) Initialize() error {
	if err := c.Connect(); err != nil {
		return err
	}

	if err := c.SetupTables(); err != nil {
		return err
	}

	return nil
}
