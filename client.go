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

	LockTableName              string // [optional] default: "locks"
	PriorityLockTableName      string // [optional] default: "priority_locks"
	PriorityLockQueueTableName string // [optional] default: "priority_lock_queue"
}

func (options *LockClientOptions) SetDefaults() {
	if options.LockTableName == "" {
		options.LockTableName = "locks"
	}
	if options.PriorityLockTableName == "" {
		options.PriorityLockTableName = "priority_locks"
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

	return LockClient{
		options: options,
	}
}

type LockClient struct {
	options LockClientOptions
	db      *sql.DB
}

func (c *LockClient) connect() error {
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

func (c *LockClient) setupTables() error {
	if err := c.createLockTable(context.Background()); err != nil {
		return err
	}

	return nil
}

func (c *LockClient) Initialize() error {
	if err := c.connect(); err != nil {
		return err
	}

	if err := c.setupTables(); err != nil {
		return err
	}

	return nil
}
