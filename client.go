package pglock

import (
	"database/sql"
	"log"

	_ "github.com/lib/pq"
)

type LockClientOptions struct {
	MaxOpenConnections int    // [optional] default: 10
	MaxIdleConnections int    // [optional] default: 5
	DatabaseURL        string // [required] example: "postgres://user:password@localhost:5432/dbname"
}

func NewLockClient(options LockClientOptions) LockClient {
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

	if c.options.MaxOpenConnections > 0 {
		db.SetMaxOpenConns(c.options.MaxOpenConnections)
	}

	if c.options.MaxIdleConnections > 0 {
		db.SetMaxIdleConns(c.options.MaxIdleConnections)
	}

	c.db = db

	return nil
}

func (c *LockClient) setupTables() error {
	// Create necessary tables here
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
