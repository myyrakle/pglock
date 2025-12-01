package pglock

import (
	"context"
	"fmt"
)

func (c *LockClient) createLockTable(ctx context.Context) error {
	tableName := "locks"

	if c.options.LockTableName != "" {
		tableName = c.options.LockTableName
	}

	createTableSQL := fmt.Sprintf(`
		CREATE TABLE IF NOT EXISTS %s (
		name TEXT PRIMARY KEY,
		owner TEXT NOT NULL,
		expires_at TIMESTAMPTZ NOT NULL
		);
	`, tableName)

	_, err := c.db.ExecContext(ctx, createTableSQL)

	return err
}
