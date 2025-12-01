package pglock

import (
	"context"
	"database/sql"
	"fmt"
	"time"
)

func (c *LockClient) createLockTable(ctx context.Context) error {
	tableName := c.options.LockTableName

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

type TryXLockParams struct {
	Name       string // Lock Name: unique identifier for the lock
	Owner      string // Lock Owner: identifier for the entity requesting the lock
	TTLSeconds int    // Time-To-Live: duration in seconds for the lock
}

type TryXLockResult struct {
	ExpiresAt time.Time // Expiration time of the lock
	Acquired  bool      // Whether the lock was successfully acquired
}

// TryXLock attempts to acquire a distributed lock.
// Returns the expiration time, whether the lock was acquired, and any error.
func (c *LockClient) TryXLock(ctx context.Context, params TryXLockParams) (TryXLockResult, error) {
	tableName := c.options.LockTableName

	// Atomic operation: insert if not exists, or update if expired
	query := fmt.Sprintf(`
		INSERT INTO %s (name, owner, expires_at)
		VALUES ($1, $2, NOW() + make_interval(secs => $3))
		ON CONFLICT (name) DO UPDATE
		SET owner = EXCLUDED.owner, expires_at = EXCLUDED.expires_at
		WHERE %s.expires_at <= NOW()
		RETURNING expires_at;
	`, tableName, tableName)

	var expires time.Time
	err := c.db.QueryRowContext(ctx, query, params.Name, params.Owner, params.TTLSeconds).Scan(&expires)
	if err == sql.ErrNoRows {
		// Lock is held by another owner and not expired
		return TryXLockResult{}, nil
	}
	if err != nil {
		return TryXLockResult{}, err
	}

	return TryXLockResult{ExpiresAt: expires, Acquired: true}, nil
}
