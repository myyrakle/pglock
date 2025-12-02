package pglock

import (
	"context"
	"database/sql"
	"fmt"
	"time"
)

func (c *lockClient) createLockTable(ctx context.Context) error {
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
func (c *lockClient) TryXLock(ctx context.Context, params TryXLockParams) (TryXLockResult, error) {
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

type XLockParams struct {
	Name             string        // Lock Name: unique identifier for the lock
	Owner            string        // Lock Owner: identifier for the entity requesting the lock
	TTLSeconds       int           // Time-To-Live: duration in seconds for the lock
	IntervalDuration time.Duration // Retry interval duration
}

type XLockResult struct {
	ExpiresAt time.Time // Expiration time of the lock
}

// Lock continuously attempts to acquire a distributed lock until successful.
func (c *lockClient) XLock(ctx context.Context, params XLockParams) (XLockResult, error) {
	for {
		result, err := c.TryXLock(ctx, TryXLockParams{
			Name:       params.Name,
			Owner:      params.Owner,
			TTLSeconds: params.TTLSeconds,
		})
		if err != nil {
			return XLockResult{}, err
		}
		if result.Acquired {
			return XLockResult{ExpiresAt: result.ExpiresAt}, nil
		}
		// Wait before retrying
		time.Sleep(params.IntervalDuration)
	}
}

type UnlockParams struct {
	Name  string // Lock Name: unique identifier for the lock
	Owner string // Lock Owner: identifier for the entity releasing the lock
}

type UnlockResult struct {
	Released bool // Whether the lock was released
}

// Unlock releases the lock if we still own it.
// Returns whether the lock was released and any error.
func (c *lockClient) Unlock(ctx context.Context, params UnlockParams) (UnlockResult, error) {
	tableName := c.options.LockTableName

	query := fmt.Sprintf(`DELETE FROM %s WHERE name = $1 AND owner = $2;`, tableName)
	res, err := c.db.ExecContext(ctx, query, params.Name, params.Owner)
	if err != nil {
		return UnlockResult{}, err
	}
	rowsAffected, _ := res.RowsAffected()
	return UnlockResult{Released: rowsAffected > 0}, nil
}
