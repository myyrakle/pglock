package pglock

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"time"
)

const (
	// DefaultRetryInterval is the default interval for retrying lock acquisition
	DefaultRetryInterval = 100 * time.Millisecond
)

func (c *lockClient) createLockTable(ctx context.Context) error {
	tableName := c.options.LockTableName

	createTableSQL := fmt.Sprintf(`
		CREATE TABLE IF NOT EXISTS %s (
			name TEXT PRIMARY KEY,
			xlock_id TEXT,
			x_expires_at TIMESTAMPTZ,
			shared_locks JSONB DEFAULT '[]'::jsonb,
			max_shared_locks INT DEFAULT -1
		);
	`, tableName)

	_, err := c.db.ExecContext(ctx, createTableSQL)
	if err != nil {
		return err
	}

	// GIN 인덱스 생성 (JSONB 검색 최적화)
	createIndexSQL := fmt.Sprintf(`
		CREATE INDEX IF NOT EXISTS idx_%s_shared ON %s USING GIN (shared_locks);
	`, tableName, tableName)

	_, err = c.db.ExecContext(ctx, createIndexSQL)

	return err
}

type TryXLockParams struct {
	Name       string // Lock Name: unique identifier for the lock
	LockID     string // Lock LockID: identifier for the entity requesting the lock
	TTLSeconds int    // Time-To-Live: duration in seconds for the lock
}

type TryXLockResult struct {
	ExpiresAt time.Time // Expiration time of the lock
	Acquired  bool      // Whether the lock was successfully acquired
}

// TryXLock attempts to acquire a distributed lock.
// Returns the expiration time, whether the lock was acquired, and any error.
func (c *lockClient) TryXLock(ctx context.Context, params TryXLockParams) (TryXLockResult, error) {
	transaction, err := c.db.BeginTx(ctx, nil)
	if err != nil {
		return TryXLockResult{}, err
	}

	tableName := c.options.LockTableName

	xExpiresAtFromParams := sql.NullTime{
		Time:  time.Now().Add(time.Duration(params.TTLSeconds) * time.Second),
		Valid: true,
	}

	// 1. lock 행 생성 (없으면)
	ensureQuery := fmt.Sprintf(`
		INSERT INTO %s (name, xlock_id, x_expires_at, shared_locks, max_shared_locks)
		VALUES ($1, $2, $3, '[]'::jsonb, -1)
		ON CONFLICT (name) DO NOTHING
		RETURNING name;
	`, tableName)
	result, err := transaction.ExecContext(ctx, ensureQuery, params.Name, params.LockID, xExpiresAtFromParams)
	if err != nil {
		_ = transaction.Rollback()
		return TryXLockResult{}, err
	}

	rowsAffected, err := result.RowsAffected()
	if err != nil {
		_ = transaction.Rollback()
		return TryXLockResult{}, err
	}

	if rowsAffected > 0 {
		// 새로 생성되어 바로 획득 성공
		if err := transaction.Commit(); err != nil {
			return TryXLockResult{}, err
		}

		return TryXLockResult{ExpiresAt: xExpiresAtFromParams.Time, Acquired: true}, nil
	}

	// 2. FOR UPDATE로 행 잠금 및 현재 상태 조회
	selectQuery := fmt.Sprintf(`
		SELECT xlock_id, x_expires_at, shared_locks
		FROM %s
		WHERE name = $1
		FOR UPDATE;
	`, tableName)

	var sharedLocksJSON []byte
	var xlockID sql.NullString
	var xExpiresAt sql.NullTime

	err = transaction.QueryRowContext(ctx, selectQuery, params.Name).Scan(
		&xlockID, &xExpiresAt, &sharedLocksJSON,
	)
	if err != nil {
		_ = transaction.Rollback()
		return TryXLockResult{}, err
	}

	// 3. 기존 XLock 확인
	if xlockID.Valid && xExpiresAt.Valid && xExpiresAt.Time.After(time.Now()) {
		_ = transaction.Rollback()
		return TryXLockResult{Acquired: false}, nil
	}

	// 4. 유효한 SLock 확인
	var sharedLocks []SharedLockEntry
	if len(sharedLocksJSON) > 0 {
		if err := json.Unmarshal(sharedLocksJSON, &sharedLocks); err != nil {
			_ = transaction.Rollback()
			return TryXLockResult{}, fmt.Errorf("failed to parse shared_locks: %w", err)
		}
	}

	for _, lock := range sharedLocks {
		if lock.ExpiresAt.After(time.Now()) {
			// 유효한 SLock이 존재
			_ = transaction.Rollback()
			return TryXLockResult{Acquired: false}, nil
		}
	}

	// 5. XLock 설정
	newExpiresAt := time.Now().Add(time.Duration(params.TTLSeconds) * time.Second)
	updateQuery := fmt.Sprintf(`
		UPDATE %s
		SET xlock_id = $1, x_expires_at = $2
		WHERE name = $3;
	`, tableName)

	_, err = transaction.ExecContext(ctx, updateQuery, params.LockID, newExpiresAt, params.Name)
	if err != nil {
		_ = transaction.Rollback()
		return TryXLockResult{}, err
	}

	if err := transaction.Commit(); err != nil {
		return TryXLockResult{}, err
	}

	return TryXLockResult{ExpiresAt: newExpiresAt, Acquired: true}, nil
}

type XLockParams struct {
	Name             string        // Lock Name: unique identifier for the lock
	LockID           string        // Lock LockID: identifier for the entity requesting the lock
	TTLSeconds       int           // Time-To-Live: duration in seconds for the lock
	IntervalDuration time.Duration // Retry interval duration (default value: 100ms)
}

type XLockResult struct {
	ExpiresAt time.Time // Expiration time of the lock
}

// Lock continuously attempts to acquire a distributed lock until successful.
func (c *lockClient) XLock(ctx context.Context, params XLockParams) (XLockResult, error) {
	// IntervalDuration이 0 이하면 기본값 사용 (타이트 루프 방지)
	if params.IntervalDuration <= 0 {
		params.IntervalDuration = DefaultRetryInterval
	}

	for {
		result, err := c.TryXLock(ctx, TryXLockParams{
			Name:       params.Name,
			LockID:     params.LockID,
			TTLSeconds: params.TTLSeconds,
		})
		if err != nil {
			return XLockResult{}, err
		}
		if result.Acquired {
			return XLockResult{ExpiresAt: result.ExpiresAt}, nil
		}

		// 컨텍스트 취소 확인
		select {
		case <-ctx.Done():
			return XLockResult{}, ctx.Err()
		case <-time.After(params.IntervalDuration):
			// 재시도
		}
	}
}

type UnlockParams struct {
	Name   string // Lock Name: unique identifier for the lock
	LockID string // Lock LockID: identifier for the entity releasing the lock
}

type UnlockResult struct {
	Released bool // Whether the lock was released
}

// SharedLockEntry represents a single shared lock entry in the JSONB array
type SharedLockEntry struct {
	LockID    string    `json:"lock_id"`
	ExpiresAt time.Time `json:"expires_at"`
}

// TrySLockParams represents the parameters for acquiring a shared lock (non-blocking)
type TrySLockParams struct {
	Name           string // Lock Name: unique identifier for the lock
	LockID         string // Lock ID: identifier for the entity requesting the lock
	TTLSeconds     int    // Time-To-Live: duration in seconds for the lock
	MaxSharedLocks int    // Maximum number of shared locks allowed (-1 for unlimited)
}

// TrySLockResult represents the result of a shared lock acquisition attempt
type TrySLockResult struct {
	ExpiresAt time.Time // Expiration time of the lock
	Acquired  bool      // Whether the lock was successfully acquired
}

// SLockParams represents the parameters for acquiring a shared lock (blocking)
type SLockParams struct {
	Name             string        // Lock Name: unique identifier for the lock
	LockID           string        // Lock ID: identifier for the entity requesting the lock
	TTLSeconds       int           // Time-To-Live: duration in seconds for the lock
	MaxSharedLocks   int           // Maximum number of shared locks allowed (-1 for unlimited)
	IntervalDuration time.Duration // Retry interval duration (default value: 100ms)
}

// SLockResult represents the result of a shared lock acquisition
type SLockResult struct {
	ExpiresAt time.Time // Expiration time of the lock
}

// TrySLock attempts to acquire a shared lock (non-blocking).
// Returns the expiration time, whether the lock was acquired, and any error.
func (c *lockClient) TrySLock(ctx context.Context, params TrySLockParams) (TrySLockResult, error) {
	transaction, err := c.db.BeginTx(ctx, nil)
	if err != nil {
		return TrySLockResult{}, err
	}

	tableName := c.options.LockTableName

	newExpiresAt := time.Now().Add(time.Duration(params.TTLSeconds) * time.Second)

	// 1. lock 행 생성 (없으면) - SLock이므로 shared_locks에 초기 엔트리 추가
	newLockEntry := SharedLockEntry{
		LockID:    params.LockID,
		ExpiresAt: newExpiresAt,
	}
	initialSharedLocks, err := json.Marshal([]SharedLockEntry{newLockEntry})
	if err != nil {
		_ = transaction.Rollback()
		return TrySLockResult{}, fmt.Errorf("failed to marshal initial shared_locks: %w", err)
	}

	ensureQuery := fmt.Sprintf(`
		INSERT INTO %s (name, xlock_id, x_expires_at, shared_locks, max_shared_locks)
		VALUES ($1, NULL, NULL, $2::jsonb, $3)
		ON CONFLICT (name) DO NOTHING
		RETURNING name;
	`, tableName)

	result, err := transaction.ExecContext(
		ctx, ensureQuery,
		params.Name, initialSharedLocks, params.MaxSharedLocks,
	)
	if err != nil {
		_ = transaction.Rollback()
		return TrySLockResult{}, err
	}

	rowsAffected, err := result.RowsAffected()
	if err != nil {
		_ = transaction.Rollback()
		return TrySLockResult{}, err
	}

	if rowsAffected > 0 {
		// 새로 생성되어 바로 획득 성공
		if err := transaction.Commit(); err != nil {
			return TrySLockResult{}, err
		}

		return TrySLockResult{ExpiresAt: newExpiresAt, Acquired: true}, nil
	}

	// 2. FOR UPDATE로 행 잠금 및 현재 상태 조회
	// 주의: SLock 간에도 JSONB 배열 업데이트 시 race condition 방지를 위해 필요
	selectQuery := fmt.Sprintf(`
		SELECT xlock_id, x_expires_at, shared_locks, max_shared_locks
		FROM %s
		WHERE name = $1
		FOR UPDATE;
	`, tableName)

	var xlockID sql.NullString
	var xExpiresAt sql.NullTime
	var sharedLocksJSON []byte
	var maxSharedLocks int

	err = transaction.QueryRowContext(ctx, selectQuery, params.Name).Scan(
		&xlockID, &xExpiresAt, &sharedLocksJSON, &maxSharedLocks,
	)
	if err != nil {
		_ = transaction.Rollback()
		return TrySLockResult{}, err
	}

	// 3. XLock 확인
	if xlockID.Valid && xExpiresAt.Valid && xExpiresAt.Time.After(time.Now()) {
		_ = transaction.Rollback()
		return TrySLockResult{Acquired: false}, nil
	}

	// 4. 만료되지 않은 SLock만 필터링
	var sharedLocks []SharedLockEntry
	if len(sharedLocksJSON) > 0 {
		if err := json.Unmarshal(sharedLocksJSON, &sharedLocks); err != nil {
			_ = transaction.Rollback()
			return TrySLockResult{}, fmt.Errorf("failed to parse shared_locks: %w", err)
		}
	}

	validLocks := []SharedLockEntry{}
	alreadyHasLock := false
	for _, lock := range sharedLocks {
		if lock.ExpiresAt.After(time.Now()) {
			if lock.LockID == params.LockID {
				alreadyHasLock = true
				// 자기 자신의 락도 validLocks에 일단 포함 (갱신용)
				validLocks = append(validLocks, lock)
			} else {
				validLocks = append(validLocks, lock)
			}
		}
	}

	// 5. 새로운 락 추가 여부 결정 및 개수 제한 확인
	if !alreadyHasLock {
		// 새 락을 추가할 때만 개수 제한 확인
		if maxSharedLocks != -1 && len(validLocks) >= maxSharedLocks {
			_ = transaction.Rollback()
			return TrySLockResult{Acquired: false}, nil
		}
	}

	// 6. SLock 추가 또는 갱신
	// newExpiresAt는 line 250에서 이미 계산됨
	if alreadyHasLock {
		// 기존 락 갱신
		for i := range validLocks {
			if validLocks[i].LockID == params.LockID {
				validLocks[i].ExpiresAt = newExpiresAt
				break
			}
		}
	} else {
		// 새 락 추가
		validLocks = append(validLocks, SharedLockEntry{
			LockID:    params.LockID,
			ExpiresAt: newExpiresAt,
		})
	}

	newSharedLocksJSON, err := json.Marshal(validLocks)
	if err != nil {
		_ = transaction.Rollback()
		return TrySLockResult{}, fmt.Errorf("failed to marshal shared_locks: %w", err)
	}

	// 7. shared_locks 업데이트
	updateQuery := fmt.Sprintf(`
		UPDATE %s
		SET shared_locks = $1
		WHERE name = $2;
	`, tableName)
	_, err = transaction.ExecContext(ctx, updateQuery, newSharedLocksJSON, params.Name)
	if err != nil {
		_ = transaction.Rollback()
		return TrySLockResult{}, err
	}

	if err := transaction.Commit(); err != nil {
		return TrySLockResult{}, err
	}

	return TrySLockResult{ExpiresAt: newExpiresAt, Acquired: true}, nil
}

// SLock continuously attempts to acquire a shared lock until successful.
func (c *lockClient) SLock(ctx context.Context, params SLockParams) (SLockResult, error) {
	// IntervalDuration이 0 이하면 기본값 사용 (타이트 루프 방지)
	if params.IntervalDuration <= 0 {
		params.IntervalDuration = DefaultRetryInterval
	}

	for {
		result, err := c.TrySLock(ctx, TrySLockParams{
			Name:           params.Name,
			LockID:         params.LockID,
			TTLSeconds:     params.TTLSeconds,
			MaxSharedLocks: params.MaxSharedLocks,
		})
		if err != nil {
			return SLockResult{}, err
		}
		if result.Acquired {
			return SLockResult{ExpiresAt: result.ExpiresAt}, nil
		}

		// 컨텍스트 취소 확인
		select {
		case <-ctx.Done():
			return SLockResult{}, ctx.Err()
		case <-time.After(params.IntervalDuration):
			// 재시도
		}
	}
}

// Unlock releases the lock if we still own it (either XLock or SLock).
// Returns whether the lock was released and any error.
func (c *lockClient) Unlock(ctx context.Context, params UnlockParams) (UnlockResult, error) {
	tx, err := c.db.BeginTx(ctx, nil)
	if err != nil {
		return UnlockResult{}, err
	}
	defer tx.Rollback()

	tableName := c.options.LockTableName

	// 1. 현재 상태 조회 및 행 잠금 (FOR UPDATE)
	selectQuery := fmt.Sprintf(`
		SELECT xlock_id, x_expires_at, shared_locks
		FROM %s
		WHERE name = $1
		FOR UPDATE;
	`, tableName)

	var xlockID sql.NullString
	var xExpiresAt sql.NullTime
	var sharedLocksJSON []byte

	err = tx.QueryRowContext(ctx, selectQuery, params.Name).Scan(
		&xlockID, &xExpiresAt, &sharedLocksJSON,
	)
	if err == sql.ErrNoRows {
		// 락이 존재하지 않음
		return UnlockResult{Released: false}, nil
	}
	if err != nil {
		return UnlockResult{}, err
	}

	released := false

	// 2. XLock 확인 및 제거
	if xlockID.Valid && xlockID.String == params.LockID {
		updateQuery := fmt.Sprintf(`
			UPDATE %s
			SET xlock_id = NULL, x_expires_at = NULL
			WHERE name = $1;
		`, tableName)
		_, err = tx.ExecContext(ctx, updateQuery, params.Name)
		if err != nil {
			return UnlockResult{}, err
		}
		released = true
	}

	// 3. SLock 확인 및 제거
	var sharedLocks []SharedLockEntry
	if len(sharedLocksJSON) > 0 {
		if err := json.Unmarshal(sharedLocksJSON, &sharedLocks); err != nil {
			return UnlockResult{}, fmt.Errorf("failed to parse shared_locks: %w", err)
		}
	}

	newSharedLocks := []SharedLockEntry{}
	for _, lock := range sharedLocks {
		if lock.LockID != params.LockID {
			newSharedLocks = append(newSharedLocks, lock)
		} else {
			released = true
		}
	}

	if len(newSharedLocks) != len(sharedLocks) {
		// SLock이 제거되었으면 업데이트
		newSharedLocksJSON, err := json.Marshal(newSharedLocks)
		if err != nil {
			return UnlockResult{}, fmt.Errorf("failed to marshal shared_locks: %w", err)
		}
		updateQuery := fmt.Sprintf(`
			UPDATE %s
			SET shared_locks = $1
			WHERE name = $2;
		`, tableName)
		_, err = tx.ExecContext(ctx, updateQuery, newSharedLocksJSON, params.Name)
		if err != nil {
			return UnlockResult{}, err
		}
	}

	err = tx.Commit()
	if err != nil {
		return UnlockResult{}, err
	}

	return UnlockResult{Released: released}, nil
}
