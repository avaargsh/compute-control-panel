// Package pgtest contains helpers for PostgreSQL integration tests.
// Production control-plane code must not import it.
package pgtest

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"
)

// catalogRoleLockKey serializes role and grant DDL across test packages that
// share one PostgreSQL cluster. Session advisory locks are per database, so
// every package must take this same key on a connection to that database.
// The value is distinct from the migration runner lock 0x636f6d70757465 so a
// migration executing on an already locked test session cannot self-deadlock.
const catalogRoleLockKey int64 = 0x434350524f4c4544 // "CCPROLED"

// catalogRaceAttempts bounds retries of SQLSTATE 40P01 and XX000
// "tuple concurrently updated". Other failures are returned immediately.
const catalogRaceAttempts = 5

// ExecCatalog runs statements in order on one dedicated connection while that
// session holds pg_advisory_lock(catalogRoleLockKey). A statement is retried
// only when PostgreSQL reports a catalog race. The first non-race error stops
// the batch. Callers that already hold LockCatalogConn must not call this:
// the lock is not reentrant across connections.
func ExecCatalog(ctx context.Context, db *sql.DB, statements ...string) error {
	if err := validateStatements(statements); err != nil {
		return err
	}
	return withCatalogLock(ctx, db, func(ctx context.Context, conn *sql.Conn) error {
		for _, statement := range statements {
			if err := execCatalogStatement(ctx, conn, statement); err != nil {
				return err
			}
		}
		return nil
	})
}

// ExecCatalogCleanup runs every statement under the same lock as ExecCatalog.
// Each statement is still retried only on catalog races. Later statements run
// after an earlier failure so a DROP ROLE result is not hidden by a failed
// DROP OWNED, and a DROP OWNED error is not discarded.
func ExecCatalogCleanup(ctx context.Context, db *sql.DB, statements ...string) error {
	if err := validateStatements(statements); err != nil {
		return err
	}
	return withCatalogLock(ctx, db, func(ctx context.Context, conn *sql.Conn) error {
		return errors.Join(runCleanupStatements(ctx, conn, statements)...)
	})
}

// CleanupRole revokes any caller-supplied preamble, then DROP OWNED BY role
// and DROP ROLE role, under one advisory-lock hold. Every statement error is
// returned. On failure, PostgreSQL's description of remaining dependencies is
// included when it is available.
func CleanupRole(ctx context.Context, db *sql.DB, role string, before ...string) error {
	if err := validRoleIdent(role); err != nil {
		return err
	}
	statements := make([]string, 0, len(before)+2)
	statements = append(statements, before...)
	statements = append(statements,
		"DROP OWNED BY "+role,
		"DROP ROLE "+role,
	)
	if err := validateStatements(statements); err != nil {
		return err
	}
	return withCatalogLock(ctx, db, func(ctx context.Context, conn *sql.Conn) error {
		errs := runCleanupStatements(ctx, conn, statements)
		if len(errs) == 0 {
			return nil
		}
		summary, sumErr := roleDependencySummary(ctx, conn, role)
		if sumErr != nil {
			errs = append(errs, fmt.Errorf("list dependencies of role %s: %w", role, sumErr))
		} else if summary != "" {
			errs = append(errs, fmt.Errorf("dependencies of role %s: %s", role, summary))
		}
		return errors.Join(errs...)
	})
}

// LockCatalogConn returns a dedicated connection that holds the shared role
// catalog lock until release is called. Hold it for an entire transaction
// that mutates roles or grants, and roll that transaction back before release
// so the next session cannot interleave with locks the transaction still holds.
// release is idempotent and unlocks with a context that outlives cancellation
// of ctx. A cancelled acquire still unlocks or discards the session so a
// locked connection is not returned to the pool.
func LockCatalogConn(ctx context.Context, db *sql.DB) (*sql.Conn, func(), error) {
	return lockConn(ctx, db, catalogRoleLockKey)
}

func withCatalogLock(ctx context.Context, db *sql.DB, fn func(context.Context, *sql.Conn) error) error {
	conn, release, err := lockConn(ctx, db, catalogRoleLockKey)
	if err != nil {
		return err
	}
	defer release()
	return fn(ctx, conn)
}

func lockConn(ctx context.Context, db *sql.DB, key int64) (*sql.Conn, func(), error) {
	if db == nil {
		return nil, nil, errors.New("catalog lock requires a database pool")
	}
	conn, err := db.Conn(ctx)
	if err != nil {
		return nil, nil, fmt.Errorf("catalog lock connection: %w", err)
	}
	var once sync.Once
	release := func() {
		once.Do(func() {
			unlockCatalogConn(conn, key)
			_ = conn.Close()
		})
	}
	if _, err := conn.ExecContext(ctx, `SELECT pg_catalog.pg_advisory_lock($1)`, key); err != nil {
		// Cancellation can arrive after PostgreSQL grants the lock. Always
		// unlock or discard this session before it can return to the pool.
		release()
		return nil, nil, fmt.Errorf("acquire catalog advisory lock: %w", err)
	}
	return conn, release, nil
}

func unlockCatalogConn(conn *sql.Conn, key int64) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	var unlocked bool
	err := conn.QueryRowContext(ctx, `SELECT pg_catalog.pg_advisory_unlock($1)`, key).Scan(&unlocked)
	if err != nil || !unlocked {
		// ErrBadConn makes database/sql close the session instead of pooling
		// a connection that may still hold the advisory lock.
		_ = conn.Raw(func(any) error { return driver.ErrBadConn })
	}
}

func runCleanupStatements(ctx context.Context, conn *sql.Conn, statements []string) []error {
	var errs []error
	for _, statement := range statements {
		if err := execCatalogStatement(ctx, conn, statement); err != nil {
			errs = append(errs, err)
			if ctx.Err() != nil {
				break
			}
		}
	}
	return errs
}

func execCatalogStatement(ctx context.Context, conn *sql.Conn, statement string) error {
	var last error
	for attempt := 1; attempt <= catalogRaceAttempts; attempt++ {
		if err := ctx.Err(); err != nil {
			if last != nil {
				return fmt.Errorf("%w (also %w)", &catalogStatementError{statement: statement, attempts: attempt - 1, err: last}, err)
			}
			return err
		}
		_, err := conn.ExecContext(ctx, statement)
		if err == nil {
			return nil
		}
		last = err
		if !catalogRace(err) || attempt == catalogRaceAttempts {
			return &catalogStatementError{statement: statement, attempts: attempt, err: err}
		}
		timer := time.NewTimer(time.Duration(attempt) * 25 * time.Millisecond)
		select {
		case <-ctx.Done():
			timer.Stop()
			return fmt.Errorf("%w (also %w)", &catalogStatementError{statement: statement, attempts: attempt, err: err}, ctx.Err())
		case <-timer.C:
		}
	}
	return &catalogStatementError{statement: statement, attempts: catalogRaceAttempts, err: last}
}

type catalogStatementError struct {
	statement string
	attempts  int
	err       error
}

func (e *catalogStatementError) Error() string {
	stmt := strings.Join(strings.Fields(e.statement), " ")
	if len(stmt) > 240 {
		stmt = stmt[:240] + "..."
	}
	return fmt.Sprintf("catalog DDL %q after %d attempt(s): %v", stmt, e.attempts, e.err)
}

func (e *catalogStatementError) Unwrap() error { return e.err }

type sqlStateError interface {
	SQLState() string
}

func catalogRace(err error) bool {
	var state sqlStateError
	if !errors.As(err, &state) {
		return false
	}
	switch state.SQLState() {
	case "40P01":
		return true
	case "XX000":
		return strings.Contains(strings.ToLower(err.Error()), "tuple concurrently updated")
	default:
		return false
	}
}

func validateStatements(statements []string) error {
	if len(statements) == 0 {
		return errors.New("catalog DDL requires at least one statement")
	}
	for _, statement := range statements {
		if strings.TrimSpace(statement) == "" {
			return errors.New("catalog DDL statement is empty")
		}
	}
	return nil
}

func validRoleIdent(role string) error {
	if role == "" || len(role) > 63 {
		return fmt.Errorf("invalid role identifier %q", role)
	}
	for i, r := range role {
		switch {
		case r == '_' || (r >= 'a' && r <= 'z'):
		case i > 0 && r >= '0' && r <= '9':
		default:
			return fmt.Errorf("invalid role identifier %q", role)
		}
	}
	return nil
}

func roleDependencySummary(ctx context.Context, conn *sql.Conn, role string) (string, error) {
	rows, err := conn.QueryContext(ctx, `
		SELECT obj, deptype FROM (
			SELECT pg_catalog.pg_describe_object(d.classid, d.objid, d.objsubid) AS obj,
			       d.deptype::text AS deptype
			FROM pg_catalog.pg_shdepend d
			JOIN pg_catalog.pg_roles r ON r.oid = d.refobjid
			WHERE r.rolname = $1
			UNION ALL
			SELECT pg_catalog.pg_describe_object(d.classid, d.objid, d.objsubid),
			       d.deptype::text
			FROM pg_catalog.pg_depend d
			JOIN pg_catalog.pg_roles r ON r.oid = d.refobjid
			WHERE r.rolname = $1
		) deps
		LIMIT 20`, role)
	if err != nil {
		return "", err
	}
	defer rows.Close()
	var parts []string
	for rows.Next() {
		var obj, depType string
		if err := rows.Scan(&obj, &depType); err != nil {
			return "", err
		}
		parts = append(parts, depType+":"+obj)
	}
	if err := rows.Err(); err != nil {
		return "", err
	}
	return strings.Join(parts, "; "), nil
}
