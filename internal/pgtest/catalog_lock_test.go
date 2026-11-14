package pgtest

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"os"
	"testing"
	"time"

	_ "github.com/jackc/pgx/v5/stdlib"
)

func TestCatalogRoleLockKeyDoesNotCollideWithMigrationRunner(t *testing.T) {
	const migrationRunnerLockKey int64 = 0x636f6d70757465
	if catalogRoleLockKey == migrationRunnerLockKey || catalogRoleLockKey == 0 {
		t.Fatalf("catalog role lock key %d is not a distinct advisory lock", catalogRoleLockKey)
	}
}

func TestCatalogRaceOnlyRetriesDeadlockAndConcurrentTupleUpdate(t *testing.T) {
	tuple := stateErr{code: "XX000", msg: "ERROR: tuple concurrently updated (SQLSTATE XX000)"}
	cases := []struct {
		name string
		err  error
		want bool
	}{
		{name: "deadlock", err: stateErr{code: "40P01", msg: "deadlock detected"}, want: true},
		{name: "concurrent tuple update", err: tuple, want: true},
		{name: "wrapped concurrent tuple update", err: fmt.Errorf("driver: %w", tuple), want: true},
		{name: "other internal error", err: stateErr{code: "XX000", msg: "cache lookup failed"}, want: false},
		{name: "dependent objects", err: stateErr{code: "2BP01", msg: "role cannot be dropped because some objects depend on it"}, want: false},
		{name: "permission denied", err: stateErr{code: "42501", msg: "permission denied"}, want: false},
		{name: "untyped concurrent tuple text", err: errors.New("tuple concurrently updated"), want: false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := catalogRace(tc.err); got != tc.want {
				t.Fatalf("catalogRace(%v)=%t, want %t", tc.err, got, tc.want)
			}
		})
	}
}

func TestCatalogDDLRejectsEmptyStatementsAndUnsafeRoles(t *testing.T) {
	if err := ExecCatalog(context.Background(), nil); err == nil {
		t.Fatal("expected empty batch to be rejected")
	}
	if err := ExecCatalog(context.Background(), nil, "   "); err == nil {
		t.Fatal("expected blank statement to be rejected")
	}
	if err := CleanupRole(context.Background(), nil, "Bad-Role"); err == nil {
		t.Fatal("expected mixed-case role to be rejected")
	}
	if err := CleanupRole(context.Background(), nil, "role;drop"); err == nil {
		t.Fatal("expected punctuation in role to be rejected")
	}
	if err := validRoleIdent("ccp_kbroad_ab12"); err != nil {
		t.Fatal(err)
	}
}

func TestCatalogLockIsExclusiveOnOneSession(t *testing.T) {
	dsn := os.Getenv("DATABASE_URL")
	if dsn == "" {
		t.Skip("DATABASE_URL is not configured; PostgreSQL integration test skipped")
	}
	db, err := sql.Open("pgx", dsn)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	db.SetMaxOpenConns(4)
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	if err := db.PingContext(ctx); err != nil {
		t.Fatalf("ping PostgreSQL: %v", err)
	}

	// Private key: the shared role key may be held by another package.
	const key int64 = 0x4343505445535401
	holder, release, err := lockConn(ctx, db, key)
	if err != nil {
		t.Fatal(err)
	}
	defer release()
	if holder == nil {
		t.Fatal("missing lock connection")
	}
	other, err := db.Conn(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer other.Close()
	var acquired bool
	if err := other.QueryRowContext(ctx, `SELECT pg_catalog.pg_try_advisory_lock($1)`, key).Scan(&acquired); err != nil {
		t.Fatal(err)
	}
	if acquired {
		_, _ = other.ExecContext(ctx, `SELECT pg_catalog.pg_advisory_unlock($1)`, key)
		t.Fatal("second session acquired the catalog lock while the first session held it")
	}
	release()
	if err := other.QueryRowContext(ctx, `SELECT pg_catalog.pg_try_advisory_lock($1)`, key).Scan(&acquired); err != nil {
		t.Fatal(err)
	}
	if !acquired {
		t.Fatal("catalog lock was not released with the holding session")
	}
	var unlocked bool
	if err := other.QueryRowContext(ctx, `SELECT pg_catalog.pg_advisory_unlock($1)`, key).Scan(&unlocked); err != nil {
		t.Fatal(err)
	}
	if !unlocked {
		t.Fatal("second session did not own the catalog lock it acquired")
	}
}

type stateErr struct {
	code string
	msg  string
}

func (e stateErr) Error() string { return e.msg }

func (e stateErr) SQLState() string { return e.code }
