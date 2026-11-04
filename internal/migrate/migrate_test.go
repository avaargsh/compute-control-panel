package migrate

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"os"
	"strings"
	"testing"
	"testing/fstest"
	"time"

	_ "github.com/jackc/pgx/v5/stdlib"
)

func TestLoadRejectsBadNamesAndGaps(t *testing.T) {
	if _, err := Load(fstest.MapFS{"bad.sql": {Data: []byte("x")}}); err == nil {
		t.Fatal("expected invalid name error")
	}
	if _, err := Load(fstest.MapFS{"0002_a.sql": {Data: []byte("x")}}); err == nil {
		t.Fatal("expected gap error")
	}
	ms, err := Load(fstest.MapFS{
		"0002_b.sql": {Data: []byte("SELECT 2")},
		"0001_a.sql": {Data: []byte("SELECT 1")},
		"README.md":  {Data: []byte("ignored")},
	})
	if err != nil || len(ms) != 2 || ms[0].Version != 1 || ms[0].Checksum == ms[1].Checksum {
		t.Fatalf("unexpected result %v %v", ms, err)
	}
}

func TestApplyIsIdempotentAndDetectsDrift(t *testing.T) {
	dsn := os.Getenv("DATABASE_URL")
	if dsn == "" {
		t.Skip("DATABASE_URL is not configured; PostgreSQL integration test skipped")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	// Isolate in a private schema pinned through the connection's search_path.
	schema := fmt.Sprintf("migrate_it_%d", time.Now().UnixNano())
	admin, err := sql.Open("pgx", dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer admin.Close()
	if _, err := admin.ExecContext(ctx, "CREATE SCHEMA "+schema); err != nil {
		t.Fatal(err)
	}
	defer admin.ExecContext(context.WithoutCancel(ctx), "DROP SCHEMA "+schema+" CASCADE")

	sep := "?"
	if strings.Contains(dsn, "?") {
		sep = "&"
	}
	db, err := sql.Open("pgx", dsn+sep+"search_path="+schema)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	src := fstest.MapFS{
		"0001_a.sql": {Data: []byte("CREATE TABLE a (id INT)")},
		"0002_b.sql": {Data: []byte("CREATE TABLE b (id INT)")},
	}
	ms, _ := Load(src)
	done, err := Apply(ctx, db, ms)
	if err != nil || len(done) != 2 {
		t.Fatalf("first apply: %v %v", done, err)
	}
	if done, err = Apply(ctx, db, ms); err != nil || len(done) != 0 {
		t.Fatalf("second apply: %v %v", done, err)
	}

	src["0001_a.sql"] = &fstest.MapFile{Data: []byte("CREATE TABLE a (id BIGINT)")}
	drift, _ := Load(src)
	if _, err := Apply(ctx, db, drift); !errors.Is(err, ErrChecksumMismatch) {
		t.Fatalf("want checksum mismatch, got %v", err)
	}
	if _, err := Apply(ctx, db, ms[:1]); !errors.Is(err, ErrMissingMigration) {
		t.Fatalf("want missing migration, got %v", err)
	}

	// The migration ledger records both the SQL checksum and source filename.
	// Reusing the same version/checksum under a different name must fail closed.
	if _, err := db.ExecContext(ctx, "UPDATE schema_migrations SET name = '0001_renamed.sql' WHERE version = 1"); err != nil {
		t.Fatal(err)
	}
	if _, err := Apply(ctx, db, ms); !errors.Is(err, ErrNameMismatch) {
		t.Fatalf("want migration name mismatch, got %v", err)
	}
}
