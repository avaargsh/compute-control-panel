// Package migrate applies forward-only, checksummed SQL migrations.
package migrate

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"errors"
	"fmt"
	"io/fs"
	"regexp"
	"sort"
	"strconv"
	"strings"
)

// lockKey is the pg_advisory_lock key serializing concurrent runners.
const lockKey int64 = 0x636f6d70757465

var fileName = regexp.MustCompile(`^(\d{4})_[a-z0-9_]+\.sql$`)

type Migration struct {
	Version  int
	Name     string
	SQL      string
	Checksum string
}

var (
	ErrChecksumMismatch = errors.New("applied migration checksum differs from file")
	ErrMissingMigration = errors.New("applied migration is missing from source")
)

// Load reads top-level NNNN_name.sql files. Versions must be contiguous from 1.
func Load(fsys fs.FS) ([]Migration, error) {
	entries, err := fs.ReadDir(fsys, ".")
	if err != nil {
		return nil, err
	}
	var out []Migration
	for _, e := range entries {
		if e.IsDir() || !strings.HasSuffix(e.Name(), ".sql") {
			continue
		}
		m := fileName.FindStringSubmatch(e.Name())
		if m == nil {
			return nil, fmt.Errorf("invalid migration file name %q", e.Name())
		}
		body, err := fs.ReadFile(fsys, e.Name())
		if err != nil {
			return nil, err
		}
		v, _ := strconv.Atoi(m[1])
		sum := sha256.Sum256(body)
		out = append(out, Migration{Version: v, Name: e.Name(), SQL: string(body), Checksum: hex.EncodeToString(sum[:])})
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Version < out[j].Version })
	for i, m := range out {
		if m.Version != i+1 {
			return nil, fmt.Errorf("migration versions must be contiguous from 0001: found %04d at position %d", m.Version, i+1)
		}
	}
	return out, nil
}

// Apply runs pending migrations, each in its own transaction, under an
// advisory lock. Already-applied migrations must match their recorded checksum.
// It returns the versions applied by this call.
func Apply(ctx context.Context, db *sql.DB, migrations []Migration) ([]int, error) {
	conn, err := db.Conn(ctx)
	if err != nil {
		return nil, err
	}
	defer conn.Close()

	if _, err := conn.ExecContext(ctx, `SELECT pg_advisory_lock($1)`, lockKey); err != nil {
		return nil, err
	}
	defer func() {
		_, _ = conn.ExecContext(context.WithoutCancel(ctx), `SELECT pg_advisory_unlock($1)`, lockKey)
	}()

	if _, err := conn.ExecContext(ctx, `CREATE TABLE IF NOT EXISTS schema_migrations (
		version INTEGER PRIMARY KEY CHECK (version > 0),
		name TEXT NOT NULL,
		checksum TEXT NOT NULL,
		applied_at TIMESTAMPTZ NOT NULL DEFAULT now())`); err != nil {
		return nil, err
	}

	applied := map[int]string{}
	rows, err := conn.QueryContext(ctx, `SELECT version, checksum FROM schema_migrations`)
	if err != nil {
		return nil, err
	}
	for rows.Next() {
		var v int
		var sum string
		if err := rows.Scan(&v, &sum); err != nil {
			rows.Close()
			return nil, err
		}
		applied[v] = sum
	}
	if err := rows.Err(); err != nil {
		rows.Close()
		return nil, err
	}
	rows.Close()

	known := map[int]Migration{}
	for _, m := range migrations {
		known[m.Version] = m
	}
	for v, sum := range applied {
		m, ok := known[v]
		if !ok {
			return nil, fmt.Errorf("%w: version %04d", ErrMissingMigration, v)
		}
		if m.Checksum != sum {
			return nil, fmt.Errorf("%w: %s", ErrChecksumMismatch, m.Name)
		}
	}

	var done []int
	for _, m := range migrations {
		if _, ok := applied[m.Version]; ok {
			continue
		}
		tx, err := conn.BeginTx(ctx, nil)
		if err != nil {
			return done, err
		}
		if _, err := tx.ExecContext(ctx, m.SQL); err != nil {
			_ = tx.Rollback()
			return done, fmt.Errorf("apply %s: %w", m.Name, err)
		}
		if _, err := tx.ExecContext(ctx, `INSERT INTO schema_migrations (version, name, checksum) VALUES ($1, $2, $3)`,
			m.Version, m.Name, m.Checksum); err != nil {
			_ = tx.Rollback()
			return done, err
		}
		if err := tx.Commit(); err != nil {
			return done, err
		}
		done = append(done, m.Version)
	}
	return done, nil
}
