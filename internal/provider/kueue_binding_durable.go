package provider

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/stdlib"
)

// kueueBindingSearchPath lists pg_temp last. PostgreSQL otherwise searches
// pg_temp first, which lets a session-local table shadow public. The binding
// statements are also schema-qualified; this pin is the same fence E1 uses
// for its trigger functions.
const kueueBindingSearchPath = "pg_catalog, public, pg_temp"

const kueueBindingRoleName = "ccp_kueue_job_binding"

// ErrKueueBindingDSNEmpty means an in-cluster adapter was constructed without
// a binding database. The constructor does not substitute an in-memory store.
var ErrKueueBindingDSNEmpty = errors.New("kueue job binding DSN is empty")

// ErrKueueBindingUnavailable means the binding database could not be opened
// or reached. Construction stops before an adapter exists.
var ErrKueueBindingUnavailable = errors.New("kueue job binding database is unreachable")

// ErrKueueBindingSchema means the binding tables or their guards are absent.
// A missing schema is not an empty pin set.
var ErrKueueBindingSchema = errors.New("kueue job binding schema is missing")

// ErrKueueBindingRole means the connected login is not the least-privilege
// binding role. Superusers, owners, and any login that can read or write
// Provider, Allocation, Workload, Operation, or Evidence tables are refused.
var ErrKueueBindingRole = errors.New("kueue job binding database role is not least privilege")

// OpenDurablePostgresKueueJobBindingStore opens the binding tables for an
// in-cluster process. dsn must authenticate as a login with only the DML
// migration 0015 grants to ccp_kueue_job_binding: SELECT and INSERT on both
// binding tables, UPDATE of updated_at on the pin table, and UPDATE of
// acknowledged and updated_at on the tombstone table. The pin UPDATE column
// exists so SELECT ... FOR UPDATE can take a lock; the pin guard rejects
// every UPDATE. An empty DSN, a DSN that sets search_path, an unreachable database,
// a missing schema, or a broader login returns an error. The returned pool
// pins search_path to pg_catalog, public, pg_temp on every connection.
//
// The caller owns db and closes it. This function never opens an in-memory
// store. NewPostgresKueueJobBindingStore remains the unchecked constructor
// for tests and the disposable kind smoke, which still uses a superuser.
func OpenDurablePostgresKueueJobBindingStore(ctx context.Context, dsn string) (*PostgresKueueJobBindingStore, *sql.DB, error) {
	if ctx == nil {
		return nil, nil, ErrKueueBindingUnavailable
	}
	if strings.TrimSpace(dsn) == "" {
		return nil, nil, ErrKueueBindingDSNEmpty
	}
	cfg, err := pgx.ParseConfig(dsn)
	if err != nil {
		return nil, nil, fmt.Errorf("%w: %v", ErrKueueBindingUnavailable, err)
	}
	if err := rejectKueueBindingDSNSearchPath(cfg); err != nil {
		return nil, nil, err
	}
	if cfg.ConnectTimeout <= 0 {
		cfg.ConnectTimeout = 5 * time.Second
	}
	db := stdlib.OpenDB(*cfg,
		stdlib.OptionBeforeConnect(func(_ context.Context, connConfig *pgx.ConnConfig) error {
			if connConfig.RuntimeParams == nil {
				connConfig.RuntimeParams = map[string]string{}
			}
			connConfig.RuntimeParams["search_path"] = kueueBindingSearchPath
			return nil
		}),
		stdlib.OptionResetSession(func(ctx context.Context, conn *pgx.Conn) error {
			_, err := conn.Exec(ctx, `SELECT pg_catalog.set_config('search_path', $1, false)`, kueueBindingSearchPath)
			return err
		}),
	)
	db.SetMaxOpenConns(4)
	if err := db.PingContext(ctx); err != nil {
		_ = db.Close()
		return nil, nil, fmt.Errorf("%w: %v", ErrKueueBindingUnavailable, err)
	}
	if err := verifyKueueBindingSchema(ctx, db); err != nil {
		_ = db.Close()
		return nil, nil, err
	}
	if err := verifyKueueBindingRole(ctx, db); err != nil {
		_ = db.Close()
		return nil, nil, err
	}
	store, err := NewPostgresKueueJobBindingStore(db)
	if err != nil {
		_ = db.Close()
		return nil, nil, err
	}
	return store, db, nil
}

func rejectKueueBindingDSNSearchPath(cfg *pgx.ConnConfig) error {
	if cfg == nil {
		return ErrKueueBindingUnavailable
	}
	if strings.TrimSpace(cfg.RuntimeParams["search_path"]) != "" ||
		strings.Contains(strings.ToLower(cfg.RuntimeParams["options"]), "search_path") {
		return fmt.Errorf("%w: DSN must not set search_path", ErrKueueBindingUnavailable)
	}
	return nil
}

func verifyKueueBindingSchema(ctx context.Context, db *sql.DB) error {
	var ready bool
	err := db.QueryRowContext(ctx, `
		SELECT
		  pg_catalog.to_regclass('public.kueue_job_create_pins') IS NOT NULL
		  AND pg_catalog.to_regclass('public.kueue_job_delete_tombstones') IS NOT NULL
		  AND (
		    SELECT count(*) FROM pg_catalog.pg_proc p
		    JOIN pg_catalog.pg_namespace n ON n.oid = p.pronamespace
		    WHERE n.nspname = 'public'
		      AND p.proname IN ('kueue_job_create_pin_guard', 'kueue_job_delete_tombstone_guard')
		      AND p.proconfig = ARRAY['search_path=pg_catalog, pg_temp']::pg_catalog.text[]
		  ) = 2
		  AND (
		    SELECT count(*) FROM (VALUES
		      ('kueue_job_create_pins'::pg_catalog.name, 'kueue_job_create_pin_no_rewrite'::pg_catalog.name),
		      ('kueue_job_create_pins'::pg_catalog.name, 'kueue_job_create_pin_no_truncate'::pg_catalog.name),
		      ('kueue_job_delete_tombstones'::pg_catalog.name, 'kueue_job_delete_tombstone_no_rewrite'::pg_catalog.name),
		      ('kueue_job_delete_tombstones'::pg_catalog.name, 'kueue_job_delete_tombstone_no_truncate'::pg_catalog.name)
		    ) AS required(relname, tgname)
		    JOIN pg_catalog.pg_class c ON c.relname = required.relname
		    JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace AND n.nspname = 'public'
		    JOIN pg_catalog.pg_trigger t
		      ON t.tgrelid = c.oid AND t.tgname = required.tgname AND t.tgenabled = 'A'
		  ) = 4
	`).Scan(&ready)
	if err != nil {
		return fmt.Errorf("%w: cannot read kueue binding schema: %v", ErrKueueBindingSchema, err)
	}
	if !ready {
		return fmt.Errorf("%w: kueue binding tables or guards are absent", ErrKueueBindingSchema)
	}
	return nil
}

// verifyKueueBindingRole attests the connected login. Catalog reads are
// schema-qualified so a pg_temp function cannot hide a grant. The check is
// the effective privileges of current_user, not the role name, so a login
// with the same grants as ccp_kueue_job_binding is accepted and any broader
// grant is refused.
func verifyKueueBindingRole(ctx context.Context, db *sql.DB) error {
	var reason string
	err := db.QueryRowContext(ctx, `
		SELECT CASE
		  WHEN pg_catalog.current_setting('search_path') IS DISTINCT FROM 'pg_catalog, public, pg_temp'
		    THEN 'search_path'
		  WHEN session_user IS DISTINCT FROM current_user THEN 'session_user'
		  WHEN pg_catalog.current_setting('session_replication_role') IS DISTINCT FROM 'origin'
		    THEN 'session_replication_role'
		  WHEN r.rolsuper THEN 'superuser'
		  WHEN r.rolbypassrls THEN 'bypassrls'
		  WHEN r.rolcreaterole THEN 'createrole'
		  WHEN r.rolcreatedb THEN 'createdb'
		  WHEN r.rolreplication THEN 'replication'
		  WHEN r.rolinherit THEN 'inherit'
		  WHEN pg_catalog.has_parameter_privilege(current_user, 'session_replication_role', 'SET')
		    OR pg_catalog.has_parameter_privilege(current_user, 'session_replication_role', 'ALTER SYSTEM')
		    THEN 'replication_parameter'
		  WHEN EXISTS (
		    SELECT 1 FROM pg_catalog.pg_auth_members m WHERE m.member = r.oid
		  ) THEN 'role_membership'
		  WHEN EXISTS (
		    SELECT 1 FROM pg_catalog.pg_roles other
		    WHERE other.oid <> r.oid
		      AND pg_catalog.pg_has_role(current_user, other.oid, 'SET')
		  ) THEN 'set_role'
		  WHEN EXISTS (
		    SELECT 1 FROM pg_catalog.pg_database d WHERE d.datdba = r.oid
		  ) THEN 'owns_database'
		  WHEN EXISTS (
		    SELECT 1 FROM pg_catalog.pg_namespace n WHERE n.nspowner = r.oid
		  ) THEN 'owns_schema'
		  WHEN EXISTS (
		    SELECT 1 FROM pg_catalog.pg_class c WHERE c.relowner = r.oid
		  ) THEN 'owns_relation'
		  WHEN EXISTS (
		    SELECT 1 FROM pg_catalog.pg_proc p WHERE p.proowner = r.oid
		  ) THEN 'owns_function'
		  WHEN pg_catalog.has_database_privilege(current_user, pg_catalog.current_database(), 'CREATE')
		    THEN 'database_create'
		  WHEN NOT pg_catalog.has_schema_privilege(current_user, 'public', 'USAGE')
		    THEN 'schema_usage'
		  WHEN pg_catalog.has_schema_privilege(current_user, 'public', 'CREATE')
		    THEN 'schema_create'
		  WHEN NOT (
		    pg_catalog.has_table_privilege(current_user, 'public.kueue_job_create_pins'::pg_catalog.regclass, 'SELECT')
		    AND pg_catalog.has_table_privilege(current_user, 'public.kueue_job_create_pins'::pg_catalog.regclass, 'INSERT')
		    AND pg_catalog.has_column_privilege(current_user, 'public.kueue_job_create_pins'::pg_catalog.regclass, 'updated_at', 'UPDATE')
		    AND pg_catalog.has_table_privilege(current_user, 'public.kueue_job_delete_tombstones'::pg_catalog.regclass, 'SELECT')
		    AND pg_catalog.has_table_privilege(current_user, 'public.kueue_job_delete_tombstones'::pg_catalog.regclass, 'INSERT')
		    AND pg_catalog.has_column_privilege(current_user, 'public.kueue_job_delete_tombstones'::pg_catalog.regclass, 'acknowledged', 'UPDATE')
		    AND pg_catalog.has_column_privilege(current_user, 'public.kueue_job_delete_tombstones'::pg_catalog.regclass, 'updated_at', 'UPDATE')
		    AND pg_catalog.has_function_privilege(current_user, 'public.kueue_job_create_pin_guard()'::pg_catalog.regprocedure, 'EXECUTE')
		    AND pg_catalog.has_function_privilege(current_user, 'public.kueue_job_delete_tombstone_guard()'::pg_catalog.regprocedure, 'EXECUTE')
		  ) THEN 'missing_binding_dml'
		  WHEN
		    pg_catalog.has_table_privilege(current_user, 'public.kueue_job_create_pins'::pg_catalog.regclass, 'DELETE')
		    OR pg_catalog.has_table_privilege(current_user, 'public.kueue_job_create_pins'::pg_catalog.regclass, 'TRUNCATE')
		    OR pg_catalog.has_table_privilege(current_user, 'public.kueue_job_create_pins'::pg_catalog.regclass, 'TRIGGER')
		    OR pg_catalog.has_table_privilege(current_user, 'public.kueue_job_create_pins'::pg_catalog.regclass, 'REFERENCES')
		    OR pg_catalog.has_table_privilege(current_user, 'public.kueue_job_delete_tombstones'::pg_catalog.regclass, 'DELETE')
		    OR pg_catalog.has_table_privilege(current_user, 'public.kueue_job_delete_tombstones'::pg_catalog.regclass, 'TRUNCATE')
		    OR pg_catalog.has_table_privilege(current_user, 'public.kueue_job_delete_tombstones'::pg_catalog.regclass, 'TRIGGER')
		    OR pg_catalog.has_table_privilege(current_user, 'public.kueue_job_delete_tombstones'::pg_catalog.regclass, 'REFERENCES')
		    OR EXISTS (
		      SELECT 1 FROM pg_catalog.pg_attribute a
		      WHERE a.attrelid = 'public.kueue_job_create_pins'::pg_catalog.regclass
		        AND a.attnum > 0 AND NOT a.attisdropped
		        AND (
		          (a.attname <> 'updated_at'
		            AND pg_catalog.has_column_privilege(current_user, a.attrelid, a.attname, 'UPDATE'))
		          OR pg_catalog.has_column_privilege(current_user, a.attrelid, a.attname, 'REFERENCES')
		        )
		    )
		    OR EXISTS (
		      SELECT 1 FROM pg_catalog.pg_attribute a
		      WHERE a.attrelid = 'public.kueue_job_delete_tombstones'::pg_catalog.regclass
		        AND a.attnum > 0 AND NOT a.attisdropped
		        AND (
		          (a.attname NOT IN ('acknowledged', 'updated_at')
		            AND pg_catalog.has_column_privilege(current_user, a.attrelid, a.attname, 'UPDATE'))
		          OR pg_catalog.has_column_privilege(current_user, a.attrelid, a.attname, 'REFERENCES')
		        )
		    )
		    THEN 'binding_privilege'
		  WHEN EXISTS (
		    SELECT 1
		    FROM pg_catalog.pg_class c
		    JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
		    WHERE n.nspname = 'public'
		      AND c.relkind IN ('r', 'p', 'v', 'm', 'f')
		      AND c.relname NOT IN ('kueue_job_create_pins', 'kueue_job_delete_tombstones')
		      AND (
		        pg_catalog.has_table_privilege(current_user, c.oid, 'SELECT')
		        OR pg_catalog.has_table_privilege(current_user, c.oid, 'INSERT')
		        OR pg_catalog.has_table_privilege(current_user, c.oid, 'UPDATE')
		        OR pg_catalog.has_table_privilege(current_user, c.oid, 'DELETE')
		        OR pg_catalog.has_table_privilege(current_user, c.oid, 'TRUNCATE')
		        OR pg_catalog.has_table_privilege(current_user, c.oid, 'REFERENCES')
		        OR pg_catalog.has_table_privilege(current_user, c.oid, 'TRIGGER')
		      )
		  ) THEN 'authority_table'
		  ELSE 'ok'
		END
		FROM pg_catalog.pg_roles r
		WHERE r.rolname = current_user
	`).Scan(&reason)
	if err != nil {
		return fmt.Errorf("%w: cannot attest kueue binding role: %v", ErrKueueBindingRole, err)
	}
	if reason != "ok" {
		return fmt.Errorf("%w: %s (expected privileges of %s)", ErrKueueBindingRole, reason, kueueBindingRoleName)
	}
	return nil
}
