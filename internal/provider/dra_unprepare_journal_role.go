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

// ErrDRAReceiptJournalRole rejects a login that can mutate or inspect CCP
// authorities beyond the DRA receipt journal. The login is not a source-key
// authority or proof that a real driver RPC completed.
var ErrDRAReceiptJournalRole = errors.New("DRA receipt journal login is not least privilege")

// OpenRestrictedDRAUnprepareReceiptJournal opens the only production-eligible
// journal database entrypoint. Its login must be independently provisioned,
// have public schema USAGE and SELECT/INSERT on the DRA receipt journal only,
// no other CCP relation privileges, ownership, role membership, or DDL. It
// refuses caller-controlled search_path and pins each pooled connection.
//
// This does NOT provision secrets, authenticate an external driver, or make
// signed receipts authoritative. No production ingress is wired yet. The
// caller must close the returned pool. The older New... constructor remains
// unchecked solely for existing disposable PostgreSQL tests.
func OpenRestrictedDRAUnprepareReceiptJournal(
	ctx context.Context, dsn string,
) (*DRAUnprepareReceiptJournal, *sql.DB, error) {
	if ctx == nil || strings.TrimSpace(dsn) == "" {
		return nil, nil, ErrDRAReceiptJournalUnavailable
	}
	cfg, err := pgx.ParseConfig(dsn)
	if err != nil {
		return nil, nil, fmt.Errorf("%w: parse DSN: %v", ErrDRAReceiptJournalUnavailable, err)
	}
	if err := rejectKueueBindingDSNSearchPath(cfg); err != nil {
		return nil, nil, fmt.Errorf("%w: DSN must not set search_path", ErrDRAReceiptJournalUnavailable)
	}
	if cfg.ConnectTimeout <= 0 {
		cfg.ConnectTimeout = 5 * time.Second
	}
	db := stdlib.OpenDB(*cfg,
		stdlib.OptionBeforeConnect(func(_ context.Context, conn *pgx.ConnConfig) error {
			if conn.RuntimeParams == nil {
				conn.RuntimeParams = map[string]string{}
			}
			conn.RuntimeParams["search_path"] = kueueBindingSearchPath
			return nil
		}),
		stdlib.OptionResetSession(func(ctx context.Context, conn *pgx.Conn) error {
			_, err := conn.Exec(ctx, `SELECT pg_catalog.set_config('search_path', $1, false)`, kueueBindingSearchPath)
			return err
		}),
	)
	db.SetMaxOpenConns(4)
	fail := func(err error) (*DRAUnprepareReceiptJournal, *sql.DB, error) {
		_ = db.Close()
		return nil, nil, err
	}
	if err := db.PingContext(ctx); err != nil {
		return fail(fmt.Errorf("%w: connect: %v", ErrDRAReceiptJournalUnavailable, err))
	}
	if err := verifyDRAJournalSchema(ctx, db); err != nil {
		return fail(err)
	}
	if err := verifyDRAJournalRole(ctx, db); err != nil {
		return fail(err)
	}
	return &DRAUnprepareReceiptJournal{db: db, restricted: true}, db, nil
}

// draJournalQueryRow is implemented by both *sql.DB and *sql.Conn; the
// privilege and trigger checks in VerifyAndRecord must run on the SAME
// connection which will issue the actual INSERT and replay SELECT.
type draJournalQueryRow interface {
	QueryRowContext(context.Context, string, ...any) *sql.Row
}

func verifyDRAJournalSchema(ctx context.Context, q draJournalQueryRow) error {
	var present bool
	if err := q.QueryRowContext(ctx,
		`SELECT pg_catalog.to_regclass('public.dra_unprepare_receipt_journal') IS NOT NULL`,
	).Scan(&present); err != nil || !present {
		return fmt.Errorf("%w: missing journal table or catalog access: %v", ErrDRAReceiptJournalUnavailable, err)
	}
	var guards int
	err := q.QueryRowContext(ctx, `
		SELECT count(*)::integer
		FROM (VALUES
			('dra_unprepare_receipt_no_update_delete'::pg_catalog.name, 27::integer),
			('dra_unprepare_receipt_no_truncate'::pg_catalog.name, 34::integer)
		) required(tgname, tgtype)
		JOIN pg_catalog.pg_trigger t
		  ON t.tgrelid='public.dra_unprepare_receipt_journal'::pg_catalog.regclass
		 AND t.tgname=required.tgname AND t.tgtype=required.tgtype
		JOIN pg_catalog.pg_proc p
		  ON p.oid=t.tgfoid
		 AND p.oid='public.dra_unprepare_receipt_journal_guard()'::pg_catalog.regprocedure
		WHERE t.tgenabled='A'
		  AND t.tgqual IS NULL AND t.tgnargs=0 AND t.tgattr::text=''
		  AND NOT t.tgdeferrable AND NOT t.tginitdeferred
		  AND NOT p.prosecdef
		  AND p.proconfig=ARRAY['search_path=pg_catalog, pg_temp']::pg_catalog.text[]
	`).Scan(&guards)
	if err != nil || guards != 2 {
		return fmt.Errorf("%w: immutable DRA journal guards absent or altered: %v", ErrDRAReceiptJournalUnavailable, err)
	}
	// Source keys and revocations must be independently administered and
	// append-only. Never accept a substituted registry with disabled guards.
	var registryGuards int
	err = q.QueryRowContext(ctx, `
		SELECT count(*)::integer
		FROM (VALUES
		  ('public.dra_unprepare_source_keys'::pg_catalog.regclass,
		   'dra_unprepare_keys_no_update_delete'::pg_catalog.name, 27::integer),
		  ('public.dra_unprepare_source_keys'::pg_catalog.regclass,
		   'dra_unprepare_keys_no_truncate'::pg_catalog.name, 34::integer),
		  ('public.dra_unprepare_source_revocations'::pg_catalog.regclass,
		   'dra_unprepare_revocations_no_update_delete'::pg_catalog.name, 27::integer),
		  ('public.dra_unprepare_source_revocations'::pg_catalog.regclass,
		   'dra_unprepare_revocations_no_truncate'::pg_catalog.name, 34::integer)
		) required(rel, name, bits)
		JOIN pg_catalog.pg_trigger t
		  ON t.tgrelid=required.rel AND t.tgname=required.name
		 AND t.tgtype=required.bits AND t.tgenabled='A'
		JOIN pg_catalog.pg_proc p
		  ON p.oid=t.tgfoid
		 AND p.oid='public.dra_unprepare_receipt_journal_guard()'::pg_catalog.regprocedure
		WHERE t.tgqual IS NULL AND t.tgnargs=0 AND t.tgattr::text=''
		  AND NOT t.tgdeferrable AND NOT t.tginitdeferred
		  AND NOT p.prosecdef
		  AND p.proconfig=ARRAY['search_path=pg_catalog, pg_temp']::pg_catalog.text[]
	`).Scan(&registryGuards)
	if err != nil || registryGuards != 4 {
		return fmt.Errorf("%w: independent DRA signer registry guards absent or altered: %v", ErrDRAReceiptJournalUnavailable, err)
	}
	return nil
}

func verifyDRAJournalRole(ctx context.Context, q draJournalQueryRow) error {
	var reason string
	err := q.QueryRowContext(ctx, `
		SELECT CASE
		  WHEN pg_catalog.current_setting('search_path') IS DISTINCT FROM 'pg_catalog, public, pg_temp'
		    THEN 'search_path'
		  WHEN session_user IS DISTINCT FROM current_user THEN 'session_user'
		  WHEN pg_catalog.current_setting('session_replication_role') IS DISTINCT FROM 'origin'
		    THEN 'replication_mode'
		  WHEN NOT r.rolcanlogin THEN 'login'
		  WHEN r.rolsuper THEN 'superuser'
		  WHEN r.rolbypassrls OR r.rolreplication OR r.rolcreatedb
		    OR r.rolcreaterole OR r.rolinherit THEN 'elevated_role'
		  WHEN pg_catalog.has_parameter_privilege(current_user, 'session_replication_role', 'SET')
		    OR pg_catalog.has_parameter_privilege(current_user, 'session_replication_role', 'ALTER SYSTEM')
		    THEN 'replication_parameter'
		  WHEN EXISTS (SELECT 1 FROM pg_catalog.pg_auth_members m WHERE m.member=r.oid)
		    THEN 'membership'
		  WHEN EXISTS (
		    SELECT 1 FROM pg_catalog.pg_roles other
		    WHERE other.oid<>r.oid AND pg_catalog.pg_has_role(current_user, other.oid, 'SET')
		  ) THEN 'set_role'
		  WHEN EXISTS (SELECT 1 FROM pg_catalog.pg_database d WHERE d.datdba=r.oid)
		    OR EXISTS (SELECT 1 FROM pg_catalog.pg_namespace n WHERE n.nspowner=r.oid)
		    OR EXISTS (SELECT 1 FROM pg_catalog.pg_class c WHERE c.relowner=r.oid)
		    OR EXISTS (SELECT 1 FROM pg_catalog.pg_proc p WHERE p.proowner=r.oid)
		    THEN 'object_owner'
		  WHEN pg_catalog.has_database_privilege(current_user, pg_catalog.current_database(), 'CREATE')
		    OR pg_catalog.has_schema_privilege(current_user, 'public', 'CREATE')
		    THEN 'create_privilege'
		  WHEN NOT pg_catalog.has_schema_privilege(current_user, 'public', 'USAGE')
		    THEN 'schema_usage'
		  WHEN NOT (
		    pg_catalog.has_table_privilege(current_user, 'public.dra_unprepare_receipt_journal'::pg_catalog.regclass, 'SELECT')
		    AND pg_catalog.has_table_privilege(current_user, 'public.dra_unprepare_receipt_journal'::pg_catalog.regclass, 'INSERT')
		  ) THEN 'missing_journal_dml'
		  WHEN NOT (
		    pg_catalog.has_table_privilege(current_user, 'public.dra_unprepare_source_keys'::pg_catalog.regclass, 'SELECT')
		    AND pg_catalog.has_table_privilege(current_user, 'public.dra_unprepare_source_revocations'::pg_catalog.regclass, 'SELECT')
		    -- A single inert column grants PG16 SELECT ... FOR UPDATE.
		    -- Its ENABLE ALWAYS immutable trigger rejects any actual UPDATE.
		    AND pg_catalog.has_column_privilege(current_user,
		      'public.dra_unprepare_source_keys'::pg_catalog.regclass, 'lock_token', 'UPDATE')
		  ) THEN 'missing_source_registry_read_or_lock'
		  WHEN EXISTS (
		    SELECT 1 FROM pg_catalog.pg_class c
		    JOIN pg_catalog.pg_namespace n ON n.oid=c.relnamespace
		    WHERE n.nspname='public'
		      AND c.relname IN ('dra_unprepare_source_keys','dra_unprepare_source_revocations')
		      AND (
		        pg_catalog.has_table_privilege(current_user, c.oid, 'INSERT')
		        OR pg_catalog.has_table_privilege(current_user, c.oid, 'UPDATE')
		        OR pg_catalog.has_table_privilege(current_user, c.oid, 'DELETE')
		        OR pg_catalog.has_table_privilege(current_user, c.oid, 'TRUNCATE')
		        OR pg_catalog.has_table_privilege(current_user, c.oid, 'TRIGGER')
		        OR pg_catalog.has_table_privilege(current_user, c.oid, 'REFERENCES')
		      )
		  ) THEN 'source_registry_write_privilege'
		  WHEN pg_catalog.has_table_privilege(current_user, 'public.dra_unprepare_receipt_journal'::pg_catalog.regclass, 'UPDATE')
		    OR pg_catalog.has_table_privilege(current_user, 'public.dra_unprepare_receipt_journal'::pg_catalog.regclass, 'DELETE')
		    OR pg_catalog.has_table_privilege(current_user, 'public.dra_unprepare_receipt_journal'::pg_catalog.regclass, 'TRUNCATE')
		    OR pg_catalog.has_table_privilege(current_user, 'public.dra_unprepare_receipt_journal'::pg_catalog.regclass, 'TRIGGER')
		    OR pg_catalog.has_table_privilege(current_user, 'public.dra_unprepare_receipt_journal'::pg_catalog.regclass, 'REFERENCES')
		    THEN 'journal_privilege'
		  WHEN EXISTS (
		    SELECT 1 FROM pg_catalog.pg_class c
		    JOIN pg_catalog.pg_namespace n ON n.oid=c.relnamespace
		    WHERE n.nspname='public'
		      AND c.relkind IN ('r', 'p', 'v', 'm', 'f')
		      AND c.relname NOT IN (
		        'dra_unprepare_receipt_journal',
		        'dra_unprepare_source_keys',
		        'dra_unprepare_source_revocations'
		      )
		      AND (
		        pg_catalog.has_table_privilege(current_user, c.oid, 'SELECT')
		        OR pg_catalog.has_table_privilege(current_user, c.oid, 'INSERT')
		        OR pg_catalog.has_table_privilege(current_user, c.oid, 'UPDATE')
		        OR pg_catalog.has_table_privilege(current_user, c.oid, 'DELETE')
		        OR pg_catalog.has_table_privilege(current_user, c.oid, 'TRUNCATE')
		        OR pg_catalog.has_table_privilege(current_user, c.oid, 'TRIGGER')
		        OR pg_catalog.has_table_privilege(current_user, c.oid, 'REFERENCES')
		      )
		  ) THEN 'authority_table'
		  WHEN EXISTS (
		    SELECT 1 FROM pg_catalog.pg_attribute a
		    JOIN pg_catalog.pg_class c ON c.oid=a.attrelid
		    JOIN pg_catalog.pg_namespace n ON n.oid=c.relnamespace
		    WHERE n.nspname='public' AND c.relkind IN ('r','p','v','m','f')
		      AND a.attnum>0 AND NOT a.attisdropped
		      AND (
		        (c.relname NOT IN ('dra_unprepare_receipt_journal',
		          'dra_unprepare_source_keys','dra_unprepare_source_revocations') AND (
		          pg_catalog.has_column_privilege(current_user, c.oid, a.attname, 'SELECT')
		          OR pg_catalog.has_column_privilege(current_user, c.oid, a.attname, 'INSERT')
		          OR pg_catalog.has_column_privilege(current_user, c.oid, a.attname, 'UPDATE')
		          OR pg_catalog.has_column_privilege(current_user, c.oid, a.attname, 'REFERENCES')
		        ))
		        OR (c.relname IN ('dra_unprepare_source_keys','dra_unprepare_source_revocations') AND (
		          pg_catalog.has_column_privilege(current_user, c.oid, a.attname, 'INSERT')
		          OR ((c.relname <> 'dra_unprepare_source_keys' OR a.attname <> 'lock_token')
		            AND pg_catalog.has_column_privilege(current_user, c.oid, a.attname, 'UPDATE'))
		          OR pg_catalog.has_column_privilege(current_user, c.oid, a.attname, 'REFERENCES')
		        ))
		        OR (c.relname='dra_unprepare_receipt_journal' AND (
		          pg_catalog.has_column_privilege(current_user, c.oid, a.attname, 'UPDATE')
		          OR pg_catalog.has_column_privilege(current_user, c.oid, a.attname, 'REFERENCES')
		        ))
		      )
		  ) THEN 'column_privilege'
		  ELSE 'ok'
		END
		FROM pg_catalog.pg_roles r
		WHERE r.rolname=current_user
	`).Scan(&reason)
	if err != nil {
		return fmt.Errorf("%w: cannot attest session: %v", ErrDRAReceiptJournalRole, err)
	}
	if reason != "ok" {
		return fmt.Errorf("%w: %s", ErrDRAReceiptJournalRole, reason)
	}
	return nil
}
