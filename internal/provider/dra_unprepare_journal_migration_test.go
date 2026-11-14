package provider

import (
	"context"
	"database/sql"
	"errors"
	"testing"
	"time"

	"github.com/avaargsh/compute-control-panel/internal/pgtest"
)

const draReceiptIngestRole = "ccp_dra_receipt_ingest"
const draReceiptIngestCITestPassword = "dra-ingest-ci-only"

// A real PostgreSQL LOGIN that was independently provisioned before
// migration 0017 must be the only journal writer. This test uses disposable
// CI credentials; it never provisions a production role or a source key.
func TestDRAReceiptCanonicalIngestRoleHasExactNarrowGrants(t *testing.T) {
	admin := openProviderPostgresDB(t)
	if !postgresCurrentUserIsSuperuser(t, admin) {
		t.Skip("requires disposable PostgreSQL superuser")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	var canLogin, noInherit, super, createDB, createRole, bypass, replication bool
	if err := admin.QueryRowContext(ctx, `SELECT rolcanlogin, NOT rolinherit, rolsuper,
		rolcreatedb, rolcreaterole, rolbypassrls, rolreplication
		FROM pg_catalog.pg_roles WHERE rolname=$1`, draReceiptIngestRole).Scan(
		&canLogin, &noInherit, &super, &createDB, &createRole, &bypass, &replication,
	); err != nil {
		t.Fatalf("independently provisioned DRA ingest role missing: %v", err)
	}
	if !canLogin || !noInherit || super || createDB || createRole || bypass || replication {
		t.Fatal("DRA ingest role did not satisfy strict LOGIN/NOINHERIT attributes")
	}
	var applied string
	if err := admin.QueryRowContext(ctx,
		`SELECT name FROM public.schema_migrations WHERE version=17`).Scan(&applied); err != nil ||
		applied != "0017_dra_unprepare_receipt_role.sql" {
		t.Fatalf("migration 0017 was not applied: %q err=%v", applied, err)
	}

	dsn := kueueBindingDSN(t, draReceiptIngestRole, draReceiptIngestCITestPassword, "")
	journal, pool, err := OpenRestrictedDRAUnprepareReceiptJournal(ctx, dsn)
	if err != nil || journal == nil || pool == nil {
		t.Fatalf("canonical restricted login rejected: journal=%t pool=%t err=%v",
			journal != nil, pool != nil, err)
	}
	defer pool.Close()
	verifier, _, receipt := journalReceiptFixture(t)
	first, err := journal.VerifyAndRecord(ctx, verifier, releaseCandidateFixture(), receipt,
		time.Date(2026, 11, 10, 10, 1, 0, 0, time.UTC))
	if err != nil || !first {
		t.Fatalf("canonical journal write failed: new=%t err=%v", first, err)
	}
	replayed, err := journal.VerifyAndRecord(ctx, verifier, releaseCandidateFixture(), receipt,
		time.Date(2026, 11, 10, 10, 1, 0, 0, time.UTC))
	if err != nil || replayed {
		t.Fatalf("canonical event replay not idempotent: new=%t err=%v", replayed, err)
	}

	// These negative assertions use the real, non-owner login. Its INSERT
	// does not grant UPDATE, DELETE, TRUNCATE, Provider, Allocation or E1
	// FINAL access. A superuser trigger-only test cannot prove that boundary.
	for _, statement := range []string{
		`UPDATE public.dra_unprepare_receipt_journal SET source_id='forged'`,
		`DELETE FROM public.dra_unprepare_receipt_journal`,
		`TRUNCATE public.dra_unprepare_receipt_journal`,
		`SELECT * FROM public.compute_providers`,
		`SELECT * FROM public.compute_allocations`,
		`SELECT * FROM public.compute_allocation_usage_evidence_sets`,
	} {
		var sqlErr error
		if statement[0] == 'S' {
			var rowCount int
			sqlErr = pool.QueryRowContext(ctx, "SELECT count(*) FROM ("+statement+") AS unsafe_read").Scan(&rowCount)
		} else {
			_, sqlErr = pool.ExecContext(ctx, statement)
		}
		if postgresSQLState(sqlErr) != "42501" {
			t.Fatalf("restricted canonical login unexpectedly allowed %q: code=%s err=%v",
				statement, postgresSQLState(sqlErr), sqlErr)
		}
	}

	// A newly granted Provider privilege must immediately invalidate the
	// existing pooled login. The test only mutates the disposable CI grant,
	// and revokes it even if its assertion fails.
	if err := pgtest.ExecCatalog(ctx, admin,
		"GRANT SELECT ON public.compute_providers TO "+draReceiptIngestRole); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		cleanupCtx, stop := context.WithTimeout(context.Background(), 15*time.Second)
		defer stop()
		if err := pgtest.ExecCatalog(cleanupCtx, admin,
			"REVOKE SELECT ON public.compute_providers FROM "+draReceiptIngestRole); err != nil {
			t.Errorf("revoke disposable drift grant: %v", err)
		}
	})
	replayed, err = journal.VerifyAndRecord(ctx, verifier, releaseCandidateFixture(), receipt,
		time.Date(2026, 11, 10, 10, 1, 0, 0, time.UTC))
	if !errors.Is(err, ErrDRAReceiptJournalRole) || replayed {
		t.Fatalf("canonical login accepted Provider grant drift: new=%t err=%v", replayed, err)
	}
}

// Missing role does not fall back to the superuser or auto-create a writer.
// The official migration preflight must reject missing/broad roles before
// any privilege is granted, and the standalone opener remains fail-closed.
func TestDRAReceiptCanonicalIngestRoleIsNotPrivileged(t *testing.T) {
	db := openProviderPostgresDB(t)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	var roleHasOtherTableDML bool
	err := db.QueryRowContext(ctx, `
		SELECT EXISTS (
			SELECT 1 FROM pg_catalog.pg_roles r
			JOIN pg_catalog.pg_class c ON c.relname IN
				('compute_providers', 'compute_allocations', 'compute_allocation_usage_evidence_sets')
			WHERE r.rolname=$1
			  AND (pg_catalog.has_table_privilege(r.oid, c.oid, 'SELECT')
			    OR pg_catalog.has_table_privilege(r.oid, c.oid, 'INSERT')
			    OR pg_catalog.has_table_privilege(r.oid, c.oid, 'UPDATE')
			    OR pg_catalog.has_table_privilege(r.oid, c.oid, 'DELETE'))
		)`, draReceiptIngestRole).Scan(&roleHasOtherTableDML)
	if err != nil && !errors.Is(err, sql.ErrNoRows) {
		t.Fatal(err)
	}
	if roleHasOtherTableDML {
		t.Fatal("canonical receipt role has unrelated Provider, Allocation or E1 permissions")
	}
}
