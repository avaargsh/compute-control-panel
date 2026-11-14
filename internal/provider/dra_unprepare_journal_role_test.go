package provider

import (
	"context"
	"errors"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/avaargsh/compute-control-panel/internal/pgtest"
)

// A signed custom receipt must never be persisted via a privileged
// application/Provider/Allocation writer. Only a separately provisioned,
// restricted journal-only login is eligible for this production entrypoint.
func TestRestrictedDRAJournalRefusesAdminMissingGrantsAndSearchPath(t *testing.T) {
	admin := openProviderPostgresDB(t)
	if !postgresCurrentUserIsSuperuser(t, admin) {
		t.Skip("disposable PostgreSQL superuser required for isolated role setup")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 25*time.Second)
	defer cancel()

	journal, pool, err := OpenRestrictedDRAUnprepareReceiptJournal(ctx, os.Getenv("DATABASE_URL"))
	if !errors.Is(err, ErrDRAReceiptJournalRole) || journal != nil || pool != nil {
		t.Fatalf("superuser accepted: journal=%v db=%v err=%v", journal != nil, pool != nil, err)
	}

	empty, pool, err := OpenRestrictedDRAUnprepareReceiptJournal(ctx, "")
	if !errors.Is(err, ErrDRAReceiptJournalUnavailable) || empty != nil || pool != nil {
		t.Fatalf("empty DSN accepted: journal=%v db=%v err=%v", empty != nil, pool != nil, err)
	}
	for _, bad := range []string{
		"postgres://ccp_dra_receipt_ingest@127.0.0.1:1/postgres?sslmode=disable&search_path=pg_temp&connect_timeout=1",
		"postgres://ccp_dra_receipt_ingest@127.0.0.1:1/postgres?sslmode=disable&options=-csearch_path%3Dpg_temp&connect_timeout=1",
	} {
		got, db, err := OpenRestrictedDRAUnprepareReceiptJournal(ctx, bad)
		if !errors.Is(err, ErrDRAReceiptJournalUnavailable) || !strings.Contains(err.Error(), "search_path") || got != nil || db != nil {
			t.Fatalf("caller search_path accepted: journal=%v db=%v err=%v", got != nil, db != nil, err)
		}
	}

	missing := kueueTestIdent(t, "ccp_dra_missing_")
	createKueueBindingRole(t, admin, missing)
	if err := pgtest.ExecCatalog(ctx, admin, "GRANT USAGE ON SCHEMA public TO "+missing); err != nil {
		t.Fatal(err)
	}
	got, db, err := OpenRestrictedDRAUnprepareReceiptJournal(ctx,
		kueueBindingDSN(t, missing, kueueBindingTestPassword, ""))
	if !errors.Is(err, ErrDRAReceiptJournalRole) || got != nil || db != nil {
		t.Fatalf("read/write-less role accepted: journal=%v db=%v err=%v", got != nil, db != nil, err)
	}
}

func TestRestrictedDRAJournalRefusesBroadRoleAndRuntimePrivilegeDrift(t *testing.T) {
	admin := openProviderPostgresDB(t)
	if !postgresCurrentUserIsSuperuser(t, admin) {
		t.Skip("disposable PostgreSQL superuser required for isolated role setup")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	role := kueueTestIdent(t, "ccp_dra_writer_")
	createKueueBindingRole(t, admin, role)
	if err := pgtest.ExecCatalog(ctx, admin,
		"GRANT USAGE ON SCHEMA public TO "+role,
		"GRANT SELECT, INSERT ON public.dra_unprepare_receipt_journal TO "+role,
	); err != nil {
		t.Fatal(err)
	}
	dsn := kueueBindingDSN(t, role, kueueBindingTestPassword, "")
	journal, pool, err := OpenRestrictedDRAUnprepareReceiptJournal(ctx, dsn)
	if err != nil || journal == nil || pool == nil {
		t.Fatalf("restricted role refused: journal=%v db=%v err=%v", journal != nil, pool != nil, err)
	}
	pool.SetMaxOpenConns(1)
	defer pool.Close()

	verifier, _, signed := journalReceiptFixture(t)
	candidate := releaseCandidateFixture()
	asOf := time.Date(2026, 11, 10, 10, 1, 0, 0, time.UTC)
	first, err := journal.VerifyAndRecord(ctx, verifier, candidate, signed, asOf)
	if err != nil || !first {
		t.Fatalf("restricted writer did not record valid evidence: new=%t err=%v", first, err)
	}
	again, err := journal.VerifyAndRecord(ctx, verifier, candidate, signed, asOf)
	if err != nil || again {
		t.Fatalf("restricted replay not deduplicated: new=%t err=%v", again, err)
	}

	// A separate DBA can alter privileges *after* journal construction.
	// Rechecks must run on the same connection used to write, not just at
	// process startup. Even an exact replay cannot bypass an unsafe role.
	if err := pgtest.ExecCatalog(ctx, admin, "GRANT SELECT ON public.compute_providers TO "+role); err != nil {
		t.Fatal(err)
	}
	again, err = journal.VerifyAndRecord(ctx, verifier, candidate, signed, asOf)
	if !errors.Is(err, ErrDRAReceiptJournalRole) || again {
		t.Fatalf("privilege drift remained trusted: new=%t err=%v", again, err)
	}
	broad, broadDB, err := OpenRestrictedDRAUnprepareReceiptJournal(ctx, dsn)
	if !errors.Is(err, ErrDRAReceiptJournalRole) || broad != nil || broadDB != nil {
		t.Fatalf("broad login accepted: journal=%v db=%v err=%v", broad != nil, broadDB != nil, err)
	}
}
