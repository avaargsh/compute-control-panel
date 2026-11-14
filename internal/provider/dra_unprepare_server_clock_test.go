package provider

import (
	"context"
	"errors"
	"net/url"
	"testing"
	"time"
)

// Caller-supplied asOf is only an observation timestamp, never a trusted
// authorization clock. Backdating it must not resurrect an expired signer
// or an old event, even when Ed25519 and historical scope both match.
func TestRestrictedDRAJournalRejectsCallerBackdatedAsOf(t *testing.T) {
	admin := openProviderPostgresDB(t)
	if !postgresCurrentUserIsSuperuser(t, admin) {
		t.Skip("disposable PostgreSQL administrator required for source-key fixture")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 25*time.Second)
	defer cancel()
	verifier, _, receipt := journalReceiptFixture(t)
	candidate := releaseCandidateFixture()
	// This is a *historical*, valid signing period. The test deliberately
	// supplies asOf within that period, several days before the DB clock.
	seedTrustedDRAReceiptSource(t, admin, verifier, receipt, candidate)
	journal, pool, err := OpenRestrictedDRAUnprepareReceiptJournal(ctx,
		kueueBindingDSN(t, draReceiptIngestRole, draReceiptIngestCITestPassword, ""))
	if err != nil {
		t.Fatal(err)
	}
	defer pool.Close()
	asOf := time.Date(2026, 11, 10, 10, 1, 0, 0, time.UTC)
	recorded, err := journal.VerifyAndRecord(ctx, verifier, candidate, receipt, asOf)
	if !errors.Is(err, ErrDRAReceiptSourceUntrusted) || recorded {
		t.Fatalf("historical asOf bypassed live source validity: added=%t err=%v", recorded, err)
	}
	var count int
	if err := admin.QueryRowContext(ctx, `SELECT count(*) FROM public.dra_unprepare_receipt_journal
		WHERE cluster_id=$1 AND source_id=$2 AND event_id=$3`,
		receipt.Event.ClusterID, receipt.Event.SourceID, receipt.Event.EventID).Scan(&count); err != nil || count != 0 {
		t.Fatalf("backdated event was persisted: count=%d err=%v", count, err)
	}
}

// The source was valid when checked, but may expire while PostgreSQL waits
// for the journal relation lock. The eventual INSERT must use database time
// *after* lock acquisition, not only the earlier trust-check snapshot.
func TestRestrictedDRAJournalRejectsSourceExpiryDuringInsertLockWait(t *testing.T) {
	admin := openProviderPostgresDB(t)
	if !postgresCurrentUserIsSuperuser(t, admin) {
		t.Skip("disposable PG16 administrator required for lock and signer fixture")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 40*time.Second)
	defer cancel()
	verifier, signed, asOf := liveJournalReceiptFixture(t)
	candidate := releaseCandidateFixture()
	validUntil := time.Now().UTC().Add(5 * time.Second)
	_, err := admin.ExecContext(ctx, `INSERT INTO public.dra_unprepare_source_keys (
		cluster_id,source_id,key_id,tenant_id,provider_id,driver,public_key,
		valid_from,valid_until,maximum_age_seconds,future_skew_seconds
	) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,600,30)`,
		signed.Event.ClusterID, signed.Event.SourceID, signed.Event.KeyID,
		candidate.Delete.TenantID, candidate.Delete.ProviderID, signed.Event.Driver,
		verifier.authority.PublicKey, signed.Event.CompletedAt.Add(-time.Minute), validUntil)
	if err != nil {
		t.Fatal(err)
	}
	dsn, err := url.Parse(kueueBindingDSN(t, draReceiptIngestRole, draReceiptIngestCITestPassword, ""))
	if err != nil {
		t.Fatal(err)
	}
	appName := kueueTestIdent(t, "ccp_dra_expiry_wait_")
	params := dsn.Query()
	params.Set("application_name", appName)
	dsn.RawQuery = params.Encode()
	journal, pool, err := OpenRestrictedDRAUnprepareReceiptJournal(ctx, dsn.String())
	if err != nil {
		t.Fatal(err)
	}
	defer pool.Close()
	pool.SetMaxOpenConns(1)

	hold, err := admin.BeginTx(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer hold.Rollback()
	if _, err := hold.ExecContext(ctx, `LOCK TABLE public.dra_unprepare_receipt_journal IN ACCESS EXCLUSIVE MODE`); err != nil {
		t.Fatal(err)
	}
	type result struct {
		added bool
		err   error
	}
	done := make(chan result, 1)
	go func() {
		added, err := journal.VerifyAndRecord(ctx, verifier, candidate, signed, asOf)
		done <- result{added: added, err: err}
	}()
	ticker := time.NewTicker(30 * time.Millisecond)
	defer ticker.Stop()
	for {
		var count int
		if err := admin.QueryRowContext(ctx, `SELECT count(*)::integer
			FROM pg_catalog.pg_stat_activity
			WHERE application_name=$1 AND wait_event_type='Lock'
			  AND query ILIKE '%INSERT INTO public.dra_unprepare_receipt_journal%'`,
			appName).Scan(&count); err != nil {
			t.Fatal(err)
		}
		if count != 0 {
			break
		}
		select {
		case got := <-done:
			t.Fatalf("write finished before journal relation lock: added=%t err=%v", got.added, got.err)
		case <-ctx.Done():
			t.Fatalf("write did not reach locked INSERT: %v", ctx.Err())
		case <-ticker.C:
		}
	}
	// Synchronize to the PostgreSQL server clock, not the Go wall clock.
	for {
		var expired bool
		if err := admin.QueryRowContext(ctx,
			`SELECT pg_catalog.clock_timestamp() >= $1`, validUntil).Scan(&expired); err != nil {
			t.Fatal(err)
		}
		if expired {
			break
		}
		select {
		case <-ctx.Done():
			t.Fatalf("source validity did not expire: %v", ctx.Err())
		case <-ticker.C:
		}
	}
	if err := hold.Rollback(); err != nil {
		t.Fatal(err)
	}
	select {
	case got := <-done:
		if got.added || !errors.Is(got.err, ErrDRAReceiptSourceUntrusted) {
			t.Fatalf("expired source was appended after lock wait: added=%t err=%v", got.added, got.err)
		}
	case <-ctx.Done():
		t.Fatalf("journal INSERT did not resume: %v", ctx.Err())
	}
	var count int
	if err := admin.QueryRowContext(ctx, `SELECT count(*) FROM public.dra_unprepare_receipt_journal
		WHERE cluster_id=$1 AND source_id=$2 AND event_id=$3`,
		signed.Event.ClusterID, signed.Event.SourceID, signed.Event.EventID).Scan(&count); err != nil || count != 0 {
		t.Fatalf("expired receipt persisted: count=%d err=%v", count, err)
	}
}
