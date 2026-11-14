package provider

import (
	"context"
	"errors"
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
