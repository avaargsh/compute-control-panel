package provider

import (
	"context"
	"crypto/ed25519"
	"errors"
	"sync"
	"testing"
	"time"
)

// This is a disposable PostgreSQL integration proof of a replay fence. It is
// not driver attestation, GPU deallocation or an Allocation RELEASED transition.
func journalReceiptFixture(t *testing.T) (*DRAUnprepareVerifier, ed25519.PrivateKey, SignedDRAUnprepare) {
	t.Helper()
	authority, private := unprepareTestAuthority()
	authority.ClusterID = kueueTestIdent(t, "cluster-")
	receipt := unprepareTestReceipt(private)
	receipt.Event.ClusterID = authority.ClusterID
	receipt.Event.EventID = kueueTestIdent(t, "event-")
	receipt.Signature = ed25519.Sign(private, draUnprepareSigningBytes(receipt.Event))
	verifier, err := NewDRAUnprepareVerifier(authority, 10*time.Minute, 30*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	return verifier, private, receipt
}

func TestDRASignedReceiptJournalDurableReplayAndEpochFence(t *testing.T) {
	db := openProviderPostgresDB(t)
	journal, err := NewDRAUnprepareReceiptJournal(db)
	if err != nil {
		t.Fatal(err)
	}
	verifier, private, signed := journalReceiptFixture(t)
	candidate := releaseCandidateFixture()
	asOf := time.Date(2026, 11, 10, 10, 1, 0, 0, time.UTC)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	newEvent, err := journal.VerifyAndRecord(ctx, verifier, candidate, signed, asOf)
	if err != nil || !newEvent {
		t.Fatalf("first verified receipt was not recorded: new=%t err=%v", newEvent, err)
	}
	replayed, err := journal.VerifyAndRecord(ctx, verifier, candidate, signed, asOf)
	if err != nil || replayed {
		t.Fatalf("exact signed replay not deduplicated: new=%t err=%v", replayed, err)
	}
	// A second process must see the same durable fence.
	restarted, err := NewDRAUnprepareReceiptJournal(db)
	if err != nil {
		t.Fatal(err)
	}
	replayed, err = restarted.VerifyAndRecord(ctx, verifier, candidate, signed, asOf)
	if err != nil || replayed {
		t.Fatalf("restart allowed an already-recorded receipt: new=%t err=%v", replayed, err)
	}

	// A valid signer cannot reuse the same event ID with different completion
	// time, even when both timestamps are within the permitted clock window.
	altered := signed
	altered.Event.CompletedAt = altered.Event.CompletedAt.Add(time.Second)
	altered.Signature = ed25519.Sign(private, draUnprepareSigningBytes(altered.Event))
	replayed, err = restarted.VerifyAndRecord(ctx, verifier, candidate, altered, asOf)
	if !errors.Is(err, ErrDRAReceiptReplayConflict) || replayed {
		t.Fatalf("changed event reused the event ID: new=%t err=%v", replayed, err)
	}

	// Rotating the event ID must not evade the unique historical Claim/Pod
	// allocation epoch fence.
	altered = signed
	altered.Event.EventID = kueueTestIdent(t, "event-")
	altered.Signature = ed25519.Sign(private, draUnprepareSigningBytes(altered.Event))
	replayed, err = restarted.VerifyAndRecord(ctx, verifier, candidate, altered, asOf)
	if !errors.Is(err, ErrDRAReceiptReplayConflict) || replayed {
		t.Fatalf("different event ID reused one allocated device epoch: new=%t err=%v", replayed, err)
	}

	// Source ID is signed, but a different independently provisioned source
	// must not claim the exact same historical claim/device epoch.
	authority2, _ := unprepareTestAuthority()
	authority2.ClusterID = signed.Event.ClusterID
	authority2.SourceID = "node-plugin-successor"
	otherVerifier, err := NewDRAUnprepareVerifier(authority2, 10*time.Minute, 30*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	altered.Event.SourceID = authority2.SourceID
	altered.Signature = ed25519.Sign(private, draUnprepareSigningBytes(altered.Event))
	replayed, err = restarted.VerifyAndRecord(ctx, otherVerifier, candidate, altered, asOf)
	if !errors.Is(err, ErrDRAReceiptReplayConflict) || replayed {
		t.Fatalf("new source reused one allocated device epoch: new=%t err=%v", replayed, err)
	}
	var count int
	if err := db.QueryRowContext(ctx, `SELECT count(*) FROM public.dra_unprepare_receipt_journal
		WHERE cluster_id=$1`, signed.Event.ClusterID).Scan(&count); err != nil || count != 1 {
		t.Fatalf("receipt count=%d err=%v, want one immutable row", count, err)
	}
}

func TestDRASignedReceiptJournalConcurrentExactlyOneNewRecord(t *testing.T) {
	db := openProviderPostgresDB(t)
	journal, err := NewDRAUnprepareReceiptJournal(db)
	if err != nil {
		t.Fatal(err)
	}
	verifier, _, signed := journalReceiptFixture(t)
	candidate := releaseCandidateFixture()
	asOf := time.Date(2026, 11, 10, 10, 1, 0, 0, time.UTC)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	const workers = 12
	newEvents := make([]bool, workers)
	errs := make([]error, workers)
	var wg sync.WaitGroup
	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			newEvents[i], errs[i] = journal.VerifyAndRecord(ctx, verifier, candidate, signed, asOf)
		}(i)
	}
	wg.Wait()
	newCount := 0
	for i, err := range errs {
		if err != nil {
			t.Fatalf("concurrent attempt %d: %v", i, err)
		}
		if newEvents[i] {
			newCount++
		}
	}
	if newCount != 1 {
		t.Fatalf("concurrent duplicates recorded %d first events, want 1", newCount)
	}
}

func TestDRASignedReceiptJournalRejectsInvalidSignatureAndImmutableRowChanges(t *testing.T) {
	db := openProviderPostgresDB(t)
	journal, err := NewDRAUnprepareReceiptJournal(db)
	if err != nil {
		t.Fatal(err)
	}
	verifier, _, signed := journalReceiptFixture(t)
	candidate := releaseCandidateFixture()
	asOf := time.Date(2026, 11, 10, 10, 1, 0, 0, time.UTC)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	invalid := signed
	invalid.Signature = append([]byte(nil), signed.Signature...)
	invalid.Signature[0] ^= 0xff
	newEvent, err := journal.VerifyAndRecord(ctx, verifier, candidate, invalid, asOf)
	if !errors.Is(err, ErrUncorrelatedDRAUnprepare) || newEvent {
		t.Fatalf("invalid signer wrote evidence: new=%t err=%v", newEvent, err)
	}
	newEvent, err = journal.VerifyAndRecord(ctx, verifier, candidate, signed, asOf)
	if err != nil || !newEvent {
		t.Fatalf("valid signed receipt was not recorded: new=%t err=%v", newEvent, err)
	}

	for _, query := range []string{
		"UPDATE public.dra_unprepare_receipt_journal SET source_id='rewritten' WHERE cluster_id=$1",
		"DELETE FROM public.dra_unprepare_receipt_journal WHERE cluster_id=$1",
		"TRUNCATE public.dra_unprepare_receipt_journal",
	} {
		tx, err := db.BeginTx(ctx, nil)
		if err != nil {
			t.Fatal(err)
		}
		if query == "TRUNCATE public.dra_unprepare_receipt_journal" {
			_, err = tx.ExecContext(ctx, query)
		} else {
			_, err = tx.ExecContext(ctx, query, signed.Event.ClusterID)
		}
		_ = tx.Rollback() // No negative DDL check may mutate another test's rows.
		if postgresSQLState(err) != "55000" {
			t.Fatalf("immutable journal allowed %s: code=%s err=%v", query, postgresSQLState(err), err)
		}
	}
}
