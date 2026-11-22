package provider

import (
	"context"
	"crypto/ed25519"
	"database/sql"
	"errors"
	"testing"
	"time"
)

// This checks only an immutable, signed event's membership in the existing
// independently registered receipt journal, under the CALLER'S transaction.
// It is not proof that a real driver RPC executed or that any GPU was freed.
func TestRecordedDRAReceiptSameTransactionPresenceReplayAndRevocation(t *testing.T) {
	admin := openProviderPostgresDB(t)
	if !postgresCurrentUserIsSuperuser(t, admin) {
		t.Skip("requires disposable PostgreSQL superuser to provision source")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	verifier, private, signed := journalReceiptFixture(t)
	asOf := time.Now().UTC()
	signed.Event.CompletedAt = asOf.Add(-25 * time.Second)
	signed.Signature = ed25519.Sign(private, draUnprepareSigningBytes(signed.Event))
	candidate := releaseCandidateFixture()
	seedTrustedDRAReceiptSource(t, admin, verifier, signed, candidate)

	read := func(v *DRAUnprepareVerifier, receipt SignedDRAUnprepare) error {
		t.Helper()
		tx, err := admin.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelReadCommitted})
		if err != nil {
			t.Fatal(err)
		}
		defer tx.Rollback()
		return RequireRecordedDRAUnprepareReceipt(ctx, tx, v, candidate, receipt, asOf)
	}
	if err := RequireRecordedDRAUnprepareReceipt(ctx, nil, verifier, candidate, signed, asOf); !errors.Is(err, ErrDRAReceiptJournalUnavailable) {
		t.Fatalf("nil transaction accepted: %v", err)
	}
	if err := read(verifier, signed); !errors.Is(err, ErrDRAReceiptNotRecorded) {
		t.Fatalf("signed but non-journaled event accepted: %v", err)
	}

	journal, ingestDB, err := OpenRestrictedDRAUnprepareReceiptJournal(ctx,
		kueueBindingDSN(t, draReceiptIngestRole, draReceiptIngestCITestPassword, ""))
	if err != nil {
		t.Fatalf("open restricted ingestion: %v", err)
	}
	defer ingestDB.Close()
	if fresh, err := journal.VerifyAndRecord(ctx, verifier, candidate, signed, asOf); err != nil || !fresh {
		t.Fatalf("DBA-registered receipt failed restricted ingest: fresh=%t err=%v", fresh, err)
	}
	if err := read(verifier, signed); err != nil {
		t.Fatalf("exact recorded and DBA-registered receipt rejected: %v", err)
	}

	// This read-only check must hold the DBA source row lock until the
	// caller's transaction ends, not merely until this helper returns.
	// A revocation FK needs KEY SHARE on that row: NOWAIT must fail with
	// 55P03 while the preflight owns FOR UPDATE, then succeed after rollback.
	holder, err := admin.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelReadCommitted})
	if err != nil {
		t.Fatal(err)
	}
	defer holder.Rollback()
	if err := RequireRecordedDRAUnprepareReceipt(ctx, holder, verifier, candidate, signed, asOf); err != nil {
		t.Fatalf("transaction-bound journal membership: %v", err)
	}
	keyLockQuery := `SELECT lock_token FROM public.dra_unprepare_source_keys
		WHERE cluster_id=$1 AND source_id=$2 AND key_id=$3 FOR KEY SHARE NOWAIT`
	keyArgs := []any{signed.Event.ClusterID, signed.Event.SourceID, signed.Event.KeyID}
	contender, err := admin.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelReadCommitted})
	if err != nil {
		t.Fatal(err)
	}
	var lockToken int16
	err = contender.QueryRowContext(ctx, keyLockQuery, keyArgs...).Scan(&lockToken)
	_ = contender.Rollback()
	if postgresSQLState(err) != "55P03" {
		t.Fatalf("source-key lock escaped caller transaction: state=%s err=%v", postgresSQLState(err), err)
	}
	if err := holder.Rollback(); err != nil {
		t.Fatal(err)
	}
	after, err := admin.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelReadCommitted})
	if err != nil {
		t.Fatal(err)
	}
	defer after.Rollback()
	if err := after.QueryRowContext(ctx, keyLockQuery, keyArgs...).Scan(&lockToken); err != nil {
		t.Fatalf("source key remained locked after rollback: %v", err)
	}
	if err := after.Rollback(); err != nil {
		t.Fatal(err)
	}

	// A valid signature on a *different* event cannot borrow the existing
	// journal row, even if signer, Claim/Pod/device and key scope match.
	other := signed
	other.Event.EventID = kueueTestIdent(t, "other_")
	other.Signature = ed25519.Sign(private, draUnprepareSigningBytes(other.Event))
	if err := read(verifier, other); !errors.Is(err, ErrDRAReceiptNotRecorded) {
		t.Fatalf("signed unrecorded event borrowed journal membership: %v", err)
	}
	// A tampered signature cannot acquire membership by reusing event IDs.
	tampered := signed
	tampered.Signature = append([]byte(nil), signed.Signature...)
	tampered.Signature[0] ^= 0xff
	if err := read(verifier, tampered); !errors.Is(err, ErrUncorrelatedDRAUnprepare) {
		t.Fatalf("tampered signed event accepted: %v", err)
	}

	// Revocation is append-only. Historical journal membership must NOT
	// confer a fresh release capability once the DBA revokes the source.
	if _, err := admin.ExecContext(ctx, `INSERT INTO public.dra_unprepare_source_revocations
		(cluster_id,source_id,key_id,reason)
		VALUES($1,$2,$3,'revoked source for read-only regression')`,
		signed.Event.ClusterID, signed.Event.SourceID, signed.Event.KeyID); err != nil {
		t.Fatal(err)
	}
	if err := read(verifier, signed); !errors.Is(err, ErrDRAReceiptSourceUntrusted) {
		t.Fatalf("revoked signer accepted from pre-existing journal row: %v", err)
	}
}

func TestRecordedDRAReceiptRejectsUnrestrictedTestOnlyJournalIngress(t *testing.T) {
	admin := openProviderPostgresDB(t)
	ctx, cancel := context.WithTimeout(context.Background(), 25*time.Second)
	defer cancel()
	verifier, _, signed := journalReceiptFixture(t)
	asOf := time.Date(2026, 11, 10, 10, 1, 0, 0, time.UTC)
	candidate := releaseCandidateFixture()
	legacy, err := NewDRAUnprepareReceiptJournal(admin)
	if err != nil {
		t.Fatal(err)
	}
	if fresh, err := legacy.VerifyAndRecord(ctx, verifier, candidate, signed, asOf); err != nil || !fresh {
		t.Fatalf("could not insert test-only unsigned-source journal row: new=%t err=%v", fresh, err)
	}
	tx, err := admin.BeginTx(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback()
	if err := RequireRecordedDRAUnprepareReceipt(ctx, tx, verifier, candidate, signed, asOf); !errors.Is(err, ErrDRAReceiptSourceUntrusted) {
		t.Fatalf("test-only journal row bypassed missing DBA source registry: %v", err)
	}
}
