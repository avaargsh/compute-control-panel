package provider

import (
	"bytes"
	"context"
	"crypto/ed25519"
	"database/sql"
	"errors"
	"testing"
	"time"
)

// Live receipt fixture is restricted-ingest only. The unchecked journal and
// pure signature/unit tests deliberately retain stable historical timestamps.
func liveJournalReceiptFixture(
	t *testing.T,
) (*DRAUnprepareVerifier, SignedDRAUnprepare, time.Time) {
	t.Helper()
	verifier, private, signed := journalReceiptFixture(t)
	asOf := time.Now().UTC()
	signed.Event.CompletedAt = asOf.Add(-30 * time.Second)
	signed.Signature = ed25519.Sign(private, draUnprepareSigningBytes(signed.Event))
	return verifier, signed, asOf
}

// seedTrustedDRAReceiptSource is disposable-test DBA provisioning only.
// Production source keys require independent identity and driver verification.
func seedTrustedDRAReceiptSource(
	t *testing.T, admin *sql.DB, verifier *DRAUnprepareVerifier,
	receipt SignedDRAUnprepare, candidate ReleaseEvidenceCandidate,
) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	_, err := admin.ExecContext(ctx, `INSERT INTO public.dra_unprepare_source_keys
		(cluster_id, source_id, key_id, tenant_id, provider_id, driver, public_key,
		 valid_from, valid_until, maximum_age_seconds, future_skew_seconds)
		VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,600,30)`,
		receipt.Event.ClusterID, receipt.Event.SourceID, receipt.Event.KeyID,
		candidate.Delete.TenantID, candidate.Delete.ProviderID,
		receipt.Event.Driver, verifier.authority.PublicKey,
		receipt.Event.CompletedAt.Add(-time.Hour),
		receipt.Event.CompletedAt.Add(time.Hour))
	if err != nil {
		t.Fatalf("seed disposable independent DRA signer: %v", err)
	}
}

func TestRestrictedDRAJournalRefusesUnregisteredKeyAndRevokedReplay(t *testing.T) {
	admin := openProviderPostgresDB(t)
	if !postgresCurrentUserIsSuperuser(t, admin) {
		t.Skip("disposable PostgreSQL superuser required to seed independently authorized key")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	dsn := kueueBindingDSN(t, draReceiptIngestRole, draReceiptIngestCITestPassword, "")
	journal, db, err := OpenRestrictedDRAUnprepareReceiptJournal(ctx, dsn)
	if err != nil {
		t.Fatalf("restricted journal: %v", err)
	}
	defer db.Close()
	verifier, receipt, asOf := liveJournalReceiptFixture(t)
	candidate := releaseCandidateFixture()

	newEvent, err := journal.VerifyAndRecord(ctx, verifier, candidate, receipt, asOf)
	if !errors.Is(err, ErrDRAReceiptSourceUntrusted) || newEvent {
		t.Fatalf("caller-supplied signing key accepted without DBA authority: new=%t err=%v", newEvent, err)
	}
	var before int
	if err := admin.QueryRowContext(ctx, `SELECT count(*) FROM public.dra_unprepare_receipt_journal
		WHERE cluster_id=$1`, receipt.Event.ClusterID).Scan(&before); err != nil || before != 0 {
		t.Fatalf("untrusted receipt inserted: count=%d err=%v", before, err)
	}

	// Independent DBA provisions a public key, binding it to the exact
	// Tenant/Provider/driver and a narrowly scoped validity window.
	_, err = admin.ExecContext(ctx, `INSERT INTO public.dra_unprepare_source_keys
		(cluster_id, source_id, key_id, tenant_id, provider_id, driver, public_key,
		 valid_from, valid_until, maximum_age_seconds, future_skew_seconds)
		VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,600,30)`,
		receipt.Event.ClusterID, receipt.Event.SourceID, receipt.Event.KeyID,
		candidate.Delete.TenantID, candidate.Delete.ProviderID, receipt.Event.Driver,
		verifier.authority.PublicKey,
		receipt.Event.CompletedAt.Add(-time.Hour),
		receipt.Event.CompletedAt.Add(time.Hour))
	if err != nil {
		t.Fatalf("DBA source key provisioning: %v", err)
	}
	newEvent, err = journal.VerifyAndRecord(ctx, verifier, candidate, receipt, asOf)
	if err != nil || !newEvent {
		t.Fatalf("trusted source not accepted as evidence-only: new=%t err=%v", newEvent, err)
	}
	replayed, err := journal.VerifyAndRecord(ctx, verifier, candidate, receipt, asOf)
	if err != nil || replayed {
		t.Fatalf("trusted replay not idempotent: new=%t err=%v", replayed, err)
	}

	// An attacker can produce another fully valid Ed25519 signature, but
	// changing the pinned key cannot acquire the source identity.
	forgedPrivate := ed25519.NewKeyFromSeed(bytes.Repeat([]byte{11}, ed25519.SeedSize))
	forgedAuthority := verifier.authority
	forgedAuthority.PublicKey = forgedPrivate.Public().(ed25519.PublicKey)
	forgedVerifier, err := NewDRAUnprepareVerifier(forgedAuthority, 10*time.Minute, 30*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	forged := receipt
	forged.Signature = ed25519.Sign(forgedPrivate, draUnprepareSigningBytes(forged.Event))
	newEvent, err = journal.VerifyAndRecord(ctx, forgedVerifier, candidate, forged, asOf)
	if !errors.Is(err, ErrDRAReceiptSourceUntrusted) || newEvent {
		t.Fatalf("caller-selected alternate Ed25519 signer bypassed trusted source: new=%t err=%v", newEvent, err)
	}

	// The journal login can read registry material, but cannot create,
	// revoke or alter trust. All such writes require a separate DBA principal.
	if _, err = db.ExecContext(ctx, `UPDATE public.dra_unprepare_source_keys
		SET driver='forged' WHERE cluster_id=$1`, receipt.Event.ClusterID); postgresSQLState(err) != "42501" {
		t.Fatalf("ingest login mutated source key: state=%s err=%v", postgresSQLState(err), err)
	}
	// PG16 FOR UPDATE requires permission on a column, but it must never
	// permit a real UPDATE even to that deliberately inert lock marker.
	if _, err = db.ExecContext(ctx, `UPDATE public.dra_unprepare_source_keys
		SET lock_token=0 WHERE cluster_id=$1`, receipt.Event.ClusterID); postgresSQLState(err) != "55000" {
		t.Fatalf("lock-only column could be rewritten: state=%s err=%v", postgresSQLState(err), err)
	}
	if _, err = db.ExecContext(ctx, `INSERT INTO public.dra_unprepare_source_revocations
		(cluster_id, source_id, key_id, reason) VALUES ($1,$2,$3,'forged')`,
		receipt.Event.ClusterID, receipt.Event.SourceID, receipt.Event.KeyID); postgresSQLState(err) != "42501" {
		t.Fatalf("ingest login revoked source: state=%s err=%v", postgresSQLState(err), err)
	}
	_, err = admin.ExecContext(ctx, `INSERT INTO public.dra_unprepare_source_revocations
		(cluster_id, source_id, key_id, reason)
		VALUES ($1,$2,$3,'DBA revoked test source')`,
		receipt.Event.ClusterID, receipt.Event.SourceID, receipt.Event.KeyID)
	if err != nil {
		t.Fatalf("DBA revocation: %v", err)
	}
	replayed, err = journal.VerifyAndRecord(ctx, verifier, candidate, receipt, asOf)
	if !errors.Is(err, ErrDRAReceiptSourceUntrusted) || replayed {
		t.Fatalf("revoked key still accepted previously journaled event: new=%t err=%v", replayed, err)
	}
}

func TestRestrictedDRAJournalRequiresExactSourceScopeAndFreshness(t *testing.T) {
	admin := openProviderPostgresDB(t)
	if !postgresCurrentUserIsSuperuser(t, admin) {
		t.Skip("requires disposable PostgreSQL superuser")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	journal, db, err := OpenRestrictedDRAUnprepareReceiptJournal(ctx,
		kueueBindingDSN(t, draReceiptIngestRole, draReceiptIngestCITestPassword, ""))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	candidate := releaseCandidateFixture()
	asOf := time.Now().UTC()
	for _, tc := range []struct {
		name       string
		tenant     string
		provider   string
		validFrom  time.Time
		validUntil time.Time
		maxAge     int
		futureSkew int
	}{
		{"cross tenant", "tenant-wrong", candidate.Delete.ProviderID, asOf.Add(-time.Hour), asOf.Add(time.Hour), 600, 30},
		{"cross provider", candidate.Delete.TenantID, "provider-wrong", asOf.Add(-time.Hour), asOf.Add(time.Hour), 600, 30},
		{"expired key", candidate.Delete.TenantID, candidate.Delete.ProviderID, asOf.Add(-time.Hour), asOf.Add(-time.Second), 600, 30},
		{"unreasonably relaxed caller age", candidate.Delete.TenantID, candidate.Delete.ProviderID, asOf.Add(-time.Hour), asOf.Add(time.Hour), 120, 30},
		{"unreasonably relaxed future skew", candidate.Delete.TenantID, candidate.Delete.ProviderID, asOf.Add(-time.Hour), asOf.Add(time.Hour), 600, 1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			verifier, receipt, _ := liveJournalReceiptFixture(t)
			_, err := admin.ExecContext(ctx, `INSERT INTO public.dra_unprepare_source_keys
				(cluster_id, source_id, key_id, tenant_id, provider_id, driver, public_key,
				valid_from, valid_until, maximum_age_seconds, future_skew_seconds)
				VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11)`,
				receipt.Event.ClusterID, receipt.Event.SourceID, receipt.Event.KeyID,
				tc.tenant, tc.provider, receipt.Event.Driver, verifier.authority.PublicKey,
				tc.validFrom, tc.validUntil, tc.maxAge, tc.futureSkew)
			if err != nil {
				t.Fatal(err)
			}
			newEvent, err := journal.VerifyAndRecord(ctx, verifier, candidate, receipt, asOf)
			if !errors.Is(err, ErrDRAReceiptSourceUntrusted) || newEvent {
				t.Fatalf("untrusted source scope accepted: new=%t err=%v", newEvent, err)
			}
		})
	}
}
