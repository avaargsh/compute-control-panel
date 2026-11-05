package postgres

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/avaargsh/compute-control-panel/internal/economics"
)

// A policy read made before an E1 derivation is insufficient: an admin can
// revoke it before the immutable FINAL record commits. The same PostgreSQL
// transaction must pin the matching administrator-provisioned policy row.
func TestEvidencePolicyCommitFenceRejectsStaleOrRevokedSnapshot(t *testing.T) {
	db := openIntegrationDB(t)
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	policy := economics.EvidencePolicy{
		Version: "v1", AsOf: time.Date(2026, 11, 4, 12, 10, 0, 0, time.UTC),
		MaximumClockSkew:               time.Minute,
		TrustedSourceTimestampAdapters: map[string]bool{"provider@v1": true},
		CertifiedDeviceAdapters:        map[string]bool{},
	}
	now := time.Now().UTC().Truncate(time.Microsecond)
	approved := TrustedEvidencePolicy{
		TenantID: "tenant-fence-" + randomSuffix(t), ProviderID: "provider-fence",
		Policy: policy, EffectiveFrom: now.Add(-time.Hour),
		EffectiveUntil: now.Add(time.Hour),
		Revision:       1,
	}
	digest, err := evidencePolicyFenceDigest(approved)
	if err != nil {
		t.Fatal(err)
	}
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	if err := lockTrustedEvidencePolicy(ctx, tx, approved); !errors.Is(err, ErrEvidencePolicyUntrusted) {
		t.Fatalf("no administrator row yet allowed policy: %v", err)
	}
	_ = tx.Rollback()
	_, err = db.ExecContext(ctx, `
		INSERT INTO compute_evidence_policy_fences (
			tenant_id, provider_id, policy_version, policy_digest,
			effective_from, effective_until, revision, revoked
		) VALUES ($1,$2,$3,$4,$5,$6,1,false)
	`, approved.TenantID, approved.ProviderID, policy.Version, digest, approved.EffectiveFrom, approved.EffectiveUntil)
	if err != nil {
		t.Fatal(err)
	}
	tx, err = db.BeginTx(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	if err := lockTrustedEvidencePolicy(ctx, tx, approved); err != nil {
		t.Fatalf("admin-provisioned policy denied: %v", err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}
	mismatch := approved
	mismatch.Policy.TrustedSourceTimestampAdapters = map[string]bool{"provider@v1": true, "untrusted@v1": true}
	tx, err = db.BeginTx(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	if err := lockTrustedEvidencePolicy(ctx, tx, mismatch); !errors.Is(err, ErrEvidencePolicyUntrusted) {
		t.Fatalf("altered policy accepted: %v", err)
	}
	_ = tx.Rollback()

	_, err = db.ExecContext(ctx, `
		UPDATE compute_evidence_policy_fences
		SET revoked=true, revision=revision+1
		WHERE tenant_id=$1 AND provider_id=$2 AND policy_version=$3
	`, approved.TenantID, approved.ProviderID, policy.Version)
	if err != nil {
		t.Fatal(err)
	}
	tx, err = db.BeginTx(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	if err := lockTrustedEvidencePolicy(ctx, tx, approved); !errors.Is(err, ErrEvidencePolicyUntrusted) {
		t.Fatalf("revoked policy accepted: %v", err)
	}
	_ = tx.Rollback()

	// Privileged administrators must not be able to silently reuse a
	// revoked revision or erase the policy history, even by mistake.
	for _, probe := range []struct {
		name, sql string
	}{
		{"unrevoke without revision bump", "UPDATE compute_evidence_policy_fences SET revoked=false WHERE tenant_id=$1 AND provider_id=$2 AND policy_version=$3"},
		{"revision regression", "UPDATE compute_evidence_policy_fences SET revision=revision-1 WHERE tenant_id=$1 AND provider_id=$2 AND policy_version=$3"},
		{"delete approved policy", "DELETE FROM compute_evidence_policy_fences WHERE tenant_id=$1 AND provider_id=$2 AND policy_version=$3"},
	} {
		t.Run(probe.name, func(t *testing.T) {
			checkTx, err := db.BeginTx(ctx, nil)
			if err != nil {
				t.Fatal(err)
			}
			_, err = checkTx.ExecContext(ctx, probe.sql,
				approved.TenantID, approved.ProviderID, policy.Version)
			_ = checkTx.Rollback()
			if err == nil {
				t.Fatalf("administrator mutation bypassed policy revision guard: %s", probe.name)
			}
		})
	}
	checkTx, err := db.BeginTx(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	_, err = checkTx.ExecContext(ctx, "TRUNCATE TABLE compute_evidence_policy_fences")
	_ = checkTx.Rollback()
	if err == nil {
		t.Fatal("policy fence registry accepted destructive TRUNCATE")
	}
}

func TestEvidencePolicyCommitFenceSerializesConcurrentRevocation(t *testing.T) {
	db := openIntegrationDB(t)
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	approved := TrustedEvidencePolicy{
		TenantID: "tenant-fence-lock-" + randomSuffix(t), ProviderID: "provider-fence",
		Policy: economics.EvidencePolicy{
			Version: "v1", AsOf: time.Date(2026, 11, 4, 12, 10, 0, 0, time.UTC),
			TrustedSourceTimestampAdapters: map[string]bool{"provider@v1": true},
		},
		EffectiveFrom:  time.Now().UTC().Truncate(time.Microsecond).Add(-time.Hour),
		EffectiveUntil: time.Now().UTC().Truncate(time.Microsecond).Add(time.Hour),
		Revision:       1,
	}
	digest, err := evidencePolicyFenceDigest(approved)
	if err != nil {
		t.Fatal(err)
	}
	_, err = db.ExecContext(ctx, `
		INSERT INTO compute_evidence_policy_fences
		(tenant_id,provider_id,policy_version,policy_digest,effective_from,effective_until,revision,revoked)
		VALUES($1,$2,$3,$4,$5,$6,1,false)
	`, approved.TenantID, approved.ProviderID, approved.Policy.Version, digest, approved.EffectiveFrom, approved.EffectiveUntil)
	if err != nil {
		t.Fatal(err)
	}
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback()
	if err := lockTrustedEvidencePolicy(ctx, tx, approved); err != nil {
		t.Fatal(err)
	}

	// The admin must not be able to revoke the policy while an E1 writer
	// holds its transaction-level read lock. PostgreSQL lock_timeout avoids
	// a timing-dependent blocked goroutine in this deterministic test.
	adminTx, err := db.BeginTx(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := adminTx.ExecContext(ctx, "SET LOCAL lock_timeout='150ms'"); err != nil {
		t.Fatal(err)
	}
	_, err = adminTx.ExecContext(ctx, `
		UPDATE compute_evidence_policy_fences SET revoked=true,revision=revision+1
		WHERE tenant_id=$1 AND provider_id=$2 AND policy_version=$3
	`, approved.TenantID, approved.ProviderID, approved.Policy.Version)
	if err == nil || !strings.Contains(err.Error(), "lock timeout") {
		t.Fatalf("revocation bypassed in-flight transaction policy fence: %v", err)
	}
	_ = adminTx.Rollback()
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}
	// Once the writer commits, the admin's revoke can proceed; all later
	// writes must fail closed against the revoked version.
	_, err = db.ExecContext(ctx, `
		UPDATE compute_evidence_policy_fences SET revoked=true,revision=revision+1
		WHERE tenant_id=$1 AND provider_id=$2 AND policy_version=$3
	`, approved.TenantID, approved.ProviderID, approved.Policy.Version)
	if err != nil {
		t.Fatalf("admin revocation after writer commit: %v", err)
	}
	next, err := db.BeginTx(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer next.Rollback()
	if err := lockTrustedEvidencePolicy(ctx, next, approved); !errors.Is(err, ErrEvidencePolicyUntrusted) {
		t.Fatalf("post-revocation write admitted: %v", err)
	}
}

func TestPersistUsageEvidenceSetRejectsUnprovisionedRevokedAndStalePolicyRevision(t *testing.T) {
	db := openIntegrationDB(t)
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	baseRepo, err := NewRepository(db)
	if err != nil {
		t.Fatal(err)
	}
	createParams := acceptanceFixture(t, "tenant-e1-fence-"+randomSuffix(t),
		"workload-key-"+randomSuffix(t), "operation-key-"+randomSuffix(t), "operation-event-"+randomSuffix(t))
	a, created, err := baseRepo.AcceptWorkload(ctx, createParams)
	if err != nil || !created {
		t.Fatalf("accept Workload: created=%t err=%v", created, err)
	}
	markUsageEvidenceProviderOperationSucceeded(t, db, a)
	params := typedUsageEvidence(a)
	approved := TrustedEvidencePolicy{
		TenantID:       params.Evidence.TenantID,
		ProviderID:     params.Evidence.ProviderID,
		Policy:         fixtureEvidencePolicy(),
		EffectiveFrom:  time.Date(2026, 11, 1, 0, 0, 0, 0, time.UTC),
		EffectiveUntil: time.Date(2027, 1, 1, 0, 0, 0, 0, time.UTC),
		Revision:       1,
	}
	repo, err := NewRepositoryWithEvidencePolicyAuthority(db, staticEvidencePolicyAuthority{policy: approved})
	if err != nil {
		t.Fatal(err)
	}
	checkDenied := func(label string) {
		t.Helper()
		if _, persisted, err := repo.PersistUsageEvidenceSet(ctx, params); persisted ||
			!errors.Is(err, ErrEvidencePolicyUntrusted) {
			t.Fatalf("%s policy fence accepted FINAL: persisted=%t err=%v", label, persisted, err)
		}
		var count int
		if err := db.QueryRowContext(ctx, `
			SELECT count(*) FROM compute_allocation_usage_evidence_sets
			WHERE tenant_id=$1 AND allocation_id=$2
		`, params.Evidence.TenantID, params.Evidence.AllocationID).Scan(&count); err != nil {
			t.Fatal(err)
		}
		if count != 0 {
			t.Fatalf("%s policy fence inserted %d E1 FINAL records", label, count)
		}
	}
	checkDenied("unprovisioned")
	digest, err := evidencePolicyFenceDigest(approved)
	if err != nil {
		t.Fatal(err)
	}
	_, err = db.ExecContext(ctx, `
		INSERT INTO compute_evidence_policy_fences
		(tenant_id,provider_id,policy_version,policy_digest,effective_from,effective_until,revision,revoked)
		VALUES($1,$2,$3,$4,$5,$6,1,true)
	`, approved.TenantID, approved.ProviderID, approved.Policy.Version, digest, approved.EffectiveFrom, approved.EffectiveUntil)
	if err != nil {
		t.Fatal(err)
	}
	checkDenied("revoked")
	_, err = db.ExecContext(ctx, `
		UPDATE compute_evidence_policy_fences SET revoked=false, revision=2
		WHERE tenant_id=$1 AND provider_id=$2 AND policy_version=$3
	`, approved.TenantID, approved.ProviderID, approved.Policy.Version)
	if err != nil {
		t.Fatal(err)
	}
	checkDenied("stale revision")
	approved.Revision = 2
	updatedRepo, err := NewRepositoryWithEvidencePolicyAuthority(db, staticEvidencePolicyAuthority{policy: approved})
	if err != nil {
		t.Fatal(err)
	}
	if _, persisted, err := updatedRepo.PersistUsageEvidenceSet(ctx, params); err != nil || !persisted {
		t.Fatalf("newly approved E1 revision should commit: persisted=%t err=%v", persisted, err)
	}
}
