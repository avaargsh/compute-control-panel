package postgres

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"
)

// An E1 write must not commit a FINAL row if the administrator-approved
// policy expires while the writer is waiting to lock the Provider operation.
func TestPersistUsageEvidenceSetRejectsPolicyExpiryDuringProviderLock(t *testing.T) {
	admin := openIntegrationDB(t)
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	tenant := "tenant-expiry-lock-" + randomSuffix(t)
	p := acceptanceFixture(t, tenant,
		"workload-key-"+randomSuffix(t), "operation-key-"+randomSuffix(t),
		"operation-event-"+randomSuffix(t))
	a, created, err := acceptTestEvidenceWorkload(t, admin, ctx, p)
	if err != nil || !created {
		t.Fatalf("AcceptWorkload: created=%t err=%v", created, err)
	}
	markUsageEvidenceProviderOperationSucceeded(t, admin, a)

	policy := fixtureEvidencePolicy()
	now := time.Now().UTC().Truncate(time.Microsecond)
	approved := TrustedEvidencePolicy{
		TenantID:       tenant,
		ProviderID:     a.Allocation.ProviderID,
		Policy:         policy,
		EffectiveFrom:  now.Add(-time.Hour),
		EffectiveUntil: now.Add(4 * time.Second),
		Revision:       1,
	}
	digest, err := evidencePolicyFenceDigest(approved)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := admin.ExecContext(ctx, `
		INSERT INTO public.compute_evidence_policy_fences
		(tenant_id,provider_id,policy_version,policy_digest,effective_from,effective_until,revision,revoked)
		VALUES ($1,$2,$3,$4,$5,$6,1,false)
	`, approved.TenantID, approved.ProviderID, policy.Version, digest,
		approved.EffectiveFrom, approved.EffectiveUntil); err != nil {
		t.Fatal(err)
	}
	writer := testEvidenceWriterDB(t, admin)
	repo, err := NewRepositoryWithEvidencePolicyAuthority(writer,
		EvidencePolicyAuthorityFunc(func(context.Context, string, string, string) (TrustedEvidencePolicy, error) {
			return approved, nil
		}))
	if err != nil {
		t.Fatal(err)
	}

	// This ordinary UPDATE obtains a row lock that conflicts with the
	// Provider verification's SELECT ... FOR SHARE.
	blocker, err := admin.BeginTx(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer blocker.Rollback()
	if _, err := blocker.ExecContext(ctx, `
		UPDATE public.compute_operations SET updated_at=clock_timestamp()
		WHERE tenant_id=$1 AND compute_operation_id=$2
	`, tenant, a.Operation.ID); err != nil {
		t.Fatal(err)
	}

	type outcome struct {
		persisted bool
		err       error
	}
	finished := make(chan outcome, 1)
	go func() {
		_, persisted, e := repo.PersistUsageEvidenceSet(ctx, typedUsageEvidence(a))
		finished <- outcome{persisted, e}
	}()

	// Establish that the writer obtained the policy FOR SHARE lock while
	// the policy was still valid, before moving past expiry. This makes
	// the negative control deterministic (rather than relying on timing).
	observedPolicyLock := false
	for time.Now().Before(approved.EffectiveUntil) {
		probe, e := admin.BeginTx(ctx, nil)
		if e != nil {
			t.Fatal(e)
		}
		var one int
		e = probe.QueryRowContext(ctx, `
			SELECT 1 FROM public.compute_evidence_policy_fences
			WHERE tenant_id=$1 AND provider_id=$2 AND policy_version=$3
			FOR UPDATE NOWAIT
		`, approved.TenantID, approved.ProviderID, policy.Version).Scan(&one)
		_ = probe.Rollback()
		if e != nil && strings.Contains(e.Error(), "could not obtain lock") {
			observedPolicyLock = true
			break
		}
		if e != nil {
			t.Fatalf("policy lock probe: %v", e)
		}
		select {
		case out := <-finished:
			t.Fatalf("E1 finished before Provider blocker released: %+v", out)
		case <-time.After(20 * time.Millisecond):
		}
	}
	if !observedPolicyLock {
		t.Fatal("E1 did not acquire valid policy lock before expiration")
	}

	// Keep the Provider lock held until PostgreSQL's own clock has passed
	// the policy end, then allow E1 to resume. Do not mutate the policy row.
	for {
		var dbNow time.Time
		if err := admin.QueryRowContext(ctx, "SELECT clock_timestamp()").Scan(&dbNow); err != nil {
			t.Fatal(err)
		}
		if !dbNow.Before(approved.EffectiveUntil.Add(50 * time.Millisecond)) {
			break
		}
		select {
		case <-ctx.Done():
			t.Fatal(ctx.Err())
		case <-time.After(30 * time.Millisecond):
		}
	}
	if err := blocker.Commit(); err != nil {
		t.Fatal(err)
	}

	select {
	case out := <-finished:
		if out.persisted || !errors.Is(out.err, ErrEvidencePolicyUntrusted) {
			t.Fatalf("E1 accepted expired policy after Provider lock wait: persisted=%t err=%v", out.persisted, out.err)
		}
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}

	var durable int
	if err := admin.QueryRowContext(ctx, `
		SELECT count(*) FROM public.compute_allocation_usage_evidence_sets
		WHERE tenant_id=$1 AND allocation_id=$2
	`, tenant, a.Allocation.AllocationID).Scan(&durable); err != nil {
		t.Fatal(err)
	}
	if durable != 0 {
		t.Fatalf("expired E1 FINAL persisted %d durable rows", durable)
	}
}
