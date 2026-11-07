package postgres

import (
	"context"
	"errors"
	"testing"
	"time"
)

// A restricted E1 login may create pg_temp relations even without CREATE on
// public. Its transaction must never resolve evidence INSERT against a temp
// shadow instead of the durable, immutable public table.
func TestE1WriterTemporaryEvidenceShadowCannotAcknowledgePhantomFinal(t *testing.T) {
	admin := openIntegrationDB(t)
	repo := newTestEvidenceRepo(t, admin)
	repo.db.SetMaxOpenConns(1)
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	p := acceptanceFixture(t, "tenant-temp-evidence-"+randomSuffix(t),
		"workload-key-"+randomSuffix(t), "operation-key-"+randomSuffix(t), "operation-event-"+randomSuffix(t))
	a, created, err := acceptTestEvidenceWorkload(t, admin, ctx, p)
	if err != nil || !created {
		t.Fatalf("AcceptWorkload: created=%t err=%v", created, err)
	}
	markUsageEvidenceProviderOperationSucceeded(t, admin, a)

	if _, err := repo.db.ExecContext(ctx, `
		CREATE TEMP TABLE compute_allocation_usage_evidence_sets
		(LIKE public.compute_allocation_usage_evidence_sets INCLUDING ALL)
		ON COMMIT PRESERVE ROWS
	`); err != nil {
		t.Fatalf("create E1 writer pg_temp evidence shadow: %v", err)
	}
	params := typedUsageEvidence(a)
	_, persisted, err := repo.PersistUsageEvidenceSet(ctx, params)
	if err != nil || !persisted {
		t.Fatalf("real FINAL must persist despite an unrelated temp shadow: persisted=%t err=%v", persisted, err)
	}

	var durableCount int
	if err := admin.QueryRowContext(ctx, `
		SELECT count(*) FROM public.compute_allocation_usage_evidence_sets
		WHERE tenant_id=$1 AND evidence_set_id=$2
	`, params.Evidence.TenantID, params.Evidence.EvidenceSetID).Scan(&durableCount); err != nil {
		t.Fatal(err)
	}
	if durableCount != 1 {
		t.Fatalf("E1 writer acknowledged FINAL without durable public evidence row: count=%d", durableCount)
	}
}

// A temp shadow of only the mutable Provider operation and Allocation tables
// must not manufacture SUCCEEDED/BOUND facts while the authoritative rows are
// still PENDING. The admin policy fence and E1 destination remain real tables.
func TestE1WriterTemporaryOperationShadowCannotForgeProviderSuccess(t *testing.T) {
	admin := openIntegrationDB(t)
	repo := newTestEvidenceRepo(t, admin)
	repo.db.SetMaxOpenConns(1)
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	p := acceptanceFixture(t, "tenant-temp-operation-"+randomSuffix(t),
		"workload-key-"+randomSuffix(t), "operation-key-"+randomSuffix(t), "operation-event-"+randomSuffix(t))
	a, created, err := acceptTestEvidenceWorkload(t, admin, ctx, p)
	if err != nil || !created {
		t.Fatalf("AcceptWorkload: created=%t err=%v", created, err)
	}
	if _, err := repo.db.ExecContext(ctx, `
		CREATE TEMP TABLE compute_operations AS
		SELECT * FROM public.compute_operations WHERE tenant_id=$1 AND compute_operation_id=$2
	`, a.Workload.TenantID, a.Operation.ID); err != nil {
		t.Fatalf("create pg_temp operation shadow: %v", err)
	}
	if _, err := repo.db.ExecContext(ctx, `
		CREATE TEMP TABLE compute_allocations AS
		SELECT * FROM public.compute_allocations WHERE tenant_id=$1 AND allocation_id=$2
	`, a.Workload.TenantID, a.Allocation.AllocationID); err != nil {
		t.Fatalf("create pg_temp Allocation shadow: %v", err)
	}
	if _, err := repo.db.ExecContext(ctx, `
		UPDATE compute_operations SET status='SUCCEEDED',
			provider_resource_id=$1, provider_resource_uid=$2
	`, "provider-resource-"+a.Allocation.AllocationID, "provider-uid-"+a.Allocation.AllocationID); err != nil {
		t.Fatalf("forge temporary Provider status: %v", err)
	}
	if _, err := repo.db.ExecContext(ctx, "UPDATE compute_allocations SET phase='BOUND'"); err != nil {
		t.Fatalf("forge temporary Allocation binding: %v", err)
	}

	params := typedUsageEvidence(a)
	_, persisted, err := repo.PersistUsageEvidenceSet(ctx, params)
	if !errors.Is(err, ErrUsageEvidenceNotReady) || persisted {
		t.Fatalf("temporary Provider/Allocation status forged FINAL: persisted=%t err=%v", persisted, err)
	}
	var durableCount int
	if err := admin.QueryRowContext(ctx, `
		SELECT count(*) FROM public.compute_allocation_usage_evidence_sets
		WHERE tenant_id=$1 AND evidence_set_id=$2
	`, params.Evidence.TenantID, params.Evidence.EvidenceSetID).Scan(&durableCount); err != nil {
		t.Fatal(err)
	}
	if durableCount != 0 {
		t.Fatalf("forged temporary Provider evidence created %d authoritative FINAL rows", durableCount)
	}
}
