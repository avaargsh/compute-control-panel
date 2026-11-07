package postgres

import (
	"context"
	"strings"
	"testing"
	"time"
)

// A narrowly restricted E1 writer still has TEMP and INSERT on the evidence
// table. It must not manufacture a successful Provider Operation by creating
// pg_temp names used by the v0006 PL/pgSQL INSERT trigger. Unlike the repository
// preflight, this exercises the *database trigger itself* via a direct INSERT.
func TestE1EvidenceTriggerRejectsTemporaryProviderOperationShadow(t *testing.T) {
	admin := openIntegrationDB(t)
	writer := testEvidenceWriterDB(t, admin)
	writer.SetMaxOpenConns(1)
	writer.SetMaxIdleConns(1)
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	p := acceptanceFixture(t, "tenant-trigger-shadow-"+randomSuffix(t),
		"workload-"+randomSuffix(t), "operation-"+randomSuffix(t), "event-"+randomSuffix(t))
	a, created, err := acceptTestEvidenceWorkload(t, admin, ctx, p)
	if err != nil || !created {
		t.Fatalf("accept pending operation: created=%t err=%v", created, err)
	}
	// The real Provider Operation remains PENDING and Allocation remains
	// unbound. The incoming evidence is otherwise syntactically complete.
	derived, err := deriveUsageEvidence(typedUsageEvidence(a))
	if err != nil {
		t.Fatalf("derive synthetic test evidence: %v", err)
	}
	row := derived.record
	if _, err := writer.ExecContext(ctx, `
		CREATE TEMP TABLE compute_operations AS
		SELECT * FROM public.compute_operations
		WHERE tenant_id=$1 AND compute_operation_id=$2
	`, row.TenantID, row.ComputeOperationID); err != nil {
		t.Fatalf("create temporary Provider operation: %v", err)
	}
	if _, err := writer.ExecContext(ctx, `
		CREATE TEMP TABLE compute_allocations AS
		SELECT * FROM public.compute_allocations
		WHERE tenant_id=$1 AND allocation_id=$2
	`, row.TenantID, row.AllocationID); err != nil {
		t.Fatalf("create temporary Allocation: %v", err)
	}
	if _, err := writer.ExecContext(ctx,
		"UPDATE compute_operations SET status='SUCCEEDED', provider_resource_id=$1, provider_resource_uid=$2",
		row.ProviderResourceID, row.ProviderResourceUID); err != nil {
		t.Fatalf("forge temporary Provider success: %v", err)
	}
	if _, err := writer.ExecContext(ctx, "UPDATE compute_allocations SET phase='BOUND'"); err != nil {
		t.Fatalf("forge temporary Allocation binding: %v", err)
	}
	_, insertErr := writer.ExecContext(ctx, `
		INSERT INTO public.compute_allocation_usage_evidence_sets (
			evidence_set_id, schema_version, tenant_id, project_id, workload_id,
			allocation_id, allocation_generation, provider_id, compute_operation_id,
			provider_request_digest, provider_resource_id, provider_resource_uid,
			pool_id, profile_id, gpu_model, capability_profile_id,
			evidence_policy_version, metering_policy_version, evidence_digest,
			usage_interval_id, usage_start, usage_end, gpu_count, source_event_ids,
			evidence_payload, usage_interval_payload, usage_unit, attribution_method,
			quality, source_coverage
		) VALUES (
			$1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,
			$17,$18,$19,$20,$21,$22,$23,$24::jsonb,$25::jsonb,$26::jsonb,
			$27,$28,$29,$30
		)
	`, row.EvidenceSetID, row.SchemaVersion, row.TenantID, row.ProjectID,
		row.WorkloadID, row.AllocationID, row.AllocationGeneration,
		row.ProviderID, row.ComputeOperationID, row.ProviderRequestDigest,
		row.ProviderResourceID, row.ProviderResourceUID, row.PoolID, row.ProfileID,
		row.GPUModel, row.CapabilityProfileID, row.EvidencePolicyVersion,
		row.MeteringPolicyVersion, row.EvidenceDigest, row.UsageIntervalID,
		row.UsageStart, row.UsageEnd, row.GPUCount, derived.sourceEventJSON,
		derived.evidenceJSON, derived.usageJSON, usageIntervalUnit,
		usageAttributionMethod, usageEvidenceQuality, 1)
	var count int
	if err := admin.QueryRowContext(ctx, `
		SELECT count(*) FROM public.compute_allocation_usage_evidence_sets
		WHERE tenant_id=$1 AND evidence_set_id=$2
	`, row.TenantID, row.EvidenceSetID).Scan(&count); err != nil {
		t.Fatal(err)
	}
	if insertErr == nil || !strings.Contains(insertErr.Error(),
		"usage evidence does not match a successful Provider operation") || count != 0 {
		t.Fatalf("temporary Provider/Allocation shadow bypassed E1 trigger: err=%v permanent_FINAL=%d", insertErr, count)
	}
}
