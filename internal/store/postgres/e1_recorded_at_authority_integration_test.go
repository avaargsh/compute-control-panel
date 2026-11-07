package postgres

import (
	"context"
	"testing"
	"time"
)

// A direct SQL writer can supply recorded_at explicitly despite the column's
// DEFAULT. That column is intentionally excluded from exact-row authorization
// as a *server-owned* timestamp; prove the database, not the caller, owns it.
func TestE1FinalRecordedAtCannotBeForgedByDirectSQL(t *testing.T) {
	admin := openIntegrationDB(t)
	writer := testEvidenceWriterDB(t, admin)
	ctx, cancel := context.WithTimeout(context.Background(), 25*time.Second)
	defer cancel()

	p := acceptanceFixture(t, "tenant-e1-stamp-"+randomSuffix(t),
		"workload-"+randomSuffix(t), "operation-"+randomSuffix(t),
		"event-"+randomSuffix(t))
	a, created, err := acceptTestEvidenceWorkload(t, admin, ctx, p)
	if err != nil || !created {
		t.Fatalf("accept Workload: created=%t err=%v", created, err)
	}
	markUsageEvidenceProviderOperationSucceeded(t, admin, a)
	derived, err := deriveUsageEvidence(typedUsageEvidence(a))
	if err != nil {
		t.Fatal(err)
	}
	row := derived.record

	now := time.Now().UTC().Truncate(time.Microsecond)
	policy := TrustedEvidencePolicy{
		TenantID: row.TenantID, ProviderID: row.ProviderID,
		Policy: fixtureEvidencePolicy(), EffectiveFrom: now.Add(-time.Hour),
		EffectiveUntil: now.Add(time.Hour), Revision: 1,
	}
	digest, err := evidencePolicyFenceDigest(policy)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = admin.ExecContext(ctx, `
		INSERT INTO public.compute_evidence_policy_fences
		  (tenant_id,provider_id,policy_version,policy_digest,
		   effective_from,effective_until,revision,revoked)
		VALUES($1,$2,$3,$4,$5,$6,1,false)
	`, policy.TenantID, policy.ProviderID, policy.Policy.Version, digest,
		policy.EffectiveFrom, policy.EffectiveUntil); err != nil {
		t.Fatal(err)
	}

	// A credentialed writer bypasses the Go repository and supplies a false
	// recording time. A legitimate immutable per-evidence approval exists:
	// the database must overwrite caller input, not deny authorized evidence.
	forged := time.Date(2000, 1, 1, 0, 0, 0, 0, time.UTC)
	lowerBound := time.Now().UTC().Add(-time.Minute)
	var persisted time.Time
	err = writer.QueryRowContext(ctx, `
		INSERT INTO public.compute_allocation_usage_evidence_sets (
			evidence_set_id,schema_version,tenant_id,project_id,workload_id,
			allocation_id,allocation_generation,provider_id,compute_operation_id,
			provider_request_digest,provider_resource_id,provider_resource_uid,
			pool_id,profile_id,gpu_model,capability_profile_id,
			evidence_policy_version,metering_policy_version,evidence_digest,
			usage_interval_id,usage_start,usage_end,gpu_count,source_event_ids,
			evidence_payload,usage_interval_payload,usage_unit,attribution_method,
			quality,source_coverage,recorded_at
		) VALUES (
			$1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,
			$17,$18,$19,$20,$21,$22,$23,$24::jsonb,$25::jsonb,$26::jsonb,
			$27,$28,$29,$30,$31
		) RETURNING recorded_at
	`, row.EvidenceSetID, row.SchemaVersion, row.TenantID, row.ProjectID,
		row.WorkloadID, row.AllocationID, row.AllocationGeneration,
		row.ProviderID, row.ComputeOperationID, row.ProviderRequestDigest,
		row.ProviderResourceID, row.ProviderResourceUID, row.PoolID,
		row.ProfileID, row.GPUModel, row.CapabilityProfileID,
		row.EvidencePolicyVersion, row.MeteringPolicyVersion,
		row.EvidenceDigest, row.UsageIntervalID, row.UsageStart, row.UsageEnd,
		row.GPUCount, derived.sourceEventJSON, derived.evidenceJSON,
		derived.usageJSON, usageIntervalUnit, usageAttributionMethod,
		usageEvidenceQuality, 1, forged).Scan(&persisted)
	if err != nil {
		t.Fatalf("authorized direct E1 FINAL unexpectedly rejected: %v", err)
	}
	if persisted.Equal(forged) || persisted.Before(lowerBound) ||
		persisted.After(time.Now().UTC().Add(time.Minute)) {
		t.Fatalf("caller controlled supposedly server-owned E1 recorded_at: input=%s persisted=%s",
			forged, persisted)
	}
}
