package postgres

import (
	"context"
	"database/sql"
	"testing"
)

// The migration-owner test fixture impersonates a SEPARATE evidence issuer.
// It is NOT an implementation of a production authorization or source-verifier
// service. The E1 writer pool cannot provision these exact-byte approvals.
func seedTestE1FinalAuthorization(t *testing.T, admin *sql.DB, a WorkloadAcceptance, revision int64) {
	t.Helper()
	derived, err := deriveUsageEvidence(typedUsageEvidence(a))
	if err != nil {
		t.Fatalf("derive test issuer's independently approved FINAL: %v", err)
	}
	row := derived.record
	_, err = admin.ExecContext(context.Background(), `
		INSERT INTO public.compute_e1_final_authorizations
			(tenant_id, evidence_set_id, provider_id, policy_version,
			 policy_revision, expected_final, expires_at)
		VALUES ($1,$2,$3,$4,$5,
			jsonb_build_object(
		'evidence_set_id', $6::text,
		'schema_version', $7::text,
		'tenant_id', $8::text,
		'project_id', $9::text,
		'workload_id', $10::text,
		'allocation_id', $11::text,
		'allocation_generation', $12::bigint,
		'provider_id', $13::text,
		'compute_operation_id', $14::text,
		'provider_request_digest', $15::text,
		'provider_resource_id', $16::text,
		'provider_resource_uid', $17::text,
		'pool_id', $18::text,
		'profile_id', $19::text,
		'gpu_model', $20::text,
		'capability_profile_id', $21::text,
		'evidence_policy_version', $22::text,
		'metering_policy_version', $23::text,
		'evidence_digest', $24::text,
		'usage_interval_id', $25::text,
		'usage_start', $26::timestamptz,
		'usage_end', $27::timestamptz,
		'gpu_count', $28::bigint,
		'source_event_ids', $29::jsonb,
		'evidence_payload', $30::jsonb,
		'usage_interval_payload', $31::jsonb,
		'usage_unit', $32::text,
		'attribution_method', $33::text,
		'quality', $34::text,
		'source_coverage', $35::numeric
			),
			clock_timestamp()+interval '1 hour')
		ON CONFLICT (tenant_id,evidence_set_id,policy_revision) DO NOTHING
	`, row.TenantID, row.EvidenceSetID, row.ProviderID, row.EvidencePolicyVersion, revision,
		row.EvidenceSetID,
		row.SchemaVersion,
		row.TenantID,
		row.ProjectID,
		row.WorkloadID,
		row.AllocationID,
		row.AllocationGeneration,
		row.ProviderID,
		row.ComputeOperationID,
		row.ProviderRequestDigest,
		row.ProviderResourceID,
		row.ProviderResourceUID,
		row.PoolID,
		row.ProfileID,
		row.GPUModel,
		row.CapabilityProfileID,
		row.EvidencePolicyVersion,
		row.MeteringPolicyVersion,
		row.EvidenceDigest,
		row.UsageIntervalID,
		row.UsageStart,
		row.UsageEnd,
		row.GPUCount,
		derived.sourceEventJSON,
		derived.evidenceJSON,
		derived.usageJSON,
		usageIntervalUnit,
		usageAttributionMethod,
		usageEvidenceQuality,
		1)
	if err != nil {
		t.Fatalf("provision test-only independent FINAL authorization: %v", err)
	}
}
