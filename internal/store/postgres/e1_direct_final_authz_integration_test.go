package postgres

import (
	"context"
	"database/sql"
	"errors"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgconn"
)

// RED: A credentialed E1 writer must not be able to turn an otherwise
// valid Provider SUCCEEDED/Allocation BOUND fact into unauthorized billing
// evidence by bypassing the Go PolicyAuthority and writing FINAL directly.
// An administrator policy row is necessary, but still is NOT a per-evidence
// attestation of source provenance and exact derived bytes.
func TestE1DirectFinalInsertRequiresIndependentEvidenceApproval(t *testing.T) {
	for _, tc := range []struct {
		name              string
		provisionPolicy   bool
		provisionApproval bool
		revokeApproval    bool
		wantSQLState      string
		replicaMode       bool
	}{
		{"no administered policy", false, false, false, "E1P01", false},
		{"policy fence exists but no evidence authorization", true, false, false, "E1A01", false},
		{"revoked exact-row authorization", true, true, true, "E1A01", false},
		{"replica mode must still reject missing authorization", false, false, false, "E1P01", true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			admin := openIntegrationDB(t)
			writer := testEvidenceWriterDB(t, admin)
			ctx, cancel := context.WithTimeout(context.Background(), 25*time.Second)
			defer cancel()
			p := acceptanceFixture(t,
				"tenant-direct-e1-"+randomSuffix(t),
				"workload-"+randomSuffix(t),
				"operation-"+randomSuffix(t),
				"event-"+randomSuffix(t))
			a, created, err := acceptTestEvidenceWorkload(t, admin, ctx, p)
			if err != nil || !created {
				t.Fatalf("accept Workload: created=%t err=%v", created, err)
			}
			// These are genuine permanent Provider facts, not pg_temp fakes.
			// Use genuine permanent Provider facts, deliberately without asking
			// the separate approval issuer to authorize this evidence row.
			_, err = admin.ExecContext(ctx, `
				UPDATE public.compute_allocations SET phase='BOUND', updated_at=clock_timestamp()
				WHERE tenant_id=$1 AND allocation_id=$2
			`, a.Workload.TenantID, a.Allocation.AllocationID)
			if err != nil {
				t.Fatalf("bind real Allocation: %v", err)
			}
			_, err = admin.ExecContext(ctx, `
				UPDATE public.compute_operations
				SET status='SUCCEEDED', provider_resource_id=$3, provider_resource_uid=$4,
					updated_at=clock_timestamp()
				WHERE tenant_id=$1 AND compute_operation_id=$2
			`, a.Workload.TenantID, a.Operation.ID,
				"provider-resource-"+a.Allocation.AllocationID,
				"provider-uid-"+a.Allocation.AllocationID)
			if err != nil {
				t.Fatalf("complete real Provider operation: %v", err)
			}
			derived, err := deriveUsageEvidence(typedUsageEvidence(a))
			if err != nil {
				t.Fatalf("derive test evidence: %v", err)
			}
			row := derived.record
			if tc.provisionPolicy {
				now := time.Now().UTC().Truncate(time.Microsecond)
				approved := TrustedEvidencePolicy{
					TenantID: row.TenantID, ProviderID: row.ProviderID,
					Policy:         fixtureEvidencePolicy(),
					EffectiveFrom:  now.Add(-time.Hour),
					EffectiveUntil: now.Add(time.Hour),
					Revision:       1,
				}
				digest, err := evidencePolicyFenceDigest(approved)
				if err != nil {
					t.Fatal(err)
				}
				_, err = admin.ExecContext(ctx, `
                    INSERT INTO public.compute_evidence_policy_fences
                      (tenant_id,provider_id,policy_version,policy_digest,
                       effective_from,effective_until,revision,revoked)
                    VALUES ($1,$2,$3,$4,$5,$6,1,false)
                `, approved.TenantID, approved.ProviderID, approved.Policy.Version,
					digest, approved.EffectiveFrom, approved.EffectiveUntil)
				if err != nil {
					t.Fatalf("provision genuine admin policy: %v", err)
				}
			}
			if tc.provisionApproval {
				seedTestE1FinalAuthorization(t, admin, a, 1)
			}
			if tc.revokeApproval {
				result, err := admin.ExecContext(ctx, `
					UPDATE public.compute_e1_final_authorizations SET revoked=true
					WHERE tenant_id=$1 AND evidence_set_id=$2 AND policy_revision=1
				`, row.TenantID, row.EvidenceSetID)
				if err != nil {
					t.Fatalf("revoke exact-row authorization: %v", err)
				}
				count, err := result.RowsAffected()
				if err != nil || count != 1 {
					t.Fatalf("expected exactly one revoked approval; rows=%d err=%v", count, err)
				}
			}
			// Deliberately bypass PersistUsageEvidenceSet / Go policy resolver.
			// The separate replica-mode case proves the native DB trigger still
			// denies an unauthorized FINAL even when the session skips normal
			// origin-only triggers. Use a rollback-only admin transaction solely
			// for this controlled PG16 fault injection; production writers must
			// NEVER receive permission to set session_replication_role.
			var insertExecutor interface {
				ExecContext(context.Context, string, ...any) (sql.Result, error)
			} = writer
			if tc.replicaMode {
				faultTx, txErr := admin.BeginTx(ctx, nil)
				if txErr != nil {
					t.Fatal(txErr)
				}
				defer faultTx.Rollback()
				if _, txErr = faultTx.ExecContext(ctx, "SET LOCAL session_replication_role = replica"); txErr != nil {
					t.Fatal(txErr)
				}
				insertExecutor = faultTx
			}
			_, insertErr := insertExecutor.ExecContext(ctx, `
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

			var durable int
			if err := admin.QueryRowContext(ctx, `
                SELECT count(*) FROM public.compute_allocation_usage_evidence_sets
                WHERE tenant_id=$1 AND evidence_set_id=$2
            `, row.TenantID, row.EvidenceSetID).Scan(&durable); err != nil {
				t.Fatal(err)
			}
			var pgErr *pgconn.PgError
			if !errors.As(insertErr, &pgErr) || pgErr.Code != tc.wantSQLState || durable != 0 {
				t.Fatalf("direct E1 SQL authorization rejection mismatch: SQL err=%v, expected state=%s, permanent_FINAL=%d", insertErr, tc.wantSQLState, durable)
			}
		})
	}
}
