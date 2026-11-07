package postgres

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgconn"
)

// An issuer lock must not extend either the independent approval or policy
// deadline. Both paths must use fresh PostgreSQL time after obtaining the lock.
func TestE1FinalRejectsApprovalExpiryDuringApprovalLock(t *testing.T) {
	for _, tc := range []struct {
		path, release string
		expirePolicy  bool
	}{
		{"direct SQL", "commit unchanged", false},
		{"direct SQL", "rollback revocation", false},
		{"repository", "commit unchanged", false},
		{"repository", "rollback revocation", false},
		{"direct SQL", "commit unchanged", true},
		{"repository", "commit unchanged", true},
	} {
		deadlineName := "approval"
		if tc.expirePolicy {
			deadlineName = "policy"
		}
		t.Run(tc.path+"/"+tc.release+"/"+deadlineName, func(t *testing.T) {
			admin := openIntegrationDB(t)
			writer := testEvidenceWriterDB(t, admin)
			ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
			defer cancel()

			p := acceptanceFixture(t, "tenant-approval-expiry-"+randomSuffix(t),
				"workload-"+randomSuffix(t), "operation-"+randomSuffix(t),
				"event-"+randomSuffix(t))
			a, created, err := acceptTestEvidenceWorkload(t, admin, ctx, p)
			if err != nil || !created {
				t.Fatalf("accept Workload: created=%t err=%v", created, err)
			}
			markUsageEvidenceProviderOperationSucceeded(t, admin, a)
			params := typedUsageEvidence(a)
			var dbNow time.Time
			if err := admin.QueryRowContext(ctx, "SELECT clock_timestamp()").Scan(&dbNow); err != nil {
				t.Fatal(err)
			}
			approved := TrustedEvidencePolicy{
				TenantID: a.Workload.TenantID, ProviderID: a.Allocation.ProviderID,
				Policy: fixtureEvidencePolicy(), EffectiveFrom: dbNow.Add(-time.Hour),
				EffectiveUntil: dbNow.Add(time.Hour), Revision: 2,
			}
			approvalTTL := "4 seconds"
			if tc.expirePolicy {
				approved.EffectiveUntil = dbNow.Add(4 * time.Second)
				approvalTTL = "1 hour"
			}
			digest, err := evidencePolicyFenceDigest(approved)
			if err != nil {
				t.Fatal(err)
			}
			if _, err := admin.ExecContext(ctx, `
					INSERT INTO public.compute_evidence_policy_fences
					(tenant_id,provider_id,policy_version,policy_digest,effective_from,effective_until,revision,revoked)
					VALUES ($1,$2,$3,$4,$5,$6,2,false)
				`, approved.TenantID, approved.ProviderID, approved.Policy.Version, digest,
				approved.EffectiveFrom, approved.EffectiveUntil); err != nil {
				t.Fatal(err)
			}
			repo, err := NewRepositoryWithEvidencePolicyAuthority(writer,
				EvidencePolicyAuthorityFunc(func(context.Context, string, string, string) (TrustedEvidencePolicy, error) {
					return approved, nil
				}))
			if err != nil {
				t.Fatal(err)
			}
			var writerLogin string
			if err := writer.QueryRowContext(ctx, "SELECT current_user").Scan(&writerLogin); err != nil {
				t.Fatal(err)
			}
			if err := verifyEvidenceWriterPrivileges(ctx, writer); err != nil {
				t.Fatalf("restricted writer fixture: %v", err)
			}

			// The existing fixture issued revision 1. Reuse its exact approved
			// bytes for a new revision-2 ticket, without mutating immutable rows.
			var expiresAt time.Time
			var expectedFinal []byte
			if err := admin.QueryRowContext(ctx, `
					INSERT INTO public.compute_e1_final_authorizations
					(tenant_id,evidence_set_id,provider_id,policy_version,policy_revision,
					 expected_final,expires_at)
					SELECT tenant_id,evidence_set_id,provider_id,policy_version,2,
					       expected_final,clock_timestamp()+$3::interval
					FROM public.compute_e1_final_authorizations
					WHERE tenant_id=$1 AND evidence_set_id=$2 AND policy_revision=1
					RETURNING expires_at,expected_final
				`, approved.TenantID, params.Evidence.EvidenceSetID, approvalTTL).Scan(&expiresAt, &expectedFinal); err != nil {
				t.Fatalf("issue short-lived independent approval: %v", err)
			}
			deadline := expiresAt
			if tc.expirePolicy {
				deadline = approved.EffectiveUntil
			}
			blocker, err := admin.BeginTx(ctx, nil)
			if err != nil {
				t.Fatal(err)
			}
			defer blocker.Rollback()
			var blockerPID int
			if err := blocker.QueryRowContext(ctx, `
					SELECT pg_backend_pid()
					FROM public.compute_e1_final_authorizations
					WHERE tenant_id=$1 AND evidence_set_id=$2 AND policy_revision=2
					FOR UPDATE
				`, approved.TenantID, params.Evidence.EvidenceSetID).Scan(&blockerPID); err != nil {
				t.Fatal(err)
			}
			if tc.release == "rollback revocation" {
				if _, err := blocker.ExecContext(ctx, `
						UPDATE public.compute_e1_final_authorizations SET revoked=true
						WHERE tenant_id=$1 AND evidence_set_id=$2 AND policy_revision=2
					`, approved.TenantID, params.Evidence.EvidenceSetID); err != nil {
					t.Fatal(err)
				}
			}

			type outcome struct {
				persisted bool
				err       error
			}
			finished := make(chan outcome, 1)
			go func() {
				if tc.path == "repository" {
					_, persisted, err := repo.PersistUsageEvidenceSet(ctx, params)
					finished <- outcome{persisted, err}
					return
				}
				// Bypass Go derivation/persistence, using only the issuer's
				// fixture-approved bytes and the restricted LOGIN connection.
				_, err := writer.ExecContext(ctx, `
						INSERT INTO public.compute_allocation_usage_evidence_sets
						SELECT * FROM jsonb_populate_record(
							NULL::public.compute_allocation_usage_evidence_sets,$1::jsonb)
					`, expectedFinal)
				finished <- outcome{err == nil, err}
			}()

			// Observe a real conflicting row-lock wait before expiry, rather
			// than assuming the writer reached the trigger after a fixed sleep.
			for {
				var blocked bool
				if err := admin.QueryRowContext(ctx, `
						SELECT EXISTS (
							SELECT 1 FROM pg_stat_activity s
							WHERE s.usename=$1 AND s.wait_event_type='Lock'
							  AND $2=ANY(pg_blocking_pids(s.pid))
						),clock_timestamp()
					`, writerLogin, blockerPID).Scan(&blocked, &dbNow); err != nil {
					t.Fatal(err)
				}
				if !dbNow.Before(deadline) {
					t.Fatalf("writer did not reach approval row-lock wait before %s expired", deadlineName)
				}
				if blocked {
					break
				}
				select {
				case out := <-finished:
					t.Fatalf("writer finished before approval lock release: %+v", out)
				case <-ctx.Done():
					t.Fatal(ctx.Err())
				case <-time.After(10 * time.Millisecond):
				}
			}
			for dbNow.Before(deadline) {
				if err := admin.QueryRowContext(ctx, "SELECT clock_timestamp()").Scan(&dbNow); err != nil {
					t.Fatal(err)
				}
				select {
				case out := <-finished:
					t.Fatalf("writer escaped issuer's approval lock: %+v", out)
				case <-ctx.Done():
					t.Fatal(ctx.Err())
				case <-time.After(10 * time.Millisecond):
				}
			}
			if tc.release == "rollback revocation" {
				err = blocker.Rollback()
			} else {
				err = blocker.Commit()
			}
			if err != nil {
				t.Fatal(err)
			}
			var out outcome
			select {
			case out = <-finished:
			case <-ctx.Done():
				t.Fatal(ctx.Err())
			}
			var durable int
			if err := admin.QueryRowContext(ctx, `
					SELECT count(*) FROM public.compute_allocation_usage_evidence_sets
					WHERE tenant_id=$1 AND evidence_set_id=$2
				`, approved.TenantID, params.Evidence.EvidenceSetID).Scan(&durable); err != nil {
				t.Fatal(err)
			}
			wantErr, wantSQLState := ErrEvidenceFinalUnauthorized, "E1A01"
			if tc.expirePolicy {
				wantErr, wantSQLState = ErrEvidencePolicyUntrusted, "E1P01"
			}
			if tc.path == "repository" {
				if out.persisted || !errors.Is(out.err, wantErr) || durable != 0 {
					t.Fatalf("expired %s authorized repository FINAL after lock wait: persisted=%t err=%v durable_FINAL=%d", deadlineName, out.persisted, out.err, durable)
				}
			} else {
				var pgErr *pgconn.PgError
				if out.persisted || !errors.As(out.err, &pgErr) || pgErr.Code != wantSQLState || durable != 0 {
					t.Fatalf("expired %s authorized direct SQL FINAL after lock wait: err=%v durable_FINAL=%d", deadlineName, out.err, durable)
				}
			}
		})
	}
}
