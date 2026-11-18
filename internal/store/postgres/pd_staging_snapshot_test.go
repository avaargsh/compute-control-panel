package postgres

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/avaargsh/compute-control-panel/internal/capacity/pd"
	"github.com/avaargsh/compute-control-panel/internal/operation"
)

func TestPDStagingSnapshotRequiresPromotionAndCurrentIndependentFence(t *testing.T) {
	db := openIntegrationDB(t)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	repo, owner, intent := pdPromotionFixture(t, ctx, db, 1)
	id := intent.Candidate.IntentID
	if got, err := repo.ReadCurrentPDStagingSnapshot(ctx, owner, id); !errors.Is(err, ErrPDPromotionBlocked) || got.Promotion.IntentID != "" {
		t.Fatalf("unpromoted proposal was accepted: %+v %v", got, err)
	}
	// Fixture-only phase mutation does NOT authenticate a GPU release.
	fixturePDRelease(t, ctx, db, intent.Candidate.Workload)
	promoted, created, err := repo.PromotePDIntent(ctx, owner, id, "pd-staging-snapshot")
	if err != nil || !created {
		t.Fatalf("promotion: %+v created=%t err=%v", promoted, created, err)
	}
	staged, err := repo.ReadCurrentPDStagingSnapshot(ctx, owner, id)
	if err != nil {
		t.Fatal(err)
	}
	if staged.Promotion != promoted || staged.Proposal != intent.Candidate ||
		staged.ProviderID != intent.ProviderID || staged.TenantID != owner.TenantID ||
		staged.WorkloadID != intent.Candidate.Workload || staged.PoolID != intent.Candidate.Scope.Pool ||
		staged.PrefillReplicas != intent.Candidate.PrefillReplicas ||
		staged.DecodeReplicas != intent.Candidate.DecodeReplicas ||
		staged.GPUsPerReplica != 1 || !staged.AuthorityExpiresAt.After(time.Now().UTC()) {
		t.Fatalf("invalid current staging projection: %+v", staged)
	}
	actual, err := repo.GetOperation(ctx, owner.TenantID, promoted.OperationID)
	if err != nil || actual.Status != operation.Pending {
		t.Fatalf("read-only snapshot mutated staging Operation: %+v %v", actual, err)
	}
	second, err := repo.ReadCurrentPDStagingSnapshot(ctx, owner, id)
	if err != nil || second != staged {
		t.Fatalf("same valid stage not idempotent: %+v %v", second, err)
	}
}

func TestPDStagingSnapshotRejectsRevokedExpiredOwnerOrSupersededGeneration(t *testing.T) {
	db := openIntegrationDB(t)
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	for _, tc := range []string{"revoked", "expired", "wrong-owner", "stale-generation"} {
		t.Run(tc, func(t *testing.T) {
			repo, owner, intent := pdPromotionFixture(t, ctx, db, 1)
			fixturePDRelease(t, ctx, db, intent.Candidate.Workload)
			_, created, err := repo.PromotePDIntent(ctx, owner, intent.Candidate.IntentID, "pd-stage-blocker")
			if err != nil || !created {
				t.Fatalf("promote: %t %v", created, err)
			}
			switch tc {
			case "revoked":
				_, err = db.ExecContext(ctx, `UPDATE public.compute_pd_intent_fences
					SET revoked=TRUE, fence_version=fence_version+1
					WHERE workload_id=$1 AND tenant_id=$2`, intent.Candidate.Workload, owner.TenantID)
			case "expired":
				_, err = db.ExecContext(ctx, `UPDATE public.compute_pd_intent_fences
					SET valid_until=pg_catalog.clock_timestamp()-interval '1 second'
					WHERE workload_id=$1 AND tenant_id=$2`, intent.Candidate.Workload, owner.TenantID)
			case "stale-generation":
				_, err = db.ExecContext(ctx, `UPDATE public.compute_workloads
					SET desired_generation=desired_generation+1, version=version+1
					WHERE workload_id=$1 AND tenant_id=$2`, intent.Candidate.Workload, owner.TenantID)
			case "wrong-owner":
				owner.Service += "-not-authorized"
			}
			if err != nil {
				t.Fatal(err)
			}
			if stage, err := repo.ReadCurrentPDStagingSnapshot(ctx, owner, intent.Candidate.IntentID); !errors.Is(err, ErrPDPromotionBlocked) {
				t.Fatalf("%s accepted revoked/stale source: %+v %v", tc, stage, err)
			}
		})
	}
}

func TestPDStagingSnapshotRefusesMultiGPUEstimatesWithoutPhaseCardinality(t *testing.T) {
	proposal := pd.Proposal{
		PrefillReplicas: 2, DecodeReplicas: 3,
		GPUsNeeded: 5, PoolRevision: 1,
		TopologyRevision: 1, LoadRevision: 1,
	}
	if !singleGPUStagingShape(proposal) {
		t.Fatal("valid one-GPU-per-replica stage refused")
	}
	for _, mutate := range []func(*pd.Proposal){
		func(p *pd.Proposal) { p.GPUsNeeded = 6 },
		func(p *pd.Proposal) { p.PrefillReplicas = 0 },
		func(p *pd.Proposal) { p.DecodeReplicas = 33 },
		func(p *pd.Proposal) { p.PoolRevision = 0 },
		func(p *pd.Proposal) { p.TopologyRevision = 0 },
		func(p *pd.Proposal) { p.LoadRevision = 0 },
	} {
		p := proposal
		mutate(&p)
		if singleGPUStagingShape(p) {
			t.Fatalf("unproven PD physical cardinality or revision accepted %+v", p)
		}
	}
}

func TestPDStagingSnapshotRejectsSignedMultiGPUProposals(t *testing.T) {
	db := openIntegrationDB(t)
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	// Default signed benchmark uses 2 prefill GPUs and 1 decode GPU per
	// replica. It must NOT pass the one-GPU first writer through inference
	// from mere totals or from a simulated ResourceClaimTemplate.
	repo, owner, intent := pdPromotionFixture(t, ctx, db)
	if intent.Candidate.GPUsNeeded == intent.Candidate.PrefillReplicas+intent.Candidate.DecodeReplicas {
		t.Fatal("the multi-GPU negative fixture was silently weakened")
	}
	fixturePDRelease(t, ctx, db, intent.Candidate.Workload)
	if _, created, err := repo.PromotePDIntent(ctx, owner, intent.Candidate.IntentID, "pd-stage-multigpu-negative"); err != nil || !created {
		t.Fatalf("multi-GPU immutable proposal should still record without dispatch: created=%t err=%v", created, err)
	}
	if stage, err := repo.ReadCurrentPDStagingSnapshot(ctx, owner, intent.Candidate.IntentID); !errors.Is(err, ErrPDPromotionBlocked) {
		t.Fatalf("unsupported multi-GPU proposal was exposed as dispatchable: %+v %v", stage, err)
	}
}
