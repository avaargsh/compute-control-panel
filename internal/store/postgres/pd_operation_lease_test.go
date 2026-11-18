package postgres

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/avaargsh/compute-control-panel/internal/operation"
)

func TestPDLeaseClaimAndStartRequireExactCurrentAuthority(t *testing.T) {
	db := openIntegrationDB(t)
	ctx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
	defer cancel()
	repo, owner, intent := pdPromotionFixture(t, ctx, db, 1)
	id := intent.Candidate.IntentID
	if _, err := repo.ClaimPDStagingOperation(ctx, owner, id, 1, "staging-worker-a", time.Minute); !errors.Is(err, ErrPDPromotionBlocked) {
		t.Fatalf("unpromoted PD accepted for lease: %v", err)
	}
	fixturePDRelease(t, ctx, db, intent.Candidate.Workload)
	promoted, created, err := repo.PromotePDIntent(ctx, owner, id, "pd-fenced-lease")
	if err != nil || !created {
		t.Fatalf("promote: %+v %t %v", promoted, created, err)
	}
	claimed, err := repo.ClaimPDStagingOperation(ctx, owner, id, 1, "staging-worker-a", time.Minute)
	if err != nil || claimed.Operation.Status != operation.Leased || claimed.Operation.Version != 2 ||
		claimed.Operation.FenceToken != 1 || claimed.Snapshot.Promotion != promoted {
		t.Fatalf("fenced claim: %+v %v", claimed, err)
	}
	// A generic provider worker MUST NOT start the dedicated PD operation.
	if _, err := repo.StartOperation(ctx, owner.TenantID, promoted.OperationID, claimed.Operation.Version, "staging-worker-a", claimed.Operation.FenceToken); !errors.Is(err, operation.ErrTransition) {
		t.Fatalf("generic start bypassed PD source authority: %v", err)
	}
	started, err := repo.StartPDStagingOperation(ctx, owner, id, claimed.Operation.Version, "staging-worker-a", claimed.Operation.FenceToken)
	if err != nil || started.Operation.Status != operation.InFlight ||
		started.Operation.Version != claimed.Operation.Version+1 ||
		started.Operation.FenceToken != claimed.Operation.FenceToken {
		t.Fatalf("staging start: %+v %v", started, err)
	}
	if _, err := repo.StartPDStagingOperation(ctx, owner, id, claimed.Operation.Version, "staging-worker-a", claimed.Operation.FenceToken); !errors.Is(err, operation.ErrStale) {
		t.Fatalf("old lease version was replayed: %v", err)
	}
	if _, err := repo.ClaimPDStagingOperation(ctx, owner, id, 1, "staging-worker-b", time.Minute); err == nil {
		t.Fatal("leased PD operation was claimed twice")
	}
	unknown, err := repo.MarkOperationUnknown(ctx, owner.TenantID, promoted.OperationID, started.Operation.Version, "staging-worker-a", started.Operation.FenceToken)
	if err != nil || unknown.Status != operation.Unknown {
		t.Fatalf("lost ACK did not persist UNKNOWN: %+v %v", unknown, err)
	}
	if _, err := repo.ClaimPDStagingOperation(ctx, owner, id, unknown.Version, "staging-worker-b", time.Minute); err == nil {
		t.Fatal("UNKNOWN was re-dispatched without independent Inspect")
	}
}

func TestPDLeaseStartRejectsRevocationAfterClaim(t *testing.T) {
	db := openIntegrationDB(t)
	ctx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
	defer cancel()
	for _, mode := range []string{"revoked", "expired", "generation", "provider-drain", "owner"} {
		t.Run(mode, func(t *testing.T) {
			repo, owner, intent := pdPromotionFixture(t, ctx, db, 1)
			fixturePDRelease(t, ctx, db, intent.Candidate.Workload)
			if _, created, err := repo.PromotePDIntent(ctx, owner, intent.Candidate.IntentID, "pd-start-fence"); err != nil || !created {
				t.Fatalf("promote: %t %v", created, err)
			}
			claimed, err := repo.ClaimPDStagingOperation(ctx, owner, intent.Candidate.IntentID, 1, "worker", time.Minute)
			if err != nil {
				t.Fatal(err)
			}
			switch mode {
			case "revoked":
				_, err = db.ExecContext(ctx, `UPDATE public.compute_pd_intent_fences SET revoked=TRUE,fence_version=fence_version+1 WHERE workload_id=$1`, intent.Candidate.Workload)
			case "expired":
				_, err = db.ExecContext(ctx, `UPDATE public.compute_pd_intent_fences SET valid_until=pg_catalog.clock_timestamp()-interval '1 second' WHERE workload_id=$1`, intent.Candidate.Workload)
			case "generation":
				_, err = db.ExecContext(ctx, `UPDATE public.compute_workloads SET desired_generation=desired_generation+1,version=version+1 WHERE workload_id=$1`, intent.Candidate.Workload)
			case "provider-drain":
				_, err = repo.BeginProviderDrain(ctx, intent.ProviderID, 1, "PD_START_TEST")
			case "owner":
				owner.ResourceID += "-wrong"
			}
			if err != nil {
				t.Fatal(err)
			}
			if next, err := repo.StartPDStagingOperation(ctx, owner, intent.Candidate.IntentID, claimed.Operation.Version, "worker", claimed.Operation.FenceToken); !errors.Is(err, ErrPDPromotionBlocked) {
				t.Fatalf("%s permitted a stale start: %+v %v", mode, next, err)
			}
			got, err := repo.GetOperation(ctx, claimed.Operation.TenantID, claimed.Operation.ID)
			if err != nil || got.Status != operation.Leased || got.Version != claimed.Operation.Version {
				t.Fatalf("source revocation mutated lease: %+v %v", got, err)
			}
		})
	}
}

func TestPDLeaseClaimExactlyOneConcurrentWinnerAndNoGenericDispatch(t *testing.T) {
	db := openIntegrationDB(t)
	ctx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
	defer cancel()
	repo, owner, intent := pdPromotionFixture(t, ctx, db, 1)
	fixturePDRelease(t, ctx, db, intent.Candidate.Workload)
	promoted, created, err := repo.PromotePDIntent(ctx, owner, intent.Candidate.IntentID, "pd-exclusive-lease")
	if err != nil || !created {
		t.Fatalf("promote: %t %v", created, err)
	}
	if _, err := repo.ClaimOperation(ctx, owner.TenantID, promoted.OperationID, 1, "generic", time.Minute); !errors.Is(err, operation.ErrTransition) {
		t.Fatalf("generic writer got a PD lease: %v", err)
	}
	const workers = 6
	var wg sync.WaitGroup
	results := make(chan error, workers)
	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, err := repo.ClaimPDStagingOperation(ctx, owner, intent.Candidate.IntentID, 1, "worker", time.Minute)
			results <- err
		}()
	}
	wg.Wait()
	close(results)
	var winners int
	for err := range results {
		if err == nil {
			winners++
		} else if !errors.Is(err, operation.ErrStale) && !errors.Is(err, ErrPDPromotionBlocked) {
			t.Fatalf("unexpected concurrent result: %v", err)
		}
	}
	if winners != 1 {
		t.Fatalf("competing staging workers won %d leases, want one", winners)
	}
}
