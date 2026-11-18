package postgres

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/avaargsh/compute-control-panel/internal/operation"
)

func pdInFlightForFence(t *testing.T, ctx context.Context, db *sql.DB) (*Repository, PDIntentOwner, PDStageLease) {
	t.Helper()
	repo, owner, intent := pdPromotionFixture(t, ctx, db, 1)
	fixturePDRelease(t, ctx, db, intent.Candidate.Workload)
	if _, created, err := repo.PromotePDIntent(ctx, owner, intent.Candidate.IntentID, "pd-sideeffect"); err != nil || !created {
		t.Fatalf("promote: created=%t err=%v", created, err)
	}
	claimed, err := repo.ClaimPDStagingOperation(ctx, owner, intent.Candidate.IntentID, 1, "pd-writer", time.Minute)
	if err != nil {
		t.Fatalf("claim: %v", err)
	}
	started, err := repo.StartPDStagingOperation(ctx, owner, intent.Candidate.IntentID, claimed.Operation.Version, "pd-writer", claimed.Operation.FenceToken)
	if err != nil {
		t.Fatalf("start: %v", err)
	}
	return repo, owner, started
}

func TestPDWriteFenceAllowsOnlyCurrentInFlightLease(t *testing.T) {
	db := openIntegrationDB(t)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	repo, owner, started := pdInFlightForFence(t, ctx, db)
	var calls int
	check := func(ctx context.Context, stage PDStageLease) error {
		calls++
		if stage.Operation != started.Operation || stage.Snapshot.Proposal != started.Snapshot.Proposal ||
			stage.Operation.Status != operation.InFlight {
			t.Fatalf("callback lacks same immutable plan and exact leased operation: %+v", stage)
		}
		return nil
	}
	_, err := repo.WithCurrentPDStagingFence(ctx, owner, started.Snapshot.Proposal.IntentID,
		started.Operation.Version, "wrong-worker", started.Operation.FenceToken, check)
	if !errors.Is(err, operation.ErrStaleLease) || calls != 0 {
		t.Fatalf("wrong worker crossed staging fence: %v calls=%d", err, calls)
	}
	_, err = repo.WithCurrentPDStagingFence(ctx, owner, started.Snapshot.Proposal.IntentID,
		started.Operation.Version, "pd-writer", started.Operation.FenceToken+1, check)
	if !errors.Is(err, operation.ErrStaleLease) || calls != 0 {
		t.Fatalf("wrong fence token crossed staging fence: %v calls=%d", err, calls)
	}
	_, err = repo.WithCurrentPDStagingFence(ctx, owner, started.Snapshot.Proposal.IntentID,
		started.Operation.Version-1, "pd-writer", started.Operation.FenceToken, check)
	if !errors.Is(err, operation.ErrStale) || calls != 0 {
		t.Fatalf("stale operation version crossed staging fence: %v calls=%d", err, calls)
	}
	staged, err := repo.WithCurrentPDStagingFence(ctx, owner, started.Snapshot.Proposal.IntentID,
		started.Operation.Version, "pd-writer", started.Operation.FenceToken, check)
	if err != nil || calls != 1 || staged != started {
		t.Fatalf("valid PD in-flight callback denied or state changed: calls=%d stage=%+v err=%v", calls, staged, err)
	}
	after, err := repo.GetOperation(ctx, owner.TenantID, started.Operation.ID)
	if err != nil || after != started.Operation {
		t.Fatalf("successful staging callback advanced durable state: %+v %v", after, err)
	}
}

func TestPDWriteFenceRejectsRevocationAndExpiredLeaseWithoutCallback(t *testing.T) {
	db := openIntegrationDB(t)
	ctx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
	defer cancel()
	for _, mode := range []string{"revoked", "stale-generation", "drain", "expired-lease", "wrong-owner"} {
		t.Run(mode, func(t *testing.T) {
			repo, owner, started := pdInFlightForFence(t, ctx, db)
			var err error
			switch mode {
			case "revoked":
				_, err = db.ExecContext(ctx, `UPDATE public.compute_pd_intent_fences SET revoked=true,fence_version=fence_version+1 WHERE workload_id=$1`, started.Operation.WorkloadID)
			case "stale-generation":
				_, err = db.ExecContext(ctx, `UPDATE public.compute_workloads SET desired_generation=desired_generation+1,version=version+1 WHERE workload_id=$1`, started.Operation.WorkloadID)
			case "drain":
				_, err = repo.BeginProviderDrain(ctx, started.Operation.ProviderID, 1, "PD_WRITE_FENCE")
			case "expired-lease":
				_, err = db.ExecContext(ctx, `UPDATE public.compute_operations SET lease_expires_at=clock_timestamp()-interval '1 second'
					WHERE compute_operation_id=$1`, started.Operation.ID)
			case "wrong-owner":
				owner.ResourceID += "-wrong"
			}
			if err != nil {
				t.Fatal(err)
			}
			calls := 0
			_, err = repo.WithCurrentPDStagingFence(ctx, owner, started.Snapshot.Proposal.IntentID,
				started.Operation.Version, "pd-writer", started.Operation.FenceToken,
				func(context.Context, PDStageLease) error { calls++; return nil })
			if err == nil || calls != 0 {
				t.Fatalf("%s allowed side effect after authority lost: err=%v calls=%d", mode, err, calls)
			}
			op, err := repo.GetOperation(ctx, started.Operation.TenantID, started.Operation.ID)
			if err != nil || op.Status != operation.InFlight || op.Version != started.Operation.Version {
				t.Fatalf("denied fence mutated operation: %+v %v", op, err)
			}
		})
	}
}

func TestPDWriteFenceUnknownOnUncertainCallbackAndNoReplay(t *testing.T) {
	db := openIntegrationDB(t)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	repo, owner, started := pdInFlightForFence(t, ctx, db)
	lost := errors.New("external POST acknowledgement lost")
	calls := 0
	got, err := repo.WithCurrentPDStagingFence(ctx, owner, started.Snapshot.Proposal.IntentID,
		started.Operation.Version, "pd-writer", started.Operation.FenceToken,
		func(context.Context, PDStageLease) error { calls++; return lost })
	if !errors.Is(err, lost) || calls != 1 || got.Operation.Status != operation.Unknown ||
		got.Operation.Version != started.Operation.Version+1 {
		t.Fatalf("uncertain side effect not fenced to UNKNOWN: %+v calls=%d err=%v", got, calls, err)
	}
	restarted, _ := NewRepository(db)
	stored, err := restarted.GetOperation(ctx, owner.TenantID, started.Operation.ID)
	if err != nil || stored.Status != operation.Unknown || stored.FenceToken != started.Operation.FenceToken {
		t.Fatalf("UNKNOWN was not durable across restart: %+v %v", stored, err)
	}
	_, err = restarted.WithCurrentPDStagingFence(ctx, owner, started.Snapshot.Proposal.IntentID,
		started.Operation.Version, "pd-writer", started.Operation.FenceToken,
		func(context.Context, PDStageLease) error { calls++; return nil })
	if err == nil || calls != 1 {
		t.Fatalf("restart replayed an uncertain CREATE: %v calls=%d", err, calls)
	}
}

func TestPDWriteFenceHoldsIndependentRevocationRowWhileCallbackRuns(t *testing.T) {
	db := openIntegrationDB(t)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	repo, owner, started := pdInFlightForFence(t, ctx, db)
	entered, release := make(chan struct{}), make(chan struct{})
	done := make(chan error, 1)
	go func() {
		_, err := repo.WithCurrentPDStagingFence(ctx, owner, started.Snapshot.Proposal.IntentID,
			started.Operation.Version, "pd-writer", started.Operation.FenceToken,
			func(context.Context, PDStageLease) error {
				close(entered)
				select {
				case <-release:
					return nil
				case <-ctx.Done():
					return ctx.Err()
				}
			})
		done <- err
	}()
	select {
	case <-entered:
	case <-ctx.Done():
		t.Fatal("staging callback did not start")
	}
	conn, err := db.Conn(ctx)
	if err != nil {
		close(release)
		t.Fatal(err)
	}
	defer conn.Close()
	// A dedicated session timeout proves the live current fence cannot be
	// revoked before the Kubernetes side-effect callback returns. No sleeps.
	if _, err := conn.ExecContext(ctx, "SET lock_timeout = '200ms'"); err != nil {
		close(release)
		t.Fatal(err)
	}
	_, err = conn.ExecContext(ctx, `UPDATE public.compute_pd_intent_fences SET revoked=true,fence_version=fence_version+1
		WHERE workload_id=$1`, started.Operation.WorkloadID)
	if err == nil || !strings.Contains(err.Error(), "lock timeout") {
		close(release)
		t.Fatalf("revocation bypassed in-flight database fence: %v", err)
	}
	close(release)
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("staged side effect failed: %v", err)
		}
	case <-ctx.Done():
		t.Fatal("staging side effect did not release DB locks")
	}
	_, err = db.ExecContext(ctx, `UPDATE public.compute_pd_intent_fences SET revoked=true,fence_version=fence_version+1
		WHERE workload_id=$1`, started.Operation.WorkloadID)
	if err != nil {
		t.Fatalf("revocation was blocked after side-effect completion: %v", err)
	}
}
