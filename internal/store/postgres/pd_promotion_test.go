package postgres

import (
	"context"
	"database/sql"
	"errors"
	"sync"
	"testing"
	"time"
)

func pdPromotionFixture(t *testing.T, ctx context.Context, db *sql.DB) (*Repository, PDIntentOwner, PDIntentRecord) {
	t.Helper()
	repo, owner, verified := preparePDIntent(t, ctx, db)
	seedPDIntentFence(t, ctx, db, verified)
	record, created, err := repo.RecordPDIntent(ctx, owner, verified, "pd-correlation")
	if err != nil || !created {
		t.Fatalf("record durable PD intent: created=%v err=%v", created, err)
	}
	return repo, owner, record
}

// The fixture-only force to RELEASED does NOT demonstrate an authorized GPU
// release: real admission depends on the existing release verifier pipeline.
func fixturePDRelease(t *testing.T, ctx context.Context, db *sql.DB, workload string) {
	t.Helper()
	_, err := db.ExecContext(ctx, `UPDATE public.compute_allocations
		SET phase='RELEASED',version=version+1
		WHERE workload_id=$1 AND desired_generation=1 AND phase='PENDING'`, workload)
	if err != nil {
		t.Fatal(err)
	}
}

func TestPromotePDIntentRejectsLiveOldAllocationAndCommitsNextGenerationAtomically(t *testing.T) {
	db := openIntegrationDB(t)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	repo, owner, intent := pdPromotionFixture(t, ctx, db)
	c := intent.Candidate
	if _, _, err := repo.PromotePDIntent(ctx, owner, c.IntentID, "pd-promotion"); !errors.Is(err, ErrPDPromotionBlocked) {
		t.Fatalf("live allocation accepted: %v", err)
	}
	fixturePDRelease(t, ctx, db, c.Workload)
	var wg sync.WaitGroup
	type result struct {
		record  PDPromotion
		created bool
		err     error
	}
	results := make(chan result, 8)
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			got, created, err := repo.PromotePDIntent(ctx, owner, c.IntentID, "pd-promotion")
			results <- result{got, created, err}
		}()
	}
	wg.Wait()
	close(results)
	var first PDPromotion
	writers := 0
	for result := range results {
		if result.err != nil || result.record.IntentID != c.IntentID || result.record.Generation != c.Generation {
			t.Fatalf("promotion: %+v %v", result.record, result.err)
		}
		if result.created {
			first = result.record
			writers++
		}
	}
	if writers != 1 {
		t.Fatalf("expected one new promotion, got %d", writers)
	}
	newRepo, _ := NewRepository(db)
	again, created, err := newRepo.PromotePDIntent(ctx, owner, c.IntentID, "pd-retry")
	if err != nil || created || again != first {
		t.Fatalf("process restart replay: %+v %t %v", again, created, err)
	}
	var generation, allocation, operation, workloadEvents, operationEvents int
	err = db.QueryRowContext(ctx, `SELECT w.desired_generation,
		(SELECT count(*) FROM public.compute_allocations a WHERE a.workload_id=w.workload_id AND a.desired_generation=$2 AND a.phase='PENDING'),
		(SELECT count(*) FROM public.compute_operations o WHERE o.workload_id=w.workload_id AND o.desired_generation=$2 AND o.status='PENDING' AND o.allocation_id=$3),
		(SELECT count(*) FROM public.compute_outbox_events e WHERE e.event_id=$4 AND e.event_type='compute.pd.workload.generation-accepted.v1'),
		(SELECT count(*) FROM public.compute_outbox_events e WHERE e.event_id=$5 AND e.event_type='compute.operation.pending.v1')
		FROM public.compute_workloads w WHERE w.workload_id=$1 AND w.tenant_id=$6`,
		c.Workload, int64(c.Generation), first.AllocationID, first.WorkloadEventID, first.OperationEventID, owner.TenantID).
		Scan(&generation, &allocation, &operation, &workloadEvents, &operationEvents)
	if err != nil || generation != 2 || allocation != 1 || operation != 1 || workloadEvents != 1 || operationEvents != 1 {
		t.Fatalf("generation/alloc/op/workload-event/op-event=%d/%d/%d/%d/%d err=%v", generation, allocation, operation, workloadEvents, operationEvents, err)
	}
}

func TestPromotePDIntentRejectsRevocationAndExpiredAuthorityWithoutPartialMutation(t *testing.T) {
	db := openIntegrationDB(t)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	for _, mode := range []string{"revoked", "stale-generation", "expired", "provider-draining"} {
		t.Run(mode, func(t *testing.T) {
			repo, owner, intent := pdPromotionFixture(t, ctx, db)
			fixturePDRelease(t, ctx, db, intent.Candidate.Workload)
			var err error
			switch mode {
			case "revoked":
				_, err = db.ExecContext(ctx, "UPDATE public.compute_pd_intent_fences SET revoked=true,fence_version=2 WHERE workload_id=$1", intent.Candidate.Workload)
			case "stale-generation":
				_, err = db.ExecContext(ctx, "UPDATE public.compute_workloads SET desired_generation=2,version=version+1 WHERE workload_id=$1", intent.Candidate.Workload)
			case "expired":
				_, err = db.ExecContext(ctx, "UPDATE public.compute_pd_intent_fences SET valid_until=clock_timestamp()-interval '1 second',fence_version=2 WHERE workload_id=$1", intent.Candidate.Workload)
			case "provider-draining":
				_, err = db.ExecContext(ctx, "UPDATE public.compute_providers SET state='DRAINING',version=version+1 WHERE provider_id=$1", intent.ProviderID)
			}
			if err != nil {
				t.Fatal(err)
			}
			if _, _, err = repo.PromotePDIntent(ctx, owner, intent.Candidate.IntentID, "blocked"); err == nil {
				t.Fatal("untrusted promotion was accepted")
			}
			var allocations, operations, events int
			err = db.QueryRowContext(ctx, `SELECT
				(SELECT count(*) FROM public.compute_allocations WHERE workload_id=$1 AND desired_generation=2),
				(SELECT count(*) FROM public.compute_operations WHERE workload_id=$1 AND desired_generation=2),
				(SELECT count(*) FROM public.compute_outbox_events WHERE event_id=$2)
			`, intent.Candidate.Workload, "pd-workload-"+intent.Candidate.IntentID).
				Scan(&allocations, &operations, &events)
			if err != nil || allocations != 0 || operations != 0 || events != 0 {
				t.Fatalf("partial promotion %d/%d/%d err=%v", allocations, operations, events, err)
			}
		})
	}
}

func TestPromotePDIntentRollbackOnOutboxCollision(t *testing.T) {
	db := openIntegrationDB(t)
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	repo, owner, intent := pdPromotionFixture(t, ctx, db)
	fixturePDRelease(t, ctx, db, intent.Candidate.Workload)
	id := "pd-workload-" + intent.Candidate.IntentID
	_, err := db.ExecContext(ctx, `INSERT INTO public.compute_outbox_events
		(event_id,producer,event_type,schema_version,tenant_id,aggregate_type,aggregate_id,aggregate_version,occurred_at,correlation_id,payload)
		VALUES($1,'fixture','fixture',1,$2,'fixture',$1,1,clock_timestamp(),'fixture','{}'::jsonb)`, id, owner.TenantID)
	if err != nil {
		t.Fatal(err)
	}
	if _, _, err := repo.PromotePDIntent(ctx, owner, intent.Candidate.IntentID, "collision"); err == nil {
		t.Fatal("outbox collision should roll back generation and allocation")
	}
	var generation, allocation, op int
	err = db.QueryRowContext(ctx, `SELECT desired_generation,
		(SELECT count(*) FROM public.compute_allocations WHERE workload_id=$1 AND desired_generation=2),
		(SELECT count(*) FROM public.compute_operations WHERE workload_id=$1 AND desired_generation=2)
		FROM public.compute_workloads WHERE workload_id=$1`, intent.Candidate.Workload).Scan(&generation, &allocation, &op)
	if err != nil || generation != 1 || allocation != 0 || op != 0 {
		t.Fatalf("partial write: %d/%d/%d %v", generation, allocation, op, err)
	}
}
