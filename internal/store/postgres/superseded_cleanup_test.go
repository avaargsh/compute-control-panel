package postgres

import (
	"context"
	"database/sql"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/avaargsh/compute-control-panel/internal/operation"
)

func supersededCreateFixture(t *testing.T, db *sql.DB, tenant string) operation.Operation {
	t.Helper()
	repo, err := NewRepository(db)
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	source := operationFixture(t, repo, ctx, tenant)
	if _, created, err := repo.CreateOperation(ctx, source, "event-"+randomSuffix(t), "superseded-source"); err != nil || !created {
		t.Fatalf("create source operation: created=%v err=%v", created, err)
	}
	// Test-only authoritative supersession fixture: the old resource UID is
	// known from Inspect, but its parent has moved to a newer generation.
	if _, err := db.ExecContext(ctx, `UPDATE public.compute_workloads
		SET desired_generation=desired_generation+1, version=version+1
		WHERE tenant_id=$1 AND workload_id=$2`, tenant, source.WorkloadID); err != nil {
		t.Fatal(err)
	}
	if _, err := db.ExecContext(ctx, `UPDATE public.compute_operations
		SET status='SUPERSEDED', version=version+1, reconcile_required=true,
			cleanup_required=true, provider_resource_id=$3, provider_resource_uid=$4
		WHERE tenant_id=$1 AND compute_operation_id=$2`,
		tenant, source.ID, "tenant-a/old-resource", "old-resource-uid"); err != nil {
		t.Fatal(err)
	}
	got, err := repo.GetOperation(ctx, tenant, source.ID)
	if err != nil || got.Status != operation.Superseded || !got.CleanupRequired {
		t.Fatalf("source fixture incomplete: %+v err=%v", got, err)
	}
	return got
}

func TestEnqueueSupersededCleanupUsesExactHistoricalUIDAndIdempotentReplay(t *testing.T) {
	db := openIntegrationDB(t)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	tenant := "tenant-cleanup-" + randomSuffix(t)
	source := supersededCreateFixture(t, db, tenant)
	repoA, _ := NewRepository(db)
	repoB, _ := NewRepository(db)

	first, created, err := repoA.EnqueueSupersededCleanup(ctx, tenant, source.ID, source.Version)
	if err != nil || !created {
		t.Fatalf("enqueue cleanup: created=%v err=%v", created, err)
	}
	if first.Kind != operation.Delete || first.Status != operation.Pending ||
		first.TenantID != tenant || first.WorkloadID != source.WorkloadID ||
		first.ProviderID != source.ProviderID || first.AllocationID != source.AllocationID ||
		first.Generation != source.Generation ||
		first.TargetResourceID != source.ProviderResourceID ||
		first.TargetResourceUID != source.ProviderResourceUID ||
		first.ProviderResourceID != "" || first.ProviderResourceUID != "" ||
		first.CleanupRequired || first.ReconcileRequired {
		t.Fatalf("cleanup lost historical UID or inherited release state: %+v", first)
	}
	replayed, created, err := repoB.EnqueueSupersededCleanup(ctx, tenant, source.ID, source.Version)
	if err != nil || created || replayed != first {
		t.Fatalf("exact cleanup replay mutated intent: created=%v got=%+v want=%+v err=%v", created, replayed, first, err)
	}
	var operations, events int
	if err := db.QueryRowContext(ctx, `SELECT count(*) FROM public.compute_operations
		WHERE tenant_id=$1 AND workload_id=$2 AND operation_kind='DELETE'`,
		tenant, source.WorkloadID).Scan(&operations); err != nil {
		t.Fatal(err)
	}
	if err := db.QueryRowContext(ctx, `SELECT count(*) FROM public.compute_outbox_events
		WHERE tenant_id=$1 AND aggregate_type='compute_operation' AND aggregate_id=$2
		AND event_type='compute.operation.recorded.v1'`,
		tenant, first.ID).Scan(&events); err != nil {
		t.Fatal(err)
	}
	if operations != 1 || events != 1 {
		t.Fatalf("duplicate cleanup history: deletes=%d recorded_events=%d", operations, events)
	}
	after, err := repoA.GetOperation(ctx, tenant, source.ID)
	if err != nil || after != source {
		t.Fatalf("cleanup enqueue altered superseded source: got=%+v want=%+v err=%v", after, source, err)
	}
}

func TestEnqueueSupersededCleanupFencesStaleAndCrossTenantRequests(t *testing.T) {
	db := openIntegrationDB(t)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	tenant := "tenant-cleanup-fence-" + randomSuffix(t)
	source := supersededCreateFixture(t, db, tenant)
	repo, _ := NewRepository(db)
	if _, created, err := repo.EnqueueSupersededCleanup(ctx, tenant, source.ID, source.Version-1); !errors.Is(err, operation.ErrStale) || created {
		t.Fatalf("stale source CAS admitted: created=%v err=%v", created, err)
	}
	if _, created, err := repo.EnqueueSupersededCleanup(ctx, tenant+"-foreign", source.ID, source.Version); !errors.Is(err, sql.ErrNoRows) || created {
		t.Fatalf("foreign tenant claimed source: created=%v err=%v", created, err)
	}
	var count int
	if err := db.QueryRowContext(ctx, `SELECT count(*) FROM public.compute_operations
		WHERE tenant_id=$1 AND operation_kind='DELETE'`, tenant).Scan(&count); err != nil || count != 0 {
		t.Fatalf("rejected requests inserted cleanup: count=%d err=%v", count, err)
	}
}

func TestEnqueueSupersededCleanupConcurrentControllersCreateOneIntent(t *testing.T) {
	db := openIntegrationDB(t)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	tenant := "tenant-cleanup-race-" + randomSuffix(t)
	source := supersededCreateFixture(t, db, tenant)
	type result struct {
		op      operation.Operation
		created bool
		err     error
	}
	var wg sync.WaitGroup
	results := make([]result, 2)
	for i := range results {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			repo, err := NewRepository(db)
			if err != nil {
				results[i].err = err
				return
			}
			results[i].op, results[i].created, results[i].err = repo.EnqueueSupersededCleanup(ctx, tenant, source.ID, source.Version)
		}(i)
	}
	wg.Wait()
	if results[0].err != nil || results[1].err != nil ||
		results[0].op.ID != results[1].op.ID ||
		results[0].op.TargetResourceUID != source.ProviderResourceUID ||
		results[1].op.TargetResourceUID != source.ProviderResourceUID {
		t.Fatalf("concurrent cleanup did not converge: %+v", results)
	}
	if results[0].created == results[1].created {
		t.Fatalf("expected exactly one durable writer and one replay: %+v", results)
	}
}
