package postgres

import (
	"context"
	"testing"
	"time"
)

// Initial acceptance must not report success unless both dispatchable events
// exist in the same permanent PostgreSQL authority as the Workload/Operation.
// A pooled session's pg_temp table cannot be accepted as an Outbox.
func TestAcceptWorkloadDoesNotDivertDispatchEventsToTempOutbox(t *testing.T) {
	db := openIntegrationDB(t)
	db.SetMaxOpenConns(1)
	db.SetMaxIdleConns(1)
	repo, err := NewRepository(db)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	if _, err := db.ExecContext(ctx, `CREATE TEMP TABLE compute_outbox_events
		(LIKE public.compute_outbox_events INCLUDING DEFAULTS)
		ON COMMIT PRESERVE ROWS`); err != nil {
		t.Fatal(err)
	}
	params := acceptanceFixture(t, "tenant-accept-shadow-"+randomSuffix(t),
		"workload-key-"+randomSuffix(t), "operation-key-"+randomSuffix(t),
		"event-operation-"+randomSuffix(t))
	_, created, err := repo.AcceptWorkload(ctx, params)
	if err != nil || !created {
		t.Fatalf("accept Workload: created=%t err=%v", created, err)
	}
	var permanent, shadow, workloads, operations int
	for _, q := range []struct {
		query string
		into  *int
	}{
		{`SELECT count(*) FROM public.compute_outbox_events WHERE event_id IN ($1,$2)`, &permanent},
		{`SELECT count(*) FROM pg_temp.compute_outbox_events WHERE event_id IN ($1,$2)`, &shadow},
	} {
		if err := db.QueryRowContext(ctx, q.query, params.Workload.EventID,
			params.OperationEventID).Scan(q.into); err != nil {
			t.Fatal(err)
		}
	}
	if err := db.QueryRowContext(ctx, `SELECT count(*) FROM public.compute_workloads
		WHERE tenant_id=$1 AND workload_id=$2`,
		params.Workload.TenantID, params.Workload.WorkloadID).Scan(&workloads); err != nil {
		t.Fatal(err)
	}
	if err := db.QueryRowContext(ctx, `SELECT count(*) FROM public.compute_operations
		WHERE tenant_id=$1 AND compute_operation_id=$2`,
		params.Workload.TenantID, params.Operation.ID).Scan(&operations); err != nil {
		t.Fatal(err)
	}
	if permanent != 2 || shadow != 0 || workloads != 1 || operations != 1 {
		t.Fatalf("accepted Workload without durable dispatch events: events permanent=%d temp=%d workload=%d operation=%d",
			permanent, shadow, workloads, operations)
	}
}

// Idempotent acceptance must replay the original authoritative Outbox IDs
// even if a session-local table masks those permanent events.
func TestAcceptWorkloadReplayIgnoresTempOutbox(t *testing.T) {
	db := openIntegrationDB(t)
	db.SetMaxOpenConns(1)
	db.SetMaxIdleConns(1)
	repo, err := NewRepository(db)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	params := acceptanceFixture(t, "tenant-replay-shadow-"+randomSuffix(t),
		"workload-key-"+randomSuffix(t), "operation-key-"+randomSuffix(t),
		"event-operation-"+randomSuffix(t))
	first, created, err := repo.AcceptWorkload(ctx, params)
	if err != nil || !created {
		t.Fatalf("seed acceptance: created=%t err=%v", created, err)
	}
	if _, err := db.ExecContext(ctx, `CREATE TEMP TABLE compute_outbox_events
		(LIKE public.compute_outbox_events INCLUDING DEFAULTS)
		ON COMMIT PRESERVE ROWS`); err != nil {
		t.Fatal(err)
	}
	got, created, err := repo.AcceptWorkload(ctx, params)
	if err != nil || created ||
		got.Workload.WorkloadID != first.Workload.WorkloadID ||
		got.Allocation.AllocationID != first.Allocation.AllocationID ||
		got.Operation.ID != first.Operation.ID ||
		got.WorkloadEventID != first.WorkloadEventID ||
		got.OperationEventID != first.OperationEventID {
		t.Fatalf("TEMP table hid durable idempotent acceptance: created=%t got=%+v err=%v", created, got, err)
	}
}
