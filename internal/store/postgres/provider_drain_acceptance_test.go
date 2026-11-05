package postgres

import (
	"context"
	"testing"
	"time"
)

func TestAcceptWorkloadDoesNotCreateOperationsOnDrainingProvider(t *testing.T) {
	db := openIntegrationDB(t)
	repo, err := NewRepository(db)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	provider := providerRegistrationFixture(t)
	if _, created, err := repo.RegisterProvider(ctx, provider); err != nil || !created {
		t.Fatalf("Provider registration: created=%t err=%v", created, err)
	}
	if _, err := repo.BeginProviderDrain(ctx, provider.ProviderID, 1, "PLANNED_MAINTENANCE"); err != nil {
		t.Fatal(err)
	}
	params := acceptanceFixture(t, "tenant-draining-"+randomSuffix(t),
		"workload-draining-"+randomSuffix(t), "operation-draining-"+randomSuffix(t),
		"operation-event-draining-"+randomSuffix(t))
	params.ProviderID = provider.ProviderID
	params.Operation.ProviderID = provider.ProviderID
	if _, created, err := repo.AcceptWorkload(ctx, params); err == nil || created {
		t.Fatalf("DRAINING Provider was assigned a new Workload: created=%t err=%v", created, err)
	}

	var workloads, allocations, operations, events int
	if err := db.QueryRowContext(ctx,
		"SELECT count(*) FROM compute_workloads WHERE tenant_id=$1 AND workload_id=$2",
		params.Workload.TenantID, params.Workload.WorkloadID).Scan(&workloads); err != nil {
		t.Fatal(err)
	}
	if err := db.QueryRowContext(ctx,
		"SELECT count(*) FROM compute_allocations WHERE tenant_id=$1 AND allocation_id=$2",
		params.Workload.TenantID, params.AllocationID).Scan(&allocations); err != nil {
		t.Fatal(err)
	}
	if err := db.QueryRowContext(ctx,
		"SELECT count(*) FROM compute_operations WHERE tenant_id=$1 AND compute_operation_id=$2",
		params.Workload.TenantID, params.Operation.ID).Scan(&operations); err != nil {
		t.Fatal(err)
	}
	if err := db.QueryRowContext(ctx,
		"SELECT count(*) FROM compute_outbox_events WHERE tenant_id=$1 AND event_id IN ($2,$3)",
		params.Workload.TenantID, params.Workload.EventID, params.OperationEventID).Scan(&events); err != nil {
		t.Fatal(err)
	}
	if workloads != 0 || allocations != 0 || operations != 0 || events != 0 {
		t.Fatalf("Draining Provider created durable Workload/Allocation/Operation/Outbox: %d/%d/%d/%d",
			workloads, allocations, operations, events)
	}
}

func TestAcceptWorkloadExactReplayRemainsAvailableAfterProviderDrain(t *testing.T) {
	db := openIntegrationDB(t)
	repo, err := NewRepository(db)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	provider := providerRegistrationFixture(t)
	if _, created, err := repo.RegisterProvider(ctx, provider); err != nil || !created {
		t.Fatalf("Provider registration: created=%t err=%v", created, err)
	}
	params := acceptanceFixture(t, "tenant-drain-replay-"+randomSuffix(t),
		"workload-drain-replay-"+randomSuffix(t), "operation-drain-replay-"+randomSuffix(t),
		"operation-event-drain-replay-"+randomSuffix(t))
	params.ProviderID = provider.ProviderID
	params.Operation.ProviderID = provider.ProviderID
	first, created, err := repo.AcceptWorkload(ctx, params)
	if err != nil || !created {
		t.Fatalf("initial Workload acceptance: created=%t err=%v", created, err)
	}
	if _, err := repo.BeginProviderDrain(ctx, provider.ProviderID, 1, "PLANNED_MAINTENANCE"); err != nil {
		t.Fatal(err)
	}

	// An exact idempotency replay is not a new Provider placement. Preserve
	// the accepted identity and both original Outbox event IDs after drain.
	replayed, created, err := repo.AcceptWorkload(ctx, params)
	if err != nil || created {
		t.Fatalf("accepted Workload replay after Drain: created=%t err=%v", created, err)
	}
	if replayed.Workload.WorkloadID != first.Workload.WorkloadID ||
		replayed.Allocation.AllocationID != first.Allocation.AllocationID ||
		replayed.Operation.ID != first.Operation.ID ||
		replayed.WorkloadEventID != first.WorkloadEventID ||
		replayed.OperationEventID != first.OperationEventID {
		t.Fatalf("replay changed stable acceptance identity: first=%+v replay=%+v", first, replayed)
	}
}
