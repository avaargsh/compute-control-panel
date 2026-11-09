package postgres

import (
	"context"
	"errors"
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
	if _, created, err := repo.AcceptWorkload(ctx, params); !errors.Is(err, ErrProviderDraining) || created {
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

// A session-local TEMP table must never override the authoritative Provider
// lifecycle state used to fence a new Workload acceptance transaction.
func TestAcceptWorkloadRejectsTempProviderShadowWhenRealProviderIsDraining(t *testing.T) {
	db := openIntegrationDB(t)
	// Pin the test to one PostgreSQL session so pg_temp takes precedence over
	// unqualified relation names in the same session as AcceptWorkload.
	db.SetMaxOpenConns(1)
	db.SetMaxIdleConns(1)
	repo, err := NewRepository(db)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	p := providerRegistrationFixture(t)
	if _, created, err := repo.RegisterProvider(ctx, p); err != nil || !created {
		t.Fatalf("register Provider: created=%t err=%v", created, err)
	}
	if _, err := repo.BeginProviderDrain(ctx, p.ProviderID, 1, "PLANNED_MAINTENANCE"); err != nil {
		t.Fatal(err)
	}
	if _, err := db.ExecContext(ctx,
		`CREATE TEMP TABLE compute_providers (provider_id text PRIMARY KEY, state text NOT NULL) ON COMMIT PRESERVE ROWS`); err != nil {
		t.Fatal(err)
	}
	if _, err := db.ExecContext(ctx,
		`INSERT INTO pg_temp.compute_providers (provider_id, state) VALUES ($1, 'REGISTERED')`, p.ProviderID); err != nil {
		t.Fatal(err)
	}
	var authoritative ProviderState
	if err := db.QueryRowContext(ctx,
		`SELECT state FROM public.compute_providers WHERE provider_id=$1`, p.ProviderID).Scan(&authoritative); err != nil {
		t.Fatal(err)
	}
	if authoritative != ProviderDraining {
		t.Fatalf("permanent Provider state = %q, want DRAINING", authoritative)
	}

	params := acceptanceFixture(t, "tenant-shadow-"+randomSuffix(t),
		"workload-shadow-"+randomSuffix(t), "operation-shadow-"+randomSuffix(t),
		"operation-event-shadow-"+randomSuffix(t))
	params.ProviderID = p.ProviderID
	params.Operation.ProviderID = p.ProviderID
	if _, created, err := repo.AcceptWorkload(ctx, params); !errors.Is(err, ErrProviderDraining) || created {
		t.Fatalf("TEMP Provider forged REGISTERED admission: created=%t err=%v", created, err)
	}
	var workloads, operations, events int
	if err := db.QueryRowContext(ctx,
		`SELECT count(*) FROM public.compute_workloads WHERE tenant_id=$1 AND workload_id=$2`,
		params.Workload.TenantID, params.Workload.WorkloadID).Scan(&workloads); err != nil {
		t.Fatal(err)
	}
	if err := db.QueryRowContext(ctx,
		`SELECT count(*) FROM public.compute_operations WHERE tenant_id=$1 AND compute_operation_id=$2`,
		params.Workload.TenantID, params.Operation.ID).Scan(&operations); err != nil {
		t.Fatal(err)
	}
	if err := db.QueryRowContext(ctx,
		`SELECT count(*) FROM public.compute_outbox_events WHERE tenant_id=$1 AND event_id IN ($2,$3)`,
		params.Workload.TenantID, params.Workload.EventID, params.OperationEventID).Scan(&events); err != nil {
		t.Fatal(err)
	}
	if workloads != 0 || operations != 0 || events != 0 {
		t.Fatalf("TEMP shadow bypass persisted Workload/Operation/Outbox: %d/%d/%d", workloads, operations, events)
	}
}
