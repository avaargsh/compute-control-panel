package postgres

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/avaargsh/compute-control-panel/internal/operation"
)

func acceptanceFixture(t *testing.T, tenantID, workloadKey, operationKey, operationEventID string) AcceptWorkloadParams {
	t.Helper()
	request := json.RawMessage("{\"api_version\":\"compute.platform/v1\",\"project_id\":\"project-int-1\",\"resources\":{\"gpu_count\":1}}")
	workloadID := "workload-accept-" + randomSuffix(t)
	allocationID := "allocation-accept-" + randomSuffix(t)
	return AcceptWorkloadParams{
		Workload: createParams(
			tenantID,
			workloadKey,
			testDigest(request),
			request,
			workloadID,
			"event-workload-accept-"+randomSuffix(t),
		),
		AllocationID: allocationID,
		ProviderID:   "provider-fake-1",
		PoolID:       "pool-h100",
		Operation: operation.Operation{
			ID:             "operation-accept-" + randomSuffix(t),
			TenantID:       tenantID,
			WorkloadID:     workloadID,
			AllocationID:   allocationID,
			ProviderID:     "provider-fake-1",
			IdempotencyKey: operationKey,
			RequestDigest:  "sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
			Generation:     1,
			Version:        1,
			Kind:           operation.Create,
			Status:         operation.Pending,
		},
		OperationEventID: operationEventID,
	}
}

func TestAcceptWorkloadCommitsWorkloadAllocationOperationAndEventsAtomically(t *testing.T) {
	db := openIntegrationDB(t)
	repo, err := NewRepository(db)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	params := acceptanceFixture(t, "tenant-accept-"+randomSuffix(t),
		"workload-key-"+randomSuffix(t), "operation-key-"+randomSuffix(t),
		"event-operation-"+randomSuffix(t))
	accepted, created, err := repo.AcceptWorkload(ctx, params)
	if err != nil || !created {
		t.Fatalf("AcceptWorkload: created=%t err=%v", created, err)
	}
	if accepted.Workload.WorkloadID != params.Workload.WorkloadID ||
		accepted.Allocation.AllocationID != params.AllocationID ||
		accepted.Operation.ID != params.Operation.ID ||
		accepted.Operation.AllocationID != accepted.Allocation.AllocationID {
		t.Fatalf("acceptance identities do not form one stable chain: %+v", accepted)
	}
	if accepted.Allocation.Phase != "PENDING" || accepted.Operation.Status != operation.Pending {
		t.Fatalf("initial acceptance phases: allocation=%s operation=%s",
			accepted.Allocation.Phase, accepted.Operation.Status)
	}

	var workloadCount, allocationCount, operationCount int
	if err := db.QueryRowContext(ctx, "SELECT count(*) FROM compute_workloads WHERE tenant_id=$1 AND workload_id=$2",
		params.Workload.TenantID, accepted.Workload.WorkloadID).Scan(&workloadCount); err != nil {
		t.Fatal(err)
	}
	if err := db.QueryRowContext(ctx, "SELECT count(*) FROM compute_allocations WHERE tenant_id=$1 AND allocation_id=$2 AND workload_id=$3 AND provider_id=$4 AND pool_id=$5 AND desired_generation=1 AND phase='PENDING'",
		params.Workload.TenantID, accepted.Allocation.AllocationID, accepted.Workload.WorkloadID,
		params.ProviderID, params.PoolID).Scan(&allocationCount); err != nil {
		t.Fatal(err)
	}
	if err := db.QueryRowContext(ctx, "SELECT count(*) FROM compute_operations WHERE tenant_id=$1 AND compute_operation_id=$2 AND workload_id=$3 AND allocation_id=$4 AND provider_id=$5 AND desired_generation=1 AND status='PENDING'",
		params.Workload.TenantID, accepted.Operation.ID, accepted.Workload.WorkloadID,
		accepted.Allocation.AllocationID, params.ProviderID).Scan(&operationCount); err != nil {
		t.Fatal(err)
	}
	if workloadCount != 1 || allocationCount != 1 || operationCount != 1 {
		t.Fatalf("durable acceptance rows workload/allocation/operation = %d/%d/%d",
			workloadCount, allocationCount, operationCount)
	}

	var acceptedEventCount, pendingEventCount int
	if err := db.QueryRowContext(ctx, "SELECT count(*) FROM compute_outbox_events WHERE tenant_id=$1 AND aggregate_type='compute_workload' AND aggregate_id=$2 AND event_type='compute.workload.accepted.v1' AND payload->>'allocation_id'=$3 AND payload->>'operation_id'=$4",
		params.Workload.TenantID, accepted.Workload.WorkloadID,
		accepted.Allocation.AllocationID, accepted.Operation.ID).Scan(&acceptedEventCount); err != nil {
		t.Fatal(err)
	}
	if err := db.QueryRowContext(ctx, "SELECT count(*) FROM compute_outbox_events WHERE tenant_id=$1 AND aggregate_type='compute_operation' AND aggregate_id=$2 AND event_type='compute.operation.pending.v1' AND payload->>'allocation_id'=$3",
		params.Workload.TenantID, accepted.Operation.ID, accepted.Allocation.AllocationID).Scan(&pendingEventCount); err != nil {
		t.Fatal(err)
	}
	if acceptedEventCount != 1 || pendingEventCount != 1 {
		t.Fatalf("acceptance events workload/operation = %d/%d", acceptedEventCount, pendingEventCount)
	}

	var workloadEventPayload, operationEventPayload []byte
	if err := db.QueryRowContext(ctx, `SELECT payload FROM compute_outbox_events
		WHERE tenant_id=$1 AND aggregate_type='compute_workload' AND aggregate_id=$2
		AND event_type='compute.workload.accepted.v1'`,
		params.Workload.TenantID, accepted.Workload.WorkloadID).Scan(&workloadEventPayload); err != nil {
		t.Fatal(err)
	}
	if err := db.QueryRowContext(ctx, `SELECT payload FROM compute_outbox_events
		WHERE tenant_id=$1 AND aggregate_type='compute_operation' AND aggregate_id=$2
		AND event_type='compute.operation.pending.v1'`,
		params.Workload.TenantID, accepted.Operation.ID).Scan(&operationEventPayload); err != nil {
		t.Fatal(err)
	}
	for _, payload := range [][]byte{workloadEventPayload, operationEventPayload} {
		var envelope map[string]json.RawMessage
		if err := json.Unmarshal(payload, &envelope); err != nil {
			t.Fatal(err)
		}
		var desiredGeneration int64
		if err := json.Unmarshal(envelope["desired_generation"], &desiredGeneration); err != nil {
			t.Fatalf("desired_generation must be a JSON integer: %s (%v)", envelope["desired_generation"], err)
		}
		if desiredGeneration != 1 {
			t.Fatalf("desired_generation = %d, want 1", desiredGeneration)
		}
	}

	var latestAllocationID string
	if err := db.QueryRowContext(ctx, "SELECT latest_allocation_id FROM compute_workloads WHERE tenant_id=$1 AND workload_id=$2",
		params.Workload.TenantID, accepted.Workload.WorkloadID).Scan(&latestAllocationID); err != nil {
		t.Fatal(err)
	}
	if latestAllocationID != accepted.Allocation.AllocationID {
		t.Fatalf("latest allocation projection = %q, want %q", latestAllocationID, accepted.Allocation.AllocationID)
	}
}

func TestAcceptWorkloadReplayReturnsStableIdentitiesAndEvents(t *testing.T) {
	db := openIntegrationDB(t)
	repo, _ := NewRepository(db)
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	params := acceptanceFixture(t, "tenant-accept-replay-"+randomSuffix(t),
		"workload-key-"+randomSuffix(t), "operation-key-"+randomSuffix(t),
		"event-operation-"+randomSuffix(t))
	first, created, err := repo.AcceptWorkload(ctx, params)
	if err != nil || !created {
		t.Fatalf("first acceptance: created=%t err=%v", created, err)
	}

	lookup := AcceptanceLookup{
		TenantID: params.Workload.TenantID,
		IdempotencyKey: params.Workload.IdempotencyKey,
		RequestDigest: params.Workload.RequestDigest,
		ProjectID: params.Workload.ProjectID,
		OwnerService: params.Workload.OwnerService,
		OwnerResourceType: params.Workload.OwnerResourceType,
		OwnerResourceID: params.Workload.OwnerResourceID,
		ComputeProfileID: params.Workload.ComputeProfileID,
		ComputeProfileRevision: params.Workload.ComputeProfileRevision,
		OperationIdempotencyKey: params.Operation.IdempotencyKey,
		OperationRequestDigest: params.Operation.RequestDigest,
	}
	lookedUp, found, err := repo.GetAcceptanceByIdempotency(ctx, lookup)
	if err != nil || !found {
		t.Fatalf("lookup accepted Workload: found=%t err=%v", found, err)
	}
	if lookedUp.Workload.WorkloadID != first.Workload.WorkloadID ||
		lookedUp.Allocation.AllocationID != first.Allocation.AllocationID ||
		lookedUp.Operation.ID != first.Operation.ID ||
		lookedUp.Allocation.ProviderID != first.Allocation.ProviderID {
		t.Fatalf("lookup did not return original acceptance identities: first=%+v lookup=%+v", first, lookedUp)
	}

	replay := params
	replay.Workload.WorkloadID = "new-candidate-workload-" + randomSuffix(t)
	replay.Workload.EventID = "new-candidate-workload-event-" + randomSuffix(t)
	replay.AllocationID = "new-candidate-allocation-" + randomSuffix(t)
	replay.Operation.ID = "new-candidate-operation-" + randomSuffix(t)
	replay.Operation.WorkloadID = replay.Workload.WorkloadID
	replay.Operation.AllocationID = replay.AllocationID
	replay.OperationEventID = "new-candidate-operation-event-" + randomSuffix(t)
	// A new retry may observe a different provider/Pool candidate. The
	// original accepted Allocation remains authoritative and must be replayed.
	replay.ProviderID = "provider-newly-selected"
	replay.PoolID = "pool-newly-selected"
	replay.Operation.ProviderID = replay.ProviderID

	got, created, err := repo.AcceptWorkload(ctx, replay)
	if err != nil || created {
		t.Fatalf("idempotent replay: created=%t err=%v", created, err)
	}
	if got.Workload.WorkloadID != first.Workload.WorkloadID ||
		got.Allocation.AllocationID != first.Allocation.AllocationID ||
		got.Operation.ID != first.Operation.ID ||
		got.WorkloadEventID != first.WorkloadEventID ||
		got.OperationEventID != first.OperationEventID {
		t.Fatalf("replay did not return committed identities/events: first=%+v got=%+v", first, got)
	}

	var workloadEvents, operationEvents int
	if err := db.QueryRowContext(ctx, "SELECT count(*) FROM compute_outbox_events WHERE tenant_id=$1 AND aggregate_type='compute_workload' AND aggregate_id=$2",
		params.Workload.TenantID, first.Workload.WorkloadID).Scan(&workloadEvents); err != nil {
		t.Fatal(err)
	}
	if err := db.QueryRowContext(ctx, "SELECT count(*) FROM compute_outbox_events WHERE tenant_id=$1 AND aggregate_type='compute_operation' AND aggregate_id=$2",
		params.Workload.TenantID, first.Operation.ID).Scan(&operationEvents); err != nil {
		t.Fatal(err)
	}
	if workloadEvents != 1 || operationEvents != 1 {
		t.Fatalf("replay emitted duplicate events: workload=%d operation=%d", workloadEvents, operationEvents)
	}
}

func TestAcceptWorkloadConcurrentReplayHasOneWinner(t *testing.T) {
	db := openIntegrationDB(t)
	repo, _ := NewRepository(db)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	base := acceptanceFixture(t, "tenant-accept-concurrent-"+randomSuffix(t),
		"workload-key-"+randomSuffix(t), "operation-key-"+randomSuffix(t),
		"event-operation-"+randomSuffix(t))
	const workers = 12
	results := make([]WorkloadAcceptance, workers)
	created := make([]bool, workers)
	errs := make([]error, workers)
	var wg sync.WaitGroup
	wg.Add(workers)
	for i := 0; i < workers; i++ {
		i := i
		go func() {
			defer wg.Done()
			params := base
			params.Workload.WorkloadID = fmt.Sprintf("candidate-workload-%d-%s", i, randomSuffix(t))
			params.Workload.EventID = fmt.Sprintf("candidate-workload-event-%d-%s", i, randomSuffix(t))
			params.AllocationID = fmt.Sprintf("candidate-allocation-%d-%s", i, randomSuffix(t))
			params.Operation.ID = fmt.Sprintf("candidate-operation-%d-%s", i, randomSuffix(t))
			params.Operation.WorkloadID = params.Workload.WorkloadID
			params.Operation.AllocationID = params.AllocationID
			params.OperationEventID = fmt.Sprintf("candidate-operation-event-%d-%s", i, randomSuffix(t))
			params.ProviderID = fmt.Sprintf("provider-candidate-%d", i)
			params.PoolID = fmt.Sprintf("pool-candidate-%d", i)
			params.Operation.ProviderID = params.ProviderID
			results[i], created[i], errs[i] = repo.AcceptWorkload(ctx, params)
		}()
	}
	wg.Wait()

	winners := 0
	for i := range results {
		if errs[i] != nil {
			t.Fatalf("concurrent AcceptWorkload[%d]: %v", i, errs[i])
		}
		if created[i] {
			winners++
		}
		if results[i].Workload.WorkloadID != results[0].Workload.WorkloadID ||
			results[i].Allocation.AllocationID != results[0].Allocation.AllocationID ||
			results[i].Operation.ID != results[0].Operation.ID ||
			results[i].Allocation.ProviderID != results[0].Allocation.ProviderID ||
			results[i].Operation.ProviderID != results[0].Operation.ProviderID {
			t.Fatalf("concurrent callers received different accepted identities: %+v vs %+v", results[i], results[0])
		}
	}
	if winners != 1 {
		t.Fatalf("created=true count = %d, want exactly one", winners)
	}
}

func TestAcceptWorkloadOutboxFailureRollsBackAllAcceptanceRows(t *testing.T) {
	db := openIntegrationDB(t)
	repo, _ := NewRepository(db)
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	tenant := "tenant-accept-rollback-" + randomSuffix(t)
	collisionEventID := "event-collision-" + randomSuffix(t)
	first := acceptanceFixture(t, tenant,
		"workload-key-"+randomSuffix(t), "operation-key-"+randomSuffix(t), collisionEventID)
	if _, created, err := repo.AcceptWorkload(ctx, first); err != nil || !created {
		t.Fatalf("seed acceptance: created=%t err=%v", created, err)
	}

	second := acceptanceFixture(t, tenant,
		"workload-key-"+randomSuffix(t), "operation-key-"+randomSuffix(t), collisionEventID)
	_, created, err := repo.AcceptWorkload(ctx, second)
	if err == nil || created {
		t.Fatalf("duplicate operation event should abort acceptance: created=%t err=%v", created, err)
	}

	var workloadCount, allocationCount, operationCount, acceptedEventCount int
	if err := db.QueryRowContext(ctx, "SELECT count(*) FROM compute_workloads WHERE tenant_id=$1 AND workload_id=$2",
		second.Workload.TenantID, second.Workload.WorkloadID).Scan(&workloadCount); err != nil {
		t.Fatal(err)
	}
	if err := db.QueryRowContext(ctx, "SELECT count(*) FROM compute_allocations WHERE tenant_id=$1 AND allocation_id=$2",
		second.Workload.TenantID, second.AllocationID).Scan(&allocationCount); err != nil {
		t.Fatal(err)
	}
	if err := db.QueryRowContext(ctx, "SELECT count(*) FROM compute_operations WHERE tenant_id=$1 AND compute_operation_id=$2",
		second.Workload.TenantID, second.Operation.ID).Scan(&operationCount); err != nil {
		t.Fatal(err)
	}
	if err := db.QueryRowContext(ctx, "SELECT count(*) FROM compute_outbox_events WHERE tenant_id=$1 AND aggregate_type='compute_workload' AND aggregate_id=$2 AND event_type='compute.workload.accepted.v1'",
		second.Workload.TenantID, second.Workload.WorkloadID).Scan(&acceptedEventCount); err != nil {
		t.Fatal(err)
	}
	if workloadCount != 0 || allocationCount != 0 || operationCount != 0 || acceptedEventCount != 0 {
		t.Fatalf("partial acceptance survived rollback workload/allocation/operation/event = %d/%d/%d/%d",
			workloadCount, allocationCount, operationCount, acceptedEventCount)
	}
}

func TestOperationAllocationForeignKeyRejectsProviderAndGenerationMismatch(t *testing.T) {
	db := openIntegrationDB(t)
	repo, _ := NewRepository(db)
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	params := acceptanceFixture(t, "tenant-allocation-fk-"+randomSuffix(t),
		"workload-key-"+randomSuffix(t), "operation-key-"+randomSuffix(t),
		"event-operation-"+randomSuffix(t))
	accepted, created, err := repo.AcceptWorkload(ctx, params)
	if err != nil || !created {
		t.Fatalf("seed accepted Allocation: created=%t err=%v", created, err)
	}

	cases := []struct {
		name       string
		providerID string
		generation int64
	}{
		{name: "provider mismatch", providerID: "provider-forged", generation: 1},
		{name: "generation mismatch", providerID: params.ProviderID, generation: 2},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := db.ExecContext(ctx, `INSERT INTO compute_operations (
				compute_operation_id, tenant_id, workload_id, allocation_id, provider_id,
				operation_kind, desired_generation, provider_request_digest, idempotency_key,
				version, fence_token, status
			) VALUES ($1,$2,$3,$4,$5,'CREATE',$6,$7,$8,1,0,'PENDING')`,
				"forged-operation-"+randomSuffix(t),
				params.Workload.TenantID,
				accepted.Workload.WorkloadID,
				accepted.Allocation.AllocationID,
				tc.providerID,
				tc.generation,
				"sha256:"+strings.Repeat("b", 64),
				"forged-idempotency-"+randomSuffix(t),
			)
			if err == nil {
				t.Fatalf("composite Allocation FK accepted %s", tc.name)
			}
		})
	}
}

func TestAcceptWorkloadRejectsCrossBoundOperationAndAllocation(t *testing.T) {
	params := acceptanceFixture(t, "tenant-accept-invalid", "workload-key", "operation-key", "event-op")
	params.Operation.ProviderID = "provider-other"
	if err := validateAcceptWorkload(params); !errors.Is(err, operation.ErrInvalid) {
		t.Fatalf("provider mismatch validation = %v, want operation.ErrInvalid", err)
	}

	params = acceptanceFixture(t, "tenant-accept-invalid", "workload-key-2", "operation-key-2", "event-op-2")
	params.Operation.AllocationID = "allocation-other"
	if err := validateAcceptWorkload(params); !errors.Is(err, operation.ErrInvalid) {
		t.Fatalf("allocation mismatch validation = %v, want operation.ErrInvalid", err)
	}
}

func TestStandaloneCreateMethodsDoNotEmitDispatchableAcceptanceEvents(t *testing.T) {
	db := openIntegrationDB(t)
	repo, _ := NewRepository(db)
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	params := acceptanceFixture(t, "tenant-standalone-"+randomSuffix(t),
		"workload-key-"+randomSuffix(t), "operation-key-"+randomSuffix(t),
		"event-operation-"+randomSuffix(t))
	workload, created, err := repo.CreateWorkload(ctx, params.Workload)
	if err != nil || !created {
		t.Fatalf("standalone workload create: created=%t err=%v", created, err)
	}
	op := params.Operation
	op.WorkloadID = workload.WorkloadID
	op.AllocationID = ""
	if _, _, err := repo.CreateOperation(ctx, op, params.OperationEventID, params.Workload.CorrelationID); err != nil {
		t.Fatal(err)
	}
	var dispatchableCount int
	if err := db.QueryRowContext(ctx, "SELECT count(*) FROM compute_outbox_events WHERE tenant_id=$1 AND event_type IN ('compute.workload.accepted.v1','compute.operation.pending.v1')",
		params.Workload.TenantID).Scan(&dispatchableCount); err != nil {
		t.Fatal(err)
	}
	if dispatchableCount != 0 {
		t.Fatalf("standalone primitives emitted %d dispatchable events", dispatchableCount)
	}
}

func TestAcceptWorkloadRejectsPartialExistingAcceptance(t *testing.T) {
	db := openIntegrationDB(t)
	repo, _ := NewRepository(db)
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	params := acceptanceFixture(t, "tenant-accept-partial-"+randomSuffix(t),
		"workload-key-"+randomSuffix(t), "operation-key-"+randomSuffix(t),
		"event-operation-"+randomSuffix(t))
	workload, created, err := repo.CreateWorkload(ctx, params.Workload)
	if err != nil || !created {
		t.Fatalf("seed low-level Workload: created=%t err=%v", created, err)
	}
	params.Workload.WorkloadID = workload.WorkloadID
	params.Operation.WorkloadID = workload.WorkloadID
	_, _, err = repo.AcceptWorkload(ctx, params)
	if !errors.Is(err, ErrAcceptanceIncomplete) {
		t.Fatalf("partial historical acceptance error = %v, want ErrAcceptanceIncomplete", err)
	}
}
