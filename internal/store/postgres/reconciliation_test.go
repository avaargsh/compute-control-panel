package postgres

import (
	"context"
	"database/sql"
	"errors"
	"testing"
	"time"

	"github.com/avaargsh/compute-control-panel/internal/operation"
	"github.com/avaargsh/compute-control-panel/internal/provider"
)

func setupReconciliation(t *testing.T) (*Repository, *sql.DB, context.Context, operation.Operation) {
	t.Helper()
	db := openIntegrationDB(t)
	repo, err := NewRepository(db)
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	initial := operationFixture(t, repo, ctx, "tenant-reconcile-"+randomSuffix(t))
	if _, _, err := repo.CreateOperation(ctx, initial, "event-initial-"+randomSuffix(t), "correlation-initial"); err != nil {
		t.Fatal(err)
	}
	claimed, err := repo.ClaimOperation(ctx, initial.TenantID, initial.ID, 1, "worker", time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	started, err := repo.StartOperation(ctx, initial.TenantID, initial.ID, claimed.Version, "worker", claimed.FenceToken)
	if err != nil {
		t.Fatal(err)
	}
	unknown, err := repo.MarkOperationUnknown(ctx, initial.TenantID, initial.ID, started.Version, "worker", started.FenceToken)
	if err != nil {
		t.Fatal(err)
	}
	reconciling, err := repo.BeginOperationReconciliation(ctx, initial.TenantID, initial.ID, unknown.Version, "", 0, "reconciler", time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	return repo, db, ctx, reconciling
}

func trustedObservation(o operation.Operation, result operation.ObservationResult) operation.Observation {
	obs := operation.Observation{
		OperationID: o.ID, TenantID: o.TenantID, WorkloadID: o.WorkloadID,
		ProviderID: o.ProviderID, DesiredGeneration: o.Generation, Result: result,
		ResourceVersion: "provider-rv-1", ObservedAt: time.Now().Add(-time.Second),
	}
	if result == operation.ResourcePresent {
		obs.ResourceID = "job-" + o.ID
		obs.ResourceUID = "uid-" + o.ID
		obs.ResourceOperationID = o.ID
		obs.ResourceGeneration = o.Generation
	}
	return obs
}

func inspectionEvidence(o operation.Operation, obs operation.Observation) InspectionEvidence {
	return InspectionEvidence{
		ObservationID: "inspection-" + o.ID, EventID: "event-reconciled-" + o.ID,
		CorrelationID: "corr-reconciled", Source: "trusted-fake-provider",
		AdapterVersion: "test-v1", Observation: obs,
	}
}

func TestReconcileAtomicEvidenceAndOutboxAfterLostACK(t *testing.T) {
	_, db, ctx, o := setupReconciliation(t)
	fake := provider.NewFakeProvider()
	req := provider.Request{
		OperationID: o.ID, TenantID: o.TenantID, WorkloadID: o.WorkloadID,
		ProviderID: o.ProviderID, IdempotencyKey: o.IdempotencyKey,
		RequestDigest: o.RequestDigest, DesiredGeneration: o.Generation, Kind: o.Kind,
	}
	fake.LoseNextAcknowledgement()
	if _, err := fake.Apply(ctx, req); !errors.Is(err, provider.ErrAcknowledgementLost) {
		t.Fatalf("fake must lose ACK after side effect: %v", err)
	}
	obs, err := fake.Inspect(ctx, req)
	if err != nil {
		t.Fatal(err)
	}
	restarted, _ := NewRepository(db)
	next, err := restarted.ReconcileOperation(ctx, o.TenantID, o.ID, o.Version, "reconciler", o.FenceToken, inspectionEvidence(o, obs))
	if err != nil || next.Status != operation.Succeeded || next.ProviderResourceUID != obs.ResourceUID {
		t.Fatalf("durable success from inspect: %+v %v", next, err)
	}
	var observations, events int
	if err := db.QueryRowContext(ctx, "SELECT count(*) FROM compute_resource_observations WHERE observation_id=$1 AND operation_result=$2", "inspection-"+o.ID, string(operation.ResourcePresent)).Scan(&observations); err != nil {
		t.Fatal(err)
	}
	if err := db.QueryRowContext(ctx, "SELECT count(*) FROM compute_outbox_events WHERE tenant_id=$1 AND aggregate_type='compute_operation' AND aggregate_id=$2", o.TenantID, o.ID).Scan(&events); err != nil {
		t.Fatal(err)
	}
	if observations != 1 || events != 2 {
		t.Fatalf("operation, observation and Outbox must commit together: observations=%d events=%d", observations, events)
	}
	// Operation state version advances on lease/ACK/recovery claims, but only
	// published events advance the aggregate event sequence. No gap is allowed.
	var eventSequence int64
	if err := db.QueryRowContext(ctx,
		"SELECT aggregate_version FROM compute_outbox_events WHERE event_id=$1",
		"event-reconciled-"+o.ID).Scan(&eventSequence); err != nil {
		t.Fatal(err)
	}
	if eventSequence != 2 {
		t.Fatalf("outbox event sequence skipped versions: got %d, want 2", eventSequence)
	}
	if _, err := restarted.ReconcileOperation(ctx, o.TenantID, o.ID, o.Version, "reconciler", o.FenceToken, inspectionEvidence(o, obs)); !errors.Is(err, operation.ErrStale) {
		t.Fatalf("stale reconciliation double committed: %v", err)
	}
}

func TestReconcileFailureRollsBackObservationAndState(t *testing.T) {
	repo, db, ctx, o := setupReconciliation(t)
	evidence := inspectionEvidence(o, trustedObservation(o, operation.ResourcePresent))
	// Reusing the initial operation's event ID must fail inside the transaction.
	if err := db.QueryRowContext(ctx, "SELECT event_id FROM compute_outbox_events WHERE tenant_id=$1 AND aggregate_id=$2", o.TenantID, o.ID).Scan(&evidence.EventID); err != nil {
		t.Fatal(err)
	}
	if _, err := repo.ReconcileOperation(ctx, o.TenantID, o.ID, o.Version, "reconciler", o.FenceToken, evidence); err == nil {
		t.Fatal("duplicate event ID did not abort the transaction")
	}
	persisted, err := repo.GetOperation(ctx, o.TenantID, o.ID)
	if err != nil || persisted.Version != o.Version || persisted.Status != operation.Reconciling {
		t.Fatalf("failed transaction mutated operation: %+v %v", persisted, err)
	}
	var count int
	if err := db.QueryRowContext(ctx, "SELECT count(*) FROM compute_resource_observations WHERE observation_id=$1", evidence.ObservationID).Scan(&count); err != nil || count != 0 {
		t.Fatalf("orphaned observation on transaction failure: count=%d err=%v", count, err)
	}
	evidence.EventID = "event-new-" + randomSuffix(t)
	if _, err := repo.ReconcileOperation(ctx, o.TenantID, o.ID, o.Version, "reconciler", o.FenceToken, evidence); err != nil {
		t.Fatalf("reconciliation cannot resume after rolled-back attempt: %v", err)
	}
}

func TestReconcileNotAppliedFinalPreservesSameKeyRetry(t *testing.T) {
	repo, db, ctx, o := setupReconciliation(t)
	evidence := inspectionEvidence(o, trustedObservation(o, operation.OperationNotAppliedFinal))
	var claimedAt time.Time
	if err := db.QueryRowContext(ctx, "SELECT updated_at FROM compute_operations WHERE tenant_id=$1 AND compute_operation_id=$2",
		o.TenantID, o.ID).Scan(&claimedAt); err != nil {
		t.Fatal(err)
	}
	// Successful finality must be observed AFTER the current recovery lease.
	evidence.Observation.ObservedAt = claimedAt.Add(time.Microsecond)
	next, err := repo.ReconcileOperation(ctx, o.TenantID, o.ID, o.Version, "reconciler", o.FenceToken, evidence)
	if err != nil || next.Status != operation.Pending || next.ID != o.ID || next.IdempotencyKey != o.IdempotencyKey || next.RequestDigest != o.RequestDigest {
		t.Fatalf("safe retry changed immutable identity: %+v %v", next, err)
	}
	if _, err := repo.ClaimOperation(ctx, o.TenantID, o.ID, next.Version, "worker-retry", time.Minute); err != nil {
		t.Fatalf("authoritative no-effect did not enable exact retry: %v", err)
	}
}

func TestReconcileStaleGenerationSchedulesCompensation(t *testing.T) {
	repo, db, ctx, o := setupReconciliation(t)
	if _, err := db.ExecContext(ctx, "UPDATE compute_workloads SET desired_generation=desired_generation+1,version=version+1 WHERE tenant_id=$1 AND workload_id=$2", o.TenantID, o.WorkloadID); err != nil {
		t.Fatal(err)
	}
	next, err := repo.ReconcileOperation(ctx, o.TenantID, o.ID, o.Version, "reconciler", o.FenceToken, inspectionEvidence(o, trustedObservation(o, operation.ResourcePresent)))
	if err != nil || next.Status != operation.Superseded || !next.CleanupRequired || !next.ReconcileRequired {
		t.Fatalf("stale create did not schedule compensation: %+v %v", next, err)
	}
}

func TestReconcileRejectsOldFenceAndForeignEvidence(t *testing.T) {
	repo, db, ctx, o := setupReconciliation(t)
	evidence := inspectionEvidence(o, trustedObservation(o, operation.ResourcePresent))
	if _, err := repo.ReconcileOperation(ctx, o.TenantID, o.ID, o.Version, "reconciler", o.FenceToken-1, evidence); !errors.Is(err, operation.ErrStaleLease) {
		t.Fatalf("old fence accepted: %v", err)
	}
	evidence.Observation.TenantID = "different-tenant"
	if _, err := repo.ReconcileOperation(ctx, o.TenantID, o.ID, o.Version, "reconciler", o.FenceToken, evidence); !errors.Is(err, operation.ErrObservation) {
		t.Fatalf("foreign observation accepted: %v", err)
	}
	var count int
	if err := db.QueryRowContext(ctx, "SELECT count(*) FROM compute_resource_observations WHERE compute_operation_id=$1", o.ID).Scan(&count); err != nil || count != 0 {
		t.Fatalf("invalid evidence persisted: count=%d err=%v", count, err)
	}
}

func TestReconcileRejectsPreLeaseNoEffectFinality(t *testing.T) {
	repo, db, ctx, o := setupReconciliation(t)
	evidence := inspectionEvidence(o, trustedObservation(o, operation.OperationNotAppliedFinal))
	// A final no-effect record from before this reconciliation lease
	// cannot prove that a more recent provider attempt did nothing.
	evidence.Observation.ObservedAt = time.Now().Add(-time.Hour)
	if _, err := repo.ReconcileOperation(ctx, o.TenantID, o.ID, o.Version, "reconciler", o.FenceToken, evidence); !errors.Is(err, operation.ErrObservation) {
		t.Fatalf("stale final no-effect evidence authorized a retry: %v", err)
	}
	persisted, err := repo.GetOperation(ctx, o.TenantID, o.ID)
	if err != nil || persisted.Status != operation.Reconciling || persisted.Version != o.Version {
		t.Fatalf("rejection changed durable operation: %+v %v", persisted, err)
	}
	var count int
	if err := db.QueryRowContext(ctx, "SELECT count(*) FROM compute_resource_observations WHERE observation_id=$1", evidence.ObservationID).Scan(&count); err != nil || count != 0 {
		t.Fatalf("stale evidence was appended: count=%d err=%v", count, err)
	}
}
