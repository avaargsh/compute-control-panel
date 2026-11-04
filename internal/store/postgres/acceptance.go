package postgres

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"strings"

	"github.com/avaargsh/compute-control-panel/internal/operation"
)

const (
	workloadAcceptedV1 = "compute.workload.accepted.v1"
	operationPendingV1 = "compute.operation.pending.v1"
)

var ErrAcceptanceIncomplete = errors.New("workload acceptance is missing allocation, operation, or outbox evidence")

type AcceptWorkloadParams struct {
	Workload         CreateWorkloadParams
	AllocationID     string
	ProviderID       string
	PoolID           string
	Operation        operation.Operation
	OperationEventID string
}

type Allocation struct {
	AllocationID      string
	TenantID          string
	WorkloadID        string
	ProviderID        string
	PoolID            string
	DesiredGeneration int64
	Phase             string
	Version           int64
}

type WorkloadAcceptance struct {
	Workload         Workload
	Allocation       Allocation
	Operation        operation.Operation
	WorkloadEventID  string
	OperationEventID string
}

type allocationScanner interface {
	Scan(...any) error
}

func scanAllocation(row allocationScanner) (Allocation, error) {
	var a Allocation
	err := row.Scan(
		&a.AllocationID,
		&a.TenantID,
		&a.WorkloadID,
		&a.ProviderID,
		&a.PoolID,
		&a.DesiredGeneration,
		&a.Phase,
		&a.Version,
	)
	if err != nil {
		return Allocation{}, err
	}
	if a.AllocationID == "" || a.TenantID == "" || a.WorkloadID == "" ||
		a.ProviderID == "" || a.PoolID == "" ||
		a.DesiredGeneration <= 0 || a.Version <= 0 {
		return Allocation{}, errors.New("invalid persisted Allocation")
	}
	return a, nil
}

const allocationColumns = "allocation_id, tenant_id, workload_id, provider_id, pool_id, desired_generation, phase, version"

func validateAcceptWorkload(p AcceptWorkloadParams) error {
	if err := validateCreateWorkload(p.Workload); err != nil {
		return err
	}
	if strings.TrimSpace(p.AllocationID) == "" ||
		strings.TrimSpace(p.ProviderID) == "" ||
		strings.TrimSpace(p.PoolID) == "" ||
		strings.TrimSpace(p.OperationEventID) == "" ||
		p.OperationEventID == p.Workload.EventID {
		return operation.ErrInvalid
	}
	o := p.Operation
	if err := o.Validate(); err != nil {
		return err
	}
	if o.Status != operation.Pending || o.Version != 1 || o.FenceToken != 0 ||
		!validOperationCounters(o) || o.Kind != operation.Create ||
		o.Generation != 1 || o.TenantID != p.Workload.TenantID ||
		o.WorkloadID != p.Workload.WorkloadID ||
		o.AllocationID != p.AllocationID || o.ProviderID != p.ProviderID ||
		o.TargetResourceID != "" || o.TargetResourceUID != "" {
		return operation.ErrInvalid
	}
	return nil
}

// AcceptWorkload is the only initial Workload acceptance boundary. It commits
// the Workload, a stable pending Allocation, its immutable CREATE Operation,
// and both outbox events in one transaction. The operation.pending event is
// therefore impossible to publish without the matching Operation and
// Allocation already being durable.
func (r *Repository) AcceptWorkload(ctx context.Context, p AcceptWorkloadParams) (WorkloadAcceptance, bool, error) {
	if err := validateAcceptWorkload(p); err != nil {
		return WorkloadAcceptance{}, false, err
	}
	tx, err := r.db.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelReadCommitted})
	if err != nil {
		return WorkloadAcceptance{}, false, fmt.Errorf("begin workload acceptance: %w", err)
	}
	defer func() { _ = tx.Rollback() }()

	const insertWorkload = "INSERT INTO compute_workloads (" +
		"workload_id, tenant_id, project_id, owner_service, owner_resource_type, " +
		"owner_resource_id, compute_profile_id, compute_profile_revision, " +
		"normalized_request, request_digest, idempotency_key, desired_generation, " +
		"version, desired_state, observed_generation, observed_phase " +
		") VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9::jsonb,$10,$11,1,1,'ACTIVE',0,'ACCEPTED') " +
		"ON CONFLICT (tenant_id, idempotency_key) DO NOTHING " +
		"RETURNING workload_id, tenant_id, project_id, request_digest, idempotency_key, " +
		"desired_generation, version, desired_state, observed_phase, " +
		"latest_allocation_id, created_at, updated_at"

	workload, err := scanWorkload(tx.QueryRowContext(ctx, insertWorkload,
		p.Workload.WorkloadID, p.Workload.TenantID, p.Workload.ProjectID,
		p.Workload.OwnerService, p.Workload.OwnerResourceType, p.Workload.OwnerResourceID,
		nullableString(p.Workload.ComputeProfileID), nullableString(p.Workload.ComputeProfileRevision),
		[]byte(p.Workload.NormalizedRequest), p.Workload.RequestDigest, p.Workload.IdempotencyKey,
	))
	if errors.Is(err, sql.ErrNoRows) {
		accepted, replayErr := r.readAcceptanceReplay(ctx, tx, p)
		if replayErr != nil {
			return WorkloadAcceptance{}, false, replayErr
		}
		if err := tx.Commit(); err != nil {
			return WorkloadAcceptance{}, false, fmt.Errorf("commit workload acceptance replay: %w", err)
		}
		return accepted, false, nil
	}
	if err != nil {
		return WorkloadAcceptance{}, false, fmt.Errorf("insert accepted Workload: %w", err)
	}

	insertAllocation := "INSERT INTO compute_allocations (" +
		"allocation_id, tenant_id, workload_id, provider_id, pool_id, " +
		"desired_generation, phase, version" +
		") VALUES ($1,$2,$3,$4,$5,1,'PENDING',1) RETURNING " + allocationColumns
	allocation, err := scanAllocation(tx.QueryRowContext(ctx, insertAllocation,
		p.AllocationID, p.Workload.TenantID, workload.WorkloadID, p.ProviderID, p.PoolID,
	))
	if err != nil {
		return WorkloadAcceptance{}, false, fmt.Errorf("insert initial Allocation: %w", err)
	}

	result, err := tx.ExecContext(ctx, "UPDATE compute_workloads "+
		"SET latest_allocation_id=$1, updated_at=clock_timestamp() "+
		"WHERE tenant_id=$2 AND workload_id=$3 AND desired_generation=1 "+
		"AND latest_allocation_id IS NULL",
		allocation.AllocationID, workload.TenantID, workload.WorkloadID,
	)
	if err != nil {
		return WorkloadAcceptance{}, false, fmt.Errorf("project initial Allocation: %w", err)
	}
	updated, err := result.RowsAffected()
	if err != nil {
		return WorkloadAcceptance{}, false, err
	}
	if updated != 1 {
		return WorkloadAcceptance{}, false, errors.New("initial Allocation projection lost its Workload generation")
	}
	workload.LatestAllocationID = sql.NullString{String: allocation.AllocationID, Valid: true}

	o := p.Operation
	insertOperation := "INSERT INTO compute_operations (" +
		"compute_operation_id, tenant_id, workload_id, allocation_id, provider_id, " +
		"operation_kind, desired_generation, provider_request_digest, idempotency_key, " +
		"version, fence_token, status, target_resource_id, target_resource_uid" +
		") VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,1,0,'PENDING',$10,$11) RETURNING " + operationColumns
	createdOperation, err := scanOperation(tx.QueryRowContext(ctx, insertOperation,
		o.ID, o.TenantID, o.WorkloadID, allocation.AllocationID, o.ProviderID,
		o.Kind, int64(o.Generation), o.RequestDigest, o.IdempotencyKey,
		nullableString(o.TargetResourceID), nullableString(o.TargetResourceUID),
	))
	if err != nil {
		return WorkloadAcceptance{}, false, fmt.Errorf("insert initial ComputeOperation: %w", err)
	}

	workloadPayload, err := json.Marshal(map[string]any{
		"workload_id":        workload.WorkloadID,
		"desired_generation": workload.DesiredGeneration,
		"request_digest":     workload.RequestDigest,
		"allocation_id":      allocation.AllocationID,
		"operation_id":       createdOperation.ID,
	})
	if err != nil {
		return WorkloadAcceptance{}, false, fmt.Errorf("marshal accepted Workload event: %w", err)
	}
	operationPayload, err := json.Marshal(map[string]any{
		"operation_id":       createdOperation.ID,
		"workload_id":        workload.WorkloadID,
		"allocation_id":      allocation.AllocationID,
		"provider_id":        createdOperation.ProviderID,
		"desired_generation": createdOperation.Generation,
		"operation_kind":     string(createdOperation.Kind),
		"request_digest":     createdOperation.RequestDigest,
	})
	if err != nil {
		return WorkloadAcceptance{}, false, fmt.Errorf("marshal pending Operation event: %w", err)
	}

	const insertOutbox = "INSERT INTO compute_outbox_events (" +
		"event_id, producer, event_type, schema_version, tenant_id, " +
		"aggregate_type, aggregate_id, aggregate_version, occurred_at, " +
		"correlation_id, causation_id, payload, delivery_state" +
		") VALUES ($1,$2,$3,1,$4,$5,$6,1,clock_timestamp(),$7,$8,$9::jsonb,'PENDING')"
	if _, err := tx.ExecContext(ctx, insertOutbox,
		p.Workload.EventID, producerName, workloadAcceptedV1, workload.TenantID,
		"compute_workload", workload.WorkloadID, p.Workload.CorrelationID,
		nullableString(p.Workload.CausationID), workloadPayload,
	); err != nil {
		return WorkloadAcceptance{}, false, fmt.Errorf("insert accepted Workload outbox event: %w", err)
	}
	if _, err := tx.ExecContext(ctx, insertOutbox,
		p.OperationEventID, producerName, operationPendingV1, workload.TenantID,
		"compute_operation", createdOperation.ID, p.Workload.CorrelationID,
		p.Workload.EventID, operationPayload,
	); err != nil {
		return WorkloadAcceptance{}, false, fmt.Errorf("insert pending Operation outbox event: %w", err)
	}

	if err := tx.Commit(); err != nil {
		return WorkloadAcceptance{}, false, fmt.Errorf("commit Workload acceptance: %w", err)
	}
	return WorkloadAcceptance{
		Workload:         workload,
		Allocation:       allocation,
		Operation:        createdOperation,
		WorkloadEventID:  p.Workload.EventID,
		OperationEventID: p.OperationEventID,
	}, true, nil
}

func (r *Repository) readAcceptanceReplay(ctx context.Context, tx *sql.Tx, p AcceptWorkloadParams) (WorkloadAcceptance, error) {
	const selectWorkload = "SELECT workload_id, tenant_id, project_id, request_digest, " +
		"idempotency_key, desired_generation, version, desired_state, observed_phase, " +
		"latest_allocation_id, created_at, updated_at " +
		"FROM compute_workloads WHERE tenant_id=$1 AND idempotency_key=$2"
	workload, err := scanWorkload(tx.QueryRowContext(ctx, selectWorkload,
		p.Workload.TenantID, p.Workload.IdempotencyKey,
	))
	if err != nil {
		return WorkloadAcceptance{}, fmt.Errorf("read idempotent Workload acceptance: %w", err)
	}
	if workload.RequestDigest != p.Workload.RequestDigest {
		return WorkloadAcceptance{}, ErrIdempotencyConflict
	}

	var sameWorkloadIdentity bool
	if err := tx.QueryRowContext(ctx, "SELECT "+
		"project_id=$3 AND owner_service=$4 AND owner_resource_type=$5 "+
		"AND owner_resource_id=$6 "+
		"AND compute_profile_id IS NOT DISTINCT FROM $7::text "+
		"AND compute_profile_revision IS NOT DISTINCT FROM $8::text "+
		"FROM compute_workloads WHERE tenant_id=$1 AND idempotency_key=$2",
		p.Workload.TenantID, p.Workload.IdempotencyKey, p.Workload.ProjectID,
		p.Workload.OwnerService, p.Workload.OwnerResourceType, p.Workload.OwnerResourceID,
		nullableString(p.Workload.ComputeProfileID), nullableString(p.Workload.ComputeProfileRevision),
	).Scan(&sameWorkloadIdentity); err != nil {
		return WorkloadAcceptance{}, fmt.Errorf("verify replay Workload identity: %w", err)
	}
	if !sameWorkloadIdentity {
		return WorkloadAcceptance{}, ErrIdempotencyConflict
	}

	allocation, err := scanAllocation(tx.QueryRowContext(ctx, "SELECT "+
		allocationColumns+" FROM compute_allocations "+
		"WHERE tenant_id=$1 AND workload_id=$2 AND desired_generation=1",
		workload.TenantID, workload.WorkloadID,
	))
	if errors.Is(err, sql.ErrNoRows) {
		return WorkloadAcceptance{}, ErrAcceptanceIncomplete
	}
	if err != nil {
		return WorkloadAcceptance{}, fmt.Errorf("read accepted Allocation: %w", err)
	}
	if allocation.ProviderID != p.ProviderID || allocation.PoolID != p.PoolID {
		return WorkloadAcceptance{}, ErrIdempotencyConflict
	}

	o, err := scanOperation(tx.QueryRowContext(ctx,
		"SELECT "+operationColumns+" FROM compute_operations WHERE tenant_id=$1 AND idempotency_key=$2",
		workload.TenantID, p.Operation.IdempotencyKey,
	))
	if errors.Is(err, sql.ErrNoRows) {
		return WorkloadAcceptance{}, ErrAcceptanceIncomplete
	}
	if err != nil {
		return WorkloadAcceptance{}, fmt.Errorf("read accepted ComputeOperation: %w", err)
	}
	if o.WorkloadID != workload.WorkloadID || o.AllocationID != allocation.AllocationID ||
		!sameAcceptanceOperationIntent(o, p.Operation) {
		return WorkloadAcceptance{}, ErrIdempotencyConflict
	}

	var workloadEventID, operationEventID string
	var workloadPayload, operationPayload []byte
	err = tx.QueryRowContext(ctx, "SELECT event_id, payload FROM compute_outbox_events "+
		"WHERE tenant_id=$1 AND aggregate_type='compute_workload' AND aggregate_id=$2 "+
		"AND aggregate_version=1 AND event_type=$3",
		workload.TenantID, workload.WorkloadID, workloadAcceptedV1,
	).Scan(&workloadEventID, &workloadPayload)
	if errors.Is(err, sql.ErrNoRows) {
		return WorkloadAcceptance{}, ErrAcceptanceIncomplete
	}
	if err != nil {
		return WorkloadAcceptance{}, fmt.Errorf("read accepted Workload event: %w", err)
	}
	err = tx.QueryRowContext(ctx, "SELECT event_id, payload FROM compute_outbox_events "+
		"WHERE tenant_id=$1 AND aggregate_type='compute_operation' AND aggregate_id=$2 "+
		"AND aggregate_version=1 AND event_type=$3",
		workload.TenantID, o.ID, operationPendingV1,
	).Scan(&operationEventID, &operationPayload)
	if errors.Is(err, sql.ErrNoRows) {
		return WorkloadAcceptance{}, ErrAcceptanceIncomplete
	}
	if err != nil {
		return WorkloadAcceptance{}, fmt.Errorf("read pending Operation event: %w", err)
	}

	var wPayload struct {
		WorkloadID   string `json:"workload_id"`
		AllocationID string `json:"allocation_id"`
		OperationID  string `json:"operation_id"`
	}
	var oPayload struct {
		OperationID  string `json:"operation_id"`
		WorkloadID   string `json:"workload_id"`
		AllocationID string `json:"allocation_id"`
	}
	if json.Unmarshal(workloadPayload, &wPayload) != nil ||
		json.Unmarshal(operationPayload, &oPayload) != nil ||
		wPayload.WorkloadID != workload.WorkloadID ||
		wPayload.AllocationID != allocation.AllocationID ||
		wPayload.OperationID != o.ID ||
		oPayload.OperationID != o.ID ||
		oPayload.WorkloadID != workload.WorkloadID ||
		oPayload.AllocationID != allocation.AllocationID {
		return WorkloadAcceptance{}, ErrAcceptanceIncomplete
	}

	return WorkloadAcceptance{
		Workload:         workload,
		Allocation:       allocation,
		Operation:        o,
		WorkloadEventID:  workloadEventID,
		OperationEventID: operationEventID,
	}, nil
}

func sameAcceptanceOperationIntent(existing, requested operation.Operation) bool {
	return existing.TenantID == requested.TenantID &&
		existing.ProviderID == requested.ProviderID &&
		existing.IdempotencyKey == requested.IdempotencyKey &&
		existing.RequestDigest == requested.RequestDigest &&
		existing.Generation == requested.Generation &&
		existing.Kind == requested.Kind &&
		existing.TargetResourceID == requested.TargetResourceID &&
		existing.TargetResourceUID == requested.TargetResourceUID
}
