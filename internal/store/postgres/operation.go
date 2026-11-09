package postgres

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"strings"
	"time"

	"github.com/avaargsh/compute-control-panel/internal/operation"
)

// Operation writes are serialized per row and guarded by version and fencing
// token. All temporal checks use PostgreSQL clock_timestamp(), not worker time.
// The repository does NOT invoke a provider; UNKNOWN must be inspected before
// any retry and its authoritative observation is deliberately not exposed here.
const operationColumns = `compute_operation_id, tenant_id, workload_id, allocation_id,
    provider_id, idempotency_key, provider_request_digest, desired_generation,
    version, fence_token, operation_kind, status, lease_owner, lease_expires_at,
    target_resource_id, target_resource_uid, provider_resource_id,
    provider_resource_uid, error_code, manual_review_code,
    cleanup_required, reconcile_required`

type operationScanner interface{ Scan(...any) error }

func scanOperation(row operationScanner) (operation.Operation, error) {
	var o operation.Operation
	var generation, version, fence int64
	var allocationID, leaseOwner, targetID, targetUID, resourceID, resourceUID sql.NullString
	var errorCode, manualCode sql.NullString
	var leaseUntil sql.NullTime
	err := row.Scan(
		&o.ID, &o.TenantID, &o.WorkloadID, &allocationID, &o.ProviderID,
		&o.IdempotencyKey, &o.RequestDigest, &generation, &version, &fence,
		&o.Kind, &o.Status, &leaseOwner, &leaseUntil,
		&targetID, &targetUID, &resourceID, &resourceUID,
		&errorCode, &manualCode, &o.CleanupRequired, &o.ReconcileRequired,
	)
	if err != nil {
		return operation.Operation{}, err
	}
	if generation <= 0 || version <= 0 || fence < 0 {
		return operation.Operation{}, errors.New("invalid persisted operation counters")
	}
	o.AllocationID = allocationID.String
	o.Generation, o.Version, o.FenceToken = uint64(generation), uint64(version), uint64(fence)
	o.LeaseOwner, o.LeaseExpiresAt = leaseOwner.String, leaseUntil.Time
	o.TargetResourceID, o.TargetResourceUID = targetID.String, targetUID.String
	o.ProviderResourceID, o.ProviderResourceUID = resourceID.String, resourceUID.String
	o.ErrorCode, o.ManualReviewCode = errorCode.String, manualCode.String
	if err := o.Validate(); err != nil {
		return operation.Operation{}, fmt.Errorf("invalid persisted operation: %w", err)
	}
	return o, nil
}

func validOperationCounters(o operation.Operation) bool {
	return o.Generation <= math.MaxInt64 && o.Version <= math.MaxInt64 && o.FenceToken <= math.MaxInt64
}

// GetOperation always filters by tenant, including during recovery.
func (r *Repository) GetOperation(ctx context.Context, tenantID, operationID string) (operation.Operation, error) {
	if strings.TrimSpace(tenantID) == "" || strings.TrimSpace(operationID) == "" {
		return operation.Operation{}, operation.ErrInvalid
	}
	return scanOperation(r.db.QueryRowContext(ctx,
		"SELECT "+operationColumns+" FROM public.compute_operations WHERE tenant_id=$1 AND compute_operation_id=$2",
		tenantID, operationID))
}

func sameOperationIntent(a, b operation.Operation) bool {
	return a.TenantID == b.TenantID && a.WorkloadID == b.WorkloadID &&
		a.AllocationID == b.AllocationID && a.ProviderID == b.ProviderID && a.IdempotencyKey == b.IdempotencyKey &&
		a.RequestDigest == b.RequestDigest && a.Generation == b.Generation &&
		a.Kind == b.Kind && a.TargetResourceID == b.TargetResourceID &&
		a.TargetResourceUID == b.TargetResourceUID
}

// CreateOperation is a low-level repository primitive. It records an immutable
// pending intent with a non-dispatchable event. Initial provider dispatch must
// use AcceptWorkload so Allocation and Operation are committed atomically.
func (r *Repository) CreateOperation(ctx context.Context, o operation.Operation, eventID, correlationID string) (operation.Operation, bool, error) {
	if err := o.Validate(); err != nil {
		return operation.Operation{}, false, err
	}
	if o.Status != operation.Pending || o.Version != 1 || o.FenceToken != 0 ||
		!validOperationCounters(o) || strings.TrimSpace(eventID) == "" || strings.TrimSpace(correlationID) == "" {
		return operation.Operation{}, false, operation.ErrInvalid
	}
	tx, err := r.db.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelReadCommitted})
	if err != nil {
		return operation.Operation{}, false, fmt.Errorf("begin create operation: %w", err)
	}
	defer func() { _ = tx.Rollback() }()

	// Existing idempotency history remains replayable even when the parent
	// workload has since advanced to a newer desired generation. Replay
	// creates no new side effect and does not claim dispatch authority.
	historical, historyErr := scanOperation(tx.QueryRowContext(ctx,
		"SELECT "+operationColumns+" FROM public.compute_operations WHERE tenant_id=$1 AND idempotency_key=$2",
		o.TenantID, o.IdempotencyKey))
	if historyErr == nil {
		if !sameOperationIntent(historical, o) {
			return operation.Operation{}, false, ErrIdempotencyConflict
		}
		if err := tx.Commit(); err != nil {
			return operation.Operation{}, false, fmt.Errorf("commit historical replay: %w", err)
		}
		return historical, false, nil
	}
	if !errors.Is(historyErr, sql.ErrNoRows) {
		return operation.Operation{}, false, historyErr
	}

	// Ensure the intent belongs to the current workload generation. The
	// workload lock excludes concurrent generation advances until commit.
	var currentGeneration int64
	err = tx.QueryRowContext(ctx, `SELECT desired_generation FROM public.compute_workloads
        WHERE tenant_id=$1 AND workload_id=$2 FOR SHARE`, o.TenantID, o.WorkloadID).Scan(&currentGeneration)
	if err != nil {
		return operation.Operation{}, false, fmt.Errorf("read operation workload: %w", err)
	}
	if uint64(currentGeneration) != o.Generation {
		return operation.Operation{}, false, operation.ErrStale
	}

	const insert = `INSERT INTO public.compute_operations(
        compute_operation_id,tenant_id,workload_id,allocation_id,provider_id,operation_kind,
        desired_generation,provider_request_digest,idempotency_key,version,fence_token,
        status,target_resource_id,target_resource_uid)
        VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,1,0,'PENDING',$10,$11)
        ON CONFLICT(tenant_id,idempotency_key) DO NOTHING
        RETURNING ` + operationColumns
	got, err := scanOperation(tx.QueryRowContext(ctx, insert, o.ID, o.TenantID, o.WorkloadID,
		nullableString(o.AllocationID), o.ProviderID, o.Kind, int64(o.Generation), o.RequestDigest, o.IdempotencyKey,
		nullableString(o.TargetResourceID), nullableString(o.TargetResourceUID)))
	if err == nil {
		payload, err := json.Marshal(map[string]any{
			"operation_id": got.ID, "workload_id": got.WorkloadID,
			"allocation_id": got.AllocationID, "provider_id": got.ProviderID, "desired_generation": got.Generation,
			"operation_kind": got.Kind, "request_digest": got.RequestDigest,
		})
		if err != nil {
			return operation.Operation{}, false, err
		}
		_, err = tx.ExecContext(ctx, `INSERT INTO public.compute_outbox_events(
            event_id,producer,event_type,schema_version,tenant_id,
            aggregate_type,aggregate_id,aggregate_version,occurred_at,
            correlation_id,payload,delivery_state)
            VALUES($1,$2,'compute.operation.recorded.v1',1,$3,
                'compute_operation',$4,1,clock_timestamp(),$5,$6::jsonb,'PENDING')`,
			eventID, producerName, o.TenantID, o.ID, correlationID, payload)
		if err != nil {
			return operation.Operation{}, false, fmt.Errorf("insert operation outbox: %w", err)
		}
		if err := tx.Commit(); err != nil {
			return operation.Operation{}, false, fmt.Errorf("commit operation create: %w", err)
		}
		return got, true, nil
	}
	if !errors.Is(err, sql.ErrNoRows) {
		return operation.Operation{}, false, fmt.Errorf("insert operation: %w", err)
	}
	// A concurrent INSERT waits on the idempotency uniqueness conflict, and
	// READ COMMITTED then observes its committed row in this new statement.
	existing, err := scanOperation(tx.QueryRowContext(ctx,
		"SELECT "+operationColumns+" FROM public.compute_operations WHERE tenant_id=$1 AND idempotency_key=$2",
		o.TenantID, o.IdempotencyKey))
	if err != nil {
		return operation.Operation{}, false, fmt.Errorf("read operation replay: %w", err)
	}
	if !sameOperationIntent(existing, o) {
		return operation.Operation{}, false, ErrIdempotencyConflict
	}
	if err := tx.Commit(); err != nil {
		return operation.Operation{}, false, fmt.Errorf("commit operation replay: %w", err)
	}
	return existing, false, nil
}

// mutateOperation is intentionally private: external callers cannot bypass
// the domain's legal transition rules by supplying arbitrary next states.
func (r *Repository) mutateOperation(ctx context.Context, tenantID, operationID string, expectedVersion uint64,
	transition func(operation.Operation, time.Time) (operation.Operation, error)) (operation.Operation, error) {
	if strings.TrimSpace(tenantID) == "" || strings.TrimSpace(operationID) == "" ||
		expectedVersion == 0 || expectedVersion > math.MaxInt64 {
		return operation.Operation{}, operation.ErrInvalid
	}
	tx, err := r.db.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelReadCommitted})
	if err != nil {
		return operation.Operation{}, err
	}
	defer func() { _ = tx.Rollback() }()
	current, err := scanOperation(tx.QueryRowContext(ctx,
		"SELECT "+operationColumns+" FROM public.compute_operations WHERE tenant_id=$1 AND compute_operation_id=$2 FOR UPDATE",
		tenantID, operationID))
	if err != nil {
		return operation.Operation{}, err
	}
	if current.Version != expectedVersion {
		return operation.Operation{}, operation.ErrStale
	}
	var databaseNow time.Time
	if err := tx.QueryRowContext(ctx, "SELECT clock_timestamp()").Scan(&databaseNow); err != nil {
		return operation.Operation{}, fmt.Errorf("database clock: %w", err)
	}
	next, err := transition(current, databaseNow)
	if err != nil {
		return operation.Operation{}, err
	}
	if !sameOperationIntent(current, next) || next.ID != current.ID ||
		next.Version != current.Version+1 || !validOperationCounters(next) {
		return operation.Operation{}, operation.ErrInvalid
	}
	if err := next.Validate(); err != nil {
		return operation.Operation{}, err
	}
	// Claim and Start must not dispatch an operation whose workload has
	// advanced. FOR SHARE stabilizes the generation until this CAS commits.
	if next.Status == operation.Leased || next.Status == operation.InFlight {
		var generation int64
		err := tx.QueryRowContext(ctx, `SELECT desired_generation FROM public.compute_workloads
            WHERE tenant_id=$1 AND workload_id=$2 FOR SHARE`, tenantID, current.WorkloadID).Scan(&generation)
		if err != nil {
			return operation.Operation{}, fmt.Errorf("check workload generation: %w", err)
		}
		if uint64(generation) != current.Generation {
			return operation.Operation{}, operation.ErrStale
		}
	}
	const update = `UPDATE public.compute_operations SET
        version=$4,fence_token=$5,status=$6,lease_owner=$7,lease_expires_at=$8,
        provider_resource_id=$9,provider_resource_uid=$10,
        error_code=$11,manual_review_code=$12,cleanup_required=$13,
        reconcile_required=$14,updated_at=clock_timestamp()
        WHERE tenant_id=$1 AND compute_operation_id=$2 AND version=$3
          AND fence_token=$15`
	result, err := tx.ExecContext(ctx, update, tenantID, operationID, int64(current.Version),
		int64(next.Version), int64(next.FenceToken), next.Status,
		nullableString(next.LeaseOwner), nullableTime(next.LeaseExpiresAt),
		nullableString(next.ProviderResourceID), nullableString(next.ProviderResourceUID),
		nullableString(next.ErrorCode), nullableString(next.ManualReviewCode),
		next.CleanupRequired, next.ReconcileRequired, int64(current.FenceToken))
	if err != nil {
		return operation.Operation{}, fmt.Errorf("persist operation transition: %w", err)
	}
	count, err := result.RowsAffected()
	if err != nil {
		return operation.Operation{}, err
	}
	if count != 1 {
		return operation.Operation{}, operation.ErrStale
	}
	if err := tx.Commit(); err != nil {
		return operation.Operation{}, fmt.Errorf("commit operation transition: %w", err)
	}
	return next, nil
}

func nullableTime(t time.Time) any {
	if t.IsZero() {
		return nil
	}
	return t
}

func (r *Repository) ClaimOperation(ctx context.Context, tenantID, operationID string, expectedVersion uint64, owner string, ttl time.Duration) (operation.Operation, error) {
	if !operation.ValidLeaseTTL(ttl) {
		return operation.Operation{}, operation.ErrInvalid
	}
	return r.mutateOperation(ctx, tenantID, operationID, expectedVersion, func(o operation.Operation, now time.Time) (operation.Operation, error) {
		return o.Claim(expectedVersion, owner, now, ttl)
	})
}

func (r *Repository) StartOperation(ctx context.Context, tenantID, operationID string, expectedVersion uint64, owner string, fence uint64) (operation.Operation, error) {
	return r.mutateOperation(ctx, tenantID, operationID, expectedVersion, func(o operation.Operation, now time.Time) (operation.Operation, error) {
		return o.Start(expectedVersion, owner, fence, now)
	})
}

func (r *Repository) MarkOperationUnknown(ctx context.Context, tenantID, operationID string, expectedVersion uint64, owner string, fence uint64) (operation.Operation, error) {
	return r.mutateOperation(ctx, tenantID, operationID, expectedVersion, func(o operation.Operation, now time.Time) (operation.Operation, error) {
		return o.MarkUnknown(expectedVersion, owner, fence, now)
	})
}

func (r *Repository) ExpireOperationLease(ctx context.Context, tenantID, operationID string, expectedVersion uint64) (operation.Operation, error) {
	return r.mutateOperation(ctx, tenantID, operationID, expectedVersion, func(o operation.Operation, now time.Time) (operation.Operation, error) {
		return o.ExpireLease(expectedVersion, now)
	})
}

func (r *Repository) BeginOperationReconciliation(ctx context.Context, tenantID, operationID string, expectedVersion uint64,
	currentOwner string, currentToken uint64, newOwner string, ttl time.Duration) (operation.Operation, error) {
	if !operation.ValidLeaseTTL(ttl) {
		return operation.Operation{}, operation.ErrInvalid
	}
	return r.mutateOperation(ctx, tenantID, operationID, expectedVersion, func(o operation.Operation, now time.Time) (operation.Operation, error) {
		return o.BeginReconciliation(expectedVersion, currentOwner, currentToken, newOwner, now, ttl)
	})
}
