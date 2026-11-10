package postgres

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"strings"

	"github.com/avaargsh/compute-control-panel/internal/operation"
)

// EnqueueSupersededCleanup records only a non-dispatchable, UID-fenced DELETE
// intent for a previously inspected CREATE from an older Workload generation.
// The original SUPERSEDED record is immutable and remains cleanup_required;
// only an independently authorized Provider/Kueue/device release chain can
// settle that liability. This routine does not call Provider.Apply, authorize
// external cleanup, make an Allocation RELEASED or close metering.
//
// CreateOperation intentionally refuses old generations; this dedicated
// lineage-bound path is the only exception and never accepts an arbitrary
// target UID or caller-authored operation request fingerprint.
func (r *Repository) EnqueueSupersededCleanup(
	ctx context.Context, tenantID, sourceOperationID string, expectedVersion uint64,
) (operation.Operation, bool, error) {
	if tenantID == "" || tenantID != strings.TrimSpace(tenantID) ||
		sourceOperationID == "" || sourceOperationID != strings.TrimSpace(sourceOperationID) ||
		expectedVersion == 0 || expectedVersion > math.MaxInt64 {
		return operation.Operation{}, false, operation.ErrInvalid
	}
	tx, err := r.db.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelReadCommitted})
	if err != nil {
		return operation.Operation{}, false, fmt.Errorf("begin superseded cleanup: %w", err)
	}
	defer func() { _ = tx.Rollback() }()

	// Lock the exact historical operation before constructing the target.
	// Competing controllers serialize on this row, not on an ambiguous Job
	// name or a caller-supplied Provider UID.
	source, err := scanOperation(tx.QueryRowContext(ctx,
		"SELECT "+operationColumns+" FROM public.compute_operations WHERE tenant_id=$1 AND compute_operation_id=$2 FOR UPDATE",
		tenantID, sourceOperationID))
	if err != nil {
		return operation.Operation{}, false, fmt.Errorf("read historical CREATE: %w", err)
	}
	if source.Version != expectedVersion {
		return operation.Operation{}, false, operation.ErrStale
	}
	if source.Kind != operation.Create || source.Status != operation.Superseded ||
		!source.CleanupRequired || !source.ReconcileRequired ||
		source.ProviderResourceID == "" || source.ProviderResourceUID == "" {
		return operation.Operation{}, false, operation.ErrTransition
	}
	var currentGeneration int64
	if err := tx.QueryRowContext(ctx, `SELECT desired_generation
		FROM public.compute_workloads WHERE tenant_id=$1 AND workload_id=$2 FOR SHARE`,
		tenantID, source.WorkloadID).Scan(&currentGeneration); err != nil {
		return operation.Operation{}, false, fmt.Errorf("read cleanup owner generation: %w", err)
	}
	if currentGeneration <= 0 || uint64(currentGeneration) <= source.Generation {
		return operation.Operation{}, false, operation.ErrStale
	}

	// Deterministic operation identity / digest across restarts. The digest
	// commits all ownership, generation and exact Provider resource fields;
	// no caller controls either the operation identity or deletion target.
	identity := sha256.Sum256([]byte(tenantID + "\x00" + source.ID))
	id := "cleanup-" + hex.EncodeToString(identity[:])
	fingerprint, err := json.Marshal(struct {
		TenantID, WorkloadID, AllocationID, ProviderID, SourceOperationID string
		SourceGeneration                                                  uint64
		ResourceID, ResourceUID                                           string
	}{
		source.TenantID, source.WorkloadID, source.AllocationID, source.ProviderID, source.ID,
		source.Generation, source.ProviderResourceID, source.ProviderResourceUID,
	})
	if err != nil {
		return operation.Operation{}, false, err
	}
	digest := sha256.Sum256(fingerprint)
	intent := operation.Operation{
		ID: id, TenantID: source.TenantID, WorkloadID: source.WorkloadID,
		AllocationID: source.AllocationID, ProviderID: source.ProviderID,
		IdempotencyKey: "cleanup:" + source.ID,
		RequestDigest:  "sha256:" + hex.EncodeToString(digest[:]),
		Generation:     source.Generation, Version: 1, Kind: operation.Delete,
		Status: operation.Pending, TargetResourceID: source.ProviderResourceID,
		TargetResourceUID: source.ProviderResourceUID,
	}
	if err := intent.Validate(); err != nil {
		return operation.Operation{}, false, err
	}

	// Replays read the existing immutable operation and must never replace a
	// target UID. A terminal or in-flight cleanup remains replayable without
	// resetting status/fence or emitting another event.
	existing, err := scanOperation(tx.QueryRowContext(ctx,
		"SELECT "+operationColumns+" FROM public.compute_operations WHERE tenant_id=$1 AND idempotency_key=$2 FOR UPDATE",
		tenantID, intent.IdempotencyKey))
	if err == nil {
		if existing.ID != intent.ID || !sameOperationIntent(existing, intent) {
			return operation.Operation{}, false, ErrIdempotencyConflict
		}
		if err := tx.Commit(); err != nil {
			return operation.Operation{}, false, fmt.Errorf("commit cleanup replay: %w", err)
		}
		return existing, false, nil
	}
	if !errors.Is(err, sql.ErrNoRows) {
		return operation.Operation{}, false, fmt.Errorf("read existing cleanup: %w", err)
	}
	const insert = `INSERT INTO public.compute_operations(
		compute_operation_id,tenant_id,workload_id,allocation_id,provider_id,operation_kind,
		desired_generation,provider_request_digest,idempotency_key,version,fence_token,
		status,target_resource_id,target_resource_uid)
		VALUES($1,$2,$3,$4,$5,'DELETE',$6,$7,$8,1,0,'PENDING',$9,$10)
		ON CONFLICT (tenant_id,idempotency_key) DO NOTHING
		RETURNING ` + operationColumns
	created, err := scanOperation(tx.QueryRowContext(ctx, insert,
		intent.ID, intent.TenantID, intent.WorkloadID, nullableString(intent.AllocationID),
		intent.ProviderID, int64(intent.Generation), intent.RequestDigest, intent.IdempotencyKey,
		intent.TargetResourceID, intent.TargetResourceUID))
	if errors.Is(err, sql.ErrNoRows) {
		return operation.Operation{}, false, ErrIdempotencyConflict
	}
	if err != nil {
		return operation.Operation{}, false, fmt.Errorf("insert cleanup DELETE: %w", err)
	}

	// recorded.v1 is a *non-dispatchable* journal event. It supplies durable
	// evidence for a future authorization/fencing worker, not permission to
	// mutate the Provider or stop its cost-liability lease.
	payload, err := json.Marshal(map[string]any{
		"operation_id": created.ID, "workload_id": created.WorkloadID,
		"allocation_id": created.AllocationID, "provider_id": created.ProviderID,
		"desired_generation": created.Generation, "operation_kind": created.Kind,
		"request_digest":              created.RequestDigest,
		"cleanup_source_operation_id": source.ID,
		"target_resource_id":          created.TargetResourceID,
		"target_resource_uid":         created.TargetResourceUID,
		"dispatch_authorized":         false,
	})
	if err != nil {
		return operation.Operation{}, false, err
	}
	_, err = tx.ExecContext(ctx, `INSERT INTO public.compute_outbox_events(
		event_id,producer,event_type,schema_version,tenant_id,
		aggregate_type,aggregate_id,aggregate_version,occurred_at,
		correlation_id,payload,delivery_state)
		VALUES($1,$2,'compute.operation.recorded.v1',1,$3,
			'compute_operation',$4,1,pg_catalog.clock_timestamp(),$5,$6::jsonb,'PENDING')`,
		"cleanup-event-"+hex.EncodeToString(identity[:]), producerName,
		intent.TenantID, intent.ID, source.ID, payload)
	if err != nil {
		return operation.Operation{}, false, fmt.Errorf("append cleanup outbox: %w", err)
	}
	if err := tx.Commit(); err != nil {
		return operation.Operation{}, false, fmt.Errorf("commit cleanup intent: %w", err)
	}
	return created, true, nil
}
