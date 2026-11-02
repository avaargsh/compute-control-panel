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
	"time"

	"github.com/avaargsh/compute-control-panel/internal/operation"
)

// InspectionEvidence must originate from a trusted Provider adapter after an
// authoritative Inspect. It is an internal persistence boundary, not an HTTP
// request DTO, attestation or proof of source authenticity.
type InspectionEvidence struct {
	ObservationID string
	EventID       string
	CorrelationID string
	Source        string
	AdapterVersion string
	Observation   operation.Observation
}

func (e InspectionEvidence) validate() error {
	for _, value := range []string{e.ObservationID, e.EventID, e.CorrelationID, e.Source, e.AdapterVersion} {
		if value == "" || value != strings.TrimSpace(value) || len(value) > 256 {
			return operation.ErrObservation
		}
	}
	return nil
}

// ReconcileOperation atomically records an inspection, advances the operation
// with CAS/fencing, and publishes the new operation version in the Outbox.
// The provider must be inspected OUTSIDE this database transaction, never
// while holding a database lock. No Allocation state is inferred from mere
// resource existence: Kueue admission/device realization needs independent
// evidence and a previously authorized allocation identity.
func (r *Repository) ReconcileOperation(
	ctx context.Context, tenantID, operationID string, expectedVersion uint64,
	leaseOwner string, fenceToken uint64, evidence InspectionEvidence,
) (operation.Operation, error) {
	if tenantID == "" || operationID == "" || leaseOwner == "" ||
		expectedVersion == 0 || expectedVersion > math.MaxInt64 ||
		fenceToken == 0 || fenceToken > math.MaxInt64 {
		return operation.Operation{}, operation.ErrInvalid
	}
	if err := evidence.validate(); err != nil {
		return operation.Operation{}, err
	}
	tx, err := r.db.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelReadCommitted})
	if err != nil {
		return operation.Operation{}, fmt.Errorf("begin inspection transaction: %w", err)
	}
	defer func() { _ = tx.Rollback() }()

	current, err := scanOperation(tx.QueryRowContext(ctx,
		"SELECT "+operationColumns+" FROM compute_operations WHERE tenant_id=$1 AND compute_operation_id=$2 FOR UPDATE",
		tenantID, operationID))
	if err != nil {
		return operation.Operation{}, fmt.Errorf("read operation for inspection: %w", err)
	}
	if current.Version != expectedVersion {
		return operation.Operation{}, operation.ErrStale
	}
	var desiredGeneration int64
	if err := tx.QueryRowContext(ctx, `SELECT desired_generation FROM compute_workloads
		WHERE tenant_id=$1 AND workload_id=$2 FOR SHARE`,
		tenantID, current.WorkloadID).Scan(&desiredGeneration); err != nil {
		return operation.Operation{}, fmt.Errorf("lock workload generation: %w", err)
	}
	var now time.Time
	if err := tx.QueryRowContext(ctx, "SELECT clock_timestamp()").Scan(&now); err != nil {
		return operation.Operation{}, fmt.Errorf("read database clock: %w", err)
	}

	next, err := current.ApplyObservation(
		expectedVersion, leaseOwner, fenceToken, now,
		uint64(desiredGeneration), evidence.Observation)
	if err != nil {
		return operation.Operation{}, err
	}
	if next.Version != current.Version+1 || !sameOperationIntent(current, next) ||
		!validOperationCounters(next) {
		return operation.Operation{}, operation.ErrInvalid
	}
	if err := next.Validate(); err != nil {
		return operation.Operation{}, err
	}

	// The phase remains UNKNOWN: provider operation finality cannot prove
	// Job/Pod readiness or Kueue admission. operation_result is a separate,
	// typed v0002 column, not a reinterpretation of the phase enum.
	observationJSON, err := json.Marshal(evidence.Observation)
	if err != nil {
		return operation.Operation{}, fmt.Errorf("encode provider inspection: %w", err)
	}
	hash := sha256.Sum256(observationJSON)
	digest := "sha256:" + hex.EncodeToString(hash[:])
	_, err = tx.ExecContext(ctx, `INSERT INTO compute_resource_observations(
		observation_id, tenant_id, workload_id, compute_operation_id,
		provider_id, desired_generation, resource_kind,
		provider_resource_id, provider_resource_uid, provider_resource_version,
		result, operation_result, source, adapter_version, observed_at,
		source_timestamp, response_digest, details)
		VALUES($1,$2,$3,$4,$5,$6,'provider.operation',
			$7,$8,$9,'UNKNOWN',$10,$11,$12,$13,$14,$15,$16::jsonb)`,
		evidence.ObservationID, current.TenantID, current.WorkloadID, current.ID,
		current.ProviderID, int64(current.Generation),
		nullableString(evidence.Observation.ResourceID),
		nullableString(evidence.Observation.ResourceUID),
		evidence.Observation.ResourceVersion, evidence.Observation.Result,
		evidence.Source, evidence.AdapterVersion, evidence.Observation.ObservedAt,
		evidence.Observation.ObservedAt, digest, observationJSON)
	if err != nil {
		return operation.Operation{}, fmt.Errorf("append provider inspection: %w", err)
	}

	const update = `UPDATE compute_operations SET
		version=$4, fence_token=$5, status=$6, lease_owner=$7,
		lease_expires_at=$8, provider_resource_id=$9, provider_resource_uid=$10,
		error_code=$11, manual_review_code=$12, cleanup_required=$13,
		reconcile_required=$14, updated_at=clock_timestamp()
		WHERE tenant_id=$1 AND compute_operation_id=$2 AND version=$3
			AND fence_token=$15`
	res, err := tx.ExecContext(ctx, update, tenantID, operationID, int64(current.Version),
		int64(next.Version), int64(next.FenceToken), next.Status,
		nullableString(next.LeaseOwner), nullableTime(next.LeaseExpiresAt),
		nullableString(next.ProviderResourceID), nullableString(next.ProviderResourceUID),
		nullableString(next.ErrorCode), nullableString(next.ManualReviewCode),
		next.CleanupRequired, next.ReconcileRequired, int64(current.FenceToken))
	if err != nil {
		return operation.Operation{}, fmt.Errorf("update reconciled operation: %w", err)
	}
	affected, err := res.RowsAffected()
	if err != nil {
		return operation.Operation{}, err
	}
	if affected != 1 {
		return operation.Operation{}, operation.ErrStale
	}
	eventPayload, err := json.Marshal(struct {
		OperationID string `json:"operation_id"`
		WorkloadID string `json:"workload_id"`
		ObservationID string `json:"observation_id"`
		Status operation.Status `json:"status"`
		DesiredGeneration uint64 `json:"desired_generation"`
		CleanupRequired bool `json:"cleanup_required"`
		ReconcileRequired bool `json:"reconcile_required"`
	}{
		OperationID: next.ID, WorkloadID: next.WorkloadID,
		ObservationID: evidence.ObservationID, Status: next.Status,
		DesiredGeneration: next.Generation, CleanupRequired: next.CleanupRequired,
		ReconcileRequired: next.ReconcileRequired,
	})
	if err != nil {
		return operation.Operation{}, fmt.Errorf("marshal operation reconciliation event: %w", err)
	}
	_, err = tx.ExecContext(ctx, `INSERT INTO compute_outbox_events(
		event_id, producer, event_type, schema_version, tenant_id,
		aggregate_type, aggregate_id, aggregate_version, occurred_at,
		correlation_id, causation_id, payload, delivery_state)
		VALUES($1,$2,'compute.operation.reconciled.v1',1,$3,
			'compute_operation',$4,$5,clock_timestamp(),$6,$7,$8::jsonb,'PENDING')`,
		evidence.EventID, producerName, tenantID, operationID,
		int64(next.Version), evidence.CorrelationID,
		evidence.ObservationID, eventPayload)
	if err != nil {
		return operation.Operation{}, fmt.Errorf("append reconciliation outbox: %w", err)
	}
	if err := tx.Commit(); err != nil {
		return operation.Operation{}, fmt.Errorf("commit inspection result: %w", err)
	}
	return next, nil
}

// compile-time reminders: use transaction errors as returned, not ambiguous
// success indicators. The caller must reconcile transaction commit UNKNOWN.
var _ = errors.Is
