package postgres

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"math"
	"strings"
	"time"

	"github.com/avaargsh/compute-control-panel/internal/operation"
)

// PDStageLease is the durable, fenced Operation position and the immutable
// source snapshot checked at the SAME PostgreSQL transaction. It does NOT
// authorize a Kubernetes POST: a production side-effect gate must separately
// revalidate the current source fence and activation policy before any write.
type PDStageLease struct {
	Snapshot  PDStagingSnapshot
	Operation operation.Operation
}

// readPDStagingOperationLocked first locks the canonical Workload, then the
// source/proposal/promotion/Provider and canonical Operation rows. Workload ->
// source -> Operation matches the original promotion lock order, avoiding
// competing PD worker lock upgrades and the stale Generation read race.
func (r *Repository) readPDStagingOperationLocked(
	ctx context.Context, tx *sql.Tx, owner PDIntentOwner, intentID string,
	status operation.Status,
) (PDStageLease, error) {
	var workload string
	err := tx.QueryRowContext(ctx, `SELECT workload_id FROM public.compute_pd_intents
		WHERE intent_id=$1 AND tenant_id=$2 AND project_id=$3
		  AND owner_service=$4 AND owner_resource_type=$5 AND owner_resource_id=$6`,
		intentID, owner.TenantID, owner.ProjectID, owner.Service, owner.ResourceType, owner.ResourceID,
	).Scan(&workload)
	if errors.Is(err, sql.ErrNoRows) {
		return PDStageLease{}, ErrPDPromotionBlocked
	}
	if err != nil {
		return PDStageLease{}, err
	}
	if _, _, err := lockPDIntentWorkload(ctx, tx, owner, workload); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return PDStageLease{}, ErrPDPromotionBlocked
		}
		return PDStageLease{}, err
	}
	snapshot, err := readCurrentPDStagingSnapshotTx(ctx, tx, owner, intentID, status)
	if err != nil {
		return PDStageLease{}, err
	}
	op, err := scanOperation(tx.QueryRowContext(ctx,
		"SELECT "+operationColumns+` FROM public.compute_operations
		WHERE tenant_id=$1 AND compute_operation_id=$2 FOR UPDATE`,
		owner.TenantID, snapshot.Promotion.OperationID))
	if err != nil {
		return PDStageLease{}, err
	}
	if op.ID != "pd-operation-"+intentID ||
		op.AllocationID != snapshot.Promotion.AllocationID ||
		op.TenantID != snapshot.TenantID || op.WorkloadID != snapshot.WorkloadID ||
		op.ProviderID != snapshot.ProviderID || op.Generation != snapshot.Promotion.Generation ||
		op.Kind != operation.Create || op.Status != status ||
		op.IdempotencyKey != "pd-create-"+intentID ||
		op.RequestDigest != "sha256:"+intentID {
		return PDStageLease{}, ErrPDPromotionBlocked
	}
	return PDStageLease{Snapshot: snapshot, Operation: op}, nil
}

// writePDLeaseTransition changes ONLY the canonical version, token, status,
// lease owner and deadline, protected by the row lock and immutable intent
// identity verified by readPDStagingOperationLocked in the same transaction.
func writePDLeaseTransition(ctx context.Context, tx *sql.Tx, previous, next operation.Operation) error {
	if !sameOperationIntent(previous, next) || previous.ID != next.ID ||
		next.Version != previous.Version+1 || !validOperationCounters(next) {
		return operation.ErrInvalid
	}
	if err := next.Validate(); err != nil {
		return err
	}
	result, err := tx.ExecContext(ctx, `UPDATE public.compute_operations
		SET version=$4,fence_token=$5,status=$6,lease_owner=$7,
		    lease_expires_at=$8,updated_at=pg_catalog.clock_timestamp()
		WHERE tenant_id=$1 AND compute_operation_id=$2 AND version=$3
		  AND fence_token=$9 AND status=$10`,
		previous.TenantID, previous.ID, int64(previous.Version),
		int64(next.Version), int64(next.FenceToken), next.Status,
		nullableString(next.LeaseOwner), nullableTime(next.LeaseExpiresAt),
		int64(previous.FenceToken), previous.Status)
	if err != nil {
		return fmt.Errorf("persist PD staging lease transition: %w", err)
	}
	count, err := result.RowsAffected()
	if err != nil {
		return err
	}
	if count != 1 {
		return operation.ErrStale
	}
	return nil
}

func validPDWorker(owner string, expectedVersion, token uint64, ttl time.Duration) bool {
	return owner != "" && owner == strings.TrimSpace(owner) && len(owner) <= 256 &&
		expectedVersion > 0 && expectedVersion <= math.MaxInt64 &&
		token <= math.MaxInt64 && operation.ValidLeaseTTL(ttl)
}

// ClaimPDStagingOperation is the only authorized PENDING -> LEASED path for
// a promoted PD CREATE. Generic ClaimOperation intentionally rejects all PD
// promotions. Source revocation, expiry, old/unsupported GPU cardinality,
// wrong owner, stale generation or a draining Provider blocks the claim before
// any durable lease state is written. No Kubernetes action occurs here.
func (r *Repository) ClaimPDStagingOperation(
	ctx context.Context, owner PDIntentOwner, intentID string,
	expectedVersion uint64, worker string, ttl time.Duration,
) (PDStageLease, error) {
	if r == nil || r.db == nil || !validPDIntentOwner(owner) ||
		!validPDIntentDigest(intentID) || !validPDWorker(worker, expectedVersion, 0, ttl) {
		return PDStageLease{}, operation.ErrInvalid
	}
	tx, err := r.db.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelReadCommitted})
	if err != nil {
		return PDStageLease{}, err
	}
	defer tx.Rollback()
	stage, err := r.readPDStagingOperationLocked(ctx, tx, owner, intentID, operation.Pending)
	if err != nil {
		return PDStageLease{}, err
	}
	if stage.Operation.Version != expectedVersion {
		return PDStageLease{}, operation.ErrStale
	}
	var now time.Time
	if err := tx.QueryRowContext(ctx, "SELECT pg_catalog.clock_timestamp()").Scan(&now); err != nil {
		return PDStageLease{}, err
	}
	// Limit the lease lifetime to the independent current authority deadline;
	// a token must never outlive the source fence it was issued under.
	if !now.Add(ttl).Before(stage.Snapshot.AuthorityExpiresAt) {
		return PDStageLease{}, ErrPDPromotionBlocked
	}
	next, err := stage.Operation.Claim(expectedVersion, worker, now, ttl)
	if err != nil {
		return PDStageLease{}, err
	}
	if err := writePDLeaseTransition(ctx, tx, stage.Operation, next); err != nil {
		return PDStageLease{}, err
	}
	if err := tx.Commit(); err != nil {
		return PDStageLease{}, err
	}
	stage.Operation = next
	return stage, nil
}

// StartPDStagingOperation transitions the *same* fenced LEASED operation to
// IN_FLIGHT only while the exact independent source remains current, and only
// for its original lease owner, token and version. It is NOT proof a JobSet
// was created or an acknowledgement received. Subsequent writes must use an
// independently enforced side-effect gate; UNKNOWN always remains inspect-first.
func (r *Repository) StartPDStagingOperation(
	ctx context.Context, owner PDIntentOwner, intentID string,
	expectedVersion uint64, worker string, fence uint64,
) (PDStageLease, error) {
	if r == nil || r.db == nil || !validPDIntentOwner(owner) ||
		!validPDIntentDigest(intentID) ||
		!validPDWorker(worker, expectedVersion, fence, time.Minute) || fence == 0 {
		return PDStageLease{}, operation.ErrInvalid
	}
	tx, err := r.db.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelReadCommitted})
	if err != nil {
		return PDStageLease{}, err
	}
	defer tx.Rollback()
	stage, err := r.readPDStagingOperationLocked(ctx, tx, owner, intentID, operation.Leased)
	if err != nil {
		return PDStageLease{}, err
	}
	if stage.Operation.Version != expectedVersion {
		return PDStageLease{}, operation.ErrStale
	}
	var now time.Time
	if err := tx.QueryRowContext(ctx, "SELECT pg_catalog.clock_timestamp()").Scan(&now); err != nil {
		return PDStageLease{}, err
	}
	if !now.Before(stage.Snapshot.AuthorityExpiresAt) ||
		stage.Operation.LeaseExpiresAt.After(stage.Snapshot.AuthorityExpiresAt) {
		return PDStageLease{}, ErrPDPromotionBlocked
	}
	next, err := stage.Operation.Start(expectedVersion, worker, fence, now)
	if err != nil {
		return PDStageLease{}, err
	}
	if err := writePDLeaseTransition(ctx, tx, stage.Operation, next); err != nil {
		return PDStageLease{}, err
	}
	if err := tx.Commit(); err != nil {
		return PDStageLease{}, err
	}
	stage.Operation = next
	return stage, nil
}
