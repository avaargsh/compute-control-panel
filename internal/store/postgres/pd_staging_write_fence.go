package postgres

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"time"

	"github.com/avaargsh/compute-control-panel/internal/operation"
)

// ErrPDStagingWriteUncertain means an external CREATE may have happened but
// its durable acknowledgement/transaction outcome is not yet authoritative.
// The only safe next action is inspect-first reconciliation, never a new POST.
var ErrPDStagingWriteUncertain = errors.New("PD JobSet staging side effect needs inspect-first reconciliation")

// WithCurrentPDStagingFence invokes a trusted staging-only side effect while
// an IN_FLIGHT Operation and the independently administered PD authority are
// locked in the SAME PostgreSQL transaction. Workload, Provider, fence,
// Allocation and Operation locks span the callback, so revoke, drain or
// generation changes cannot commit between source verification and the
// external CREATE. The callback MUST use the separately restricted JobSet
// staging ServiceAccount (GET/CREATE only) and MUST honor its context.
//
// This method is intended ONLY for inert, suspended, AdmissionGatedBy JobSet
// creation. It is NOT an activation permit, a generic provider dispatcher, a
// physical GPU attestation or permission to run Pods. The callback cannot
// start its own transaction or perform another repository operation on these
// locked objects: such re-entry would deadlock and breach the time budget.
//
// The independent authority may expire while a write is in flight, although
// revocation writes remain blocked by the row lock. If callback outcome is
// uncertain, or either deadline passes, the Operation is moved to UNKNOWN
// durably when possible. A database failure after a Kubernetes POST can
// leave IN_FLIGHT until lease expiry, which is ALSO inspect-first; this
// function deliberately never retries or automatically adopts a JobSet UID.
func (r *Repository) WithCurrentPDStagingFence(
	ctx context.Context, owner PDIntentOwner, intentID string,
	expectedVersion uint64, worker string, fenceToken uint64,
	stageCreate func(context.Context, PDStageLease) error,
) (PDStageLease, error) {
	if r == nil || r.db == nil || !validPDIntentOwner(owner) ||
		!validPDIntentDigest(intentID) || stageCreate == nil ||
		!validPDWorker(worker, expectedVersion, fenceToken, time.Millisecond) ||
		fenceToken == 0 {
		return PDStageLease{}, operation.ErrInvalid
	}
	tx, err := r.db.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelReadCommitted})
	if err != nil {
		return PDStageLease{}, err
	}
	defer tx.Rollback()
	stage, err := r.readPDStagingOperationLocked(ctx, tx, owner, intentID, operation.InFlight)
	if err != nil {
		return PDStageLease{}, err
	}
	if stage.Operation.Version != expectedVersion {
		return PDStageLease{}, operation.ErrStale
	}
	if stage.Operation.LeaseOwner != worker || stage.Operation.FenceToken != fenceToken {
		return PDStageLease{}, operation.ErrStaleLease
	}
	var now time.Time
	if err := tx.QueryRowContext(ctx, "SELECT pg_catalog.clock_timestamp()").Scan(&now); err != nil {
		return PDStageLease{}, err
	}
	if !now.Before(stage.Snapshot.AuthorityExpiresAt) {
		return PDStageLease{}, ErrPDPromotionBlocked
	}
	if !now.Before(stage.Operation.LeaseExpiresAt) {
		return PDStageLease{}, operation.ErrLeaseExpired
	}
	if stage.Operation.LeaseExpiresAt.After(stage.Snapshot.AuthorityExpiresAt) {
		return PDStageLease{}, ErrPDPromotionBlocked
	}

	// The DB clock defines the remaining time budget. Use its duration, not
	// wall-clock synchronization assumptions, to bound the external request.
	deadline := stage.Operation.LeaseExpiresAt
	if stage.Snapshot.AuthorityExpiresAt.Before(deadline) {
		deadline = stage.Snapshot.AuthorityExpiresAt
	}
	remaining := deadline.Sub(now)
	if remaining < time.Millisecond {
		return PDStageLease{}, ErrPDPromotionBlocked
	}
	callCtx, cancel := context.WithTimeout(ctx, remaining)
	defer cancel()
	// Network side effect executes WHILE the source, lease and provider are
	// row-locked. The callback must never trigger any work outside the inert
	// staging writer and must return on callCtx cancellation.
	applyErr := stageCreate(callCtx, stage)
	if err := tx.QueryRowContext(ctx, "SELECT pg_catalog.clock_timestamp()").Scan(&now); err != nil {
		return PDStageLease{}, fmt.Errorf("%w: database clock after callback: %v", ErrPDStagingWriteUncertain, err)
	}
	expired := !now.Before(stage.Operation.LeaseExpiresAt) ||
		!now.Before(stage.Snapshot.AuthorityExpiresAt)
	if applyErr != nil || expired {
		// Conservatively record UNKNOWN even if the external client reports
		// an error. The write may have reached Kubernetes before the network
		// or acknowledgement failed. A timed-out lease must advance its fence.
		var next operation.Operation
		if now.Before(stage.Operation.LeaseExpiresAt) {
			next, err = stage.Operation.MarkUnknown(expectedVersion, worker, fenceToken, now)
		} else {
			next, err = stage.Operation.ExpireLease(expectedVersion, now)
		}
		if err != nil {
			return PDStageLease{}, fmt.Errorf("%w: could not fence uncertain side effect: %v", ErrPDStagingWriteUncertain, err)
		}
		if err := writePDLeaseTransition(ctx, tx, stage.Operation, next); err != nil {
			return PDStageLease{}, fmt.Errorf("%w: could not persist UNKNOWN: %v", ErrPDStagingWriteUncertain, err)
		}
		if err := tx.Commit(); err != nil {
			return PDStageLease{}, fmt.Errorf("%w: UNKNOWN commit ambiguous: %v", ErrPDStagingWriteUncertain, err)
		}
		stage.Operation = next
		if applyErr != nil {
			return stage, fmt.Errorf("%w: %w", ErrPDStagingWriteUncertain, applyErr)
		}
		return stage, ErrPDStagingWriteUncertain
	}
	if err := tx.Commit(); err != nil {
		return PDStageLease{}, fmt.Errorf("%w: commit after external CREATE: %v", ErrPDStagingWriteUncertain, err)
	}
	// A successful POST is only an API acknowledgement; the Operation stays
	// IN_FLIGHT pending independent UID-pinned Inspect and Kueue/DRA evidence.
	return stage, nil
}
