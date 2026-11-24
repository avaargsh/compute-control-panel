package postgres

import (
	"context"
	"database/sql"
	"time"

	"github.com/avaargsh/compute-control-panel/internal/provider"
)

// ValidateRecordedReleaseSameTx derives the current release fence from
// authoritative PostgreSQL rows inside the caller-owned transaction. The
// caller supplies only tenant-scoped lookup identities; resource UID,
// Provider ID, generations and row versions are never caller assertions.
//
// The caller retains ALL database row locks (Workload, Allocation, CREATE,
// DELETE, and the registered DRA source key) until that same transaction ends.
// Do not perform a protected state transition in another transaction after
// this preflight.
//
// A successful return is still evidence-only. This function does NOT
// authenticate the Kubernetes/Provider collector transport, prove that a
// driver executed NodeUnprepareResources, prove physical GPU deallocation, or
// authorize RELEASED/Cost FINAL. No durable state is mutated.
func ValidateRecordedReleaseSameTx(
	ctx context.Context, tx *sql.Tx,
	tenantID, workloadID, allocationID, createOperationID, deleteOperationID string,
	verifier *provider.DRAUnprepareVerifier,
	candidate provider.ReleaseEvidenceCandidate,
	signed provider.SignedDRAUnprepare,
	asOf time.Time,
) error {
	if ctx == nil || tx == nil {
		return ErrReleaseCommitFence
	}
	verified, err := verifier.Correlate(candidate, signed, asOf)
	if err != nil {
		return err
	}
	fence, err := ResolveReleaseCommitFence(
		ctx, tx, tenantID, workloadID, allocationID, createOperationID, deleteOperationID,
	)
	if err != nil {
		return err
	}
	if err := ValidateCorrelatedReleasePreflight(ctx, tx, fence, verified.Release); err != nil {
		return err
	}
	return provider.RequireRecordedDRAUnprepareReceipt(
		ctx, tx, verifier, candidate, signed, asOf,
	)
}
