package postgres

import (
	"context"
	"database/sql"
	"time"

	"github.com/avaargsh/compute-control-panel/internal/provider"
)

// ValidateRecordedReleaseSameTx is a read-only NECESSARY evidence preflight.
// It verifies three independently scoped claims within the *same caller-owned
// PostgreSQL transaction*:
//  1. structural source observations and the signed NodeUnprepare assertion
//     correlate to one historical Allocation, Job/Pod/Claim and device;
//  2. the current Workload, Allocation and both CREATE/DELETE Operation rows
//     still match the exact Generation, version, Provider and target UID;
//  3. the signed custom receipt is in the immutable journal, and its DBA
//     registered source key is live, correctly scoped and not revoked.
//
// The caller retains ALL database row locks (Workload, Allocation, CREATE,
// DELETE, source key) until that same transaction ends. Do not perform a
// protected state transition in another transaction after this check.
//
// This function does NOT authenticate snapshot/driver transport; a registered
// signer can still assert something untrue about real NodeUnprepareResources.
// It creates no permit, no RELEASED or cost FINAL writes, and is not a
// production release decision or physical GPU deallocation proof.
func ValidateRecordedReleaseSameTx(
	ctx context.Context, tx *sql.Tx, fence ReleaseCommitFence,
	verifier *provider.DRAUnprepareVerifier,
	candidate provider.ReleaseEvidenceCandidate,
	signed provider.SignedDRAUnprepare, asOf time.Time,
) error {
	if ctx == nil || tx == nil {
		return ErrReleaseCommitFence
	}
	verified, err := verifier.Correlate(candidate, signed, asOf)
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
