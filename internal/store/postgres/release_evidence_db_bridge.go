package postgres

import (
	"context"
	"database/sql"
	"strings"

	"github.com/avaargsh/compute-control-panel/internal/provider"
)

// presentReleaseIdentity checks only the shape of an already supplied token.
// It never authenticates the source of that token.
func presentReleaseIdentity(value string) bool {
	return value != "" && strings.TrimSpace(value) == value &&
		!strings.ContainsRune(value, 0)
}

// correlatedReleaseMatchesFence is a NECESSARY cross-layer identity check,
// not evidence authentication. ReleaseEvidenceCorrelation is a public Go
// struct; a caller can fabricate one. No code may treat this predicate as
// permission to release a GPU or finalize cost.
func correlatedReleaseMatchesFence(
	p ReleaseCommitFence, c provider.ReleaseEvidenceCorrelation,
) bool {
	if !validReleaseCommitFence(p) ||
		c.TenantID != p.TenantID ||
		c.WorkloadID != p.WorkloadID ||
		c.AllocationID != p.AllocationID ||
		c.ProviderID != p.ProviderID ||
		c.DeleteOperationID != p.DeleteOperationID {
		return false
	}
	job, pod, claim, device := c.Job, c.Pod, c.Claim, c.Device
	for _, value := range []string{
		job.Namespace, job.Name, job.UID, job.ResourceVersion,
		pod.Namespace, pod.Name, pod.UID, pod.ResourceVersion,
		claim.Namespace, claim.Name, claim.UID, claim.ResourceVersion,
		device.Request, device.Driver, device.Pool, device.Device,
	} {
		if !presentReleaseIdentity(value) {
			return false
		}
	}
	if strings.ContainsAny(job.Namespace, "/\n\r") ||
		strings.ContainsAny(job.Name, "/\n\r") ||
		p.TargetResourceID != job.Namespace+"/"+job.Name ||
		p.TargetResourceUID != job.UID ||
		pod.Namespace != job.Namespace ||
		claim.Namespace != job.Namespace {
		return false
	}
	return true
}

// ValidateCorrelatedReleasePreflight checks that separately supplied
// structural Kubernetes/Provider identity tuples refer to exactly the same
// historical target as the CURRENT, LOCKED PostgreSQL Workload, Allocation,
// CREATE and newer-generation DELETE rows. It locks and reads ONLY: no UPDATE,
// dispatch, permit, RELEASED transition, receipt consumption or cost posting.
//
// A successful return does NOT prove that the correlation came from a trusted
// collector, that a driver performed NodeUnprepare, or that a device is free.
// The caller must never treat this as an authorization result. Any future
// state-changing action must independently authenticate all sources and run
// under the SAME transaction as the durable check; a two-transaction
// preflight/commit split is an unsafe TOCTOU pattern.
func ValidateCorrelatedReleasePreflight(
	ctx context.Context, tx *sql.Tx,
	p ReleaseCommitFence, c provider.ReleaseEvidenceCorrelation,
) error {
	if ctx == nil || tx == nil || !correlatedReleaseMatchesFence(p, c) {
		return ErrReleaseCommitFence
	}
	return ValidateReleaseCommitFence(ctx, tx, p)
}
