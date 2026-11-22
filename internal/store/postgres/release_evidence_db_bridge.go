package postgres

import (
	"context"
	"database/sql"
	"regexp"
	"strings"

	"github.com/avaargsh/compute-control-panel/internal/provider"
)

// presentReleaseIdentity checks only the shape of an already supplied token.
// It never authenticates the source of that token.
func presentReleaseIdentity(value string) bool {
	return value != "" && strings.TrimSpace(value) == value &&
		!strings.ContainsRune(value, 0)
}

// These checks mirror the pinned Provider/Kubernetes parser boundaries:
// a Kubernetes DNS label (63 ASCII bytes), and a DRA identity (253 bytes,
// no ASCII controls, whitespace or backslash). Keep them deliberately
// independent of the untrusted caller-supplied correlation object.
var releaseKubeDNSLabel = regexp.MustCompile(`^[a-z0-9]([-a-z0-9]*[a-z0-9])?$`)

func validReleaseKubeName(value string) bool {
	return len(value) > 0 && len(value) <= 63 &&
		releaseKubeDNSLabel.MatchString(value)
}

func validReleaseDRAIdentity(value string) bool {
	return len(value) > 0 && len(value) <= 253 &&
		value == strings.TrimSpace(value) &&
		strings.IndexFunc(value, func(char rune) bool {
			return char <= 32 || char == 92
		}) == -1
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
	if !validReleaseKubeName(job.Namespace) ||
		!validReleaseKubeName(job.Name) ||
		!validReleaseKubeName(pod.Name) ||
		!validReleaseKubeName(claim.Name) ||
		!validReleaseKubeName(device.Request) ||
		!validReleaseDRAIdentity(job.UID) ||
		!validReleaseDRAIdentity(pod.UID) ||
		!validReleaseDRAIdentity(claim.UID) ||
		!validReleaseDRAIdentity(device.Driver) ||
		!validReleaseDRAIdentity(device.Pool) ||
		!validReleaseDRAIdentity(device.Device) ||
		(device.ShareID != "" && !validReleaseDRAIdentity(device.ShareID)) ||
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
