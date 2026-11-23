package postgres

import (
	"context"
	"database/sql"
	"errors"
	"strings"
)

// ErrReleaseCommitFence is returned whenever the current durable state cannot
// be attested to match the proposed release. It says NOTHING about device
// deallocation, trusted source receipts, or authorization to transition state.
var ErrReleaseCommitFence = errors.New("durable release generation fence did not match current state")

// ReleaseCommitFence pins a historical CREATE, a newer successful DELETE and
// the precise Allocation/Workload revisions a trusted release writer would
// need to re-check inside its own transaction. It is NOT a capability or a
// driver attestation. All identities come from trusted durable records.
type ReleaseCommitFence struct {
	TenantID               string
	WorkloadID             string
	AllocationID           string
	ProviderID             string
	CreateOperationID      string
	DeleteOperationID      string
	TargetResourceID       string
	TargetResourceUID      string
	AllocationGeneration   int64
	DeleteGeneration       int64
	WorkloadVersion        int64
	AllocationVersion      int64
	CreateOperationVersion int64
	DeleteOperationVersion int64
}

func validReleaseCommitFence(p ReleaseCommitFence) bool {
	for _, s := range []string{
		p.TenantID, p.WorkloadID, p.AllocationID, p.ProviderID,
		p.CreateOperationID, p.DeleteOperationID, p.TargetResourceID, p.TargetResourceUID,
	} {
		if s == "" || strings.TrimSpace(s) != s || strings.ContainsRune(s, 0) {
			return false
		}
	}
	return p.AllocationGeneration > 0 &&
		p.DeleteGeneration > p.AllocationGeneration &&
		p.WorkloadVersion > 0 && p.AllocationVersion > 0 &&
		p.CreateOperationVersion > 0 && p.DeleteOperationVersion > 0 &&
		p.CreateOperationID != p.DeleteOperationID
}

// releaseCommitFenceSnapshot is only a locked database state comparison. A
// matching snapshot cannot prove signed source provenance or physical release.
type releaseCommitFenceSnapshot struct {
	WorkloadGeneration   int64
	WorkloadVersion      int64
	DesiredState         string
	WorkloadPhase        string
	LatestAllocationID   string
	AllocationGeneration int64
	AllocationVersion    int64
	AllocationPhase      string
	CreateGeneration     int64
	CreateVersion        int64
	CreateKind           string
	CreateStatus         string
	CreateResourceID     string
	CreateResourceUID    string
	DeleteGeneration     int64
	DeleteVersion        int64
	DeleteKind           string
	DeleteStatus         string
	DeleteTargetID       string
	DeleteTargetUID      string
}

func (s releaseCommitFenceSnapshot) matches(p ReleaseCommitFence) bool {
	return validReleaseCommitFence(p) &&
		s.WorkloadGeneration == p.DeleteGeneration &&
		s.WorkloadVersion == p.WorkloadVersion &&
		s.DesiredState == "DELETE_REQUESTED" && s.WorkloadPhase == "RELEASING" &&
		s.LatestAllocationID == p.AllocationID &&
		s.AllocationGeneration == p.AllocationGeneration &&
		s.AllocationVersion == p.AllocationVersion && s.AllocationPhase == "RELEASING" &&
		s.CreateGeneration == p.AllocationGeneration &&
		s.CreateVersion == p.CreateOperationVersion &&
		s.CreateKind == "CREATE" && s.CreateStatus == "SUCCEEDED" &&
		s.CreateResourceID == p.TargetResourceID && s.CreateResourceUID == p.TargetResourceUID &&
		s.DeleteGeneration == p.DeleteGeneration &&
		s.DeleteVersion == p.DeleteOperationVersion &&
		s.DeleteKind == "DELETE" && s.DeleteStatus == "SUCCEEDED" &&
		s.DeleteTargetID == p.TargetResourceID && s.DeleteTargetUID == p.TargetResourceUID
}

// ValidateReleaseCommitFence checks four authoritative rows while holding
// PostgreSQL row locks in THE CALLER'S transaction. The caller MUST perform
// any later allowed state transition in this same transaction; a preflight
// followed by another transaction is a TOCTOU bug. This method does not
// mutate Allocation, Workload, Operations, Usage, or Costs, and does NOT
// authorize release. The caller must separately verify authenticated driver,
// Provider, Kueue, Pod, and DRA source evidence before any protected effect.
//
// A newer-generation DELETE has NULL allocation_id under the existing
// migration 0003 FK: the historical Allocation still belongs to the CREATE
// generation. The exact original resource is bound via the successful CREATE
// operation, not a cross-generation DELETE foreign key. Do not weaken the FK.
//
// This deliberately excludes SUPERSEDED cleanup; it is not a generalized
// release writer. A missing, changed or contradictory row fails closed.
func ValidateReleaseCommitFence(ctx context.Context, tx *sql.Tx, p ReleaseCommitFence) error {
	if ctx == nil || tx == nil || !validReleaseCommitFence(p) {
		return ErrReleaseCommitFence
	}
	var s releaseCommitFenceSnapshot
	err := tx.QueryRowContext(ctx, `
		SELECT w.desired_generation, w.version, w.desired_state, w.observed_phase,
		       COALESCE(w.latest_allocation_id, ''),
		       a.desired_generation, a.version, a.phase,
		       c.desired_generation, c.version, c.operation_kind, c.status,
		       COALESCE(c.provider_resource_id, ''), COALESCE(c.provider_resource_uid, ''),
		       d.desired_generation, d.version, d.operation_kind, d.status,
		       COALESCE(d.target_resource_id, ''), COALESCE(d.target_resource_uid, '')
		FROM public.compute_workloads w
		JOIN public.compute_allocations a
		  ON a.tenant_id=w.tenant_id AND a.workload_id=w.workload_id
		 AND a.allocation_id=$3 AND a.provider_id=$4
		JOIN public.compute_operations c
		  ON c.tenant_id=w.tenant_id AND c.workload_id=w.workload_id
		 AND c.allocation_id=a.allocation_id AND c.provider_id=a.provider_id
		 AND c.compute_operation_id=$5
		JOIN public.compute_operations d
		  ON d.tenant_id=w.tenant_id AND d.workload_id=w.workload_id
		 AND d.allocation_id IS NULL AND d.provider_id=a.provider_id
		 AND d.compute_operation_id=$6
		WHERE w.tenant_id=$1 AND w.workload_id=$2
		FOR UPDATE OF w, a, c, d
	`, p.TenantID, p.WorkloadID, p.AllocationID, p.ProviderID,
		p.CreateOperationID, p.DeleteOperationID).Scan(
		&s.WorkloadGeneration, &s.WorkloadVersion, &s.DesiredState, &s.WorkloadPhase, &s.LatestAllocationID,
		&s.AllocationGeneration, &s.AllocationVersion, &s.AllocationPhase,
		&s.CreateGeneration, &s.CreateVersion, &s.CreateKind, &s.CreateStatus,
		&s.CreateResourceID, &s.CreateResourceUID,
		&s.DeleteGeneration, &s.DeleteVersion, &s.DeleteKind, &s.DeleteStatus,
		&s.DeleteTargetID, &s.DeleteTargetUID,
	)
	if err != nil || !s.matches(p) {
		return ErrReleaseCommitFence
	}
	return nil
}
