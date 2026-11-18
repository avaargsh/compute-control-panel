package postgres

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"time"

	"github.com/avaargsh/compute-control-panel/internal/capacity/pd"
	"github.com/avaargsh/compute-control-panel/internal/operation"
)

// PDStagingSnapshot is a read-only, NON-DISPATCHABLE view of one promoted
// Prefill/Decode intent. All fields originate from the canonical Workload,
// immutable Intent/Promotion and independently administered fence. It does NOT
// contain a signed activation permit, a trusted ClaimTemplate cardinality,
// physical GPU evidence, or an authorization to invoke Provider.Apply.
//
// An execution worker must separately acquire a fenced Operation lease and
// recheck current authority at its actual external side-effect boundary.
type PDStagingSnapshot struct {
	Promotion            PDPromotion
	Proposal             pd.Proposal
	TenantID, WorkloadID string
	ProviderID, PoolID   string
	PrefillReplicas      int
	DecodeReplicas       int
	GPUsPerReplica       int
	AuthorityExpiresAt   time.Time
}

func singleGPUStagingShape(p pd.Proposal) bool {
	// The first inert JobSet writer has no attested ClaimTemplate device
	// cardinality. In particular, the total number of GPUs in a Proposal is
	// not enough to infer phase-specific multi-GPU requests. v1 only accepts
	// one GPU per Pod/replica and rejects everything else.
	return p.PrefillReplicas > 0 && p.PrefillReplicas <= 32 &&
		p.DecodeReplicas > 0 && p.DecodeReplicas <= 32 &&
		p.PrefillReplicas+p.DecodeReplicas <= 64 &&
		p.GPUsNeeded == p.PrefillReplicas+p.DecodeReplicas &&
		p.PoolRevision > 0 && p.TopologyRevision > 0 &&
		p.LoadRevision > 0
}

// ReadCurrentPDStagingSnapshot checks the exact independently pinned proposal,
// immutable promotion, current owner and desired generation, provider state,
// PENDING Allocation/CREATE Operation, and both valid deadlines under row
// locks. It does not grant generic dispatch eligibility or mutate any row.
// Replaying an expired, revoked, superseded or already-claimed promotion fails
// closed. The source/owner must be authenticated by the service before call.
//
// This short-lived snapshot MUST NOT be cached as an execution permit: the
// fence can be revoked immediately after this read-only transaction commits.
// ReadCurrentPDStagingSnapshot returns NON-DISPATCHABLE current authority
// only while the canonical CREATE Operation remains PENDING. It does not
// authorize an external Kubernetes POST.
func (r *Repository) ReadCurrentPDStagingSnapshot(
	ctx context.Context, owner PDIntentOwner, intentID string,
) (PDStagingSnapshot, error) {
	if r == nil || r.db == nil || !validPDIntentOwner(owner) || !validPDIntentDigest(intentID) {
		return PDStagingSnapshot{}, ErrPDPromotionBlocked
	}
	tx, err := r.db.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelReadCommitted})
	if err != nil {
		return PDStagingSnapshot{}, err
	}
	defer tx.Rollback()
	snapshot, err := readCurrentPDStagingSnapshotTx(ctx, tx, owner, intentID, operation.Pending)
	if err != nil {
		return PDStagingSnapshot{}, err
	}
	if err := tx.Commit(); err != nil {
		return PDStagingSnapshot{}, err
	}
	return snapshot, nil
}

// readCurrentPDStagingSnapshotTx verifies exact immutable proposal, independent
// current source fence, canonical promotion, provider and Operation phase in
// the caller's transaction. The lease writer locks Workload before calling
// this helper, then transitions the matching Operation in the SAME transaction.
func readCurrentPDStagingSnapshotTx(
	ctx context.Context, tx *sql.Tx, owner PDIntentOwner, intentID string,
	expectedStatus operation.Status,
) (PDStagingSnapshot, error) {
	reject := func() (PDStagingSnapshot, error) {
		return PDStagingSnapshot{}, ErrPDPromotionBlocked
	}
	if tx == nil || !validPDIntentOwner(owner) || !validPDIntentDigest(intentID) ||
		(expectedStatus != operation.Pending && expectedStatus != operation.Leased) {
		return reject()
	}
	var workload, providerID, poolID string
	var gen int64
	var raw []byte
	var intentUntil, fenceUntil, databaseNow time.Time
	err = tx.QueryRowContext(ctx, `SELECT i.workload_id,i.target_generation,i.candidate,
	    i.provider_id,pr.pool_id,i.valid_until,f.valid_until,pg_catalog.clock_timestamp()
	FROM public.compute_pd_intents i
	JOIN public.compute_pd_intent_fences f
	  ON f.workload_id=i.workload_id AND f.tenant_id=i.tenant_id
	 AND f.project_id=i.project_id AND f.provider_id=i.provider_id
	 AND f.target_generation=i.target_generation AND f.candidate=i.candidate
	 AND f.fence_version=i.fence_version AND NOT f.revoked
	JOIN public.compute_pd_promotions pr
	  ON pr.intent_id=i.intent_id AND pr.workload_id=i.workload_id
	 AND pr.tenant_id=i.tenant_id AND pr.project_id=i.project_id
	 AND pr.provider_id=i.provider_id AND pr.target_generation=i.target_generation
	JOIN public.compute_workloads w
	  ON w.workload_id=i.workload_id AND w.tenant_id=i.tenant_id
	 AND w.project_id=i.project_id AND w.owner_service=i.owner_service
	 AND w.owner_resource_type=i.owner_resource_type
	 AND w.owner_resource_id=i.owner_resource_id
	JOIN public.compute_providers p ON p.provider_id=i.provider_id
	JOIN public.compute_allocations a
	  ON a.allocation_id=pr.allocation_id AND a.workload_id=i.workload_id
	 AND a.tenant_id=i.tenant_id AND a.provider_id=i.provider_id
	 AND a.desired_generation=i.target_generation
	JOIN public.compute_operations op
	  ON op.compute_operation_id=pr.operation_id AND op.workload_id=i.workload_id
	 AND op.tenant_id=i.tenant_id AND op.provider_id=i.provider_id
	 AND op.allocation_id=a.allocation_id AND op.desired_generation=i.target_generation
	WHERE i.intent_id=$1 AND i.tenant_id=$2 AND i.project_id=$3
	  AND i.owner_service=$4 AND i.owner_resource_type=$5 AND i.owner_resource_id=$6
	  AND w.desired_state='ACTIVE' AND w.desired_generation=i.target_generation
	  AND w.latest_allocation_id=a.allocation_id AND a.phase='PENDING'
	  AND op.operation_kind='CREATE' AND op.status=$7
	  AND op.provider_request_digest='sha256:' || i.intent_id
	  AND op.idempotency_key='pd-create-' || i.intent_id
	  AND p.state='REGISTERED'
	FOR SHARE OF i,f,pr,w,p,a,op`,
		intentID, owner.TenantID, owner.ProjectID, owner.Service,
		owner.ResourceType, owner.ResourceID, expectedStatus,
	).Scan(&workload, &gen, &raw, &providerID, &poolID,
		&intentUntil, &fenceUntil, &databaseNow)
	if errors.Is(err, sql.ErrNoRows) {
		return reject()
	}
	if err != nil {
		return PDStagingSnapshot{}, err
	}
	if gen <= 1 || !databaseNow.Before(intentUntil) ||
		!databaseNow.Before(fenceUntil) {
		return reject()
	}

	var candidate pd.Proposal
	if err := json.Unmarshal(raw, &candidate); err != nil ||
		!validPDIntentCandidate(owner, candidate) ||
		candidate.IntentID != intentID || candidate.Workload != workload ||
		candidate.Generation != uint64(gen) || candidate.Scope.Pool != poolID ||
		!singleGPUStagingShape(candidate) {
		return reject()
	}
	intent, err := readPDIntent(ctx, tx, owner, workload, uint64(gen))
	if err != nil || intent.Candidate != candidate || intent.ProviderID != providerID {
		return reject()
	}
	promotion, err := readPDPromotion(ctx, tx, owner, workload, intentID)
	if err != nil || promotion.ProviderID != providerID ||
		promotion.PoolID != poolID || promotion.Generation != uint64(gen) {
		return reject()
	}
	expiresAt := intentUntil
	if fenceUntil.Before(expiresAt) {
		expiresAt = fenceUntil
	}
	// Check the PostgreSQL clock again after all reads, not the worker clock.
	var current bool
	if err := tx.QueryRowContext(ctx, `SELECT pg_catalog.clock_timestamp() < $1`, expiresAt).Scan(&current); err != nil {
		return PDStagingSnapshot{}, err
	}
	if !current {
		return reject()
	}
	return PDStagingSnapshot{}, err
	}
	return PDStagingSnapshot{
		Promotion: promotion, Proposal: candidate,
		TenantID: owner.TenantID, WorkloadID: workload,
		ProviderID: providerID, PoolID: poolID,
		PrefillReplicas: candidate.PrefillReplicas,
		DecodeReplicas:  candidate.DecodeReplicas,
		GPUsPerReplica:  1, AuthorityExpiresAt: expiresAt,
	}, nil
}
