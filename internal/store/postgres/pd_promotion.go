package postgres

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"math"

	"github.com/avaargsh/compute-control-panel/internal/operation"
)

// ErrPDPromotionBlocked means the existing workload, independent authority,
// previous release or Provider state is not currently eligible.
var ErrPDPromotionBlocked = errors.New("PD promotion denied by generation, release, provider or policy fence")

// This distinct event never grants generic provider dispatch eligibility.
const pdOperationStagedV1 = "compute.pd.operation.staged.v1"

// PDPromotion binds a previously accepted, immutable PD intent to the same
// canonical Workload and Operation state machines. It is not readiness proof.
type PDPromotion struct {
	IntentID, AllocationID, OperationID string
	WorkloadEventID, OperationEventID   string
	ProviderID, PoolID                  string
	Generation                          uint64
}

func readPDPromotion(ctx context.Context, tx *sql.Tx, o PDIntentOwner, workload, intentID string) (PDPromotion, error) {
	var p PDPromotion
	var gen int64
	err := tx.QueryRowContext(ctx, `SELECT p.intent_id,p.allocation_id,p.operation_id,
		p.workload_event_id,p.operation_event_id,p.provider_id,p.pool_id,p.target_generation
		FROM public.compute_pd_promotions p
		JOIN public.compute_allocations a
		  ON a.allocation_id=p.allocation_id AND a.workload_id=p.workload_id
		  AND a.tenant_id=p.tenant_id AND a.provider_id=p.provider_id
		  AND a.desired_generation=p.target_generation
		JOIN public.compute_operations op
		  ON op.compute_operation_id=p.operation_id AND op.workload_id=p.workload_id
		  AND op.tenant_id=p.tenant_id AND op.provider_id=p.provider_id
		  AND op.allocation_id=a.allocation_id AND op.desired_generation=p.target_generation
		  AND op.operation_kind='CREATE'
		JOIN public.compute_outbox_events w
		  ON w.event_id=p.workload_event_id AND w.tenant_id=p.tenant_id
		  AND w.producer='compute-control-panel' AND w.event_type='compute.pd.workload.generation-accepted.v1'
		  AND w.aggregate_type='compute_workload' AND w.aggregate_id=p.workload_id
		JOIN public.compute_outbox_events e
		  ON e.event_id=p.operation_event_id AND e.tenant_id=p.tenant_id
		  AND e.producer='compute-control-panel' AND e.event_type='compute.pd.operation.staged.v1'
		  AND e.aggregate_type='compute_operation' AND e.aggregate_id=p.operation_id
		WHERE p.intent_id=$1 AND p.workload_id=$2 AND p.tenant_id=$3
		  AND p.project_id=$4 AND EXISTS (
		    SELECT 1 FROM public.compute_workloads owner
		    WHERE owner.workload_id=p.workload_id AND owner.tenant_id=p.tenant_id
		    AND owner.project_id=p.project_id AND owner.owner_service=$5
		    AND owner.owner_resource_type=$6 AND owner.owner_resource_id=$7
		  )`, intentID, workload, o.TenantID, o.ProjectID, o.Service, o.ResourceType, o.ResourceID).
		Scan(&p.IntentID, &p.AllocationID, &p.OperationID, &p.WorkloadEventID, &p.OperationEventID,
			&p.ProviderID, &p.PoolID, &gen)
	if err != nil {
		return PDPromotion{}, err
	}
	if gen <= 1 || p.IntentID != intentID || p.AllocationID != "pd-allocation-"+intentID ||
		p.OperationID != "pd-operation-"+intentID ||
		p.WorkloadEventID != "pd-workload-"+intentID ||
		p.OperationEventID != "pd-operation-event-"+intentID {
		return PDPromotion{}, ErrPDPromotionBlocked
	}
	p.Generation = uint64(gen)
	return p, nil
}

// PromotePDIntent advances only a previously recorded AND independently
// authorized PD proposal. It holds the existing Workload row lock while
// checking prior Allocation release, current source policy and Provider Drain.
// Generation, PENDING Allocation/CREATE Operation, projection and both Outbox
// events commit together. No JobSet CREATE, GPU READY or finance FINAL is
// inferred; dispatch requires a separately authorized paired JobSet adapter.
func (r *Repository) PromotePDIntent(ctx context.Context, owner PDIntentOwner, intentID, correlationID string) (PDPromotion, bool, error) {
	if !validPDIntentOwner(owner) || !validPDIntentDigest(intentID) || !outboxLabel(correlationID) {
		return PDPromotion{}, false, ErrPDPromotionBlocked
	}
	tx, err := r.db.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelReadCommitted})
	if err != nil {
		return PDPromotion{}, false, err
	}
	defer tx.Rollback()

	var workload string
	var nextGen int64
	err = tx.QueryRowContext(ctx, `SELECT workload_id,target_generation
		FROM public.compute_pd_intents WHERE intent_id=$1 AND tenant_id=$2 AND project_id=$3
		  AND owner_service=$4 AND owner_resource_type=$5 AND owner_resource_id=$6`,
		intentID, owner.TenantID, owner.ProjectID, owner.Service, owner.ResourceType, owner.ResourceID).Scan(&workload, &nextGen)
	if err != nil {
		return PDPromotion{}, false, err
	}
	if nextGen <= 1 || !outboxLabel(workload) {
		return PDPromotion{}, false, ErrPDPromotionBlocked
	}

	current, state, err := lockPDIntentWorkload(ctx, tx, owner, workload)
	if err != nil {
		return PDPromotion{}, false, err
	}
	existing, err := readPDPromotion(ctx, tx, owner, workload, intentID)
	if err == nil {
		if err = tx.Commit(); err != nil {
			return PDPromotion{}, false, err
		}
		return existing, false, nil
	}
	if !errors.Is(err, sql.ErrNoRows) {
		return PDPromotion{}, false, err
	}
	if state != "ACTIVE" || current == math.MaxInt64 || current+1 != nextGen {
		return PDPromotion{}, false, ErrPDPromotionBlocked
	}
	intent, err := readPDIntent(ctx, tx, owner, workload, uint64(nextGen))
	if err != nil || intent.Candidate.IntentID != intentID ||
		intent.Candidate.Generation != uint64(nextGen) {
		return PDPromotion{}, false, ErrPDPromotionBlocked
	}

	var fenceVersion int64
	var expiresAt, now sql.NullTime
	var providerID string
	err = tx.QueryRowContext(ctx, `SELECT f.fence_version,f.valid_until,p.provider_id,clock_timestamp()
		FROM public.compute_pd_intent_fences f
		JOIN public.compute_providers p ON p.provider_id=f.provider_id
		WHERE f.workload_id=$1 AND f.tenant_id=$2 AND f.project_id=$3
		AND f.target_generation=$4 AND f.candidate=$5::jsonb
		AND NOT f.revoked AND p.state='REGISTERED'
		FOR SHARE OF f,p`,
		workload, owner.TenantID, owner.ProjectID, nextGen, mustJSONPDProposal(intent.Candidate)).
		Scan(&fenceVersion, &expiresAt, &providerID, &now)
	if err != nil || !now.Valid || !expiresAt.Valid || !now.Time.Before(expiresAt.Time) ||
		!now.Time.Before(intent.ValidUntil) || fenceVersion != intent.FenceVersion ||
		providerID != intent.ProviderID || !validPDIntentCandidate(owner, intent.Candidate) {
		return PDPromotion{}, false, ErrPDPromotionBlocked
	}
	// A new generation may not overlap an unproven previous GPU allocation.
	// The existing release reconciliation pipeline alone may mark RELEASED.
	var previousPhase string
	err = tx.QueryRowContext(ctx, `SELECT a.phase FROM public.compute_allocations a
		JOIN public.compute_workloads w ON w.workload_id=a.workload_id
		WHERE w.workload_id=$1 AND w.tenant_id=$2
		  AND a.allocation_id=w.latest_allocation_id
		  AND a.desired_generation=$3
		FOR SHARE OF a`, workload, owner.TenantID, current).Scan(&previousPhase)
	if err != nil || previousPhase != "RELEASED" {
		return PDPromotion{}, false, ErrPDPromotionBlocked
	}

	promotion := PDPromotion{
		IntentID: intentID, AllocationID: "pd-allocation-" + intentID,
		OperationID: "pd-operation-" + intentID, WorkloadEventID: "pd-workload-" + intentID,
		OperationEventID: "pd-operation-event-" + intentID, ProviderID: providerID,
		PoolID: intent.Candidate.Scope.Pool, Generation: uint64(nextGen),
	}
	if !outboxLabel(promotion.PoolID) {
		return PDPromotion{}, false, ErrPDPromotionBlocked
	}
	update, err := tx.ExecContext(ctx, `UPDATE public.compute_workloads
		SET desired_generation=$1,version=version+1,
		    latest_allocation_id=NULL,observed_phase='ACCEPTED',
		    updated_at=clock_timestamp()
		WHERE workload_id=$2 AND tenant_id=$3 AND desired_generation=$4
		  AND desired_state='ACTIVE'`, nextGen, workload, owner.TenantID, current)
	if err != nil {
		return PDPromotion{}, false, fmt.Errorf("PD generation CAS: %w", err)
	}
	rows, err := update.RowsAffected()
	if err != nil || rows != 1 {
		return PDPromotion{}, false, ErrPDPromotionBlocked
	}

	_, err = tx.ExecContext(ctx, `INSERT INTO public.compute_allocations
		(allocation_id,tenant_id,workload_id,provider_id,pool_id,desired_generation,phase,version)
		VALUES($1,$2,$3,$4,$5,$6,'PENDING',1)`,
		promotion.AllocationID, owner.TenantID, workload, providerID, promotion.PoolID, nextGen)
	if err != nil {
		return PDPromotion{}, false, fmt.Errorf("create PD Allocation: %w", err)
	}
	_, err = tx.ExecContext(ctx, `UPDATE public.compute_workloads SET
		latest_allocation_id=$1 WHERE workload_id=$2 AND tenant_id=$3
		  AND desired_generation=$4`, promotion.AllocationID, workload, owner.TenantID, nextGen)
	if err != nil {
		return PDPromotion{}, false, err
	}

	digest := "sha256:" + intentID
	_, err = tx.ExecContext(ctx, `INSERT INTO public.compute_operations
		(compute_operation_id,tenant_id,workload_id,allocation_id,provider_id,
		operation_kind,desired_generation,provider_request_digest,idempotency_key,
		version,fence_token,status)
		VALUES($1,$2,$3,$4,$5,'CREATE',$6,$7,$8,1,0,'PENDING')`,
		promotion.OperationID, owner.TenantID, workload, promotion.AllocationID,
		providerID, nextGen, digest, "pd-create-"+intentID)
	if err != nil {
		return PDPromotion{}, false, fmt.Errorf("create PD operation: %w", err)
	}
	// The Workload lock serializes promotions of this aggregate.
	var aggregateVersion int64
	err = tx.QueryRowContext(ctx, `SELECT COALESCE(MAX(aggregate_version),0)+1
		FROM public.compute_outbox_events WHERE producer=$1 AND tenant_id=$2
		  AND aggregate_type='compute_workload' AND aggregate_id=$3`,
		producerName, owner.TenantID, workload).Scan(&aggregateVersion)
	if err != nil {
		return PDPromotion{}, false, err
	}
	workloadPayload, _ := json.Marshal(map[string]any{
		"workload_id": workload, "desired_generation": nextGen, "intent_id": intentID,
		"allocation_id": promotion.AllocationID, "operation_id": promotion.OperationID,
	})
	operationPayload, _ := json.Marshal(map[string]any{
		"operation_id": promotion.OperationID, "workload_id": workload, "allocation_id": promotion.AllocationID,
		"provider_id": providerID, "desired_generation": nextGen,
		"operation_kind": string(operation.Create), "request_digest": digest,
		"pd_intent_id": intentID,
	})
	_, err = tx.ExecContext(ctx, `INSERT INTO public.compute_outbox_events
		(event_id,producer,event_type,schema_version,tenant_id,aggregate_type,aggregate_id,
		 aggregate_version,occurred_at,correlation_id,causation_id,payload)
		VALUES($1,$2,'compute.pd.workload.generation-accepted.v1',1,$3,'compute_workload',
		  $4,$5,clock_timestamp(),$6,$7,$8::jsonb)`,
		promotion.WorkloadEventID, producerName, owner.TenantID, workload, aggregateVersion,
		correlationID, intent.EventID, workloadPayload)
	if err != nil {
		return PDPromotion{}, false, fmt.Errorf("PD workload outbox: %w", err)
	}
	_, err = tx.ExecContext(ctx, `INSERT INTO public.compute_outbox_events
		(event_id,producer,event_type,schema_version,tenant_id,aggregate_type,aggregate_id,
		 aggregate_version,occurred_at,correlation_id,causation_id,payload)
		VALUES($1,$2,$3,1,$4,'compute_operation',$5,1,clock_timestamp(),$6,$7,$8::jsonb)`,
		promotion.OperationEventID, producerName, pdOperationStagedV1, owner.TenantID,
		promotion.OperationID, correlationID, promotion.WorkloadEventID, operationPayload)
	if err != nil {
		return PDPromotion{}, false, fmt.Errorf("PD operation outbox: %w", err)
	}
	_, err = tx.ExecContext(ctx, `INSERT INTO public.compute_pd_promotions
		(intent_id,workload_id,tenant_id,project_id,target_generation,provider_id,pool_id,
		 allocation_id,operation_id,workload_event_id,operation_event_id)
		VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11)`,
		intentID, workload, owner.TenantID, owner.ProjectID, nextGen, providerID,
		promotion.PoolID, promotion.AllocationID, promotion.OperationID,
		promotion.WorkloadEventID, promotion.OperationEventID)
	if err != nil {
		return PDPromotion{}, false, fmt.Errorf("PD promotion journal: %w", err)
	}
	if err = tx.Commit(); err != nil {
		return PDPromotion{}, false, fmt.Errorf("commit PD promotion: %w", err)
	}
	return promotion, true, nil
}

func mustJSONPDProposal(p interface{}) string {
	b, _ := json.Marshal(p)
	return string(b)
}
