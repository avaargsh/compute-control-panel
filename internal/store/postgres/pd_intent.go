package postgres

import (
	"context"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"strings"
	"time"

	"github.com/avaargsh/compute-control-panel/internal/capacity/pd"
)

var ErrPDIntentFence = errors.New("PD intent authority or generation is not current")

const pdIntentRecordedV1 = "compute.pd.intent.recorded.v1"

// PDIntentOwner must be resolved from authenticated server-side authorization,
// never from an untrusted request body.
type PDIntentOwner struct {
	TenantID, ProjectID, Service, ResourceType, ResourceID string
}

// PDIntentRecord is a durable, non-actuating proposal receipt.
type PDIntentRecord struct {
	Candidate              pd.Proposal
	ProviderID, EventID    string
	FenceVersion           int64
	ValidUntil, RecordedAt time.Time
}

func validPDIntentOwner(o PDIntentOwner) bool {
	for _, s := range []string{o.TenantID, o.ProjectID, o.Service, o.ResourceType, o.ResourceID} {
		if !outboxLabel(s) {
			return false
		}
	}
	return true
}

func validPDIntentDigest(s string) bool {
	_, err := hex.DecodeString(s)
	return len(s) == 64 && strings.ToLower(s) == s && err == nil
}

func validPDIntentCandidate(o PDIntentOwner, c pd.Proposal) bool {
	return validPDIntentOwner(o) && c.Scope.Tenant == o.TenantID &&
		c.Scope.Project == o.ProjectID && outboxLabel(c.Workload) &&
		c.Generation > 1 && c.Generation <= math.MaxInt64 &&
		validPDIntentDigest(c.IntentID) && validPDIntentDigest(c.GenerationKey)
}

func lockPDIntentWorkload(ctx context.Context, tx *sql.Tx, o PDIntentOwner, workload string) (int64, string, error) {
	var generation int64
	var state string
	err := tx.QueryRowContext(ctx, `SELECT desired_generation,desired_state
		FROM public.compute_workloads WHERE workload_id=$1 AND tenant_id=$2
		AND project_id=$3 AND owner_service=$4 AND owner_resource_type=$5 AND owner_resource_id=$6
		FOR UPDATE`, workload, o.TenantID, o.ProjectID, o.Service, o.ResourceType, o.ResourceID).Scan(&generation, &state)
	return generation, state, err
}

func readPDIntent(ctx context.Context, tx *sql.Tx, o PDIntentOwner, workload string, generation uint64) (PDIntentRecord, error) {
	var r PDIntentRecord
	var candidate []byte
	var eventMatches bool
	err := tx.QueryRowContext(ctx, `SELECT i.candidate,i.provider_id,i.event_id,i.fence_version,
		i.valid_until,i.recorded_at,COALESCE(e.producer='compute-control-panel'
		AND e.event_type='compute.pd.intent.recorded.v1' AND e.schema_version=1
		AND e.tenant_id=i.tenant_id AND e.aggregate_type='compute_pd_intent'
		AND e.aggregate_id=i.intent_id AND e.aggregate_version=1
		AND e.payload=pg_catalog.jsonb_build_object('provider_id',i.provider_id,'candidate',i.candidate),false)
		FROM public.compute_pd_intents i
		LEFT JOIN public.compute_outbox_events e ON e.event_id=i.event_id
		WHERE i.workload_id=$1 AND i.tenant_id=$2 AND i.project_id=$3
		AND i.owner_service=$4 AND i.owner_resource_type=$5 AND i.owner_resource_id=$6
		AND i.target_generation=$7`, workload, o.TenantID, o.ProjectID, o.Service, o.ResourceType, o.ResourceID, int64(generation)).
		Scan(&candidate, &r.ProviderID, &r.EventID, &r.FenceVersion, &r.ValidUntil, &r.RecordedAt, &eventMatches)
	if err != nil {
		return PDIntentRecord{}, err
	}
	if err := json.Unmarshal(candidate, &r.Candidate); err != nil {
		return PDIntentRecord{}, err
	}
	if !eventMatches || !validPDIntentCandidate(o, r.Candidate) || r.Candidate.Workload != workload ||
		r.Candidate.Generation != generation || r.EventID != "pd-intent-"+r.Candidate.IntentID {
		return PDIntentRecord{}, ErrPDIntentFence
	}
	return r, nil
}

// RecordPDIntent records a proposal for an existing Workload's next generation.
// It checks the independently administered exact revision fence under locks,
// inserts the immutable journal and Outbox together, and does not advance the
// Workload generation or create an Allocation/Operation.
func (r *Repository) RecordPDIntent(ctx context.Context, owner PDIntentOwner, verified pd.VerifiedProposal, correlationID string) (PDIntentRecord, bool, error) {
	c := verified.Candidate()
	providerID := verified.ProviderID()
	if !validPDIntentCandidate(owner, c) || !outboxLabel(providerID) ||
		!outboxLabel(correlationID) || verified.ValidUntil().IsZero() {
		return PDIntentRecord{}, false, ErrPDIntentFence
	}
	tx, err := r.db.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelReadCommitted})
	if err != nil {
		return PDIntentRecord{}, false, err
	}
	defer tx.Rollback()
	generation, state, err := lockPDIntentWorkload(ctx, tx, owner, c.Workload)
	if err != nil {
		return PDIntentRecord{}, false, err
	}
	existing, err := readPDIntent(ctx, tx, owner, c.Workload, c.Generation)
	if err == nil {
		if existing.Candidate != c || existing.ProviderID != providerID {
			return PDIntentRecord{}, false, ErrIdempotencyConflict
		}
		if err = tx.Commit(); err != nil {
			return PDIntentRecord{}, false, err
		}
		return existing, false, nil
	}
	if !errors.Is(err, sql.ErrNoRows) {
		return PDIntentRecord{}, false, err
	}
	if state != "ACTIVE" || generation < 1 || uint64(generation)+1 != c.Generation {
		return PDIntentRecord{}, false, ErrPDIntentFence
	}
	raw, err := json.Marshal(c)
	if err != nil {
		return PDIntentRecord{}, false, err
	}
	record := PDIntentRecord{Candidate: c, ProviderID: providerID, EventID: "pd-intent-" + c.IntentID}
	err = tx.QueryRowContext(ctx, `SELECT fence_version,valid_until
		FROM public.compute_pd_intent_fences
		WHERE workload_id=$1 AND tenant_id=$2 AND project_id=$3 AND provider_id=$4
		AND target_generation=$5 AND candidate=$6::jsonb AND NOT revoked FOR SHARE`,
		c.Workload, owner.TenantID, owner.ProjectID, providerID, int64(c.Generation), string(raw)).
		Scan(&record.FenceVersion, &record.ValidUntil)
	if errors.Is(err, sql.ErrNoRows) {
		return PDIntentRecord{}, false, ErrPDIntentFence
	}
	if err != nil {
		return PDIntentRecord{}, false, err
	}
	if verified.ValidUntil().Before(record.ValidUntil) {
		record.ValidUntil = verified.ValidUntil()
	}
	var current bool
	if err := tx.QueryRowContext(ctx, "SELECT clock_timestamp() < $1", record.ValidUntil).Scan(&current); err != nil {
		return PDIntentRecord{}, false, err
	}
	if !current {
		return PDIntentRecord{}, false, ErrPDIntentFence
	}
	err = tx.QueryRowContext(ctx, `INSERT INTO public.compute_pd_intents
		(intent_id,generation_key,workload_id,tenant_id,project_id,owner_service,owner_resource_type,
		owner_resource_id,provider_id,target_generation,candidate,fence_version,valid_until,event_id)
		VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11::jsonb,$12,$13,$14) RETURNING recorded_at`,
		c.IntentID, c.GenerationKey, c.Workload, owner.TenantID, owner.ProjectID, owner.Service,
		owner.ResourceType, owner.ResourceID, providerID, int64(c.Generation), string(raw),
		record.FenceVersion, record.ValidUntil, record.EventID).Scan(&record.RecordedAt)
	if err != nil {
		return PDIntentRecord{}, false, fmt.Errorf("record PD intent: %w", err)
	}
	_, err = tx.ExecContext(ctx, `INSERT INTO public.compute_outbox_events
		(event_id,producer,event_type,schema_version,tenant_id,aggregate_type,aggregate_id,aggregate_version,
		occurred_at,correlation_id,payload)
		VALUES($1,'compute-control-panel',$2,1,$3,'compute_pd_intent',$4,1,$5,$6,
		pg_catalog.jsonb_build_object('provider_id',$7::text,'candidate',$8::jsonb))`,
		record.EventID, pdIntentRecordedV1, owner.TenantID, c.IntentID, record.RecordedAt, correlationID, providerID, string(raw))
	if err != nil {
		return PDIntentRecord{}, false, fmt.Errorf("record PD intent Outbox: %w", err)
	}
	if err = tx.Commit(); err != nil {
		return PDIntentRecord{}, false, fmt.Errorf("commit PD intent: %w", err)
	}
	return record, true, nil
}

// GetRecordedPDIntent returns historical acceptance only, not a fresh grant
// to dispatch. It authenticates the owner and verifies the exact Outbox event.
func (r *Repository) GetRecordedPDIntent(ctx context.Context, owner PDIntentOwner, workload string, generation uint64, intentID string) (PDIntentRecord, error) {
	if !validPDIntentOwner(owner) || !outboxLabel(workload) ||
		generation <= 1 || generation > math.MaxInt64 || !validPDIntentDigest(intentID) {
		return PDIntentRecord{}, ErrPDIntentFence
	}
	tx, err := r.db.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelReadCommitted})
	if err != nil {
		return PDIntentRecord{}, err
	}
	defer tx.Rollback()
	if _, _, err = lockPDIntentWorkload(ctx, tx, owner, workload); err != nil {
		return PDIntentRecord{}, err
	}
	record, err := readPDIntent(ctx, tx, owner, workload, generation)
	if err != nil {
		return PDIntentRecord{}, err
	}
	if record.Candidate.IntentID != intentID {
		return PDIntentRecord{}, ErrIdempotencyConflict
	}
	if err = tx.Commit(); err != nil {
		return PDIntentRecord{}, err
	}
	return record, nil
}
