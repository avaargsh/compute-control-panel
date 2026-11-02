// Package postgres implements tenant-scoped persistence for Compute Control Plane.
package postgres

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"database/sql"
)

var (
	ErrInvalidRequest      = errors.New("invalid workload create request")
	ErrIdempotencyConflict = errors.New("idempotency key reused with a different request digest")
)

const (
	producerName       = "compute-control-panel"
	workloadAcceptedV1 = "compute.workload.accepted.v1"
)

type Repository struct {
	db *sql.DB
}

func NewRepository(db *sql.DB) (*Repository, error) {
	if db == nil {
		return nil, errors.New("postgres repository requires a database")
	}
	return &Repository{db: db}, nil
}

type CreateWorkloadParams struct {
	WorkloadID             string
	TenantID               string
	ProjectID              string
	OwnerService           string
	OwnerResourceType      string
	OwnerResourceID        string
	ComputeProfileID       string
	ComputeProfileRevision string
	NormalizedRequest      json.RawMessage
	RequestDigest          string
	IdempotencyKey         string
	EventID                string
	CorrelationID          string
	CausationID            string
}

type Workload struct {
	WorkloadID         string
	TenantID           string
	ProjectID          string
	RequestDigest      string
	IdempotencyKey     string
	DesiredGeneration  int64
	Version            int64
	DesiredState       string
	ObservedPhase      string
	LatestAllocationID sql.NullString
	CreatedAt          time.Time
	UpdatedAt          time.Time
}

// CreateWorkload commits the idempotency claim, workload and accepted event in
// one transaction. A replay with the same tenant/key/digest returns the original
// resource; a changed digest is rejected without another outbox event.
func (r *Repository) CreateWorkload(ctx context.Context, p CreateWorkloadParams) (Workload, bool, error) {
	if err := validateCreateWorkload(p); err != nil {
		return Workload{}, false, err
	}

	tx, err := r.db.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelReadCommitted})
	if err != nil {
		return Workload{}, false, fmt.Errorf("begin workload create transaction: %w", err)
	}
	defer func() { _ = tx.Rollback() }()

	const insertWorkload = `INSERT INTO compute_workloads (
		workload_id, tenant_id, project_id, owner_service, owner_resource_type,
		owner_resource_id, compute_profile_id, compute_profile_revision,
		normalized_request, request_digest, idempotency_key, desired_generation,
		version, desired_state, observed_generation, observed_phase
	) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9::jsonb,$10,$11,1,1,'ACTIVE',0,'ACCEPTED')
	ON CONFLICT (tenant_id, idempotency_key) DO NOTHING
	RETURNING workload_id, tenant_id, project_id, request_digest, idempotency_key,
		desired_generation, version, desired_state, observed_phase,
		latest_allocation_id, created_at, updated_at`

	workload, err := scanWorkload(tx.QueryRowContext(ctx, insertWorkload,
		p.WorkloadID, p.TenantID, p.ProjectID, p.OwnerService, p.OwnerResourceType,
		p.OwnerResourceID, nullableString(p.ComputeProfileID), nullableString(p.ComputeProfileRevision),
		[]byte(p.NormalizedRequest), p.RequestDigest, p.IdempotencyKey))
	if err == nil {
		payload, marshalErr := json.Marshal(struct {
			WorkloadID        string `json:"workload_id"`
			DesiredGeneration int64  `json:"desired_generation"`
			RequestDigest     string `json:"request_digest"`
		}{WorkloadID: workload.WorkloadID, DesiredGeneration: workload.DesiredGeneration, RequestDigest: workload.RequestDigest})
		if marshalErr != nil {
			return Workload{}, false, fmt.Errorf("marshal workload accepted event: %w", marshalErr)
		}

		const insertOutbox = `INSERT INTO compute_outbox_events (
			event_id, producer, event_type, schema_version, tenant_id,
			aggregate_type, aggregate_id, aggregate_version, occurred_at,
			correlation_id, causation_id, payload, delivery_state
		) VALUES ($1,$2,$3,1,$4,'compute_workload',$5,$6,now(),$7,$8,$9::jsonb,'PENDING')`
		if _, err := tx.ExecContext(ctx, insertOutbox, p.EventID, producerName,
			workloadAcceptedV1, p.TenantID, workload.WorkloadID, workload.Version,
			p.CorrelationID, nullableString(p.CausationID), payload); err != nil {
			return Workload{}, false, fmt.Errorf("insert workload outbox event: %w", err)
		}
		if err := tx.Commit(); err != nil {
			return Workload{}, false, fmt.Errorf("commit workload create transaction: %w", err)
		}
		return workload, true, nil
	}
	if !errors.Is(err, sql.ErrNoRows) {
		return Workload{}, false, fmt.Errorf("insert workload: %w", err)
	}

	// ON CONFLICT waits for a concurrent creator. Under READ COMMITTED this
	// following statement sees the committed row and verifies its fingerprint.
	const selectExisting = `SELECT workload_id, tenant_id, project_id, request_digest,
		idempotency_key, desired_generation, version, desired_state, observed_phase,
		latest_allocation_id, created_at, updated_at
		FROM compute_workloads WHERE tenant_id=$1 AND idempotency_key=$2`
	existing, err := scanWorkload(tx.QueryRowContext(ctx, selectExisting, p.TenantID, p.IdempotencyKey))
	if err != nil {
		return Workload{}, false, fmt.Errorf("read idempotent workload replay: %w", err)
	}
	if existing.RequestDigest != p.RequestDigest {
		return Workload{}, false, ErrIdempotencyConflict
	}
	// The canonical request digest alone is insufficient: the original
	// Workload creation also binds immutable tenant-scoped metadata which
	// is stored outside normalized_request. Reusing the same key/digest
	// with a different project, owner or profile must not return another
	// principal's Workload as though it were an exact replay.
	var sameIdentity bool
	if err := tx.QueryRowContext(ctx, `SELECT
		project_id=$3 AND owner_service=$4 AND owner_resource_type=$5
		AND owner_resource_id=$6
		AND compute_profile_id IS NOT DISTINCT FROM $7::text
		AND compute_profile_revision IS NOT DISTINCT FROM $8::text
		FROM compute_workloads
		WHERE tenant_id=$1 AND idempotency_key=$2`,
		p.TenantID, p.IdempotencyKey, p.ProjectID, p.OwnerService,
		p.OwnerResourceType, p.OwnerResourceID,
		nullableString(p.ComputeProfileID), nullableString(p.ComputeProfileRevision),
	).Scan(&sameIdentity); err != nil {
		return Workload{}, false, fmt.Errorf("verify idempotent Workload identity: %w", err)
	}
	if !sameIdentity {
		return Workload{}, false, ErrIdempotencyConflict
	}
	if err := tx.Commit(); err != nil {
		return Workload{}, false, fmt.Errorf("commit idempotent replay transaction: %w", err)
	}
	return existing, false, nil
}

func validateCreateWorkload(p CreateWorkloadParams) error {
	for _, value := range []string{
		p.WorkloadID, p.TenantID, p.ProjectID, p.OwnerService,
		p.OwnerResourceType, p.OwnerResourceID, p.IdempotencyKey,
		p.EventID, p.CorrelationID,
	} {
		if strings.TrimSpace(value) == "" {
			return ErrInvalidRequest
		}
	}
	if (p.ComputeProfileID == "") != (p.ComputeProfileRevision == "") {
		return ErrInvalidRequest
	}
	if len(p.NormalizedRequest) == 0 || !json.Valid(p.NormalizedRequest) {
		return ErrInvalidRequest
	}
	var object map[string]json.RawMessage
	if err := json.Unmarshal(p.NormalizedRequest, &object); err != nil || object == nil {
		return ErrInvalidRequest
	}
	// Project scope is part of both the authenticated Workload identity and
	// the normalized request sent across provider boundaries. Disagreement
	// must fail closed before persisting intent or dispatching an operation.
	var requestProjectID string
	if raw, ok := object["project_id"]; !ok ||
		json.Unmarshal(raw, &requestProjectID) != nil ||
		requestProjectID != p.ProjectID || requestProjectID == "" {
		return ErrInvalidRequest
	}
	if !validSHA256Digest(p.RequestDigest) {
		return ErrInvalidRequest
	}
	computedDigest, err := DigestRequest(p.NormalizedRequest)
	if err != nil || computedDigest != p.RequestDigest {
		return ErrInvalidRequest
	}
	return nil
}

func validSHA256Digest(value string) bool {
	if !strings.HasPrefix(value, "sha256:") || len(value) != len("sha256:")+64 {
		return false
	}
	digest := strings.TrimPrefix(value, "sha256:")
	if strings.ToLower(digest) != digest {
		return false
	}
	_, err := hex.DecodeString(digest)
	return err == nil
}

// DigestRequest hashes an RFC 8785-canonicalized JSON object. The caller must
// canonicalize the request before passing bytes here; CreateWorkload verifies
// that the stored request bytes and supplied digest agree exactly.
func DigestRequest(normalizedRequest []byte) (string, error) {
	if len(normalizedRequest) == 0 || !json.Valid(normalizedRequest) {
		return "", ErrInvalidRequest
	}
	var object map[string]json.RawMessage
	if err := json.Unmarshal(normalizedRequest, &object); err != nil || object == nil {
		return "", ErrInvalidRequest
	}
	digest := sha256.Sum256(normalizedRequest)
	return "sha256:" + hex.EncodeToString(digest[:]), nil
}

func scanWorkload(row *sql.Row) (Workload, error) {
	var workload Workload
	err := row.Scan(
		&workload.WorkloadID, &workload.TenantID, &workload.ProjectID,
		&workload.RequestDigest, &workload.IdempotencyKey, &workload.DesiredGeneration,
		&workload.Version, &workload.DesiredState, &workload.ObservedPhase,
		&workload.LatestAllocationID, &workload.CreatedAt, &workload.UpdatedAt,
	)
	return workload, err
}

func nullableString(value string) any {
	if value == "" {
		return nil
	}
	return value
}
