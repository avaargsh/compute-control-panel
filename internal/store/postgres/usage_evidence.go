package postgres

import (
	"context"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"
)

const (
	AllocationUsageEvidenceSchemaVersion = "compute.economics.allocation-usage-evidence.v1"
	usageIntervalSchemaVersion           = "compute.economics.usage-interval.v1"
	usageIntervalUnit                    = "GPU_DEVICE_COUNT"
	usageAttributionMethod               = "DEVICE_BINDING"
	usageEvidenceQuality                 = "DERIVED"
)

var (
	ErrInvalidUsageEvidence = errors.New("invalid Allocation usage evidence")
	ErrUsageEvidenceNotReady = errors.New("Allocation usage evidence is not backed by a successful Provider operation")
	ErrUsageEvidenceConflict = errors.New("Allocation usage evidence conflicts with an existing immutable record")
)

// PersistUsageEvidenceParams is the normalized final evidence envelope accepted
// by the PostgreSQL boundary. EvidencePayload contains source identity and
// provenance references/digests only; it must not contain raw secrets or
// unredacted provider response bodies.
type PersistUsageEvidenceParams struct {
	SchemaVersion         string
	EvidenceSetID         string
	TenantID              string
	ProjectID             string
	WorkloadID            string
	AllocationID          string
	AllocationGeneration  int64
	ProviderID            string
	ComputeOperationID    string
	ProviderResourceID    string
	ProviderResourceUID   string
	PoolID                string
	ProfileID             string
	GPUModel              string
	CapabilityProfileID   string
	MeteringPolicyVersion string
	EvidenceDigest        string
	UsageIntervalID       string
	UsageStart            time.Time
	UsageEnd              time.Time
	GPUCount              int64
	SourceEventIDs        []string
	EvidencePayload       json.RawMessage
	UsageIntervalPayload  json.RawMessage
}

// UsageEvidenceSet is the immutable persistence projection for one FINAL
// UsageInterval. One Allocation generation can have only one such record in v1.
type UsageEvidenceSet struct {
	SchemaVersion         string
	EvidenceSetID         string
	TenantID              string
	ProjectID             string
	WorkloadID            string
	AllocationID          string
	AllocationGeneration  int64
	ProviderID            string
	ComputeOperationID    string
	ProviderResourceID    string
	ProviderResourceUID   string
	PoolID                string
	ProfileID             string
	GPUModel              string
	CapabilityProfileID   string
	MeteringPolicyVersion string
	EvidenceDigest        string
	UsageIntervalID       string
	UsageStart            time.Time
	UsageEnd              time.Time
	GPUCount              int64
	SourceEventIDs        []string
	EvidencePayload       json.RawMessage
	UsageIntervalPayload  json.RawMessage
	RecordedAt            time.Time
}

type usageEvidenceScanner interface {
	Scan(...any) error
}

type allocationEvidencePayload struct {
	SchemaVersion         string
	EvidenceSetID         string
	TenantID              string
	ProjectID             string
	WorkloadID            string
	AllocationID          string
	AllocationGeneration  int64
	ProviderID            string
	PoolID                string
	ProfileID             string
	CapabilityProfileID   string
	MeteringPolicyVersion string
	GPUModel              string
	GPUCount              string
	Provider              struct {
		Operation struct {
			ID                string
			TenantID          string
			ProjectID         string
			WorkloadID        string
			AllocationID      string
			DesiredGeneration int64
			ProviderID        string
			Status            string
		}
		ResourceID  string
		ResourceUID string
	}
}

type persistedUsageIntervalPayload struct {
	SchemaVersion         string
	UsageIntervalID       string
	TenantID              string
	ProjectID             string
	WorkloadID            string
	AllocationID          string
	AllocationGeneration  int64
	ProviderID            string
	PoolID                string
	ProfileID             string
	ResourceClass         string
	GPUModel              string
	Start                 time.Time
	End                   time.Time
	GPUCount              string
	Unit                  string
	MeteringPolicyVersion string
	SourceEventIDs        []string
	Quality               string
	AttributionMethod     string
	SourceCoverage        float64
	EvidenceSetID         string
	EvidenceDigest        string
	CapabilityProfileID   string
}

const usageEvidenceSetColumns = `evidence_set_id, schema_version, tenant_id, project_id,
	workload_id, allocation_id, allocation_generation, provider_id, compute_operation_id,
	provider_resource_id, provider_resource_uid, pool_id, profile_id, gpu_model,
	capability_profile_id, metering_policy_version, evidence_digest, usage_interval_id,
	usage_start, usage_end, gpu_count, source_event_ids, evidence_payload,
	usage_interval_payload, recorded_at`

func validateUsageEvidenceParams(p PersistUsageEvidenceParams) error {
	for _, value := range []string{
		p.EvidenceSetID, p.TenantID, p.ProjectID, p.WorkloadID, p.AllocationID,
		p.ProviderID, p.ComputeOperationID, p.ProviderResourceID, p.ProviderResourceUID,
		p.PoolID, p.ProfileID, p.GPUModel, p.CapabilityProfileID, p.MeteringPolicyVersion,
	} {
		if strings.TrimSpace(value) == "" {
			return fmt.Errorf("%w: required identity or policy field is empty", ErrInvalidUsageEvidence)
		}
	}
	if p.SchemaVersion != AllocationUsageEvidenceSchemaVersion ||
		p.AllocationGeneration <= 0 ||
		!validUsageEvidenceDigest(p.EvidenceDigest) ||
		p.UsageIntervalID != "usage_"+strings.TrimPrefix(p.EvidenceDigest, "sha256:") ||
		p.UsageStart.IsZero() || p.UsageEnd.IsZero() || !p.UsageEnd.After(p.UsageStart) ||
		p.GPUCount <= 0 {
		return fmt.Errorf("%w: invalid schema, identity, digest or usage interval", ErrInvalidUsageEvidence)
	}
	if len(p.SourceEventIDs) == 0 {
		return fmt.Errorf("%w: source event references are required", ErrInvalidUsageEvidence)
	}
	seen := make(map[string]struct{}, len(p.SourceEventIDs))
	for _, eventID := range p.SourceEventIDs {
		if strings.TrimSpace(eventID) == "" {
			return fmt.Errorf("%w: empty source event reference", ErrInvalidUsageEvidence)
		}
		if _, exists := seen[eventID]; exists {
			return fmt.Errorf("%w: duplicate source event reference", ErrInvalidUsageEvidence)
		}
		seen[eventID] = struct{}{}
	}

	if _, err := strictJSONObject(p.EvidencePayload); err != nil {
		return fmt.Errorf("%w: evidence payload: %v", ErrInvalidUsageEvidence, err)
	}
	if _, err := strictJSONObject(p.UsageIntervalPayload); err != nil {
		return fmt.Errorf("%w: UsageInterval payload: %v", ErrInvalidUsageEvidence, err)
	}
	if err := validateAllocationEvidencePayload(p); err != nil {
		return err
	}
	if err := validatePersistedUsageIntervalPayload(p); err != nil {
		return err
	}
	return nil
}

func validUsageEvidenceDigest(value string) bool {
	if !strings.HasPrefix(value, "sha256:") || len(value) != len("sha256:")+64 {
		return false
	}
	_, err := hex.DecodeString(strings.TrimPrefix(value, "sha256:"))
	return err == nil
}

func validateAllocationEvidencePayload(p PersistUsageEvidenceParams) error {
	var payload allocationEvidencePayload
	if err := json.Unmarshal(p.EvidencePayload, &payload); err != nil {
		return fmt.Errorf("%w: decode allocation evidence payload: %v", ErrInvalidUsageEvidence, err)
	}
	op := payload.Provider.Operation
	if payload.SchemaVersion != p.SchemaVersion ||
		payload.EvidenceSetID != p.EvidenceSetID ||
		payload.TenantID != p.TenantID ||
		payload.ProjectID != p.ProjectID ||
		payload.WorkloadID != p.WorkloadID ||
		payload.AllocationID != p.AllocationID ||
		payload.AllocationGeneration != p.AllocationGeneration ||
		payload.ProviderID != p.ProviderID ||
		payload.PoolID != p.PoolID ||
		payload.ProfileID != p.ProfileID ||
		payload.GPUModel != p.GPUModel ||
		payload.GPUCount != fmt.Sprint(p.GPUCount) ||
		payload.CapabilityProfileID != p.CapabilityProfileID ||
		payload.MeteringPolicyVersion != p.MeteringPolicyVersion ||
		payload.Provider.ResourceID != p.ProviderResourceID ||
		payload.Provider.ResourceUID != p.ProviderResourceUID ||
		op.ID != p.ComputeOperationID ||
		op.TenantID != p.TenantID ||
		op.ProjectID != p.ProjectID ||
		op.WorkloadID != p.WorkloadID ||
		op.AllocationID != p.AllocationID ||
		op.DesiredGeneration != p.AllocationGeneration ||
		op.ProviderID != p.ProviderID ||
		op.Status != "SUCCEEDED" {
		return fmt.Errorf("%w: evidence payload identity does not match persistence scope", ErrInvalidUsageEvidence)
	}
	return nil
}

func validatePersistedUsageIntervalPayload(p PersistUsageEvidenceParams) error {
	var payload persistedUsageIntervalPayload
	if err := json.Unmarshal(p.UsageIntervalPayload, &payload); err != nil {
		return fmt.Errorf("%w: decode UsageInterval payload: %v", ErrInvalidUsageEvidence, err)
	}
	if payload.SchemaVersion != usageIntervalSchemaVersion ||
		payload.UsageIntervalID != p.UsageIntervalID ||
		payload.TenantID != p.TenantID ||
		payload.ProjectID != p.ProjectID ||
		payload.WorkloadID != p.WorkloadID ||
		payload.AllocationID != p.AllocationID ||
		payload.AllocationGeneration != p.AllocationGeneration ||
		payload.ProviderID != p.ProviderID ||
		payload.PoolID != p.PoolID ||
		payload.ProfileID != p.ProfileID ||
		payload.ResourceClass != "GPU" ||
		payload.GPUModel != p.GPUModel ||
		payload.Start.IsZero() || !payload.Start.Equal(p.UsageStart) ||
		payload.End.IsZero() || !payload.End.Equal(p.UsageEnd) ||
		payload.GPUCount != fmt.Sprint(p.GPUCount) ||
		payload.Unit != usageIntervalUnit ||
		payload.MeteringPolicyVersion != p.MeteringPolicyVersion ||
		!sameStringSlice(payload.SourceEventIDs, p.SourceEventIDs) ||
		payload.Quality != usageEvidenceQuality ||
		payload.AttributionMethod != usageAttributionMethod ||
		payload.SourceCoverage != 1 ||
		payload.EvidenceSetID != p.EvidenceSetID ||
		payload.EvidenceDigest != p.EvidenceDigest ||
		payload.CapabilityProfileID != p.CapabilityProfileID {
		return fmt.Errorf("%w: UsageInterval payload does not match evidence scope", ErrInvalidUsageEvidence)
	}
	return nil
}

func sameStringSlice(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

func scanUsageEvidenceSet(row usageEvidenceScanner) (UsageEvidenceSet, error) {
	var record UsageEvidenceSet
	var sourceIDs, evidencePayload, usagePayload []byte
	err := row.Scan(
		&record.EvidenceSetID, &record.SchemaVersion, &record.TenantID, &record.ProjectID,
		&record.WorkloadID, &record.AllocationID, &record.AllocationGeneration,
		&record.ProviderID, &record.ComputeOperationID, &record.ProviderResourceID,
		&record.ProviderResourceUID, &record.PoolID, &record.ProfileID, &record.GPUModel,
		&record.CapabilityProfileID, &record.MeteringPolicyVersion, &record.EvidenceDigest,
		&record.UsageIntervalID, &record.UsageStart, &record.UsageEnd, &record.GPUCount,
		&sourceIDs, &evidencePayload, &usagePayload, &record.RecordedAt,
	)
	if err != nil {
		return UsageEvidenceSet{}, err
	}
	if err := json.Unmarshal(sourceIDs, &record.SourceEventIDs); err != nil {
		return UsageEvidenceSet{}, fmt.Errorf("decode persisted evidence source IDs: %w", err)
	}
	record.EvidencePayload = append([]byte(nil), evidencePayload...)
	record.UsageIntervalPayload = append([]byte(nil), usagePayload...)
	if record.EvidenceSetID == "" || record.TenantID == "" || record.AllocationGeneration <= 0 ||
		!validUsageEvidenceDigest(record.EvidenceDigest) || record.GPUCount <= 0 ||
		!record.UsageEnd.After(record.UsageStart) || len(record.SourceEventIDs) == 0 {
		return UsageEvidenceSet{}, errors.New("invalid persisted Allocation usage evidence")
	}
	return record, nil
}

// PersistUsageEvidenceSet stores a complete E1 evidence set after rechecking
// the exact successful Provider Operation and workload Project in PostgreSQL.
// A single FINAL record per Allocation generation prevents overlapping
// evidence from being costed twice. Exact replay is idempotent.
func (r *Repository) PersistUsageEvidenceSet(ctx context.Context, p PersistUsageEvidenceParams) (UsageEvidenceSet, bool, error) {
	if err := validateUsageEvidenceParams(p); err != nil {
		return UsageEvidenceSet{}, false, err
	}
	sourceIDs, err := json.Marshal(p.SourceEventIDs)
	if err != nil {
		return UsageEvidenceSet{}, false, fmt.Errorf("marshal evidence source IDs: %w", err)
	}

	tx, err := r.db.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelReadCommitted})
	if err != nil {
		return UsageEvidenceSet{}, false, fmt.Errorf("begin usage evidence transaction: %w", err)
	}
	defer func() { _ = tx.Rollback() }()

	var operationID string
	err = tx.QueryRowContext(ctx, `
		SELECT o.compute_operation_id
		FROM compute_operations o
		JOIN compute_workloads w
		  ON w.workload_id=o.workload_id AND w.tenant_id=o.tenant_id
		JOIN compute_allocations a
		  ON a.allocation_id=o.allocation_id
		 AND a.workload_id=o.workload_id
		 AND a.tenant_id=o.tenant_id
		 AND a.provider_id=o.provider_id
		 AND a.desired_generation=o.desired_generation
		WHERE o.compute_operation_id=$1
		  AND o.tenant_id=$2
		  AND o.workload_id=$3
		  AND o.allocation_id=$4
		  AND o.provider_id=$5
		  AND o.desired_generation=$6
		  AND o.operation_kind IN ('CREATE', 'UPDATE')
		  AND o.status='SUCCEEDED'
		  AND o.provider_resource_id=$7
		  AND o.provider_resource_uid=$8
		  AND w.project_id=$9
		FOR SHARE OF o, w, a`,
		p.ComputeOperationID, p.TenantID, p.WorkloadID, p.AllocationID,
		p.ProviderID, p.AllocationGeneration, p.ProviderResourceID,
		p.ProviderResourceUID, p.ProjectID,
	).Scan(&operationID)
	if errors.Is(err, sql.ErrNoRows) {
		return UsageEvidenceSet{}, false, ErrUsageEvidenceNotReady
	}
	if err != nil {
		return UsageEvidenceSet{}, false, fmt.Errorf("verify successful Provider evidence operation: %w", err)
	}

	insert := `INSERT INTO compute_allocation_usage_evidence_sets (
		evidence_set_id, schema_version, tenant_id, project_id, workload_id,
		allocation_id, allocation_generation, provider_id, compute_operation_id,
		provider_resource_id, provider_resource_uid, pool_id, profile_id, gpu_model,
		capability_profile_id, metering_policy_version, evidence_digest, usage_interval_id,
		usage_start, usage_end, gpu_count, source_event_ids, evidence_payload,
		usage_interval_payload, usage_unit, attribution_method, quality, source_coverage
	) VALUES (
		$1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17,$18,
		$19,$20,$21,$22::jsonb,$23::jsonb,$24::jsonb,$25,$26,$27,$28
	)
	ON CONFLICT DO NOTHING
	RETURNING ` + usageEvidenceSetColumns
	record, err := scanUsageEvidenceSet(tx.QueryRowContext(ctx, insert,
		p.EvidenceSetID, p.SchemaVersion, p.TenantID, p.ProjectID, p.WorkloadID,
		p.AllocationID, p.AllocationGeneration, p.ProviderID, p.ComputeOperationID,
		p.ProviderResourceID, p.ProviderResourceUID, p.PoolID, p.ProfileID, p.GPUModel,
		p.CapabilityProfileID, p.MeteringPolicyVersion, p.EvidenceDigest, p.UsageIntervalID,
		p.UsageStart, p.UsageEnd, p.GPUCount, sourceIDs, []byte(p.EvidencePayload),
		[]byte(p.UsageIntervalPayload), usageIntervalUnit, usageAttributionMethod,
		usageEvidenceQuality, 1,
	))
	if err == nil {
		if err := tx.Commit(); err != nil {
			return UsageEvidenceSet{}, false, fmt.Errorf("commit usage evidence set: %w", err)
		}
		return record, true, nil
	}
	if !errors.Is(err, sql.ErrNoRows) {
		return UsageEvidenceSet{}, false, fmt.Errorf("insert usage evidence set: %w", err)
	}

	existing, payloadsMatch, err := findUsageEvidenceConflict(ctx, tx, p, sourceIDs)
	if errors.Is(err, sql.ErrNoRows) {
		return UsageEvidenceSet{}, false, ErrUsageEvidenceConflict
	}
	if err != nil {
		return UsageEvidenceSet{}, false, fmt.Errorf("read conflicting usage evidence set: %w", err)
	}
	if !payloadsMatch || !sameUsageEvidenceIntent(existing, p) {
		return UsageEvidenceSet{}, false, ErrUsageEvidenceConflict
	}
	if err := tx.Commit(); err != nil {
		return UsageEvidenceSet{}, false, fmt.Errorf("commit usage evidence replay: %w", err)
	}
	return existing, false, nil
}

func findUsageEvidenceConflict(
	ctx context.Context,
	tx *sql.Tx,
	p PersistUsageEvidenceParams,
	sourceIDs []byte,
) (UsageEvidenceSet, bool, error) {
	query := `SELECT ` + usageEvidenceSetColumns + `,
		(evidence_payload=$5::jsonb
		 AND usage_interval_payload=$6::jsonb
		 AND source_event_ids=$7::jsonb) AS payloads_match
		FROM compute_allocation_usage_evidence_sets
		WHERE tenant_id=$1
		  AND (evidence_set_id=$2
		       OR (allocation_id=$3 AND allocation_generation=$4)
		       OR usage_interval_id=$8)
		ORDER BY CASE WHEN evidence_set_id=$2 THEN 0
		              WHEN allocation_id=$3 AND allocation_generation=$4 THEN 1
		              ELSE 2 END
		LIMIT 1
		FOR SHARE`
	var record UsageEvidenceSet
	var sourceEventJSON, evidencePayload, usagePayload []byte
	var payloadsMatch bool
	err := tx.QueryRowContext(ctx, query,
		p.TenantID, p.EvidenceSetID, p.AllocationID, p.AllocationGeneration,
		[]byte(p.EvidencePayload), []byte(p.UsageIntervalPayload), sourceIDs,
		p.UsageIntervalID,
	).Scan(
		&record.EvidenceSetID, &record.SchemaVersion, &record.TenantID, &record.ProjectID,
		&record.WorkloadID, &record.AllocationID, &record.AllocationGeneration,
		&record.ProviderID, &record.ComputeOperationID, &record.ProviderResourceID,
		&record.ProviderResourceUID, &record.PoolID, &record.ProfileID, &record.GPUModel,
		&record.CapabilityProfileID, &record.MeteringPolicyVersion, &record.EvidenceDigest,
		&record.UsageIntervalID, &record.UsageStart, &record.UsageEnd, &record.GPUCount,
		&sourceEventJSON, &evidencePayload, &usagePayload, &record.RecordedAt,
		&payloadsMatch,
	)
	if err != nil {
		return UsageEvidenceSet{}, false, err
	}
	if err := json.Unmarshal(sourceEventJSON, &record.SourceEventIDs); err != nil {
		return UsageEvidenceSet{}, false, fmt.Errorf("decode conflicting evidence source IDs: %w", err)
	}
	record.EvidencePayload = append([]byte(nil), evidencePayload...)
	record.UsageIntervalPayload = append([]byte(nil), usagePayload...)
	return record, payloadsMatch, nil
}

func sameUsageEvidenceIntent(existing UsageEvidenceSet, p PersistUsageEvidenceParams) bool {
	return existing.SchemaVersion == p.SchemaVersion &&
		existing.EvidenceSetID == p.EvidenceSetID &&
		existing.TenantID == p.TenantID &&
		existing.ProjectID == p.ProjectID &&
		existing.WorkloadID == p.WorkloadID &&
		existing.AllocationID == p.AllocationID &&
		existing.AllocationGeneration == p.AllocationGeneration &&
		existing.ProviderID == p.ProviderID &&
		existing.ComputeOperationID == p.ComputeOperationID &&
		existing.ProviderResourceID == p.ProviderResourceID &&
		existing.ProviderResourceUID == p.ProviderResourceUID &&
		existing.PoolID == p.PoolID &&
		existing.ProfileID == p.ProfileID &&
		existing.GPUModel == p.GPUModel &&
		existing.CapabilityProfileID == p.CapabilityProfileID &&
		existing.MeteringPolicyVersion == p.MeteringPolicyVersion &&
		existing.EvidenceDigest == p.EvidenceDigest &&
		existing.UsageIntervalID == p.UsageIntervalID &&
		existing.UsageStart.Equal(p.UsageStart) &&
		existing.UsageEnd.Equal(p.UsageEnd) &&
		existing.GPUCount == p.GPUCount &&
		sameStringSlice(existing.SourceEventIDs, p.SourceEventIDs)
}

// GetUsageEvidenceSet returns evidence only within the requested tenant scope.
// Project/service authorization remains the HTTP/API caller's responsibility.
func (r *Repository) GetUsageEvidenceSet(ctx context.Context, tenantID, evidenceSetID string) (UsageEvidenceSet, error) {
	if strings.TrimSpace(tenantID) == "" || strings.TrimSpace(evidenceSetID) == "" {
		return UsageEvidenceSet{}, ErrInvalidUsageEvidence
	}
	return scanUsageEvidenceSet(r.db.QueryRowContext(ctx,
		"SELECT "+usageEvidenceSetColumns+" FROM compute_allocation_usage_evidence_sets WHERE tenant_id=$1 AND evidence_set_id=$2",
		tenantID, evidenceSetID,
	))
}
