package postgres

import (
	"context"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/avaargsh/compute-control-panel/internal/economics"
)

const (
	usageIntervalUnit      = "GPU_DEVICE_COUNT"
	usageAttributionMethod = "DEVICE_BINDING"
	usageEvidenceQuality   = "DERIVED"
)

var (
	ErrInvalidUsageEvidence  = errors.New("invalid Allocation usage evidence")
	ErrUsageEvidenceNotReady = errors.New("Allocation usage evidence is not backed by a successful Provider operation")
	ErrUsageEvidenceConflict = errors.New("Allocation usage evidence conflicts with an existing immutable record")
)

// PersistUsageEvidenceParams contains source facts and trusted platform policy.
// The repository derives the UsageInterval itself; callers cannot submit a
// self-asserted FINAL status, digest, or arbitrary JSON payload.
type PersistUsageEvidenceParams struct {
	Evidence economics.AllocationUsageEvidence
	Policy   economics.EvidencePolicy
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
	EvidencePolicyVersion string
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

type usageEvidencePolicySnapshot struct {
	Version                        string
	AsOf                           time.Time
	MaximumClockSkew               time.Duration
	TrustedSourceTimestampAdapters []string
	CertifiedDeviceAdapters        []string
}

type usageEvidenceEnvelope struct {
	Evidence economics.AllocationUsageEvidence
	Policy   usageEvidencePolicySnapshot
}

type derivedUsageEvidence struct {
	record          UsageEvidenceSet
	sourceEventJSON []byte
	evidenceJSON    []byte
	usageJSON       []byte
}

const usageEvidenceSetColumns = `evidence_set_id, schema_version, tenant_id, project_id,
	workload_id, allocation_id, allocation_generation, provider_id, compute_operation_id,
	provider_resource_id, provider_resource_uid, pool_id, profile_id, gpu_model,
	capability_profile_id, evidence_policy_version, metering_policy_version,
	evidence_digest, usage_interval_id, usage_start, usage_end, gpu_count,
	source_event_ids, evidence_payload, usage_interval_payload, recorded_at`

func deriveUsageEvidence(p PersistUsageEvidenceParams) (derivedUsageEvidence, error) {
	if strings.TrimSpace(p.Policy.Version) == "" {
		return derivedUsageEvidence{}, fmt.Errorf("%w: trusted evidence policy version is empty", ErrInvalidUsageEvidence)
	}
	derivation, err := economics.DeriveUsageInterval(p.Evidence, p.Policy)
	if err != nil {
		return derivedUsageEvidence{}, fmt.Errorf("%w: derive Allocation usage: %v", ErrInvalidUsageEvidence, err)
	}
	if derivation.Status != economics.EvidenceStatusFinal || derivation.UsageInterval == nil {
		reason := derivation.ReasonCode
		if reason == "" {
			reason = derivation.Status
		}
		return derivedUsageEvidence{}, fmt.Errorf("%w: E1 derivation returned %s", ErrUsageEvidenceNotReady, reason)
	}

	usage := *derivation.UsageInterval
	if derivation.EvidenceDigest == "" || derivation.EvidenceDigest != usage.EvidenceDigest ||
		usage.SchemaVersion != "compute.economics.usage-interval.v1" ||
		usage.SourceCoverage != 1 || usage.Quality != usageEvidenceQuality ||
		usage.AttributionMethod != usageAttributionMethod ||
		len(usage.SourceEventIDs) == 0 {
		return derivedUsageEvidence{}, fmt.Errorf("%w: derived UsageInterval provenance is incomplete", ErrInvalidUsageEvidence)
	}
	gpuCount, err := strconv.ParseInt(usage.GPUCount, 10, 64)
	if err != nil || gpuCount <= 0 {
		return derivedUsageEvidence{}, fmt.Errorf("%w: derived GPU count must be a positive whole integer", ErrInvalidUsageEvidence)
	}

	trustedSources := make([]string, 0, len(p.Policy.TrustedSourceTimestampAdapters))
	for key, enabled := range p.Policy.TrustedSourceTimestampAdapters {
		if enabled {
			trustedSources = append(trustedSources, key)
		}
	}
	certifiedAdapters := make([]string, 0, len(p.Policy.CertifiedDeviceAdapters))
	for key, enabled := range p.Policy.CertifiedDeviceAdapters {
		if enabled {
			certifiedAdapters = append(certifiedAdapters, key)
		}
	}
	sort.Strings(trustedSources)
	sort.Strings(certifiedAdapters)

	envelope := usageEvidenceEnvelope{
		Evidence: p.Evidence,
		Policy: usageEvidencePolicySnapshot{
			Version:                         p.Policy.Version,
			AsOf:                            p.Policy.AsOf.UTC(),
			MaximumClockSkew:                p.Policy.MaximumClockSkew,
			TrustedSourceTimestampAdapters: trustedSources,
			CertifiedDeviceAdapters:        certifiedAdapters,
		},
	}
	evidenceJSON, err := json.Marshal(envelope)
	if err != nil {
		return derivedUsageEvidence{}, fmt.Errorf("%w: encode immutable evidence envelope: %v", ErrInvalidUsageEvidence, err)
	}
	usageJSON, err := json.Marshal(usage)
	if err != nil {
		return derivedUsageEvidence{}, fmt.Errorf("%w: encode derived UsageInterval: %v", ErrInvalidUsageEvidence, err)
	}
	sourceEventJSON, err := json.Marshal(usage.SourceEventIDs)
	if err != nil {
		return derivedUsageEvidence{}, fmt.Errorf("%w: encode source event references: %v", ErrInvalidUsageEvidence, err)
	}

	provider := p.Evidence.Provider
	record := UsageEvidenceSet{
		SchemaVersion:         p.Evidence.SchemaVersion,
		EvidenceSetID:         p.Evidence.EvidenceSetID,
		TenantID:              usage.TenantID,
		ProjectID:             usage.ProjectID,
		WorkloadID:            usage.WorkloadID,
		AllocationID:          usage.AllocationID,
		AllocationGeneration:  usage.AllocationGeneration,
		ProviderID:            usage.ProviderID,
		ComputeOperationID:    provider.Operation.ID,
		ProviderResourceID:    provider.ResourceID,
		ProviderResourceUID:   provider.ResourceUID,
		PoolID:                usage.PoolID,
		ProfileID:             usage.ProfileID,
		GPUModel:              usage.GPUModel,
		CapabilityProfileID:   usage.CapabilityProfileID,
		EvidencePolicyVersion: p.Policy.Version,
		MeteringPolicyVersion: usage.MeteringPolicyVersion,
		EvidenceDigest:        derivation.EvidenceDigest,
		UsageIntervalID:       usage.UsageIntervalID,
		UsageStart:            usage.Start.UTC(),
		UsageEnd:              usage.End.UTC(),
		GPUCount:              gpuCount,
		SourceEventIDs:        append([]string(nil), usage.SourceEventIDs...),
		EvidencePayload:       append([]byte(nil), evidenceJSON...),
		UsageIntervalPayload:  append([]byte(nil), usageJSON...),
	}
	if record.SchemaVersion != economics.AllocationUsageEvidenceSchemaVersion ||
		strings.TrimSpace(record.EvidenceSetID) == "" ||
		!validUsageEvidenceDigest(record.EvidenceDigest) ||
		record.UsageIntervalID != "usage_"+strings.TrimPrefix(record.EvidenceDigest, "sha256:") ||
		!record.UsageEnd.After(record.UsageStart) {
		return derivedUsageEvidence{}, fmt.Errorf("%w: invalid final evidence identity or interval", ErrInvalidUsageEvidence)
	}
	return derivedUsageEvidence{
		record: record, sourceEventJSON: sourceEventJSON,
		evidenceJSON: evidenceJSON, usageJSON: usageJSON,
	}, nil
}

func validUsageEvidenceDigest(value string) bool {
	if !strings.HasPrefix(value, "sha256:") || len(value) != len("sha256:")+64 {
		return false
	}
	_, err := hex.DecodeString(strings.TrimPrefix(value, "sha256:"))
	return err == nil
}

func scanUsageEvidenceSet(row usageEvidenceScanner) (UsageEvidenceSet, error) {
	var record UsageEvidenceSet
	var sourceIDs, evidencePayload, usagePayload []byte
	err := row.Scan(
		&record.EvidenceSetID, &record.SchemaVersion, &record.TenantID, &record.ProjectID,
		&record.WorkloadID, &record.AllocationID, &record.AllocationGeneration,
		&record.ProviderID, &record.ComputeOperationID, &record.ProviderResourceID,
		&record.ProviderResourceUID, &record.PoolID, &record.ProfileID, &record.GPUModel,
		&record.CapabilityProfileID, &record.EvidencePolicyVersion, &record.MeteringPolicyVersion,
		&record.EvidenceDigest, &record.UsageIntervalID, &record.UsageStart, &record.UsageEnd,
		&record.GPUCount, &sourceIDs, &evidencePayload, &usagePayload, &record.RecordedAt,
	)
	if err != nil {
		return UsageEvidenceSet{}, err
	}
	if err := json.Unmarshal(sourceIDs, &record.SourceEventIDs); err != nil {
		return UsageEvidenceSet{}, fmt.Errorf("decode persisted evidence source IDs: %w", err)
	}
	record.EvidencePayload = append([]byte(nil), evidencePayload...)
	record.UsageIntervalPayload = append([]byte(nil), usagePayload...)
	if record.EvidenceSetID == "" || record.TenantID == "" ||
		record.AllocationGeneration <= 0 || record.EvidencePolicyVersion == "" ||
		!validUsageEvidenceDigest(record.EvidenceDigest) || record.GPUCount <= 0 ||
		!record.UsageEnd.After(record.UsageStart) || len(record.SourceEventIDs) == 0 {
		return UsageEvidenceSet{}, errors.New("invalid persisted Allocation usage evidence")
	}
	return record, nil
}

// PersistUsageEvidenceSet reruns E1 derivation using the supplied source facts
// and trusted platform policy, then stores only a FINAL result. The database
// independently checks the successful Provider Operation, resource UID and
// Workload Project before the immutable row can be inserted.
func (r *Repository) PersistUsageEvidenceSet(ctx context.Context, p PersistUsageEvidenceParams) (UsageEvidenceSet, bool, error) {
	derived, err := deriveUsageEvidence(p)
	if err != nil {
		return UsageEvidenceSet{}, false, err
	}
	record := derived.record
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
		  AND a.phase IN ('BOUND', 'RELEASING', 'RELEASED')
		  AND o.provider_resource_id=$7
		  AND o.provider_resource_uid=$8
		  AND w.project_id=$9
		FOR SHARE OF o, w, a`,
		record.ComputeOperationID, record.TenantID, record.WorkloadID,
		record.AllocationID, record.ProviderID, record.AllocationGeneration,
		record.ProviderResourceID, record.ProviderResourceUID, record.ProjectID,
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
		capability_profile_id, evidence_policy_version, metering_policy_version,
		evidence_digest, usage_interval_id, usage_start, usage_end, gpu_count,
		source_event_ids, evidence_payload, usage_interval_payload,
		usage_unit, attribution_method, quality, source_coverage
	) VALUES (
		$1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17,$18,
		$19,$20,$21,$22,$23::jsonb,$24::jsonb,$25::jsonb,$26,$27,$28,$29
	)
	ON CONFLICT DO NOTHING
	RETURNING ` + usageEvidenceSetColumns
	record, err = scanUsageEvidenceSet(tx.QueryRowContext(ctx, insert,
		record.EvidenceSetID, record.SchemaVersion, record.TenantID, record.ProjectID,
		record.WorkloadID, record.AllocationID, record.AllocationGeneration,
		record.ProviderID, record.ComputeOperationID, record.ProviderResourceID,
		record.ProviderResourceUID, record.PoolID, record.ProfileID, record.GPUModel,
		record.CapabilityProfileID, record.EvidencePolicyVersion, record.MeteringPolicyVersion,
		record.EvidenceDigest, record.UsageIntervalID, record.UsageStart, record.UsageEnd,
		record.GPUCount, derived.sourceEventJSON, derived.evidenceJSON, derived.usageJSON,
		usageIntervalUnit, usageAttributionMethod, usageEvidenceQuality, 1,
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

	existing, payloadsMatch, err := findUsageEvidenceConflict(ctx, tx, record, derived.sourceEventJSON, derived.evidenceJSON, derived.usageJSON)
	if errors.Is(err, sql.ErrNoRows) {
		return UsageEvidenceSet{}, false, ErrUsageEvidenceConflict
	}
	if err != nil {
		return UsageEvidenceSet{}, false, fmt.Errorf("read conflicting usage evidence set: %w", err)
	}
	if !payloadsMatch || !sameUsageEvidenceIntent(existing, record) {
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
	record UsageEvidenceSet,
	sourceIDs, evidenceJSON, usageJSON []byte,
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
	var existing UsageEvidenceSet
	var persistedSourceIDs, persistedEvidence, persistedUsage []byte
	var payloadsMatch bool
	err := tx.QueryRowContext(ctx, query,
		record.TenantID, record.EvidenceSetID, record.AllocationID, record.AllocationGeneration,
		evidenceJSON, usageJSON, sourceIDs, record.UsageIntervalID,
	).Scan(
		&existing.EvidenceSetID, &existing.SchemaVersion, &existing.TenantID, &existing.ProjectID,
		&existing.WorkloadID, &existing.AllocationID, &existing.AllocationGeneration,
		&existing.ProviderID, &existing.ComputeOperationID, &existing.ProviderResourceID,
		&existing.ProviderResourceUID, &existing.PoolID, &existing.ProfileID, &existing.GPUModel,
		&existing.CapabilityProfileID, &existing.EvidencePolicyVersion, &existing.MeteringPolicyVersion,
		&existing.EvidenceDigest, &existing.UsageIntervalID, &existing.UsageStart, &existing.UsageEnd,
		&existing.GPUCount, &persistedSourceIDs, &persistedEvidence, &persistedUsage,
		&existing.RecordedAt, &payloadsMatch,
	)
	if err != nil {
		return UsageEvidenceSet{}, false, err
	}
	if err := json.Unmarshal(persistedSourceIDs, &existing.SourceEventIDs); err != nil {
		return UsageEvidenceSet{}, false, fmt.Errorf("decode conflicting evidence source IDs: %w", err)
	}
	existing.EvidencePayload = append([]byte(nil), persistedEvidence...)
	existing.UsageIntervalPayload = append([]byte(nil), persistedUsage...)
	return existing, payloadsMatch, nil
}

func sameUsageEvidenceIntent(existing, record UsageEvidenceSet) bool {
	return existing.SchemaVersion == record.SchemaVersion &&
		existing.EvidenceSetID == record.EvidenceSetID &&
		existing.TenantID == record.TenantID &&
		existing.ProjectID == record.ProjectID &&
		existing.WorkloadID == record.WorkloadID &&
		existing.AllocationID == record.AllocationID &&
		existing.AllocationGeneration == record.AllocationGeneration &&
		existing.ProviderID == record.ProviderID &&
		existing.ComputeOperationID == record.ComputeOperationID &&
		existing.ProviderResourceID == record.ProviderResourceID &&
		existing.ProviderResourceUID == record.ProviderResourceUID &&
		existing.PoolID == record.PoolID &&
		existing.ProfileID == record.ProfileID &&
		existing.GPUModel == record.GPUModel &&
		existing.CapabilityProfileID == record.CapabilityProfileID &&
		existing.EvidencePolicyVersion == record.EvidencePolicyVersion &&
		existing.MeteringPolicyVersion == record.MeteringPolicyVersion &&
		existing.EvidenceDigest == record.EvidenceDigest &&
		existing.UsageIntervalID == record.UsageIntervalID &&
		existing.UsageStart.Equal(record.UsageStart) &&
		existing.UsageEnd.Equal(record.UsageEnd) &&
		existing.GPUCount == record.GPUCount &&
		sameStringSlice(existing.SourceEventIDs, record.SourceEventIDs)
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
