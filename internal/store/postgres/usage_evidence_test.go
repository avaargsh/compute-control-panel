package postgres

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"sync"
	"testing"
	"time"
)

func usageEvidenceFixture(accepted WorkloadAcceptance) PersistUsageEvidenceParams {
	digest := "sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
	params := PersistUsageEvidenceParams{
		SchemaVersion:         AllocationUsageEvidenceSchemaVersion,
		EvidenceSetID:         "evidence-set-" + accepted.Allocation.AllocationID,
		TenantID:              accepted.Workload.TenantID,
		ProjectID:              accepted.Workload.ProjectID,
		WorkloadID:             accepted.Workload.WorkloadID,
		AllocationID:           accepted.Allocation.AllocationID,
		AllocationGeneration:   accepted.Allocation.DesiredGeneration,
		ProviderID:             accepted.Allocation.ProviderID,
		ComputeOperationID:     accepted.Operation.ID,
		ProviderResourceID:     "provider-resource-" + accepted.Allocation.AllocationID,
		ProviderResourceUID:    "provider-resource-uid-" + accepted.Allocation.AllocationID,
		PoolID:                 accepted.Allocation.PoolID,
		ProfileID:              "profile-h100",
		GPUModel:               "NVIDIA-H100",
		CapabilityProfileID:    "capability-profile-h100-v1",
		MeteringPolicyVersion:  "metering-policy-v1",
		EvidenceDigest:         digest,
		UsageIntervalID:        "usage_" + digest[len("sha256:"):],
		UsageStart:             time.Date(2026, 11, 4, 8, 0, 0, 0, time.UTC),
		UsageEnd:               time.Date(2026, 11, 4, 9, 30, 0, 0, time.UTC),
		GPUCount:               2,
		SourceEventIDs: []string{
			"device-assignment-" + accepted.Allocation.AllocationID,
			"device-release-" + accepted.Allocation.AllocationID,
			"kueue-admission-" + accepted.Allocation.AllocationID,
			"kueue-release-" + accepted.Allocation.AllocationID,
			"pod-scheduled-" + accepted.Allocation.AllocationID,
			"provider-binding-" + accepted.Allocation.AllocationID,
			"provider-liability-start-" + accepted.Allocation.AllocationID,
		},
	}
	params.EvidencePayload = marshalAllocationEvidencePayload(params)
	params.UsageIntervalPayload = marshalUsageIntervalPayload(params)
	return params
}

func validUsageEvidenceParams() PersistUsageEvidenceParams {
	params := PersistUsageEvidenceParams{
		SchemaVersion:         AllocationUsageEvidenceSchemaVersion,
		EvidenceSetID:         "evidence-valid",
		TenantID:              "tenant-valid",
		ProjectID:              "project-valid",
		WorkloadID:             "workload-valid",
		AllocationID:           "allocation-valid",
		AllocationGeneration:   1,
		ProviderID:             "provider-valid",
		ComputeOperationID:     "operation-valid",
		ProviderResourceID:     "resource-valid",
		ProviderResourceUID:    "resource-uid-valid",
		PoolID:                 "pool-valid",
		ProfileID:              "profile-valid",
		GPUModel:               "GPU-valid",
		CapabilityProfileID:    "capability-profile-valid",
		MeteringPolicyVersion:  "metering-policy-v1",
		EvidenceDigest:         "sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
		UsageIntervalID:        "usage_aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
		UsageStart:             time.Date(2026, 11, 4, 8, 0, 0, 0, time.UTC),
		UsageEnd:               time.Date(2026, 11, 4, 9, 0, 0, 0, time.UTC),
		GPUCount:               1,
		SourceEventIDs:         []string{"source-a", "source-b"},
	}
	params.EvidencePayload = marshalAllocationEvidencePayload(params)
	params.UsageIntervalPayload = marshalUsageIntervalPayload(params)
	return params
}

func marshalAllocationEvidencePayload(p PersistUsageEvidenceParams) json.RawMessage {
	var payload allocationEvidencePayload
	payload.SchemaVersion = p.SchemaVersion
	payload.EvidenceSetID = p.EvidenceSetID
	payload.TenantID = p.TenantID
	payload.ProjectID = p.ProjectID
	payload.WorkloadID = p.WorkloadID
	payload.AllocationID = p.AllocationID
	payload.AllocationGeneration = p.AllocationGeneration
	payload.ProviderID = p.ProviderID
	payload.PoolID = p.PoolID
	payload.ProfileID = p.ProfileID
	payload.GPUModel = p.GPUModel
	payload.GPUCount = jsonNumber(p.GPUCount)
	payload.CapabilityProfileID = p.CapabilityProfileID
	payload.MeteringPolicyVersion = p.MeteringPolicyVersion
	payload.Provider.ResourceID = p.ProviderResourceID
	payload.Provider.ResourceUID = p.ProviderResourceUID
	payload.Provider.Operation.ID = p.ComputeOperationID
	payload.Provider.Operation.TenantID = p.TenantID
	payload.Provider.Operation.ProjectID = p.ProjectID
	payload.Provider.Operation.WorkloadID = p.WorkloadID
	payload.Provider.Operation.AllocationID = p.AllocationID
	payload.Provider.Operation.DesiredGeneration = p.AllocationGeneration
	payload.Provider.Operation.ProviderID = p.ProviderID
	payload.Provider.Operation.Status = "SUCCEEDED"
	data, err := json.Marshal(payload)
	if err != nil {
		panic(err)
	}
	return data
}

func marshalUsageIntervalPayload(p PersistUsageEvidenceParams) json.RawMessage {
	payload := persistedUsageIntervalPayload{
		SchemaVersion:         usageIntervalSchemaVersion,
		UsageIntervalID:       p.UsageIntervalID,
		TenantID:              p.TenantID,
		ProjectID:              p.ProjectID,
		WorkloadID:             p.WorkloadID,
		AllocationID:           p.AllocationID,
		AllocationGeneration:   p.AllocationGeneration,
		ProviderID:             p.ProviderID,
		PoolID:                 p.PoolID,
		ProfileID:              p.ProfileID,
		ResourceClass:          "GPU",
		GPUModel:               p.GPUModel,
		Start:                  p.UsageStart,
		End:                    p.UsageEnd,
		GPUCount:               jsonNumber(p.GPUCount),
		Unit:                   usageIntervalUnit,
		MeteringPolicyVersion:  p.MeteringPolicyVersion,
		SourceEventIDs:         append([]string(nil), p.SourceEventIDs...),
		Quality:                usageEvidenceQuality,
		AttributionMethod:      usageAttributionMethod,
		SourceCoverage:         1,
		EvidenceSetID:          p.EvidenceSetID,
		EvidenceDigest:         p.EvidenceDigest,
		CapabilityProfileID:    p.CapabilityProfileID,
	}
	data, err := json.Marshal(payload)
	if err != nil {
		panic(err)
	}
	return data
}

func jsonNumber(value int64) string {
	return fmt.Sprint(value)
}

func markUsageEvidenceProviderOperationSucceeded(t *testing.T, db *sql.DB, accepted WorkloadAcceptance) {
	t.Helper()
	_, err := db.ExecContext(context.Background(), `
		UPDATE compute_operations
		SET status='SUCCEEDED', provider_resource_id=$3, provider_resource_uid=$4,
		    updated_at=clock_timestamp()
		WHERE tenant_id=$1 AND compute_operation_id=$2`,
		accepted.Workload.TenantID, accepted.Operation.ID,
		"provider-resource-"+accepted.Allocation.AllocationID,
		"provider-resource-uid-"+accepted.Allocation.AllocationID,
	)
	if err != nil {
		t.Fatalf("mark Provider operation successful: %v", err)
	}
}

func TestPersistUsageEvidenceSetRequiresSuccessfulProviderOperation(t *testing.T) {
	db := openIntegrationDB(t)
	repo, err := NewRepository(db)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	acceptedParams := acceptanceFixture(t, "tenant-evidence-pending-"+randomSuffix(t),
		"workload-key-"+randomSuffix(t), "operation-key-"+randomSuffix(t),
		"operation-event-"+randomSuffix(t))
	accepted, created, err := repo.AcceptWorkload(ctx, acceptedParams)
	if err != nil || !created {
		t.Fatalf("AcceptWorkload: created=%t err=%v", created, err)
	}

	params := usageEvidenceFixture(accepted)
	if _, persisted, err := repo.PersistUsageEvidenceSet(ctx, params); !errors.Is(err, ErrUsageEvidenceNotReady) || persisted {
		t.Fatalf("PENDING Provider operation persisted evidence: persisted=%t err=%v, want ErrUsageEvidenceNotReady", persisted, err)
	}
	var count int
	if err := db.QueryRowContext(ctx, `
		SELECT count(*) FROM compute_allocation_usage_evidence_sets
		WHERE tenant_id=$1 AND allocation_id=$2`,
		params.TenantID, params.AllocationID).Scan(&count); err != nil {
		t.Fatal(err)
	}
	if count != 0 {
		t.Fatalf("PENDING Provider operation created %d evidence rows, want 0", count)
	}
}

func TestPersistUsageEvidenceSetIsImmutableIdempotentAndTenantScoped(t *testing.T) {
	db := openIntegrationDB(t)
	repo, err := NewRepository(db)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	acceptedParams := acceptanceFixture(t, "tenant-evidence-"+randomSuffix(t),
		"workload-key-"+randomSuffix(t), "operation-key-"+randomSuffix(t),
		"operation-event-"+randomSuffix(t))
	accepted, created, err := repo.AcceptWorkload(ctx, acceptedParams)
	if err != nil || !created {
		t.Fatalf("AcceptWorkload: created=%t err=%v", created, err)
	}
	markUsageEvidenceProviderOperationSucceeded(t, db, accepted)

	params := usageEvidenceFixture(accepted)
	first, persisted, err := repo.PersistUsageEvidenceSet(ctx, params)
	if err != nil || !persisted {
		t.Fatalf("first PersistUsageEvidenceSet: persisted=%t err=%v", persisted, err)
	}
	replayed, persisted, err := repo.PersistUsageEvidenceSet(ctx, params)
	if err != nil || persisted {
		t.Fatalf("exact replay: persisted=%t err=%v, want idempotent replay", persisted, err)
	}
	if replayed.EvidenceSetID != first.EvidenceSetID ||
		replayed.EvidenceDigest != first.EvidenceDigest ||
		replayed.UsageIntervalID != first.UsageIntervalID ||
		!replayed.RecordedAt.Equal(first.RecordedAt) {
		t.Fatalf("exact replay changed immutable evidence: first=%+v replay=%+v", first, replayed)
	}

	changed := params
	changed.EvidenceDigest = "sha256:bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
	changed.UsageIntervalID = "usage_" + changed.EvidenceDigest[len("sha256:"):]
	changed.EvidencePayload = marshalAllocationEvidencePayload(changed)
	changed.UsageIntervalPayload = marshalUsageIntervalPayload(changed)
	if _, persisted, err := repo.PersistUsageEvidenceSet(ctx, changed); !errors.Is(err, ErrUsageEvidenceConflict) || persisted {
		t.Fatalf("same EvidenceSetID with changed digest: persisted=%t err=%v, want conflict", persisted, err)
	}

	duplicateAllocation := params
	duplicateAllocation.EvidenceSetID += "-duplicate"
	duplicateAllocation.EvidencePayload = marshalAllocationEvidencePayload(duplicateAllocation)
	duplicateAllocation.UsageIntervalPayload = marshalUsageIntervalPayload(duplicateAllocation)
	if _, persisted, err := repo.PersistUsageEvidenceSet(ctx, duplicateAllocation); !errors.Is(err, ErrUsageEvidenceConflict) || persisted {
		t.Fatalf("second FINAL evidence set for one Allocation generation: persisted=%t err=%v, want conflict", persisted, err)
	}

	if _, err := repo.GetUsageEvidenceSet(ctx, "tenant-from-another-customer", params.EvidenceSetID); !errors.Is(err, sql.ErrNoRows) {
		t.Fatalf("cross-tenant evidence read error = %v, want sql.ErrNoRows", err)
	}
	got, err := repo.GetUsageEvidenceSet(ctx, params.TenantID, params.EvidenceSetID)
	if err != nil {
		t.Fatalf("GetUsageEvidenceSet: %v", err)
	}
	if got.AllocationID != params.AllocationID || got.ProviderResourceUID != params.ProviderResourceUID ||
		got.EvidenceDigest != params.EvidenceDigest || got.GPUCount != params.GPUCount {
		t.Fatalf("persisted evidence scope mismatch: %+v", got)
	}

	if _, err := db.ExecContext(ctx, `
		UPDATE compute_allocation_usage_evidence_sets
		SET evidence_digest='sha256:' || repeat('c', 64)
		WHERE tenant_id=$1 AND evidence_set_id=$2`,
		params.TenantID, params.EvidenceSetID); err == nil {
		t.Fatal("immutable evidence set accepted UPDATE")
	}
	if _, err := db.ExecContext(ctx, `
		DELETE FROM compute_allocation_usage_evidence_sets
		WHERE tenant_id=$1 AND evidence_set_id=$2`,
		params.TenantID, params.EvidenceSetID); err == nil {
		t.Fatal("immutable evidence set accepted DELETE")
	}
}

func TestPersistUsageEvidenceSetConcurrentReplayHasOneWinner(t *testing.T) {
	db := openIntegrationDB(t)
	repo, err := NewRepository(db)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	acceptedParams := acceptanceFixture(t, "tenant-evidence-race-"+randomSuffix(t),
		"workload-key-"+randomSuffix(t), "operation-key-"+randomSuffix(t),
		"operation-event-"+randomSuffix(t))
	accepted, created, err := repo.AcceptWorkload(ctx, acceptedParams)
	if err != nil || !created {
		t.Fatalf("AcceptWorkload: created=%t err=%v", created, err)
	}
	markUsageEvidenceProviderOperationSucceeded(t, db, accepted)
	params := usageEvidenceFixture(accepted)

	const workers = 8
	results := make([]UsageEvidenceSet, workers)
	createdFlags := make([]bool, workers)
	errs := make([]error, workers)
	var wg sync.WaitGroup
	wg.Add(workers)
	for i := 0; i < workers; i++ {
		i := i
		go func() {
			defer wg.Done()
			results[i], createdFlags[i], errs[i] = repo.PersistUsageEvidenceSet(ctx, params)
		}()
	}
	wg.Wait()

	winners := 0
	for i := range results {
		if errs[i] != nil {
			t.Fatalf("concurrent PersistUsageEvidenceSet[%d]: %v", i, errs[i])
		}
		if createdFlags[i] {
			winners++
		}
		if results[i].EvidenceSetID != params.EvidenceSetID ||
			results[i].EvidenceDigest != params.EvidenceDigest {
			t.Fatalf("concurrent caller %d received different evidence: %+v", i, results[i])
		}
	}
	if winners != 1 {
		t.Fatalf("created=true count = %d, want exactly one", winners)
	}

	var count int
	if err := db.QueryRowContext(ctx, `
		SELECT count(*) FROM compute_allocation_usage_evidence_sets
		WHERE tenant_id=$1 AND allocation_id=$2 AND allocation_generation=$3`,
		params.TenantID, params.AllocationID, params.AllocationGeneration).Scan(&count); err != nil {
		t.Fatal(err)
	}
	if count != 1 {
		t.Fatalf("concurrent replay created %d rows, want exactly one", count)
	}
}

func TestValidateUsageEvidenceParamsRejectsInvalidOrAmbiguousEvidence(t *testing.T) {
	valid := validUsageEvidenceParams()
	cases := []struct {
		name   string
		mutate func(*PersistUsageEvidenceParams)
	}{
		{"missing tenant", func(p *PersistUsageEvidenceParams) { p.TenantID = "" }},
		{"invalid digest", func(p *PersistUsageEvidenceParams) { p.EvidenceDigest = "sha256:BAD" }},
		{"usage ID does not bind evidence digest", func(p *PersistUsageEvidenceParams) { p.UsageIntervalID = "usage_unrelated" }},
		{"incomplete interval", func(p *PersistUsageEvidenceParams) { p.UsageEnd = p.UsageStart }},
		{"non-positive GPU count", func(p *PersistUsageEvidenceParams) { p.GPUCount = 0 }},
		{"empty source references", func(p *PersistUsageEvidenceParams) { p.SourceEventIDs = nil }},
		{"duplicate source references", func(p *PersistUsageEvidenceParams) { p.SourceEventIDs = []string{"source-a", "source-a"} }},
		{"duplicate JSON key", func(p *PersistUsageEvidenceParams) { p.EvidencePayload = []byte(`{"evidence":"a","evidence":"b"}`) }},
		{"non-object interval payload", func(p *PersistUsageEvidenceParams) { p.UsageIntervalPayload = []byte(`[]`) }},
		{"invalid UTF-8", func(p *PersistUsageEvidenceParams) { p.EvidencePayload = []byte{0xff, 0xfe} }},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			params := valid
			params.SourceEventIDs = append([]string(nil), valid.SourceEventIDs...)
			tc.mutate(&params)
			if err := validateUsageEvidenceParams(params); err == nil {
				t.Fatal("invalid evidence parameters were accepted")
			}
		})
	}
}

func TestPersistUsageEvidenceSetRejectsWorkloadProjectMismatch(t *testing.T) {
	db := openIntegrationDB(t)
	repo, err := NewRepository(db)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	acceptedParams := acceptanceFixture(t, "tenant-evidence-project-"+randomSuffix(t),
		"workload-key-"+randomSuffix(t), "operation-key-"+randomSuffix(t),
		"operation-event-"+randomSuffix(t))
	accepted, created, err := repo.AcceptWorkload(ctx, acceptedParams)
	if err != nil || !created {
		t.Fatalf("AcceptWorkload: created=%t err=%v", created, err)
	}
	markUsageEvidenceProviderOperationSucceeded(t, db, accepted)

	params := usageEvidenceFixture(accepted)
	params.ProjectID = "project-forged"
	params.EvidencePayload = marshalAllocationEvidencePayload(params)
	params.UsageIntervalPayload = marshalUsageIntervalPayload(params)
	if _, persisted, err := repo.PersistUsageEvidenceSet(ctx, params); !errors.Is(err, ErrUsageEvidenceNotReady) || persisted {
		t.Fatalf("forged Project attribution: persisted=%t err=%v, want ErrUsageEvidenceNotReady", persisted, err)
	}
	var count int
	if err := db.QueryRowContext(ctx, `
		SELECT count(*) FROM compute_allocation_usage_evidence_sets
		WHERE tenant_id=$1 AND allocation_id=$2`, params.TenantID, params.AllocationID).Scan(&count); err != nil {
		t.Fatal(err)
	}
	if count != 0 {
		t.Fatalf("forged Project attribution created %d rows, want 0", count)
	}
}

func TestValidateUsageEvidenceParamsAcceptsCompleteWholeGPUEvidence(t *testing.T) {
	params := validUsageEvidenceParams()
	if err := validateUsageEvidenceParams(params); err != nil {
		t.Fatalf("complete whole-GPU evidence was rejected: %v", err)
	}
	params.GPUCount = -1
	if err := validateUsageEvidenceParams(params); err == nil {
		t.Fatal("negative GPU count was accepted")
	}
	params = validUsageEvidenceParams()
	params.SchemaVersion = "compute.economics.allocation-usage-evidence.v2"
	if err := validateUsageEvidenceParams(params); err == nil {
		t.Fatal("unsupported evidence schema version was accepted")
	}
}
