package postgres

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"
)

func usageEvidenceFixture(accepted WorkloadAcceptance) PersistUsageEvidenceParams {
	digest := "sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
	params := PersistUsageEvidenceParams{}
	params.SchemaVersion = AllocationUsageEvidenceSchemaVersion
	params.EvidenceSetID = "evidence-set-" + accepted.Allocation.AllocationID
	params.TenantID = accepted.Workload.TenantID
	params.ProjectID = accepted.Workload.ProjectID
	params.WorkloadID = accepted.Workload.WorkloadID
	params.AllocationID = accepted.Allocation.AllocationID
	params.AllocationGeneration = accepted.Allocation.DesiredGeneration
	params.ProviderID = accepted.Allocation.ProviderID
	params.ComputeOperationID = accepted.Operation.ID
	params.ProviderResourceID = "provider-resource-" + accepted.Allocation.AllocationID
	params.ProviderResourceUID = "provider-resource-uid-" + accepted.Allocation.AllocationID
	params.PoolID = accepted.Allocation.PoolID
	params.ProfileID = "profile-h100"
	params.GPUModel = "NVIDIA-H100"
	params.CapabilityProfileID = "capability-profile-h100-v1"
	params.MeteringPolicyVersion = "metering-policy-v1"
	params.EvidenceDigest = digest
	params.UsageIntervalID = "usage_" + digest[len("sha256:"):]
	params.UsageStart = time.Date(2026, 11, 4, 8, 0, 0, 0, time.UTC)
	params.UsageEnd = time.Date(2026, 11, 4, 9, 30, 0, 0, time.UTC)
	params.GPUCount = 1
	params.SourceEventIDs = []string{
		"device-assignment-" + accepted.Allocation.AllocationID,
		"device-release-" + accepted.Allocation.AllocationID,
		"kueue-admission-" + accepted.Allocation.AllocationID,
		"kueue-release-" + accepted.Allocation.AllocationID,
		"pod-scheduled-" + accepted.Allocation.AllocationID,
		"provider-binding-" + accepted.Allocation.AllocationID,
		"provider-liability-start-" + accepted.Allocation.AllocationID,
	}
	params.EvidencePayload = marshalAllocationEvidencePayload(params)
	params.UsageIntervalPayload = marshalUsageIntervalPayload(params)
	return params
}

func validUsageEvidenceParams() PersistUsageEvidenceParams {
	params := PersistUsageEvidenceParams{}
	params.SchemaVersion = AllocationUsageEvidenceSchemaVersion
	params.EvidenceSetID = "evidence-valid"
	params.TenantID = "tenant-valid"
	params.ProjectID = "project-valid"
	params.WorkloadID = "workload-valid"
	params.AllocationID = "allocation-valid"
	params.AllocationGeneration = 1
	params.ProviderID = "provider-valid"
	params.ComputeOperationID = "operation-valid"
	params.ProviderResourceID = "resource-valid"
	params.ProviderResourceUID = "resource-uid-valid"
	params.PoolID = "pool-valid"
	params.ProfileID = "profile-valid"
	params.GPUModel = "GPU-valid"
	params.CapabilityProfileID = "capability-profile-valid"
	params.MeteringPolicyVersion = "metering-policy-v1"
	params.EvidenceDigest = "sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
	params.UsageIntervalID = "usage_aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
	params.UsageStart = time.Date(2026, 11, 4, 8, 0, 0, 0, time.UTC)
	params.UsageEnd = time.Date(2026, 11, 4, 9, 0, 0, 0, time.UTC)
	params.GPUCount = 1
	params.SourceEventIDs = []string{"source-a", "source-b"}
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
	payload := persistedUsageIntervalPayload{}
	payload.SchemaVersion = usageIntervalSchemaVersion
	payload.UsageIntervalID = p.UsageIntervalID
	payload.TenantID = p.TenantID
	payload.ProjectID = p.ProjectID
	payload.WorkloadID = p.WorkloadID
	payload.AllocationID = p.AllocationID
	payload.AllocationGeneration = p.AllocationGeneration
	payload.ProviderID = p.ProviderID
	payload.PoolID = p.PoolID
	payload.ProfileID = p.ProfileID
	payload.ResourceClass = "GPU"
	payload.GPUModel = p.GPUModel
	payload.Start = p.UsageStart
	payload.End = p.UsageEnd
	payload.GPUCount = jsonNumber(p.GPUCount)
	payload.Unit = usageIntervalUnit
	payload.MeteringPolicyVersion = p.MeteringPolicyVersion
	payload.SourceEventIDs = append([]string(nil), p.SourceEventIDs...)
	payload.Quality = usageEvidenceQuality
	payload.AttributionMethod = usageAttributionMethod
	payload.SourceCoverage = 1
	payload.EvidenceSetID = p.EvidenceSetID
	payload.EvidenceDigest = p.EvidenceDigest
	payload.CapabilityProfileID = p.CapabilityProfileID
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
		UPDATE compute_allocations
		SET phase='BOUND', updated_at=clock_timestamp()
		WHERE tenant_id=$1 AND allocation_id=$2`,
		accepted.Workload.TenantID, accepted.Allocation.AllocationID,
	)
	if err != nil {
		t.Fatalf("mark Allocation bound: %v", err)
	}
	_, err = db.ExecContext(context.Background(), `
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
