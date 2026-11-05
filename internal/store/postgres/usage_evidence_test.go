package postgres

import (
	"context"
	"database/sql"
	"errors"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/avaargsh/compute-control-panel/internal/economics"
	"github.com/avaargsh/compute-control-panel/internal/operation"
)

func usageRecord(a WorkloadAcceptance, id, source, uid, at string) economics.EvidenceRecord {
	observed, err := time.Parse(time.RFC3339Nano, at)
	if err != nil {
		panic(err)
	}
	sourceTime := observed
	return economics.EvidenceRecord{
		ObservationID: id + "-" + a.Allocation.AllocationID,
		TenantID:      a.Workload.TenantID, ProjectID: a.Workload.ProjectID,
		WorkloadID: a.Workload.WorkloadID, AllocationID: a.Allocation.AllocationID,
		AllocationGeneration: a.Allocation.DesiredGeneration, ProviderID: a.Allocation.ProviderID,
		Source: source, AdapterVersion: "v1", NativeUID: uid,
		NativeResourceVersion: "rv-" + id, ObservedAt: observed,
		RecordedAt: observed.Add(time.Second), SourceTimestamp: &sourceTime,
		ResponseDigest: "sha256:" + strings.Repeat("a", 64), EvidenceRef: "evidence://" + id,
	}
}

func typedUsageEvidence(a WorkloadAcceptance) PersistUsageEvidenceParams {
	id := a.Allocation.AllocationID
	providerUID := "provider-uid-" + id
	kueueUID := "kueue-uid-" + id
	jobUID := "job-uid-" + id
	podUID := "pod-uid-" + id
	claimUID := "claim-uid-" + id

	e := economics.AllocationUsageEvidence{
		SchemaVersion: economics.AllocationUsageEvidenceSchemaVersion,
		EvidenceSetID: "evidence-set-" + id,
		TenantID:      a.Workload.TenantID, ProjectID: a.Workload.ProjectID,
		WorkloadID: a.Workload.WorkloadID, AllocationID: id,
		AllocationGeneration: a.Allocation.DesiredGeneration, ProviderID: a.Allocation.ProviderID,
		PoolID: a.Allocation.PoolID, ProfileID: "profile-h100",
		CapabilityProfileID: "capability-profile-h100-v1", GPUModel: "H100",
		GPUCount: "1", MeteringPolicyVersion: "allocation-evidence-v1",
	}
	e.Provider = economics.ProviderUsageEvidence{
		Operation: economics.ComputeOperationUsageEvidence{
			ID: a.Operation.ID, TenantID: a.Workload.TenantID, ProjectID: a.Workload.ProjectID,
			WorkloadID: a.Workload.WorkloadID, AllocationID: id,
			DesiredGeneration: a.Allocation.DesiredGeneration, ProviderID: a.Allocation.ProviderID,
			Status: "SUCCEEDED", RequestDigest: a.Operation.RequestDigest,
		},
		ResourceID: "provider-resource-" + id, ResourceUID: providerUID,
		PoolID: a.Allocation.PoolID, ProfileID: "profile-h100", GPUModel: "H100", GPUCount: 1,
		Binding:            usageRecord(a, "provider-binding", "provider", providerUID, "2026-11-04T09:05:00Z"),
		CostLiabilityStart: usageRecord(a, "provider-liability-start", "provider", providerUID, "2026-11-04T08:35:00Z"),
	}
	e.Provider.Binding.ComputeOperationID = a.Operation.ID
	providerEnd := usageRecord(a, "provider-liability-end", "provider", providerUID, "2026-11-04T12:00:00Z")
	e.Provider.CostLiabilityEnd = &providerEnd

	e.Kueue = economics.KueueUsageEvidence{
		WorkloadUID: kueueUID, JobUID: jobUID, ProfileID: "profile-h100",
		LocalQueue: "gpu-queue", ClusterQueue: "gpu-cluster-queue",
		ResourceFlavor: "h100-flavor", GPUResourceName: "vendor.example.com/gpu",
		Admitted: true, AdmittedGPUCount: 1,
		Admission: usageRecord(a, "kueue-admission", "kueue", kueueUID, "2026-11-04T09:20:00Z"),
	}
	kueueRelease := usageRecord(a, "kueue-release", "kueue", kueueUID, "2026-11-04T11:50:00Z")
	e.Kueue.Release = &kueueRelease

	e.Pod = economics.PodUsageEvidence{
		PodUID: podUID, JobUID: jobUID, NodeName: "gpu-node-1", Scheduled: true,
		ScheduledRecord: usageRecord(a, "pod-scheduled", "kubernetes", podUID, "2026-11-04T09:25:00Z"),
	}
	e.Device = economics.DeviceUsageEvidence{
		Mode: economics.DeviceEvidenceModeDRA, CapabilityProfileID: e.CapabilityProfileID,
		ClaimUID: claimUID, PodUID: podUID, Driver: "gpu.example.com",
		PoolID: e.PoolID, ProfileID: e.ProfileID, ResourceUID: claimUID,
		GPUCount: 1, DeviceIDs: []string{"gpu-device-01"},
		Assignment: usageRecord(a, "device-assignment", "dra", claimUID, "2026-11-04T09:30:00Z"),
	}
	deviceRelease := usageRecord(a, "device-release", "dra", claimUID, "2026-11-04T11:55:00Z")
	e.Device.Release = &deviceRelease

	return PersistUsageEvidenceParams{Evidence: e, Policy: fixtureEvidencePolicy()}
}

// fixtureEvidencePolicy is approved ONLY by the explicitly injected test
// authority. Production callers must never copy this local fake registry.
func fixtureEvidencePolicy() economics.EvidencePolicy {
	return economics.EvidencePolicy{
		Version:          "allocation-evidence-v1",
		AsOf:             time.Date(2026, 11, 4, 12, 10, 0, 0, time.UTC),
		MaximumClockSkew: 2 * time.Minute,
		TrustedSourceTimestampAdapters: map[string]bool{
			"provider@v1": true, "kueue@v1": true, "kubernetes@v1": true, "dra@v1": true,
		},
		CertifiedDeviceAdapters: map[string]bool{},
	}
}

func newTestEvidenceRepo(t *testing.T, db *sql.DB) *Repository {
	t.Helper()
	repo, err := NewRepositoryWithEvidencePolicyAuthority(db, EvidencePolicyAuthorityFunc(
		func(_ context.Context, tenantID, providerID, version string) (TrustedEvidencePolicy, error) {
			policy := fixtureEvidencePolicy()
			if version != policy.Version {
				return TrustedEvidencePolicy{}, errors.New("unrecognized test policy version")
			}
			now := time.Now().UTC()
			return TrustedEvidencePolicy{
				TenantID: tenantID, ProviderID: providerID, Policy: policy,
				EffectiveFrom: now.Add(-time.Hour), EffectiveUntil: now.Add(time.Hour),
			}, nil
		},
	))
	if err != nil {
		t.Fatal(err)
	}
	return repo
}

func markUsageEvidenceProviderOperationSucceeded(t *testing.T, db *sql.DB, a WorkloadAcceptance) {
	t.Helper()
	_, err := db.ExecContext(context.Background(), `
		UPDATE compute_allocations SET phase='BOUND', updated_at=clock_timestamp()
		WHERE tenant_id=$1 AND allocation_id=$2`, a.Workload.TenantID, a.Allocation.AllocationID)
	if err != nil {
		t.Fatalf("mark Allocation bound: %v", err)
	}
	_, err = db.ExecContext(context.Background(), `
		UPDATE compute_operations
		SET status='SUCCEEDED', provider_resource_id=$3, provider_resource_uid=$4,
		    updated_at=clock_timestamp()
		WHERE tenant_id=$1 AND compute_operation_id=$2`,
		a.Workload.TenantID, a.Operation.ID,
		"provider-resource-"+a.Allocation.AllocationID, "provider-uid-"+a.Allocation.AllocationID)
	if err != nil {
		t.Fatalf("mark Provider operation successful: %v", err)
	}
}

func TestPersistUsageEvidenceSetRequiresSuccessfulProviderOperation(t *testing.T) {
	db := openIntegrationDB(t)
	repo := newTestEvidenceRepo(t, db)
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	p := acceptanceFixture(t, "tenant-evidence-pending-"+randomSuffix(t),
		"workload-key-"+randomSuffix(t), "operation-key-"+randomSuffix(t), "operation-event-"+randomSuffix(t))
	a, created, err := repo.AcceptWorkload(ctx, p)
	if err != nil || !created {
		t.Fatalf("AcceptWorkload: created=%t err=%v", created, err)
	}

	if _, persisted, err := repo.PersistUsageEvidenceSet(ctx, typedUsageEvidence(a)); !errors.Is(err, ErrUsageEvidenceNotReady) || persisted {
		t.Fatalf("PENDING Provider operation persisted evidence: persisted=%t err=%v", persisted, err)
	}
	var count int
	if err := db.QueryRowContext(ctx, `SELECT count(*) FROM compute_allocation_usage_evidence_sets WHERE tenant_id=$1 AND allocation_id=$2`,
		a.Workload.TenantID, a.Allocation.AllocationID).Scan(&count); err != nil {
		t.Fatal(err)
	}
	if count != 0 {
		t.Fatalf("PENDING Provider operation created %d evidence rows, want 0", count)
	}
}

func TestPersistUsageEvidenceSetIsImmutableIdempotentAndTenantScoped(t *testing.T) {
	db := openIntegrationDB(t)
	repo := newTestEvidenceRepo(t, db)
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	p := acceptanceFixture(t, "tenant-evidence-"+randomSuffix(t),
		"workload-key-"+randomSuffix(t), "operation-key-"+randomSuffix(t), "operation-event-"+randomSuffix(t))
	a, created, err := repo.AcceptWorkload(ctx, p)
	if err != nil || !created {
		t.Fatalf("AcceptWorkload: created=%t err=%v", created, err)
	}
	markUsageEvidenceProviderOperationSucceeded(t, db, a)

	params := typedUsageEvidence(a)
	first, persisted, err := repo.PersistUsageEvidenceSet(ctx, params)
	if err != nil || !persisted {
		t.Fatalf("first persist: persisted=%t err=%v", persisted, err)
	}
	replayed, persisted, err := repo.PersistUsageEvidenceSet(ctx, params)
	if err != nil || persisted {
		t.Fatalf("exact replay: persisted=%t err=%v", persisted, err)
	}
	if replayed.EvidenceDigest != first.EvidenceDigest || !replayed.RecordedAt.Equal(first.RecordedAt) {
		t.Fatalf("replay changed immutable evidence: first=%+v replay=%+v", first, replayed)
	}

	changed := params
	changed.Evidence.Device.DeviceIDs = []string{"gpu-device-changed"}
	if _, persisted, err := repo.PersistUsageEvidenceSet(ctx, changed); !errors.Is(err, ErrUsageEvidenceConflict) || persisted {
		t.Fatalf("changed source evidence: persisted=%t err=%v", persisted, err)
	}
	duplicate := params
	duplicate.Evidence.EvidenceSetID += "-duplicate"
	if _, persisted, err := repo.PersistUsageEvidenceSet(ctx, duplicate); !errors.Is(err, ErrUsageEvidenceConflict) || persisted {
		t.Fatalf("second set for Allocation generation: persisted=%t err=%v", persisted, err)
	}

	if _, err := repo.GetUsageEvidenceSet(ctx, "tenant-another", first.EvidenceSetID); !errors.Is(err, sql.ErrNoRows) {
		t.Fatalf("cross-tenant read = %v, want sql.ErrNoRows", err)
	}
	got, err := repo.GetUsageEvidenceSet(ctx, params.Evidence.TenantID, first.EvidenceSetID)
	if err != nil {
		t.Fatal(err)
	}
	if got.AllocationID != params.Evidence.AllocationID ||
		got.ProviderResourceUID != params.Evidence.Provider.ResourceUID ||
		got.ProviderRequestDigest != params.Evidence.Provider.Operation.RequestDigest ||
		got.EvidenceDigest != first.EvidenceDigest || got.GPUCount != 1 ||
		got.EvidencePolicyVersion != params.Policy.Version {
		t.Fatalf("persisted evidence scope mismatch: %+v", got)
	}
	if _, err := db.ExecContext(ctx, `UPDATE compute_allocation_usage_evidence_sets SET evidence_digest='sha256:' || repeat('c',64) WHERE tenant_id=$1 AND evidence_set_id=$2`,
		params.Evidence.TenantID, first.EvidenceSetID); err == nil {
		t.Fatal("immutable evidence accepted UPDATE")
	}
	if _, err := db.ExecContext(ctx, `DELETE FROM compute_allocation_usage_evidence_sets WHERE tenant_id=$1 AND evidence_set_id=$2`,
		params.Evidence.TenantID, first.EvidenceSetID); err == nil {
		t.Fatal("immutable evidence accepted DELETE")
	}
	if _, err := db.ExecContext(ctx, "TRUNCATE compute_allocation_usage_evidence_sets"); err == nil {
		t.Fatal("immutable evidence accepted TRUNCATE")
	}
}

func TestPersistUsageEvidenceSetConcurrentReplayHasOneWinner(t *testing.T) {
	db := openIntegrationDB(t)
	repo := newTestEvidenceRepo(t, db)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	p := acceptanceFixture(t, "tenant-evidence-race-"+randomSuffix(t),
		"workload-key-"+randomSuffix(t), "operation-key-"+randomSuffix(t), "operation-event-"+randomSuffix(t))
	a, created, err := repo.AcceptWorkload(ctx, p)
	if err != nil || !created {
		t.Fatalf("AcceptWorkload: created=%t err=%v", created, err)
	}
	markUsageEvidenceProviderOperationSucceeded(t, db, a)
	params := typedUsageEvidence(a)

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
			t.Fatalf("concurrent persist[%d]: %v", i, errs[i])
		}
		if createdFlags[i] {
			winners++
		}
		if results[i].EvidenceDigest == "" {
			t.Fatalf("concurrent persist[%d] returned empty digest", i)
		}
	}
	if winners != 1 {
		t.Fatalf("created=true count = %d, want exactly one", winners)
	}
	var count int
	if err := db.QueryRowContext(ctx, `SELECT count(*) FROM compute_allocation_usage_evidence_sets WHERE tenant_id=$1 AND allocation_id=$2 AND allocation_generation=$3`,
		params.Evidence.TenantID, params.Evidence.AllocationID, params.Evidence.AllocationGeneration).Scan(&count); err != nil {
		t.Fatal(err)
	}
	if count != 1 {
		t.Fatalf("concurrent replay created %d rows, want 1", count)
	}
}

func TestPersistUsageEvidenceSetRejectsIncompleteOrForgedSourceFacts(t *testing.T) {
	a := WorkloadAcceptance{
		Workload:   Workload{TenantID: "tenant-1", ProjectID: "project-1", WorkloadID: "workload-1"},
		Allocation: Allocation{AllocationID: "allocation-1", DesiredGeneration: 7, ProviderID: "provider-1", PoolID: "pool-h100"},
		Operation:  operation.Operation{ID: "compute-operation-1", RequestDigest: "sha256:" + strings.Repeat("d", 64)},
	}
	valid := typedUsageEvidence(a)
	cases := []struct {
		name   string
		mutate func(*PersistUsageEvidenceParams)
	}{
		{"Kueue not admitted", func(p *PersistUsageEvidenceParams) { p.Evidence.Kueue.Admitted = false }},
		{"missing DRA release", func(p *PersistUsageEvidenceParams) { p.Evidence.Device.Release = nil }},
		{"cross-Project source", func(p *PersistUsageEvidenceParams) { p.Evidence.ProjectID = "project-forged" }},
		{"invalid Provider request digest", func(p *PersistUsageEvidenceParams) { p.Evidence.Provider.Operation.RequestDigest = "invalid" }},
		{"duplicate observation identity", func(p *PersistUsageEvidenceParams) {
			p.Evidence.Device.Release.ObservationID = p.Evidence.Device.Assignment.ObservationID
		}},
		{"wrong Provider authority", func(p *PersistUsageEvidenceParams) { p.Evidence.Provider.Binding.Source = "kubernetes" }},
		{"missing trusted policy version", func(p *PersistUsageEvidenceParams) { p.Policy.Version = "" }},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			changed := valid
			if valid.Evidence.Provider.CostLiabilityEnd != nil {
				record := *valid.Evidence.Provider.CostLiabilityEnd
				changed.Evidence.Provider.CostLiabilityEnd = &record
			}
			if valid.Evidence.Kueue.Release != nil {
				record := *valid.Evidence.Kueue.Release
				changed.Evidence.Kueue.Release = &record
			}
			if valid.Evidence.Device.Release != nil {
				record := *valid.Evidence.Device.Release
				changed.Evidence.Device.Release = &record
			}
			tc.mutate(&changed)
			if _, err := deriveUsageEvidence(changed); err == nil {
				t.Fatal("invalid or incomplete E1 evidence was accepted")
			}
		})
	}
}

func TestPersistUsageEvidenceSetRequiresExactProviderRequestDigest(t *testing.T) {
	db := openIntegrationDB(t)
	repo := newTestEvidenceRepo(t, db)
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	p := acceptanceFixture(t, "tenant-evidence-digest-"+randomSuffix(t),
		"workload-key-"+randomSuffix(t), "operation-key-"+randomSuffix(t), "operation-event-"+randomSuffix(t))
	a, created, err := repo.AcceptWorkload(ctx, p)
	if err != nil || !created {
		t.Fatalf("AcceptWorkload: created=%t err=%v", created, err)
	}
	markUsageEvidenceProviderOperationSucceeded(t, db, a)

	params := typedUsageEvidence(a)
	params.Evidence.Provider.Operation.RequestDigest = "sha256:" + strings.Repeat("e", 64)
	if _, persisted, err := repo.PersistUsageEvidenceSet(ctx, params); !errors.Is(err, ErrUsageEvidenceNotReady) || persisted {
		t.Fatalf("mismatched Provider request digest: persisted=%t err=%v, want ErrUsageEvidenceNotReady", persisted, err)
	}
	var count int
	if err := db.QueryRowContext(ctx, `
		SELECT count(*) FROM compute_allocation_usage_evidence_sets
		WHERE tenant_id=$1 AND allocation_id=$2`,
		a.Workload.TenantID, a.Allocation.AllocationID).Scan(&count); err != nil {
		t.Fatal(err)
	}
	if count != 0 {
		t.Fatalf("mismatched Provider request digest created %d evidence rows, want 0", count)
	}
}

func TestUsageEvidenceMigrationPreservesProviderV5(t *testing.T) {
	db := openIntegrationDB(t)
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	rows, err := db.QueryContext(ctx, `SELECT version, name FROM schema_migrations
		WHERE version IN (5,6) ORDER BY version`)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	got := make(map[int]string)
	for rows.Next() {
		var version int
		var name string
		if err := rows.Scan(&version, &name); err != nil {
			t.Fatal(err)
		}
		got[version] = name
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	if got[5] != "0005_provider_registry_drain.sql" ||
		got[6] != "0006_allocation_usage_evidence_sets.sql" ||
		len(got) != 2 {
		t.Fatalf("migration lineage changed or incomplete: %+v", got)
	}

	// Both schemas must remain readable after the forward-only migration.
	var providerRegistryExists, evidenceTableExists bool
	if err := db.QueryRowContext(ctx,
		`SELECT to_regclass('compute_providers') IS NOT NULL,
		        to_regclass('compute_allocation_usage_evidence_sets') IS NOT NULL`,
	).Scan(&providerRegistryExists, &evidenceTableExists); err != nil {
		t.Fatal(err)
	}
	if !providerRegistryExists || !evidenceTableExists {
		t.Fatalf("Provider v5 or evidence v6 schema missing: provider=%t evidence=%t",
			providerRegistryExists, evidenceTableExists)
	}
}
