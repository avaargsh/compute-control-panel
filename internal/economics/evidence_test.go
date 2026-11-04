package economics

import (
	"strings"
	"testing"
	"time"
)

func evidenceTime(t *testing.T, value string) time.Time {
	t.Helper()
	parsed, err := time.Parse(time.RFC3339Nano, value)
	if err != nil {
		t.Fatalf("parse %q: %v", value, err)
	}
	return parsed
}

func evidenceRecord(t *testing.T, id, source, nativeUID, observedAt, sourceTimestamp string) EvidenceRecord {
	t.Helper()
	observed := evidenceTime(t, observedAt)
	recorded := observed.Add(5 * time.Second)
	record := EvidenceRecord{
		ObservationID: id,
		TenantID: "tenant-1",
		ProjectID: "project-1",
		WorkloadID: "workload-1",
		AllocationID: "allocation-1",
		AllocationGeneration: 7,
		ProviderID: "provider-1",
		Source: source,
		AdapterVersion: "v1",
		NativeUID: nativeUID,
		NativeResourceVersion: "rv-" + id,
		ObservedAt: observed,
		RecordedAt: recorded,
		ResponseDigest: "sha256:" + strings.Repeat("a", 64),
		EvidenceRef: "evidence://" + id,
	}
	if sourceTimestamp != "" {
		timestamp := evidenceTime(t, sourceTimestamp)
		record.SourceTimestamp = &timestamp
	}
	return record
}

func validAllocationUsageEvidence(t *testing.T) (AllocationUsageEvidence, EvidencePolicy) {
	t.Helper()
	evidence := AllocationUsageEvidence{
		SchemaVersion: AllocationUsageEvidenceSchemaVersion,
		EvidenceSetID: "evidence-set-1",
		TenantID: "tenant-1",
		ProjectID: "project-1",
		WorkloadID: "workload-1",
		AllocationID: "allocation-1",
		AllocationGeneration: 7,
		ProviderID: "provider-1",
		PoolID: "pool-h100",
		ProfileID: "profile-h100",
		CapabilityProfileID: "k8s-kueue-dra-profile-v1",
		GPUModel: "H100",
		GPUCount: "2",
		MeteringPolicyVersion: "allocation-evidence-v1",
		Provider: ProviderUsageEvidence{
			ResourceID: "provider-resource-1",
			ResourceUID: "provider-resource-uid-1",
			PoolID: "pool-h100",
			ProfileID: "profile-h100",
			GPUModel: "H100",
			GPUCount: 2,
			Binding: evidenceRecord(t, "provider-binding", "provider", "provider-resource-uid-1", "2026-11-04T09:05:00Z", "2026-11-04T09:00:00Z"),
			CostLiabilityStart: evidenceRecord(t, "provider-cost-start", "provider", "provider-resource-uid-1", "2026-11-04T08:35:00Z", "2026-11-04T08:30:00Z"),
			CostLiabilityEnd: recordPtr(evidenceRecord(t, "provider-cost-end", "provider", "provider-resource-uid-1", "2026-11-04T12:00:00Z", "2026-11-04T11:55:00Z")),
		},
		Kueue: KueueUsageEvidence{
			WorkloadUID: "kueue-workload-uid-1",
			JobUID: "job-uid-1",
			ProfileID: "profile-h100",
			LocalQueue: "gpu-queue",
			ClusterQueue: "gpu-cluster-queue",
			ResourceFlavor: "h100-flavor",
			Admitted: true,
			AdmittedGPUCount: 2,
			Admission: evidenceRecord(t, "kueue-admission", "kueue", "kueue-workload-uid-1", "2026-11-04T09:20:00Z", "2026-11-04T09:10:00Z"),
			Release: recordPtr(evidenceRecord(t, "kueue-release", "kueue", "kueue-workload-uid-1", "2026-11-04T11:50:00Z", "2026-11-04T11:45:00Z")),
		},
		Pod: PodUsageEvidence{
			PodUID: "pod-uid-1",
			JobUID: "job-uid-1",
			NodeName: "gpu-node-1",
			Scheduled: true,
			ScheduledRecord: evidenceRecord(t, "pod-scheduled", "kubernetes", "pod-uid-1", "2026-11-04T09:25:00Z", "2026-11-04T09:12:00Z"),
		},
		Device: DeviceUsageEvidence{
			Mode: DeviceEvidenceModeDRA,
			CapabilityProfileID: "k8s-kueue-dra-profile-v1",
			ClaimUID: "resource-claim-uid-1",
			PodUID: "pod-uid-1",
			Driver: "gpu.example.com",
			PoolID: "pool-h100",
			ProfileID: "profile-h100",
			ResourceUID: "resource-claim-uid-1",
			GPUCount: 2,
			DeviceIDs: []string{"gpu-device-01", "gpu-device-02"},
			Assignment: evidenceRecord(t, "device-assignment", "dra", "resource-claim-uid-1", "2026-11-04T09:30:00Z", "2026-11-04T09:15:00Z"),
			Release: recordPtr(evidenceRecord(t, "device-release", "dra", "resource-claim-uid-1", "2026-11-04T11:55:00Z", "2026-11-04T11:50:00Z")),
		},
	}
	policy := EvidencePolicy{
		Version: "allocation-evidence-v1",
		AsOf: evidenceTime(t, "2026-11-04T12:10:00Z"),
		MaximumClockSkew: 2 * time.Minute,
		TrustedSourceTimestampAdapters: map[string]bool{
			"provider@v1": true,
			"kueue@v1": true,
			"kubernetes@v1": true,
			"dra@v1": true,
		},
		CertifiedDeviceAdapters: map[string]bool{},
	}
	return evidence, policy
}

func recordPtr(record EvidenceRecord) *EvidenceRecord { return &record }

func TestDeriveUsageIntervalRequiresAllAuthoritiesAndUsesWindowIntersection(t *testing.T) {
	evidence, policy := validAllocationUsageEvidence(t)

	got, err := DeriveUsageInterval(evidence, policy)
	if err != nil {
		t.Fatal(err)
	}
	if got.Status != EvidenceStatusFinal || got.ReasonCode != "" || got.UsageInterval == nil {
		t.Fatalf("unexpected derivation: %+v", got)
	}
	usage := got.UsageInterval
	if !usage.Start.Equal(evidenceTime(t, "2026-11-04T09:15:00Z")) {
		t.Fatalf("costable start = %s, want 2026-11-04T09:15:00Z", usage.Start)
	}
	if !usage.End.Equal(evidenceTime(t, "2026-11-04T11:45:00Z")) {
		t.Fatalf("costable end = %s, want 2026-11-04T11:45:00Z", usage.End)
	}
	if usage.GPUCount != "2" || usage.SourceCoverage != 1 ||
		usage.Quality != "DERIVED" || usage.AttributionMethod != "DEVICE_BINDING" {
		t.Fatalf("unexpected derived usage: %+v", usage)
	}
	if usage.EvidenceSetID != evidence.EvidenceSetID ||
		usage.CapabilityProfileID != evidence.CapabilityProfileID ||
		len(usage.EvidenceDigest) != len("sha256:")+64 ||
		len(usage.SourceEventIDs) != 8 {
		t.Fatalf("missing provenance binding: %+v", usage)
	}
}

func TestDeriveUsageIntervalUsesObservedAtWhenSourceTimestampIsNotTrusted(t *testing.T) {
	evidence, policy := validAllocationUsageEvidence(t)
	policy.TrustedSourceTimestampAdapters = map[string]bool{}

	got, err := DeriveUsageInterval(evidence, policy)
	if err != nil {
		t.Fatal(err)
	}
	if got.Status != EvidenceStatusFinal || got.UsageInterval == nil {
		t.Fatalf("unexpected derivation: %+v", got)
	}
	if !got.UsageInterval.Start.Equal(evidenceTime(t, "2026-11-04T09:30:00Z")) ||
		!got.UsageInterval.End.Equal(evidenceTime(t, "2026-11-04T11:50:00Z")) {
		t.Fatalf("untrusted source timestamps must fall back conservatively to observedAt: %+v", got.UsageInterval)
	}
}

func TestDeriveUsageIntervalFailsClosedForMissingAdmissionAndRelease(t *testing.T) {
	t.Run("not admitted", func(t *testing.T) {
		evidence, policy := validAllocationUsageEvidence(t)
		evidence.Kueue.Admitted = false
		got, err := DeriveUsageInterval(evidence, policy)
		if err != nil || got.Status != EvidenceStatusIncomplete ||
			got.ReasonCode != ReasonKueueNotAdmitted || got.UsageInterval != nil {
			t.Fatalf("unexpected result=%+v err=%v", got, err)
		}
	})
	t.Run("release evidence missing", func(t *testing.T) {
		evidence, policy := validAllocationUsageEvidence(t)
		evidence.Device.Release = nil
		got, err := DeriveUsageInterval(evidence, policy)
		if err != nil || got.Status != EvidenceStatusIncomplete ||
			got.ReasonCode != ReasonReleaseUnconfirmed || got.UsageInterval != nil {
			t.Fatalf("unexpected result=%+v err=%v", got, err)
		}
	})
}

func TestDeriveUsageIntervalRejectsIdentityGenerationAndQuantityConflicts(t *testing.T) {
	tests := []struct {
		name string
		mutate func(*AllocationUsageEvidence)
		want string
	}{
		{
			name: "stale generation",
			mutate: func(e *AllocationUsageEvidence) { e.Device.Assignment.AllocationGeneration++ },
			want: ReasonGenerationMismatch,
		},
		{
			name: "provider UID mismatch",
			mutate: func(e *AllocationUsageEvidence) { e.Provider.Binding.NativeUID = "foreign-provider-uid" },
			want: ReasonProviderIdentityMismatch,
		},
		{
			name: "Kueue Job and Pod owner mismatch",
			mutate: func(e *AllocationUsageEvidence) { e.Pod.JobUID = "foreign-job-uid" },
			want: ReasonKueueIdentityMismatch,
		},
		{
			name: "device claim references another pod",
			mutate: func(e *AllocationUsageEvidence) { e.Device.PodUID = "foreign-pod-uid" },
			want: ReasonDeviceAssignmentMissing,
		},
		{
			name: "device count mismatch",
			mutate: func(e *AllocationUsageEvidence) { e.Device.GPUCount = 1 },
			want: ReasonDeviceCountMismatch,
		},
		{
			name: "provider count mismatch",
			mutate: func(e *AllocationUsageEvidence) { e.Provider.GPUCount = 4 },
			want: ReasonDeviceCountMismatch,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			evidence, policy := validAllocationUsageEvidence(t)
			test.mutate(&evidence)
			got, err := DeriveUsageInterval(evidence, policy)
			if err != nil || got.Status != EvidenceStatusIncomplete ||
				got.ReasonCode != test.want || got.UsageInterval != nil {
				t.Fatalf("unexpected result=%+v err=%v, want reason %s", got, err, test.want)
			}
		})
	}
}

func TestDeriveUsageIntervalRequiresCertifiedAdapterRegistration(t *testing.T) {
	evidence, policy := validAllocationUsageEvidence(t)
	evidence.Device.Mode = DeviceEvidenceModeCertifiedAdapter
	evidence.Device.ClaimUID = ""
	evidence.Device.ResourceUID = "certified-assignment-uid"
	evidence.Device.Assignment = evidenceRecord(t, "device-assignment-certified", "vendor-device", "certified-assignment-uid", "2026-11-04T09:30:00Z", "2026-11-04T09:15:00Z")
	evidence.Device.Release = recordPtr(evidenceRecord(t, "device-release-certified", "vendor-device", "certified-assignment-uid", "2026-11-04T11:55:00Z", "2026-11-04T11:50:00Z"))
	policy.TrustedSourceTimestampAdapters["vendor-device@v1"] = true

	got, err := DeriveUsageInterval(evidence, policy)
	if err != nil || got.Status != EvidenceStatusIncomplete ||
		got.ReasonCode != ReasonDeviceAssignmentMissing || got.UsageInterval != nil {
		t.Fatalf("uncertified adapter must fail closed: result=%+v err=%v", got, err)
	}
	policy.CertifiedDeviceAdapters["k8s-kueue-dra-profile-v1@vendor-device@v1"] = true
	got, err = DeriveUsageInterval(evidence, policy)
	if err != nil || got.Status != EvidenceStatusFinal || got.UsageInterval == nil {
		t.Fatalf("registered certified adapter should be accepted: result=%+v err=%v", got, err)
	}
}

func TestDeriveUsageIntervalRejectsDuplicateObservationAndUntrustedFutureBoundary(t *testing.T) {
	t.Run("duplicate observation identity", func(t *testing.T) {
		evidence, policy := validAllocationUsageEvidence(t)
		evidence.Device.Release.ObservationID = evidence.Device.Assignment.ObservationID
		got, err := DeriveUsageInterval(evidence, policy)
		if err != nil || got.Status != EvidenceStatusIncomplete ||
			got.ReasonCode != ReasonSourceConflict || got.UsageInterval != nil {
			t.Fatalf("unexpected result=%+v err=%v", got, err)
		}
	})
	t.Run("source clock beyond collector observation", func(t *testing.T) {
		evidence, policy := validAllocationUsageEvidence(t)
		future := evidenceTime(t, "2026-11-04T10:00:00Z")
		evidence.Device.Assignment.SourceTimestamp = &future
		got, err := DeriveUsageInterval(evidence, policy)
		if err != nil || got.Status != EvidenceStatusIncomplete ||
			got.ReasonCode != ReasonTimestampUntrusted || got.UsageInterval != nil {
			t.Fatalf("unexpected result=%+v err=%v", got, err)
		}
	})
}

func TestEvidenceDigestIsDeterministicAndBindsSourceFacts(t *testing.T) {
	evidence, policy := validAllocationUsageEvidence(t)
	first, err := DeriveUsageInterval(evidence, policy)
	if err != nil || first.Status != EvidenceStatusFinal {
		t.Fatalf("first derivation=%+v err=%v", first, err)
	}
	evidence.Device.DeviceIDs[0], evidence.Device.DeviceIDs[1] =
		evidence.Device.DeviceIDs[1], evidence.Device.DeviceIDs[0]
	second, err := DeriveUsageInterval(evidence, policy)
	if err != nil || second.Status != EvidenceStatusFinal {
		t.Fatalf("second derivation=%+v err=%v", second, err)
	}
	if first.UsageInterval.EvidenceDigest != second.UsageInterval.EvidenceDigest {
		t.Fatal("device list ordering changed canonical evidence digest")
	}
	evidence.Device.DeviceIDs[0] = "corrected-device-id"
	corrected, err := DeriveUsageInterval(evidence, policy)
	if err != nil || corrected.Status != EvidenceStatusFinal {
		t.Fatalf("corrected derivation=%+v err=%v", corrected, err)
	}
	if second.UsageInterval.EvidenceDigest == corrected.UsageInterval.EvidenceDigest {
		t.Fatal("changed device identity reused the prior evidence digest")
	}
}


func TestCalculatorDigestBindsEvidenceSetAndCapabilityProfile(t *testing.T) {
	usage, rates := goldenInputs(t)
	usage.EvidenceSetID = "evidence-set-1"
	usage.EvidenceDigest = "sha256:" + strings.Repeat("b", 64)
	usage.CapabilityProfileID = "k8s-kueue-dra-profile-v1"

	first, err := Calculate(usage, rates)
	if err != nil {
		t.Fatal(err)
	}
	usage.EvidenceDigest = "sha256:" + strings.Repeat("c", 64)
	second, err := Calculate(usage, rates)
	if err != nil {
		t.Fatal(err)
	}
	if first.Segments[0].CalculationInputDigest == second.Segments[0].CalculationInputDigest {
		t.Fatal("calculator digest did not bind the evidence set digest")
	}
}

func TestCalculatorRejectsPartialEvidenceProvenance(t *testing.T) {
	usage, rates := goldenInputs(t)
	usage.EvidenceSetID = "evidence-set-1"
	if _, err := Calculate(usage, rates); !errors.Is(err, ErrInvalidUsage) {
		t.Fatalf("Calculate error = %v, want ErrInvalidUsage for partial evidence provenance", err)
	}
}


func TestDeriveUsageIntervalUsesEarliestProviderLiabilityEnd(t *testing.T) {
	evidence, policy := validAllocationUsageEvidence(t)
	providerEnd := evidenceRecord(t, "provider-cost-end-early", "provider", "provider-resource-uid-1", "2026-11-04T10:05:00Z", "2026-11-04T10:00:00Z")
	evidence.Provider.CostLiabilityEnd = recordPtr(providerEnd)

	got, err := DeriveUsageInterval(evidence, policy)
	if err != nil || got.Status != EvidenceStatusFinal || got.UsageInterval == nil {
		t.Fatalf("unexpected result=%+v err=%v", got, err)
	}
	if !got.UsageInterval.End.Equal(evidenceTime(t, "2026-11-04T10:00:00Z")) {
		t.Fatalf("end = %s, want earliest Provider liability end 10:00Z", got.UsageInterval.End)
	}
}

func TestDeriveUsageIntervalRejectsEvidenceFromWrongAuthority(t *testing.T) {
	evidence, policy := validAllocationUsageEvidence(t)
	evidence.Provider.Binding.Source = "kubernetes"
	got, err := DeriveUsageInterval(evidence, policy)
	if err != nil || got.Status != EvidenceStatusIncomplete ||
		got.ReasonCode != ReasonProviderIdentityMismatch || got.UsageInterval != nil {
		t.Fatalf("unexpected result=%+v err=%v", got, err)
	}
}


func TestEvidenceDigestBindsNormalizedAdmissionAndPlacementFacts(t *testing.T) {
	evidence, policy := validAllocationUsageEvidence(t)
	first, err := DeriveUsageInterval(evidence, policy)
	if err != nil || first.Status != EvidenceStatusFinal {
		t.Fatalf("first derivation=%+v err=%v", first, err)
	}

	evidence.Kueue.ClusterQueue = "different-cluster-queue"
	second, err := DeriveUsageInterval(evidence, policy)
	if err != nil || second.Status != EvidenceStatusFinal {
		t.Fatalf("second derivation=%+v err=%v", second, err)
	}
	if first.EvidenceDigest == second.EvidenceDigest {
		t.Fatal("changing normalized admission facts did not change evidence digest")
	}

	evidence.Kueue.ClusterQueue = "gpu-cluster-queue"
	evidence.Pod.NodeName = "different-gpu-node"
	third, err := DeriveUsageInterval(evidence, policy)
	if err != nil || third.Status != EvidenceStatusFinal {
		t.Fatalf("third derivation=%+v err=%v", third, err)
	}
	if first.EvidenceDigest == third.EvidenceDigest {
		t.Fatal("changing normalized Pod placement did not change evidence digest")
	}
}

func TestEvidenceGenerationMustBePositive(t *testing.T) {
	evidence, policy := validAllocationUsageEvidence(t)
	evidence.AllocationGeneration = 0
	if _, err := DeriveUsageInterval(evidence, policy); !errors.Is(err, ErrInvalidEvidence) {
		t.Fatalf("DeriveUsageInterval error = %v, want ErrInvalidEvidence", err)
	}
}
