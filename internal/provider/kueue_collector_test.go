package provider

import (
	"context"
	"errors"
	"reflect"
	"strings"
	"testing"
	"time"
)

func kueueQueryFixture() KubernetesJobQuery {
	return KubernetesJobQuery{
		TenantID: "tenant-a", WorkloadID: "compute-workload-a",
		DesiredGeneration: 7, Namespace: "tenant-a",
		JobName: "job-a", ExpectedJobUID: "job-uid-a",
	}
}

func newTestKueueCollector(t *testing.T) *FakeCollector {
	t.Helper()
	collector, err := NewFakeCollector("fake-kueue-collector", "test-v1")
	if err != nil {
		t.Fatal(err)
	}
	collector.SetClock(func() time.Time {
		return time.Date(2026, 11, 10, 3, 4, 5, 0, time.FixedZone("UTC+2", 2*3600))
	})
	return collector
}

func TestFakeCollectorReturnsEvidenceAndProvenance(t *testing.T) {
	observedAt := time.Date(2026, 11, 10, 1, 4, 5, 0, time.UTC)
	tests := []struct {
		name   string
		change func(*KueueJobEvidence)
		want   KueueObservationStage
	}{
		{
			name:   "placed pod is structural placement only",
			change: func(*KueueJobEvidence) {},
			want:   KueuePodPlaced,
		},
		{
			name: "admitted without pod",
			change: func(e *KueueJobEvidence) {
				e.Pod = KubernetesResourceRef{}
				e.PodOwnerJobUID = ""
				e.PodScheduled = false
				e.PodNodeName = ""
			},
			want: KueueAdmitted,
		},
		{
			name: "matching job without queue evidence stays unproven",
			change: func(e *KueueJobEvidence) {
				e.KueueWorkload = KubernetesResourceRef{}
				e.KueueOwnerJobUID = ""
				e.KueueAdmitted = false
				e.Pod = KubernetesResourceRef{}
				e.PodOwnerJobUID = ""
				e.PodScheduled = false
				e.PodNodeName = ""
			},
			want: KueueUnproven,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			evidence := kueueEvidenceFixture()
			tc.change(&evidence)
			collector := newTestKueueCollector(t)
			if err := collector.Seed(evidence); err != nil {
				t.Fatal(err)
			}
			got, err := collector.Collect(context.Background(), kueueQueryFixture())
			if err != nil || got.Stage != tc.want || got.Evidence != evidence {
				t.Fatalf("collection=%+v err=%v, want stage %s", got, err, tc.want)
			}
			if got.Provenance.Source != "fake-kueue-collector" ||
				got.Provenance.AdapterVersion != "test-v1" ||
				!got.Provenance.ObservedAt.Equal(observedAt) ||
				got.Provenance.ObservedAt.Location() != time.UTC {
				t.Fatalf("provenance=%+v", got.Provenance)
			}
			if strings.Contains(string(got.Stage), "READY") || strings.Contains(string(got.Stage), "DEVICE") {
				t.Fatalf("collection promoted readiness or device assignment: %s", got.Stage)
			}
		})
	}
}

func TestFakeCollectorIdentityMismatchDoesNotPromoteStatus(t *testing.T) {
	tests := []struct {
		name     string
		evidence func(*KueueJobEvidence)
		query    func(*KubernetesJobQuery)
		seed     bool
	}{
		{
			name: "foreign tenant",
			evidence: func(e *KueueJobEvidence) {
				e.TenantID = "tenant-b"
				e.JobTenantID = "tenant-b"
			},
			seed: true,
		},
		{
			name: "foreign workload",
			evidence: func(e *KueueJobEvidence) {
				e.WorkloadID = "other-workload"
				e.JobWorkloadID = "other-workload"
			},
			seed: true,
		},
		{
			name: "stale generation",
			evidence: func(e *KueueJobEvidence) {
				e.DesiredGeneration = 6
				e.JobDesiredGeneration = 6
			},
			seed: true,
		},
		{
			name: "canonical tenant disagrees with job annotation",
			evidence: func(e *KueueJobEvidence) {
				e.TenantID = "tenant-b"
			},
			seed: true,
		},
		{
			name: "successor job uid",
			evidence: func(e *KueueJobEvidence) {
				e.Job.UID = "job-uid-b"
				e.KueueOwnerJobUID = "job-uid-b"
				e.PodOwnerJobUID = "job-uid-b"
			},
			seed: true,
		},
		{
			name: "foreign kueue owner",
			evidence: func(e *KueueJobEvidence) {
				e.KueueOwnerJobUID = "other-job-uid"
			},
			seed: true,
		},
		{
			name: "foreign pod namespace",
			evidence: func(e *KueueJobEvidence) {
				e.Pod.Namespace = "tenant-b"
			},
			seed: true,
		},
		{
			name: "missing snapshot",
			seed: false,
		},
		{
			name: "blank tenant query",
			query: func(q *KubernetesJobQuery) {
				q.TenantID = ""
			},
			seed: true,
		},
		{
			name: "zero desired generation",
			query: func(q *KubernetesJobQuery) {
				q.DesiredGeneration = 0
			},
			seed: true,
		},
		{
			name: "unpinned job uid",
			query: func(q *KubernetesJobQuery) {
				q.ExpectedJobUID = ""
			},
			seed: true,
		},
		{
			name: "padded workload id",
			query: func(q *KubernetesJobQuery) {
				q.WorkloadID = " compute-workload-a "
			},
			seed: true,
		},
		{
			name: "different job name",
			query: func(q *KubernetesJobQuery) {
				q.JobName = "job-b"
			},
			seed: true,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			collector := newTestKueueCollector(t)
			if tc.seed {
				evidence := kueueEvidenceFixture()
				if tc.evidence != nil {
					tc.evidence(&evidence)
				}
				if err := collector.Seed(evidence); err != nil {
					t.Fatal(err)
				}
			}
			query := kueueQueryFixture()
			if tc.query != nil {
				tc.query(&query)
			}
			got, err := collector.Collect(context.Background(), query)
			if !errors.Is(err, ErrInvalidKueueObservation) || got.Stage != KueueUnproven ||
				got.Evidence != (KueueJobEvidence{}) || got.Provenance != (KueueEvidenceProvenance{}) {
				t.Fatalf("identity mismatch promoted status: %+v err=%v", got, err)
			}
		})
	}
}

func TestFakeCollectorPinsSuccessorJobUID(t *testing.T) {
	collector := newTestKueueCollector(t)
	original := kueueEvidenceFixture()
	if err := collector.Seed(original); err != nil {
		t.Fatal(err)
	}
	first, err := collector.Collect(context.Background(), kueueQueryFixture())
	if err != nil || first.Stage != KueuePodPlaced || first.Evidence.Job.UID != "job-uid-a" {
		t.Fatalf("original job was not observable: %+v %v", first, err)
	}
	successor := original
	successor.Job.UID = "job-uid-b"
	successor.KueueOwnerJobUID = "job-uid-b"
	successor.PodOwnerJobUID = "job-uid-b"
	if err := collector.Seed(successor); err != nil {
		t.Fatal(err)
	}
	stale, err := collector.Collect(context.Background(), kueueQueryFixture())
	if !errors.Is(err, ErrInvalidKueueObservation) || stale.Stage != KueueUnproven ||
		stale.Evidence != (KueueJobEvidence{}) {
		t.Fatalf("stale UID observed successor job: %+v %v", stale, err)
	}
	next := kueueQueryFixture()
	next.ExpectedJobUID = "job-uid-b"
	got, err := collector.Collect(context.Background(), next)
	if err != nil || got.Stage != KueuePodPlaced || got.Evidence.Job.UID != "job-uid-b" ||
		strings.Contains(string(got.Stage), "READY") {
		t.Fatalf("successor identity was not independently observable: %+v %v", got, err)
	}
}

func TestFakeCollectorKeepsTenantSnapshotsSeparate(t *testing.T) {
	collector := newTestKueueCollector(t)
	first := kueueEvidenceFixture()
	second := first
	second.TenantID = "tenant-b"
	second.JobTenantID = "tenant-b"
	second.Job.Namespace = "tenant-b"
	second.KueueWorkload.Namespace = "tenant-b"
	second.Pod.Namespace = "tenant-b"
	second.Job.Name = "job-b"
	second.Job.UID = "job-uid-b"
	second.KueueOwnerJobUID = "job-uid-b"
	second.PodOwnerJobUID = "job-uid-b"
	if err := collector.Seed(first); err != nil {
		t.Fatal(err)
	}
	if err := collector.Seed(second); err != nil {
		t.Fatal(err)
	}
	got, err := collector.Collect(context.Background(), kueueQueryFixture())
	if err != nil || got.Evidence.TenantID != "tenant-a" || got.Evidence.Job.UID != "job-uid-a" {
		t.Fatalf("tenant-a collection crossed tenants: %+v %v", got, err)
	}
	other := kueueQueryFixture()
	other.TenantID = "tenant-b"
	other.Namespace = "tenant-b"
	other.JobName = "job-b"
	other.ExpectedJobUID = "job-uid-b"
	got, err = collector.Collect(context.Background(), other)
	if err != nil || got.Evidence != second || got.Stage != KueuePodPlaced {
		t.Fatalf("tenant-b collection: %+v %v", got, err)
	}
}

func TestFakeCollectorDoesNotMutateSeedOrExposeWrites(t *testing.T) {
	collector := newTestKueueCollector(t)
	evidence := kueueEvidenceFixture()
	if err := collector.Seed(evidence); err != nil {
		t.Fatal(err)
	}
	got, err := collector.Collect(context.Background(), kueueQueryFixture())
	if err != nil {
		t.Fatal(err)
	}
	got.Evidence.TenantID = "mutated"
	got.Evidence.PodNodeName = "mutated-node"
	got.Stage = KueueAdmitted
	again, err := collector.Collect(context.Background(), kueueQueryFixture())
	if err != nil || again.Evidence != evidence || again.Stage != KueuePodPlaced {
		t.Fatalf("returned collection aliased the seed: %+v %v", again, err)
	}
	method, _ := reflect.TypeOf(collector).MethodByName("Collect")
	if method.Type == nil {
		t.Fatal("Collect missing")
	}
	for _, name := range []string{"Apply", "Create", "Update", "Delete", "Patch", "Bind", "Admit", "MarkReady"} {
		if _, ok := reflect.TypeOf(collector).MethodByName(name); ok {
			t.Fatalf("collector exposes write method %s", name)
		}
	}
}

func TestFakeCollectorRejectsIncompleteProvenanceAndCanceledContext(t *testing.T) {
	if _, err := NewFakeCollector(" ", "test-v1"); !errors.Is(err, ErrInvalidKueueObservation) {
		t.Fatalf("blank source accepted: %v", err)
	}
	if _, err := NewFakeCollector("fake-kueue-collector", ""); !errors.Is(err, ErrInvalidKueueObservation) {
		t.Fatalf("blank adapter version accepted: %v", err)
	}
	collector := newTestKueueCollector(t)
	if err := collector.Seed(kueueEvidenceFixture()); err != nil {
		t.Fatal(err)
	}
	collector.SetClock(func() time.Time { return time.Time{} })
	got, err := collector.Collect(context.Background(), kueueQueryFixture())
	if !errors.Is(err, ErrInvalidKueueObservation) || got.Stage != KueueUnproven ||
		got.Evidence != (KueueJobEvidence{}) {
		t.Fatalf("zero observation time was trusted: %+v %v", got, err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	got, err = collector.Collect(ctx, kueueQueryFixture())
	if !errors.Is(err, context.Canceled) || got != (KueueJobCollection{}) {
		t.Fatalf("canceled collect promoted status: %+v %v", got, err)
	}
	if err := collector.Seed(KueueJobEvidence{}); !errors.Is(err, ErrInvalidKueueObservation) {
		t.Fatalf("unaddressable snapshot seeded: %v", err)
	}
}

func TestKubernetesObservationReaderImplementsCollectorWithExplicitUnattestedProvenance(t *testing.T) {
	reader, reads, closeServer := testKubeReader(t, testKubeJob, testKueueWorkloads, testKubePods, 0)
	defer closeServer()
	var collector Collector = reader
	got, err := collector.Collect(context.Background(), observedJobQuery())
	if err != nil || got.Stage != KueuePodPlaced ||
		got.Evidence.Job.UID != "job-uid-a" ||
		got.Provenance.Source != "kubernetes-https-readonly-unattested" ||
		got.Provenance.AdapterVersion != "kueue-v1beta1-single-pod-v1" ||
		got.Provenance.ObservedAt.IsZero() || reads.Load() < 3 {
		t.Fatalf("UID-pinned read-only collection: %+v err=%v reads=%d", got, err, reads.Load())
	}
}

func TestKubernetesObservationReaderCollectDoesNotLeakSuccessorOrForeignEvidence(t *testing.T) {
	reader, _, closeServer := testKubeReader(t, testKubeJob, testKueueWorkloads, testKubePods, 0)
	defer closeServer()
	var collector Collector = reader
	query := observedJobQuery()
	query.ExpectedJobUID = "successor-uid"
	got, err := collector.Collect(context.Background(), query)
	if !errors.Is(err, ErrInvalidKueueObservation) ||
		got.Stage != KueueUnproven || got.Evidence != (KueueJobEvidence{}) ||
		got.Provenance != (KueueEvidenceProvenance{}) {
		t.Fatalf("successor UID leaked evidence: %+v err=%v", got, err)
	}
}
