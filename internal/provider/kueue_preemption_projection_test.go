package provider

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

func admittedPreemptionEvidence() KueueJobEvidence {
	e := kueueEvidenceFixture()
	e.KueueEvictedCondition = KueueConditionEvidence{Status: "MISSING"}
	e.KueuePreemptedCondition = KueueConditionEvidence{Status: "MISSING"}
	return e
}

func preemptedPreemptionEvidence() KueueJobEvidence {
	e := admittedPreemptionEvidence()
	e.KueueQuotaReserved = false
	e.KueueAdmitted = false
	e.KueueClusterQueue = ""
	e.KueuePodSetAssignmentsJSON = ""
	e.KueueQuotaCondition = KueueConditionEvidence{Status: "False", Reason: "Pending"}
	e.KueueAdmissionCondition = KueueConditionEvidence{Status: "False", Reason: "NoReservation"}
	e.KueueEvicted = true
	e.KueuePreempted = true
	e.KueueEvictedCondition = KueueConditionEvidence{Status: "True", Reason: "Preempted", Message: "preempted"}
	e.KueuePreemptedCondition = KueueConditionEvidence{Status: "True", Reason: "InClusterQueue", Message: "in cluster queue"}
	e.KueueWorkload.ResourceVersion = "202"
	e.Job.ResourceVersion = "201"
	e.Pod = KubernetesResourceRef{}
	e.PodOwnerJobUID = ""
	e.PodScheduled = false
	e.PodNodeName = ""
	return e
}

func TestProjectKueueAdmissionBeforePreemptionStaysInactive(t *testing.T) {
	got, err := ProjectKueueAdmissionBeforePreemption(admittedPreemptionEvidence())
	if err != nil {
		t.Fatal(err)
	}
	if got.Phase != "ADMITTED" || got.Disposition != "ADMITTED" || got.Stage != KueuePodPlaced ||
		!got.QuotaReserved || !got.Admitted || got.Evicted || got.Preempted ||
		got.WorkloadUID != "kueue-uid-a" || got.WorkloadResourceVersion != "102" ||
		got.JobUID != "job-uid-a" || got.JobResourceVersion != "101" ||
		got.PodUID != "pod-uid-a" || !got.PodScheduled || got.NodeName != "node-1" ||
		got.Allocated || got.Active || got.Ready || got.Costable || got.Released {
		t.Fatalf("admission was promoted: %+v", got)
	}
	unplaced := admittedPreemptionEvidence()
	unplaced.Pod = KubernetesResourceRef{}
	unplaced.PodOwnerJobUID = ""
	unplaced.PodScheduled = false
	unplaced.PodNodeName = ""
	got, err = ProjectKueueAdmissionBeforePreemption(unplaced)
	if err != nil || got.Phase != "ADMITTED" || got.Stage != KueueAdmitted || got.PodScheduled || got.Ready || got.Released {
		t.Fatalf("unplaced admission: %+v err=%v", got, err)
	}
}

func TestProjectKueuePreemptedWorkloadIsNotRelease(t *testing.T) {
	e := preemptedPreemptionEvidence()
	stage, err := AssessKueueJobObservation(e)
	if err != nil || stage != KueueUnproven {
		t.Fatalf("preemption changed the structural stage: %s err=%v", stage, err)
	}
	got, err := ProjectKueuePreemptedWorkload(e)
	if err != nil {
		t.Fatal(err)
	}
	if got.Phase != "NOT_READY" || got.Disposition != "PREEMPTED" || got.Stage != KueueUnproven ||
		!got.Evicted || got.EvictedReason != "Preempted" || !got.Preempted || got.PreemptedReason != "InClusterQueue" ||
		got.QuotaReserved || got.Admitted || got.ClusterQueue != "" ||
		got.WorkloadUID != "kueue-uid-a" || got.WorkloadResourceVersion != "202" ||
		got.JobUID != "job-uid-a" || got.JobUID == got.WorkloadUID ||
		got.PodUID != "" || got.Allocated || got.Active || got.Ready || got.Costable || got.Released {
		t.Fatalf("preemption was not NOT_READY: %+v", got)
	}
}

func TestPreemptionProjectionRejectsMissingOrContradictoryEvidence(t *testing.T) {
	cases := []struct {
		name   string
		change func(*KueueJobEvidence)
	}{
		{name: "evicted while still admitted", change: func(e *KueueJobEvidence) {
			e.KueueEvicted = true
			e.KueueEvictedCondition = KueueConditionEvidence{Status: "True", Reason: "Preempted"}
		}},
		{name: "preempted condition without eviction", change: func(e *KueueJobEvidence) {
			*e = preemptedPreemptionEvidence()
			e.KueueEvicted = false
			e.KueueEvictedCondition = KueueConditionEvidence{Status: "False", Reason: "Pending"}
		}},
		{name: "quota still reserved", change: func(e *KueueJobEvidence) {
			*e = preemptedPreemptionEvidence()
			e.KueueQuotaReserved = true
			e.KueueQuotaCondition.Status = "True"
			e.KueueClusterQueue = "cq-a"
			e.KueuePodSetAssignmentsJSON = `[{"name":"main","resourceUsage":{"cpu":"1"}}]`
		}},
		{name: "admitted condition missing", change: func(e *KueueJobEvidence) {
			*e = preemptedPreemptionEvidence()
			e.KueueAdmissionCondition = KueueConditionEvidence{Status: "MISSING"}
		}},
		{name: "wrong preemption reason", change: func(e *KueueJobEvidence) {
			*e = preemptedPreemptionEvidence()
			e.KueuePreemptedCondition.Reason = "InCohortReclamation"
		}},
		{name: "conditions not read", change: func(e *KueueJobEvidence) {
			e.KueueEvictedCondition = KueueConditionEvidence{}
			e.KueuePreemptedCondition = KueueConditionEvidence{}
		}},
		{name: "same job and workload uid", change: func(e *KueueJobEvidence) {
			*e = preemptedPreemptionEvidence()
			e.KueueWorkload.UID = e.Job.UID
		}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			e := admittedPreemptionEvidence()
			tc.change(&e)
			admitted, admittedErr := ProjectKueueAdmissionBeforePreemption(e)
			preempted, preemptedErr := ProjectKueuePreemptedWorkload(e)
			if admittedErr == nil || preemptedErr == nil ||
				admitted != (KueuePreemptionProjection{}) || preempted != (KueuePreemptionProjection{}) ||
				admitted.Ready || admitted.Released || preempted.Ready || preempted.Released ||
				preempted.Phase == "RELEASED" || admitted.Phase == "READY" {
				t.Fatalf("promoted contradictory evidence: admitted=%+v %v preempted=%+v %v", admitted, admittedErr, preempted, preemptedErr)
			}
		})
	}
}

func TestPreemptionFenceRejectsStaleAndCrossProjection(t *testing.T) {
	admitted, err := ProjectKueueAdmissionBeforePreemption(admittedPreemptionEvidence())
	if err != nil {
		t.Fatal(err)
	}
	admitted.ReadSequence = 1
	fence, err := AdvanceKueuePreemptionFence(KueuePreemptionFence{}, admitted)
	if err != nil || fence.Phase != "ADMITTED" || fence.JobUID != admitted.JobUID || fence.ReadSequence != 1 {
		t.Fatalf("initial fence: %+v err=%v", fence, err)
	}
	again, err := AdvanceKueuePreemptionFence(fence, admitted)
	if err != nil || again != fence {
		t.Fatalf("duplicate event was not deduped: %+v err=%v", again, err)
	}
	preempted, err := ProjectKueuePreemptedWorkload(preemptedPreemptionEvidence())
	if err != nil {
		t.Fatal(err)
	}
	preempted.ReadSequence = 2
	if _, err := AdvanceKueuePreemptionFence(KueuePreemptionFence{}, preempted); !errors.Is(err, ErrInvalidKueueObservation) {
		t.Fatalf("preemption without an admitted fence: %v", err)
	}
	next, err := AdvanceKueuePreemptionFence(fence, preempted)
	if err != nil || next.Disposition != "PREEMPTED" || next.Phase != "NOT_READY" || next.WorkloadResourceVersion != "202" || next.ReadSequence != 2 {
		t.Fatalf("preemption fence: %+v err=%v", next, err)
	}
	stale, err := AdvanceKueuePreemptionFence(next, admitted)
	if !errors.Is(err, ErrStaleKueuePreemption) || stale != next {
		t.Fatalf("earlier read overwrote the fence: %+v err=%v", stale, err)
	}
	other := preempted
	other.JobUID = "job-uid-b"
	other.WorkloadUID = "kueue-uid-b"
	other.JobResourceVersion = "rv-b"
	other.WorkloadResourceVersion = "abc-b"
	other.ReadSequence = 3
	crossed, err := AdvanceKueuePreemptionFence(next, other)
	if !errors.Is(err, ErrCrossKueuePreemption) || crossed != next {
		t.Fatalf("cross projection overwrote A: %+v err=%v", crossed, err)
	}
	bAdmitted := admitted
	bAdmitted.JobUID = "job-uid-b"
	bAdmitted.WorkloadUID = "kueue-uid-b"
	bAdmitted.JobName = "job-b"
	bAdmitted.WorkloadName = "kueue-workload-b"
	bAdmitted.ReadSequence = 1
	bFence, err := AdvanceKueuePreemptionFence(KueuePreemptionFence{}, bAdmitted)
	if err != nil || bFence.JobUID != "job-uid-b" || bFence.WorkloadUID == fence.WorkloadUID {
		t.Fatalf("B did not get its own fence: %+v err=%v", bFence, err)
	}
	readmitted := admitted
	readmitted.JobResourceVersion = "rv-later"
	readmitted.WorkloadResourceVersion = "abc-later"
	readmitted.ReadSequence = 4
	kept, err := AdvanceKueuePreemptionFence(next, readmitted)
	if err == nil || kept != next || kept.Phase != "NOT_READY" || kept.Disposition != "PREEMPTED" {
		t.Fatalf("later admission cleared preemption: %+v err=%v", kept, err)
	}
}

func TestPreemptionFenceOrdersOpaqueResourceVersionsByCCPRead(t *testing.T) {
	evidence := admittedPreemptionEvidence()
	evidence.Job.ResourceVersion = "rv-a"
	evidence.KueueWorkload.ResourceVersion = "abc"
	admitted, err := ProjectKueueAdmissionBeforePreemption(evidence)
	if err != nil {
		t.Fatal(err)
	}
	admitted.JobResourceVersion = "100"
	admitted.WorkloadResourceVersion = "100"
	admitted.ReadSequence = 1
	fence, err := AdvanceKueuePreemptionFence(KueuePreemptionFence{}, admitted)
	if err != nil || fence.JobResourceVersion != "100" || fence.WorkloadResourceVersion != "100" {
		t.Fatalf("opaque resourceVersion was rejected: %+v err=%v", fence, err)
	}
	blank := evidence
	blank.Job.ResourceVersion = ""
	if _, err := ProjectKueueAdmissionBeforePreemption(blank); err == nil {
		t.Fatal("empty resourceVersion was projected")
	}
	preempted, err := ProjectKueuePreemptedWorkload(preemptedPreemptionEvidence())
	if err != nil {
		t.Fatal(err)
	}
	// Numerically smaller than the fenced "100", but from a later CCP read.
	preempted.JobResourceVersion = "9"
	preempted.WorkloadResourceVersion = "9"
	preempted.ReadSequence = 5
	next, err := AdvanceKueuePreemptionFence(fence, preempted)
	if err != nil || next.Phase != "NOT_READY" || next.WorkloadResourceVersion != "9" || next.ReadSequence != 5 || next.JobResourceVersion != "9" {
		t.Fatalf("later read with a smaller resourceVersion was rejected: %+v err=%v", next, err)
	}
	earlier := preempted
	earlier.JobResourceVersion = "999"
	earlier.WorkloadResourceVersion = "1000"
	earlier.ReadSequence = 4
	kept, err := AdvanceKueuePreemptionFence(next, earlier)
	if !errors.Is(err, ErrStaleKueuePreemption) || kept != next {
		t.Fatalf("earlier read with a different resourceVersion was accepted: %+v err=%v", kept, err)
	}
	repeat := preempted
	repeat.ReadSequence = 5
	again, err := AdvanceKueuePreemptionFence(next, repeat)
	if err != nil || again != next {
		t.Fatalf("identical repeat was not idempotent: %+v err=%v", again, err)
	}
	laterSame := preempted
	laterSame.ReadSequence = 6
	bumped, err := AdvanceKueuePreemptionFence(next, laterSame)
	if err != nil || bumped.ReadSequence != 6 || bumped.WorkloadResourceVersion != next.WorkloadResourceVersion || bumped.Phase != "NOT_READY" {
		t.Fatalf("later identical resourceVersion did not stay the same state: %+v err=%v", bumped, err)
	}
	empty := preempted
	empty.WorkloadResourceVersion = ""
	empty.ReadSequence = 7
	rejected, err := AdvanceKueuePreemptionFence(bumped, empty)
	if err == nil || rejected != bumped || rejected.Phase == "READY" || rejected.Phase == "RELEASED" {
		t.Fatalf("empty resourceVersion was fenced: %+v err=%v", rejected, err)
	}
	noRead := preempted
	noRead.ReadSequence = 0
	noRead.JobResourceVersion = "rv-z"
	rejected, err = AdvanceKueuePreemptionFence(bumped, noRead)
	if err == nil || rejected != bumped {
		t.Fatalf("read without a CCP sequence was fenced: %+v err=%v", rejected, err)
	}
}

func TestReaderProjectsPreemptedWorkloadFromConditions(t *testing.T) {
	workloads := `{"metadata":{"resourceVersion":"21"},"items":[{"metadata":{"name":"kueue-a","namespace":"tenant-a","uid":"kueue-uid-a","resourceVersion":"44","ownerReferences":[{"apiVersion":"batch/v1","kind":"Job","uid":"job-uid-a","controller":true}]},"status":{"conditions":[{"type":"QuotaReserved","status":"False","reason":"Pending"},{"type":"Admitted","status":"False","reason":"NoReservation"},{"type":"Evicted","status":"True","reason":"Preempted","message":"preempted","lastTransitionTime":"2026-11-12T13:00:00Z"},{"type":"Preempted","status":"True","reason":"InClusterQueue","message":"in cluster queue","lastTransitionTime":"2026-11-12T13:00:00Z"}]}}]}`
	reader, _, closeServer := testKubeReader(t, testKubeJob, workloads, `{"items":[]}`, 0)
	defer closeServer()
	evidence, stage, err := reader.ObserveJob(context.Background(), observedJobQuery())
	if err != nil || stage != KueueUnproven {
		t.Fatalf("reader stage=%s err=%v evidence=%+v", stage, err, evidence)
	}
	if evidence.Job.UID != "job-uid-a" || evidence.KueueWorkload.UID != "kueue-uid-a" ||
		evidence.KueueWorkload.ResourceVersion != "44" ||
		!evidence.KueueEvicted || evidence.KueueEvictedCondition.Reason != "Preempted" ||
		!evidence.KueuePreempted || evidence.KueuePreemptedCondition.Reason != "InClusterQueue" ||
		evidence.KueueQuotaReserved || evidence.KueueQuotaCondition.Status != "False" ||
		evidence.KueueAdmitted || evidence.KueueAdmissionCondition.Status != "False" {
		t.Fatalf("conditions were not read by UID: %+v", evidence)
	}
	got, err := ProjectKueuePreemptedWorkload(evidence)
	if err != nil || got.Phase != "NOT_READY" || got.Disposition != "PREEMPTED" || got.Released || got.Ready {
		t.Fatalf("reader evidence did not project preemption: %+v err=%v", got, err)
	}
	ambiguous := strings.Replace(workloads, `{"type":"Evicted"`, `{"type":"Evicted","status":"True","reason":"Preempted"},{"type":"Evicted"`, 1)
	reader, _, closeServer = testKubeReader(t, testKubeJob, ambiguous, `{"items":[]}`, 0)
	defer closeServer()
	if _, _, err := reader.ObserveJob(context.Background(), observedJobQuery()); !errors.Is(err, ErrInvalidKueueObservation) {
		t.Fatalf("duplicate Evicted condition was accepted: %v", err)
	}
}

func TestQueuedWorkloadPriorityClassIsOptIn(t *testing.T) {
	api := &kueueAPI{t: t, jobs: make(map[string]kubeObject)}
	server := httptest.NewTLSServer(http.HandlerFunc(api.serve))
	defer server.Close()
	client := server.Client()
	client.Timeout = 3 * time.Second
	adapter, err := NewQueuedKueueJobAdapterWithBindings(server.URL, "tenant-a", "ccp-preempt", client, NewMemoryKueueJobBindingStore())
	if err != nil {
		t.Fatal(err)
	}
	if err := adapter.UseQueuedWorkloadPriorityClass("Not A Name"); err == nil {
		t.Fatal("invalid priority class accepted")
	}
	if err := adapter.UseQueuedWorkloadPriorityClass("ccp-preempt-low"); err != nil {
		t.Fatal(err)
	}
	req := kueueAdapterRequest()
	if _, err := adapter.Apply(context.Background(), req); err != nil {
		t.Fatal(err)
	}
	var submitted kueueBatchJob
	for _, call := range api.snapshot() {
		if call.method == http.MethodPost {
			if err := json.Unmarshal(call.body, &submitted); err != nil {
				t.Fatal(err)
			}
		}
	}
	if submitted.Metadata.Labels["kueue.x-k8s.io/queue-name"] != "ccp-preempt" ||
		submitted.Metadata.Labels["kueue.x-k8s.io/priority-class"] != "ccp-preempt-low" ||
		submitted.Spec.Suspend {
		t.Fatalf("priority class was not submitted on the queued Job: %+v", submitted)
	}
	plain, err := NewKueueJobAdapter(server.URL, "tenant-a", client)
	if err != nil {
		t.Fatal(err)
	}
	if err := plain.UseQueuedWorkloadPriorityClass("ccp-preempt-low"); !errors.Is(err, ErrInvalidKueueObservation) {
		t.Fatalf("unqueued adapter accepted a priority class: %v", err)
	}
	api.mu.Lock()
	for name, job := range api.jobs {
		job.Metadata.Labels["kueue.x-k8s.io/priority-class"] = "ccp-preempt-high"
		api.jobs[name] = job
	}
	api.mu.Unlock()
	if _, err := adapter.Apply(context.Background(), req); !errors.Is(err, ErrOperationConflict) {
		t.Fatalf("changed priority class was adopted: %v", err)
	}
}
