package provider

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	"github.com/avaargsh/compute-control-panel/internal/operation"
)

// This is a pure structural join of separately supplied observations. It
// cannot authenticate any source or authorize RELEASED/cost FINAL.
func releaseCandidateFixture() ReleaseEvidenceCandidate {
	q := previousReservedQueue()
	q.Job.TenantID = "tenant-a"
	q.Job.WorkloadID = "workload-a"
	q.Job.DesiredGeneration = 7
	q.Job.JobTenantID = "tenant-a"
	q.Job.JobWorkloadID = "workload-a"
	q.Job.JobDesiredGeneration = 7
	q.Job.Pod = KubernetesResourceRef{
		Namespace: "tenant-a", Name: "gpu-pod-a", UID: "pod-uid-a", ResourceVersion: "221",
	}
	q.Job.PodOwnerJobUID = "job-uid-a"
	q.Job.PodScheduled = true
	q.Job.PodNodeName = "gpu-node-a"
	q.Stage = KueuePodPlaced
	// Historical POD_PLACED requires native Kueue admission as well as
	// quota reservation; these represent distinct controller conditions.
	q.Job.KueueAdmitted = true
	q.Job.KueueAdmissionCondition = KueueConditionEvidence{Status: "True"}

	del := operation.Operation{
		ID: "delete-op-a", TenantID: "tenant-a", WorkloadID: "workload-a",
		ProviderID:     "provider-a",
		IdempotencyKey: "delete-a", RequestDigest: "sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
		Generation: 8, Version: 4, Kind: operation.Delete, Status: operation.Succeeded,
		TargetResourceID: "tenant-a/job-a", TargetResourceUID: "job-uid-a",
	}
	obs := operation.Observation{
		OperationID: del.ID, TenantID: del.TenantID, WorkloadID: del.WorkloadID,
		ProviderID: del.ProviderID, DesiredGeneration: del.Generation,
		Result:           operation.TargetAbsent,
		TargetResourceID: del.TargetResourceID, TargetResourceUID: del.TargetResourceUID,
		ResourceVersion: "delete-journal-5", ObservedAt: time.Date(2026, 11, 10, 10, 0, 0, 0, time.UTC),
	}
	create := operation.Operation{
		ID: "create-op-a", TenantID: "tenant-a", WorkloadID: "workload-a",
		AllocationID: "allocation-a", ProviderID: "provider-a",
		IdempotencyKey: "create-a", RequestDigest: "sha256:bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
		Generation: 7, Version: 4, Kind: operation.Create, Status: operation.Succeeded,
		ProviderResourceID: "tenant-a/job-a", ProviderResourceUID: "job-uid-a",
	}
	claim := previousDRAClaimAllocation()
	candidate := ReleaseEvidenceCandidate{
		ExpectedAllocationID: "allocation-a",

		Create:          create,
		Delete:          del,
		ProviderAbsence: obs,
		PodAbsent:       KubernetesPodAbsentObservation{PreviousPod: q.Job.Pod, PodListResourceVersion: "300"},
		PriorQueue:      q,
		QuotaCleared: KueueQuotaClearedObservation{
			PreviousWorkload:            q.Job.KueueWorkload,
			WorkloadListResourceVersion: "70",
			LocalQueue:                  q.LocalQueue,
			ClusterQueue: KubernetesResourceRef{
				Name: "cq-a", UID: "cluster-uid-a", ResourceVersion: "65",
			},
		},
		PriorPodClaim: DRAPodClaimObservation{
			Pod: q.Job.Pod, NodeName: q.Job.PodNodeName,
			PodClaimAlias: "gpu", ConsumerContainer: "worker", Claim: claim,
		},
		ClaimUnallocated: DRAClaimUnallocatedObservation{
			PreviousClaim: claim.Claim, PreviousPodUID: claim.PodUID,
			PreviousDevice: claim.Device,
			CurrentClaim: KubernetesResourceRef{
				Namespace: "tenant-a", Name: "gpu-claim-a",
				UID: "claim-uid-a", ResourceVersion: "151",
			},
		},
	}
	candidate.PreDeleteSnapshot = releaseCandidatePreDeleteSnapshot(candidate)
	return candidate
}

func TestReleaseEvidenceCorrelationRejectsCrossAllocationSplicing(t *testing.T) {
	tests := []struct {
		name   string
		mutate func(*ReleaseEvidenceCandidate)
		valid  bool
	}{
		{name: "all exact resource identities for one Allocation correlate but do not release", valid: true},
		{name: "historical SUPERSEDED CREATE with cleanup liability correlates", mutate: func(e *ReleaseEvidenceCandidate) {
			e.Create.Status = operation.Superseded
			e.Create.CleanupRequired = true
			e.Create.ReconcileRequired = true
		}, valid: true},
		{name: "SUPERSEDED CREATE without cleanup-required identity", mutate: func(e *ReleaseEvidenceCandidate) {
			e.Create.Status = operation.Superseded
			e.Create.ReconcileRequired = true
		}},
		{name: "CREATE missing durable Allocation ID", mutate: func(e *ReleaseEvidenceCandidate) { e.Create.AllocationID = "" }},
		{name: "CREATE belongs to different Allocation", mutate: func(e *ReleaseEvidenceCandidate) { e.Create.AllocationID = "allocation-b" }},
		{name: "CREATE belongs to different Provider", mutate: func(e *ReleaseEvidenceCandidate) { e.Create.ProviderID = "provider-b" }},
		{name: "CREATE not confirmed", mutate: func(e *ReleaseEvidenceCandidate) {
			e.Create.Status = operation.Unknown
			e.Create.ProviderResourceID = ""
			e.Create.ProviderResourceUID = ""
		}},
		{name: "CREATE Job UID differs", mutate: func(e *ReleaseEvidenceCandidate) { e.Create.ProviderResourceUID = "other-job-uid" }},
		{name: "CREATE Job name differs", mutate: func(e *ReleaseEvidenceCandidate) { e.Create.ProviderResourceID = "tenant-a/job-b" }},
		{name: "CREATE generation mismatches Kueue pin", mutate: func(e *ReleaseEvidenceCandidate) { e.Create.Generation = 9 }},
		{name: "DELETE references older-than-CREATE generation", mutate: func(e *ReleaseEvidenceCandidate) { e.Delete.Generation = 6; e.ProviderAbsence.DesiredGeneration = 6 }},
		{name: "DELETE pending not terminal", mutate: func(e *ReleaseEvidenceCandidate) { e.Delete.Status = operation.Pending }},
		{name: "DELETE from wrong kind", mutate: func(e *ReleaseEvidenceCandidate) { e.Delete.Kind = operation.Update }},
		{name: "DELETE illegally binds historical Allocation FK", mutate: func(e *ReleaseEvidenceCandidate) { e.Delete.AllocationID = "allocation-a" }},
		{name: "DELETE falsely binds successor Allocation FK", mutate: func(e *ReleaseEvidenceCandidate) { e.Delete.AllocationID = "allocation-b" }},
		{name: "expected historical Allocation missing", mutate: func(e *ReleaseEvidenceCandidate) { e.ExpectedAllocationID = "" }},
		{name: "expected historical Allocation replaced", mutate: func(e *ReleaseEvidenceCandidate) { e.ExpectedAllocationID = "allocation-b" }},
		{name: "expected historical Allocation has padding", mutate: func(e *ReleaseEvidenceCandidate) { e.ExpectedAllocationID = " allocation-a " }},
		{name: "DELETE wrong tenant", mutate: func(e *ReleaseEvidenceCandidate) { e.Delete.TenantID = "tenant-b" }},
		{name: "DELETE wrong workload", mutate: func(e *ReleaseEvidenceCandidate) { e.Delete.WorkloadID = "workload-b" }},
		{name: "DELETE resource name differs", mutate: func(e *ReleaseEvidenceCandidate) { e.Delete.TargetResourceID = "tenant-a/job-other" }},
		{name: "DELETE resource UID differs", mutate: func(e *ReleaseEvidenceCandidate) { e.Delete.TargetResourceUID = "foreign-job-uid" }},
		{name: "Provider absence missing", mutate: func(e *ReleaseEvidenceCandidate) { e.ProviderAbsence = operation.Observation{} }},
		{name: "Provider absence is only operation in progress", mutate: func(e *ReleaseEvidenceCandidate) { e.ProviderAbsence.Result = operation.OperationInProgress }},
		{name: "Provider absence belongs to different operation", mutate: func(e *ReleaseEvidenceCandidate) { e.ProviderAbsence.OperationID = "other-delete" }},
		{name: "Provider absence belongs to different Provider", mutate: func(e *ReleaseEvidenceCandidate) { e.ProviderAbsence.ProviderID = "provider-b" }},
		{name: "Provider absence belongs to different generation", mutate: func(e *ReleaseEvidenceCandidate) { e.ProviderAbsence.DesiredGeneration = 3 }},
		{name: "Provider absence wrong UID", mutate: func(e *ReleaseEvidenceCandidate) { e.ProviderAbsence.TargetResourceUID = "other-job-uid" }},
		{name: "Provider absent but resource UID also present", mutate: func(e *ReleaseEvidenceCandidate) { e.ProviderAbsence.ResourceUID = "still-live-uid" }},
		{name: "Provider absence resourceVersion contains NUL", mutate: func(e *ReleaseEvidenceCandidate) { e.ProviderAbsence.ResourceVersion = "rv\x00injected" }},
		{name: "Provider absence resourceVersion has surrounding whitespace", mutate: func(e *ReleaseEvidenceCandidate) { e.ProviderAbsence.ResourceVersion = " rv " }},
		{name: "Provider observation lacks timestamp", mutate: func(e *ReleaseEvidenceCandidate) { e.ProviderAbsence.ObservedAt = time.Time{} }},
		{name: "Provider observation has unresolved reason", mutate: func(e *ReleaseEvidenceCandidate) { e.ProviderAbsence.ReasonCode = "UNKNOWN" }},
		{name: "Kueue prior from different tenant", mutate: func(e *ReleaseEvidenceCandidate) { e.PriorQueue.Job.TenantID = "tenant-b" }},
		{name: "Kueue prior from different Workload", mutate: func(e *ReleaseEvidenceCandidate) { e.PriorQueue.Job.JobWorkloadID = "workload-b" }},
		{name: "old Job UID differs", mutate: func(e *ReleaseEvidenceCandidate) { e.PriorQueue.Job.Job.UID = "foreign-job" }},
		{name: "old Job generation after DELETE", mutate: func(e *ReleaseEvidenceCandidate) {
			e.PriorQueue.Job.DesiredGeneration = 9
			e.PriorQueue.Job.JobDesiredGeneration = 9
		}},
		{name: "Kueue previous reservation missing", mutate: func(e *ReleaseEvidenceCandidate) { e.PriorQueue.Job.KueueQuotaReserved = false }},
		{name: "POD_PLACED was never admitted", mutate: func(e *ReleaseEvidenceCandidate) {
			e.PriorQueue.Job.KueueAdmitted = false
			e.PriorQueue.Job.KueueAdmissionCondition = KueueConditionEvidence{Status: "False"}
		}},
		{name: "admission flag true but native condition false", mutate: func(e *ReleaseEvidenceCandidate) {
			e.PriorQueue.Job.KueueAdmissionCondition = KueueConditionEvidence{Status: "False"}
		}},
		{name: "admission native condition unknown", mutate: func(e *ReleaseEvidenceCandidate) {
			e.PriorQueue.Job.KueueAdmissionCondition = KueueConditionEvidence{Status: "Unknown"}
		}},
		{name: "native admission not observed", mutate: func(e *ReleaseEvidenceCandidate) {
			e.PriorQueue.Job.KueueAdmissionCondition = KueueConditionEvidence{}
		}},
		{name: "Kueue previous stage did not prove Pod", mutate: func(e *ReleaseEvidenceCandidate) { e.PriorQueue.Stage = KueueAdmitted }},
		{name: "quota clear snapshot missing", mutate: func(e *ReleaseEvidenceCandidate) { e.QuotaCleared = KueueQuotaClearedObservation{} }},
		{name: "quota clear from different Workload UID", mutate: func(e *ReleaseEvidenceCandidate) { e.QuotaCleared.PreviousWorkload.UID = "other-kueue" }},
		{name: "quota clear from different LocalQueue", mutate: func(e *ReleaseEvidenceCandidate) { e.QuotaCleared.LocalQueue.UID = "foreign-local" }},
		{name: "quota clear from different ClusterQueue", mutate: func(e *ReleaseEvidenceCandidate) { e.QuotaCleared.ClusterQueue.UID = "foreign-cq" }},
		{name: "quota clear lacks list resourceVersion", mutate: func(e *ReleaseEvidenceCandidate) { e.QuotaCleared.WorkloadListResourceVersion = "" }},
		{name: "quota clear LIST resourceVersion contains NUL", mutate: func(e *ReleaseEvidenceCandidate) {
			e.QuotaCleared.WorkloadListResourceVersion = "rv\x00forged"
		}},
		{name: "quota clear ClusterQueue resourceVersion contains NUL", mutate: func(e *ReleaseEvidenceCandidate) {
			e.QuotaCleared.ClusterQueue.ResourceVersion = "rv\x00forged"
		}},
		{name: "missing independent Pod absence", mutate: func(e *ReleaseEvidenceCandidate) { e.PodAbsent = KubernetesPodAbsentObservation{} }},
		{name: "Pod absence belongs to successor UID", mutate: func(e *ReleaseEvidenceCandidate) { e.PodAbsent.PreviousPod.UID = "successor-uid" }},
		{name: "Pod absence belongs to other tenant", mutate: func(e *ReleaseEvidenceCandidate) { e.PodAbsent.PreviousPod.Namespace = "tenant-b" }},
		{name: "Pod absence has no list resourceVersion", mutate: func(e *ReleaseEvidenceCandidate) { e.PodAbsent.PodListResourceVersion = "" }},
		{name: "Pod absence has whitespace-only list resourceVersion", mutate: func(e *ReleaseEvidenceCandidate) { e.PodAbsent.PodListResourceVersion = " " }},
		{name: "old Pod was never observed", mutate: func(e *ReleaseEvidenceCandidate) { e.PriorQueue.Job.Pod = KubernetesResourceRef{} }},
		{name: "DRA Pod UID differs from Kueue Pod", mutate: func(e *ReleaseEvidenceCandidate) { e.PriorPodClaim.Pod.UID = "foreign-pod-uid" }},
		{name: "DRA Pod resourceVersion differs", mutate: func(e *ReleaseEvidenceCandidate) { e.PriorPodClaim.Pod.ResourceVersion = "222" }},
		{name: "DRA Pod placed on different node", mutate: func(e *ReleaseEvidenceCandidate) { e.PriorPodClaim.NodeName = "gpu-node-b" }},
		{name: "DRA Pod claim consumer missing", mutate: func(e *ReleaseEvidenceCandidate) { e.PriorPodClaim.ConsumerContainer = "" }},
		{name: "DRA Claim Pod UID differs", mutate: func(e *ReleaseEvidenceCandidate) { e.PriorPodClaim.Claim.PodUID = "other-pod-uid" }},
		{name: "DRA Claim belongs to different namespace", mutate: func(e *ReleaseEvidenceCandidate) { e.PriorPodClaim.Claim.Claim.Namespace = "tenant-b" }},
		{name: "cleared Claim belongs to different UID", mutate: func(e *ReleaseEvidenceCandidate) { e.ClaimUnallocated.CurrentClaim.UID = "other-claim" }},
		{name: "cleared Claim has no transition resourceVersion", mutate: func(e *ReleaseEvidenceCandidate) { e.ClaimUnallocated.CurrentClaim.ResourceVersion = "150" }},
		{name: "cleared Claim original UID differs", mutate: func(e *ReleaseEvidenceCandidate) { e.ClaimUnallocated.PreviousClaim.UID = "other-claim" }},
		{name: "cleared Claim previous Pod UID differs", mutate: func(e *ReleaseEvidenceCandidate) { e.ClaimUnallocated.PreviousPodUID = "other-pod-uid" }},
		{name: "cleared Claim original device differs", mutate: func(e *ReleaseEvidenceCandidate) { e.ClaimUnallocated.PreviousDevice.Device = "device-b" }},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			input := releaseCandidateFixture()
			if tc.mutate != nil {
				tc.mutate(&input)
			}
			got, err := CorrelateReleaseEvidence(input)
			if tc.valid {
				if err != nil || got.AllocationID != "allocation-a" ||
					got.TenantID != "tenant-a" || got.WorkloadID != "workload-a" ||
					got.DeleteOperationID != "delete-op-a" ||
					got.Job.UID != "job-uid-a" || got.Pod.UID != "pod-uid-a" ||
					got.Claim.UID != "claim-uid-a" || got.Device.Device != "device-a" {
					t.Fatalf("exact source correlation missing: %+v err=%v", got, err)
				}
			} else if err == nil || got != (ReleaseEvidenceCorrelation{}) {
				t.Fatalf("cross-Allocation evidence was accepted: %+v err=%v", got, err)
			}
		})
	}
}

// A DELETE observation for the old desired generation must not be joined to
// a CREATE as release evidence. A release is a new, fenced desired transition.
func TestReleaseEvidenceRejectsDeleteWithoutGenerationAdvance(t *testing.T) {
	e := releaseCandidateFixture()
	e.Delete.Generation = e.Create.Generation
	e.ProviderAbsence.DesiredGeneration = e.Delete.Generation

	got, err := CorrelateReleaseEvidence(e)
	if err != ErrUncorrelatedReleaseEvidence || got != (ReleaseEvidenceCorrelation{}) {
		t.Fatalf("same-generation DELETE must fail closed: correlation=%+v err=%v", got, err)
	}
}

func TestReleaseEvidenceClaimUIDAbsenceAlternativeIsIdentityFenced(t *testing.T) {
	// The historical Claim may be deleted rather than remain unallocated.
	// The existing LIST observer reports old UID/name absence but not driver
	// NodeUnprepare; correlation remains non-authoritative.
	testCases := []struct {
		name   string
		mutate func(*ReleaseEvidenceCandidate)
		valid  bool
	}{
		{name: "stable old Claim UID absence with exact historic identity", valid: true},
		{name: "missing both Claim alternatives", mutate: func(e *ReleaseEvidenceCandidate) {
			e.ClaimAbsent = nil
		}},
		{name: "unallocated Claim also asserted", mutate: func(e *ReleaseEvidenceCandidate) {
			e.ClaimUnallocated = releaseCandidateFixture().ClaimUnallocated
		}},
		{name: "other historical Claim UID", mutate: func(e *ReleaseEvidenceCandidate) {
			e.ClaimAbsent.PreviousClaim.UID = "foreign-claim-uid"
		}},
		{name: "successor Claim name", mutate: func(e *ReleaseEvidenceCandidate) {
			e.ClaimAbsent.PreviousClaim.Name = "other-claim"
		}},
		{name: "wrong tenant namespace", mutate: func(e *ReleaseEvidenceCandidate) {
			e.ClaimAbsent.PreviousClaim.Namespace = "tenant-b"
		}},
		{name: "missing complete LIST resourceVersion", mutate: func(e *ReleaseEvidenceCandidate) {
			e.ClaimAbsent.ClaimListResourceVersion = ""
		}},
		{name: "malformed LIST resourceVersion", mutate: func(e *ReleaseEvidenceCandidate) {
			e.ClaimAbsent.ClaimListResourceVersion = " 440 "
		}},
		{name: "NUL in LIST resourceVersion", mutate: func(e *ReleaseEvidenceCandidate) {
			e.ClaimAbsent.ClaimListResourceVersion = "440\x00untrusted"
		}},
		{name: "prior DRA Pod UID differs", mutate: func(e *ReleaseEvidenceCandidate) {
			e.PriorPodClaim.Claim.PodUID = "other-pod"
		}},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			e := releaseCandidateFixture()
			prior := e.PriorPodClaim.Claim
			e.ClaimUnallocated = DRAClaimUnallocatedObservation{}
			e.ClaimAbsent = &DRAClaimUIDAbsentObservation{
				PreviousClaim: prior.Claim, ClaimListResourceVersion: "440",
			}
			if tc.mutate != nil {
				tc.mutate(&e)
			}
			got, err := CorrelateReleaseEvidence(e)
			if tc.valid {
				if err != nil || got.AllocationID != "allocation-a" ||
					got.Pod.UID != "pod-uid-a" ||
					got.Claim != prior.Claim || got.Device != prior.Device {
					t.Fatalf("exact old Claim structural absence not correlated: got=%+v err=%v", got, err)
				}
			} else if err != ErrUncorrelatedReleaseEvidence || got != (ReleaseEvidenceCorrelation{}) {
				t.Fatalf("unsafe Claim absence was correlated: got=%+v err=%v", got, err)
			}
		})
	}
}

func TestReleaseCorrelationCollectsActualClaimUIDAbsenceReadOnly(t *testing.T) {
	// Exercise the existing Kubernetes API reader and release correlation
	// together. This TLS fixture is not a real cluster or GPU driver.
	var reads, writes atomic.Int32
	api := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			writes.Add(1)
			http.Error(w, "read-only", http.StatusMethodNotAllowed)
			return
		}
		if r.URL.RequestURI() != "/apis/resource.k8s.io/v1/namespaces/tenant-a/resourceclaims" {
			t.Errorf("unexpected Kubernetes API read: %s", r.URL.RequestURI())
			http.NotFound(w, r)
			return
		}
		reads.Add(1)
		w.Header().Set("Content-Type", "application/json")
		_, _ = fmt.Fprint(w, absentClaimList)
	}))
	defer api.Close()

	client := api.Client()
	client.Timeout = 3 * time.Second
	reader, err := NewKubernetesObservationReader(api.URL, client)
	if err != nil {
		t.Fatal(err)
	}
	candidate := releaseCandidateFixture()
	absent, err := reader.ObserveDRAClaimUIDAbsent(context.Background(), candidate.PriorPodClaim.Claim)
	if err != nil {
		t.Fatalf("independent two-read Claim absence failed: %v", err)
	}
	candidate.ClaimUnallocated = DRAClaimUnallocatedObservation{}
	candidate.ClaimAbsent = &absent
	correlated, err := CorrelateReleaseEvidence(candidate)
	if err != nil || correlated.Claim.UID != "claim-uid-a" ||
		correlated.AllocationID != "allocation-a" || correlated.Device.Device != "device-a" {
		t.Fatalf("read-only Claim absence did not correlate: %+v err=%v", correlated, err)
	}
	if reads.Load() != 2 || writes.Load() != 0 {
		t.Fatalf("expected exactly two Kubernetes GETs and no writes: reads=%d writes=%d", reads.Load(), writes.Load())
	}
}
