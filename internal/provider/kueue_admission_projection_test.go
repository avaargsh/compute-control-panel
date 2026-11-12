package provider

import (
	"errors"
	"testing"
	"time"
)

func TestProjectAdmissionWithoutPlacementStaysNotReady(t *testing.T) {
	e := kueueEvidenceFixture()
	e.PodScheduled = false
	e.PodNodeName = ""
	got, err := ProjectAdmissionWithoutPlacement(e)
	if err != nil {
		t.Fatalf("unplaced admission: %v", err)
	}
	if got.Phase != "NOT_READY" || got.Stage != KueueAdmitted ||
		!got.QuotaReserved || !got.Admitted || got.ClusterQueue != "cq-a" ||
		got.PodSetAssignmentsJSON == "" ||
		got.WorkloadNamespace != "tenant-a" || got.WorkloadName != "kueue-workload-a" ||
		got.WorkloadUID != "kueue-uid-a" || got.WorkloadResourceVersion != "102" ||
		got.JobUID != "job-uid-a" || got.JobResourceVersion != "101" ||
		got.PodUID != "pod-uid-a" || got.PodResourceVersion != "103" ||
		got.PodScheduled || got.NodeBound || got.NodeName != "" ||
		got.Allocated || got.Active || got.Ready || got.Costable || got.Released {
		t.Fatalf("projection mixed admission with placement or readiness: %+v", got)
	}
	if got.JobUID == got.PodUID || got.JobUID == got.WorkloadUID || got.PodUID == got.WorkloadUID {
		t.Fatalf("admission, job, and pod identities were not separate: %+v", got)
	}
}

func TestProjectAdmissionWithoutPlacementRejectsPlacementAndMissingPod(t *testing.T) {
	cases := []struct {
		name   string
		change func(*KueueJobEvidence)
	}{
		{name: "pod placed on a node", change: func(*KueueJobEvidence) {}},
		{name: "admitted before a pod exists", change: func(e *KueueJobEvidence) {
			e.Pod = KubernetesResourceRef{}
			e.PodOwnerJobUID = ""
			e.PodScheduled = false
			e.PodNodeName = ""
		}},
		{name: "quota not reserved", change: func(e *KueueJobEvidence) {
			e.KueueQuotaReserved = false
			e.KueueQuotaCondition.Status = "False"
			e.KueueAdmitted = false
			e.KueueAdmissionCondition.Status = "False"
			e.KueueClusterQueue = ""
			e.KueuePodSetAssignmentsJSON = ""
			e.Pod = KubernetesResourceRef{}
			e.PodOwnerJobUID = ""
			e.PodScheduled = false
			e.PodNodeName = ""
		}},
		{name: "same uid for job and pod", change: func(e *KueueJobEvidence) {
			e.PodScheduled = false
			e.PodNodeName = ""
			e.Pod.UID = e.Job.UID
		}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			e := kueueEvidenceFixture()
			tc.change(&e)
			got, err := ProjectAdmissionWithoutPlacement(e)
			if !errors.Is(err, ErrInvalidKueueObservation) || got != (AdmissionPlacementProjection{}) {
				t.Fatalf("promoted or partially projected: %+v err=%v", got, err)
			}
			if got.Allocated || got.Active || got.Ready || got.Costable || got.Released || got.Phase == "READY" || got.Phase == "RELEASED" {
				t.Fatalf("failed projection still carried a readiness phase: %+v", got)
			}
		})
	}
}


// The integration of Kueue admission without placement (#99) and release
// correlation (#98) must not reinterpret admitted/reserved quota as a
// previously placed, device-assigned Allocation. Even fabricated later Pod,
// quota and Claim-clear observations do not upgrade this historical stage.
func TestAdmittedUnplacedPodCannotBeSplicedIntoReleaseEvidence(t *testing.T) {
	candidate := releaseCandidateFixture()
	job := &candidate.PriorQueue.Job
	job.KueueAdmitted = true
	job.KueueAdmissionCondition.Status = "True"
	job.KueuePodSetAssignmentsJSON = `[{"name":"main","resourceUsage":{"cpu":"2"}}]`
	job.PodScheduled = false
	job.PodNodeName = ""
	candidate.PriorQueue.Stage = KueueAdmitted

	projection, err := ProjectAdmissionWithoutPlacement(*job)
	if err != nil {
		t.Fatalf("expected real admission-without-placement state: %v", err)
	}
	if projection.Phase != "NOT_READY" || projection.Allocated ||
		projection.Active || projection.Ready || projection.Costable || projection.Released {
		t.Fatalf("unplaced admission incorrectly promoted: %+v", projection)
	}

	release, err := CorrelateReleaseEvidence(candidate)
	if !errors.Is(err, ErrUncorrelatedReleaseEvidence) ||
		release != (ReleaseEvidenceCorrelation{}) {
		t.Fatalf("unplaced admission correlated to release: %+v err=%v", release, err)
	}
	authority, private := unprepareTestAuthority()
	verifier, err := NewDRAUnprepareVerifier(authority, 10*time.Minute, 30*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	signed := unprepareTestReceipt(private)
	result, err := verifier.Correlate(candidate, signed, time.Date(2026, 11, 10, 10, 1, 0, 0, time.UTC))
	if !errors.Is(err, ErrUncorrelatedDRAUnprepare) ||
		result != (DRAUnprepareCorrelation{}) {
		t.Fatalf("unplaced admission with signed claim event correlated: %+v err=%v", result, err)
	}
}
