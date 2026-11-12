package provider

import (
	"errors"
	"testing"
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
