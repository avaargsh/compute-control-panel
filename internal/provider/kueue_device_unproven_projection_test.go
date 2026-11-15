package provider

import (
	"errors"
	"testing"
)

// A placed Pod is still NOT_READY until an independently authenticated DRA
// or certified device-plugin assignment is correlated to this exact Pod.
func TestProjectPlacementWithoutDeviceProofStaysNotReady(t *testing.T) {
	e := kueueEvidenceFixture()
	got, err := ProjectPlacementWithoutDeviceProof(e)
	if err != nil {
		t.Fatalf("placed Pod without device proof: %v", err)
	}
	if got.Phase != "NOT_READY" || got.Stage != KueuePodPlaced ||
		!got.QuotaReserved || !got.Admitted || got.ClusterQueue != "cq-a" ||
		got.PodSetAssignmentsJSON == "" ||
		got.WorkloadNamespace != "tenant-a" || got.WorkloadUID != "kueue-uid-a" ||
		got.WorkloadResourceVersion != "102" ||
		got.JobNamespace != "tenant-a" || got.JobUID != "job-uid-a" ||
		got.JobResourceVersion != "101" ||
		got.PodNamespace != "tenant-a" || got.PodUID != "pod-uid-a" ||
		got.PodResourceVersion != "103" ||
		!got.PodScheduled || !got.NodeBound || got.NodeName != "node-1" ||
		got.Allocated || got.Active || got.Ready || got.Costable || got.Released {
		t.Fatalf("Pod placement promoted to device assignment/READY: %+v", got)
	}
}

func TestProjectPlacementWithoutDeviceProofRejectsInvalidOrPreemptedEvidence(t *testing.T) {
	cases := []struct {
		name   string
		change func(*KueueJobEvidence)
	}{
		{"admitted but Pod has no binding", func(e *KueueJobEvidence) {
			e.PodScheduled = false
			e.PodNodeName = ""
		}},
		{"missing Pod", func(e *KueueJobEvidence) {
			e.Pod = KubernetesResourceRef{}
			e.PodOwnerJobUID = ""
			e.PodScheduled = false
			e.PodNodeName = ""
		}},
		{"foreign Pod owner", func(e *KueueJobEvidence) {
			e.PodOwnerJobUID = "another-job-uid"
		}},
		{"foreign Pod namespace", func(e *KueueJobEvidence) {
			e.Pod.Namespace = "tenant-b"
		}},
		{"stale generation", func(e *KueueJobEvidence) {
			e.JobDesiredGeneration--
		}},
		{"same UID for Job and Pod", func(e *KueueJobEvidence) {
			e.Pod.UID = e.Job.UID
		}},
		{"same UID for Kueue Workload and Pod", func(e *KueueJobEvidence) {
			e.Pod.UID = e.KueueWorkload.UID
		}},
		{"quota not reserved", func(e *KueueJobEvidence) {
			e.KueueQuotaReserved = false
			e.KueueQuotaCondition.Status = "False"
		}},
		{"Kueue preempted", func(e *KueueJobEvidence) {
			e.KueuePreempted = true
			e.KueuePreemptedCondition.Status = "True"
		}},
		{"Kueue evicted", func(e *KueueJobEvidence) {
			e.KueueEvicted = true
			e.KueueEvictedCondition.Status = "True"
		}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			e := kueueEvidenceFixture()
			tc.change(&e)
			got, err := ProjectPlacementWithoutDeviceProof(e)
			if !errors.Is(err, ErrInvalidKueueObservation) || got != (AdmissionPlacementProjection{}) {
				t.Fatalf("invalid evidence received an authoritative placement projection: %+v err=%v", got, err)
			}
		})
	}
}
