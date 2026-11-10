package provider

import (
	"errors"
	"testing"
)

func kueueEvidenceFixture() KueueJobEvidence {
	return KueueJobEvidence{
		TenantID: "tenant-a", WorkloadID: "compute-workload-a", DesiredGeneration: 7,
		Job: KubernetesResourceRef{
			Namespace: "tenant-a", Name: "job-a", UID: "job-uid-a", ResourceVersion: "101",
		},
		JobTenantID: "tenant-a", JobWorkloadID: "compute-workload-a", JobDesiredGeneration: 7,
		KueueWorkload: KubernetesResourceRef{
			Namespace: "tenant-a", Name: "kueue-workload-a", UID: "kueue-uid-a", ResourceVersion: "102",
		},
		KueueOwnerJobUID: "job-uid-a", KueueAdmitted: true,
		Pod: KubernetesResourceRef{
			Namespace: "tenant-a", Name: "pod-a", UID: "pod-uid-a", ResourceVersion: "103",
		},
		PodOwnerJobUID: "job-uid-a", PodScheduled: true, PodNodeName: "node-1",
	}
}

func TestKueueObservationSeparatesAdmissionFromPlacement(t *testing.T) {
	tests := []struct {
		name   string
		change func(*KueueJobEvidence)
		want   KueueObservationStage
	}{
		{
			name: "job exists but queue evidence absent",
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
		{
			name: "kueue workload exists but admission absent",
			change: func(e *KueueJobEvidence) {
				e.KueueAdmitted = false
				e.Pod = KubernetesResourceRef{}
				e.PodOwnerJobUID = ""
				e.PodScheduled = false
				e.PodNodeName = ""
			},
			want: KueueUnproven,
		},
		{
			name: "admitted but pod absent",
			change: func(e *KueueJobEvidence) {
				e.Pod = KubernetesResourceRef{}
				e.PodOwnerJobUID = ""
				e.PodScheduled = false
				e.PodNodeName = ""
			},
			want: KueueAdmitted,
		},
		{
			name: "admitted but pod unscheduled",
			change: func(e *KueueJobEvidence) {
				e.PodScheduled = false
				e.PodNodeName = ""
			},
			want: KueueAdmitted,
		},
		{
			name:   "pod scheduled without device proof is placement only",
			change: func(e *KueueJobEvidence) {},
			want:   KueuePodPlaced,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			e := kueueEvidenceFixture()
			tc.change(&e)
			got, err := AssessKueueJobObservation(e)
			if err != nil || got != tc.want {
				t.Fatalf("stage=%s err=%v, want %s", got, err, tc.want)
			}
		})
	}
}

func TestKueueObservationRejectsContradictoryOrCrossScopeEvidence(t *testing.T) {
	tests := []struct {
		name string
		change func(*KueueJobEvidence)
	}{
		{"missing canonical tenant", func(e *KueueJobEvidence) { e.TenantID = "" }},
		{"missing desired generation", func(e *KueueJobEvidence) { e.DesiredGeneration = 0 }},
		{"foreign job tenant", func(e *KueueJobEvidence) { e.JobTenantID = "tenant-b" }},
		{"foreign job workload", func(e *KueueJobEvidence) { e.JobWorkloadID = "other-workload" }},
		{"stale job generation", func(e *KueueJobEvidence) { e.JobDesiredGeneration-- }},
		{"missing job UID", func(e *KueueJobEvidence) { e.Job.UID = "" }},
		{"missing kueue resource version", func(e *KueueJobEvidence) { e.KueueWorkload.ResourceVersion = "" }},
		{"foreign kueue namespace", func(e *KueueJobEvidence) { e.KueueWorkload.Namespace = "tenant-b" }},
		{"foreign kueue job owner", func(e *KueueJobEvidence) { e.KueueOwnerJobUID = "other-job-uid" }},
		{"foreign pod namespace", func(e *KueueJobEvidence) { e.Pod.Namespace = "tenant-b" }},
		{"foreign pod job owner", func(e *KueueJobEvidence) { e.PodOwnerJobUID = "other-job-uid" }},
		{"missing pod resource version", func(e *KueueJobEvidence) { e.Pod.ResourceVersion = "" }},
		{"scheduled without node name", func(e *KueueJobEvidence) { e.PodNodeName = "" }},
		{"node name without scheduled", func(e *KueueJobEvidence) { e.PodScheduled = false }},
		{"pod placement without kueue admission", func(e *KueueJobEvidence) { e.KueueAdmitted = false }},
		{"kueue admission without kueue identity", func(e *KueueJobEvidence) { e.KueueWorkload = KubernetesResourceRef{} }},
		{"pod evidence without job identity", func(e *KueueJobEvidence) { e.Job = KubernetesResourceRef{} }},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			e := kueueEvidenceFixture()
			tc.change(&e)
			got, err := AssessKueueJobObservation(e)
			if !errors.Is(err, ErrInvalidKueueObservation) || got != KueueUnproven {
				t.Fatalf("contradictory evidence accepted: stage=%s err=%v", got, err)
			}
		})
	}
}
