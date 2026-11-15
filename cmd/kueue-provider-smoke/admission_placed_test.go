package main

import (
	"errors"
	"testing"
	"time"

	"github.com/avaargsh/compute-control-panel/internal/provider"
)

func placedObservationFixture() (provider.KueueCreatePin, provider.KueueJobCollection) {
	req := admissionCreateRequest()
	pin := provider.KueueCreatePin{
		Namespace: smokeNamespace, Name: "ccp-admission-job", UID: "job-uid-a", Request: req,
	}
	e := provider.KueueJobEvidence{
		TenantID: req.TenantID, WorkloadID: req.WorkloadID, DesiredGeneration: req.DesiredGeneration,
		Job: provider.KubernetesResourceRef{Namespace: smokeNamespace, Name: pin.Name, UID: pin.UID, ResourceVersion: "101"},
		JobTenantID: req.TenantID, JobWorkloadID: req.WorkloadID, JobDesiredGeneration: req.DesiredGeneration,
		KueueWorkload: provider.KubernetesResourceRef{Namespace: smokeNamespace, Name: "kueue-workload", UID: "kueue-uid-a", ResourceVersion: "102"},
		KueueOwnerJobUID: pin.UID,
		KueueQuotaReserved: true, KueueAdmitted: true, KueueClusterQueue: "ccp-smoke",
		KueueQuotaCondition: provider.KueueConditionEvidence{Status: "True"},
		KueueAdmissionCondition: provider.KueueConditionEvidence{Status: "True"},
		KueuePodSetAssignmentsJSON: `[{"name":"main","resourceUsage":{"cpu":"100m"}}]`,
		Pod: provider.KubernetesResourceRef{Namespace: smokeNamespace, Name: "pod-a", UID: "pod-uid-a", ResourceVersion: "103"},
		PodOwnerJobUID: pin.UID, PodScheduled: true, PodNodeName: "ccp-kind-node",
	}
	return pin, provider.KueueJobCollection{
		Stage: provider.KueuePodPlaced,
		Evidence: e,
		Provenance: provider.KueueEvidenceProvenance{
			Source: "kubernetes-https-readonly-unattested",
			AdapterVersion: "test-only", ObservedAt: time.Now().UTC(),
		},
	}
}

func TestPinnedPlacedObservationStaysNotReady(t *testing.T) {
	pin, first := placedObservationFixture()
	second := first
	second.Provenance.ObservedAt = first.Provenance.ObservedAt.Add(time.Second)
	got, err := projectStablePinnedPlacement(pin, first, second)
	if err != nil {
		t.Fatal(err)
	}
	if got.Phase != "inspect-placed" || got.Projection != "NOT_READY" ||
		got.Stage != string(provider.KueuePodPlaced) ||
		got.Result != "RESOURCE_PRESENT" || !got.QuotaReserved || !got.Admitted ||
		got.ClusterQueue != "ccp-smoke" || got.JobUID != pin.UID ||
		got.WorkloadUID != "kueue-uid-a" || got.PodUID != "pod-uid-a" ||
		got.PodResourceVersion != "103" || got.JobResourceVersion != "101" ||
		got.WorkloadResourceVersion != "102" ||
		!got.PodScheduled || !got.NodeBound || got.NodeName != "ccp-kind-node" ||
		got.Allocated || got.Active || got.Ready || got.Costable || got.Released ||
		got.ReleaseAttested || got.AbsenceObserved || got.OperationID != pin.Request.OperationID {
		t.Fatalf("placed Pod promoted or lost pinned UID/RV: %+v", got)
	}
}

func TestPinnedPlacedObservationFailsClosedOnIdentityOrReadDrift(t *testing.T) {
	tests := []struct {
		name string
		alter func(*provider.KueueCreatePin, *provider.KueueJobCollection, *provider.KueueJobCollection)
	}{
		{"missing durable pin", func(pin *provider.KueueCreatePin, _, _ *provider.KueueJobCollection) { pin.UID = "" }},
		{"untrusted operation pin", func(pin *provider.KueueCreatePin, _, _ *provider.KueueJobCollection) { pin.Request.OperationID = "other" }},
		{"foreign pinned tenant", func(pin *provider.KueueCreatePin, _, _ *provider.KueueJobCollection) { pin.Request.TenantID = "tenant-b" }},
		{"same name new Job UID", func(_ *provider.KueueCreatePin, _, second *provider.KueueJobCollection) { second.Evidence.Job.UID = "new-job-uid" }},
		{"Pod UID replaced", func(_ *provider.KueueCreatePin, _, second *provider.KueueJobCollection) { second.Evidence.Pod.UID = "new-pod-uid" }},
		{"Pod RV changed", func(_ *provider.KueueCreatePin, _, second *provider.KueueJobCollection) { second.Evidence.Pod.ResourceVersion = "104" }},
		{"Kueue Workload RV changed", func(_ *provider.KueueCreatePin, _, second *provider.KueueJobCollection) { second.Evidence.KueueWorkload.ResourceVersion = "104" }},
		{"Job RV changed", func(_ *provider.KueueCreatePin, _, second *provider.KueueJobCollection) { second.Evidence.Job.ResourceVersion = "105" }},
		{"second read unbound", func(_ *provider.KueueCreatePin, _, second *provider.KueueJobCollection) {
			second.Stage = provider.KueueAdmitted
			second.Evidence.PodScheduled = false
			second.Evidence.PodNodeName = ""
		}},
		{"preempted after first read", func(_ *provider.KueueCreatePin, _, second *provider.KueueJobCollection) {
			second.Evidence.KueuePreempted = true
			second.Evidence.KueuePreemptedCondition.Status = "True"
		}},
		{"foreign Pod namespace", func(_ *provider.KueueCreatePin, first, second *provider.KueueJobCollection) {
			first.Evidence.Pod.Namespace = "tenant-b"
			second.Evidence.Pod.Namespace = "tenant-b"
		}},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			pin, first := placedObservationFixture()
			second := first
			tc.alter(&pin, &first, &second)
			got, err := projectStablePinnedPlacement(pin, first, second)
			if !errors.Is(err, provider.ErrInvalidKueueObservation) || got != (admissionReport{}) {
				t.Fatalf("untrusted/drifting observation accepted: %+v err=%v", got, err)
			}
		})
	}
}

func TestAdmissionInspectPlacedIsExplicitOptIn(t *testing.T) {
	phase, subject, err := parseAdmissionArgs([]string{"admission", "inspect", "placed"})
	if err != nil || phase != "inspect" || subject != "placed" {
		t.Fatalf("placed phase=%q subject=%q err=%v", phase, subject, err)
	}
	if _, _, err := parseAdmissionArgs([]string{"admission", "inspect", "placed", "ignore-conflicts"}); err == nil {
		t.Fatal("accepted bypassing unsafe inspect flag")
	}
}
