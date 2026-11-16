package main

import (
	"github.com/avaargsh/compute-control-panel/internal/provider"
	"testing"
)

func TestSingleWorkloadDRAEvidence(t *testing.T) {
	pin, first := placedObservationFixture()
	inv := provider.DRAPoolInventoryObservation{
		PodClaim: provider.DRAPodClaimObservation{Pod: first.Evidence.Pod, NodeName: first.Evidence.PodNodeName,
			PodClaimAlias: "gpu", ConsumerContainer: "ccp-queued",
			Claim: provider.DRAClaimObservation{Claim: provider.KubernetesResourceRef{Namespace: smokeNamespace, Name: "claim-a", UID: "claim-uid", ResourceVersion: "201"}, PodUID: first.Evidence.Pod.UID,
				Device: provider.DRAAllocatedDevice{Request: "gpu", Driver: "gpu.example.com", Pool: "pool-a", Device: "device-a"}}},
		Slice: provider.KubernetesResourceRef{Name: "slice-a", UID: "slice-uid", ResourceVersion: "301"}, PoolGeneration: 1, PoolResourceSliceCount: 1,
	}
	got, err := singleWorkloadDRAEvidence(pin, first, first, inv)
	if err != nil {
		t.Fatal(err)
	}
	if got["allocation_ready"] != false || got["physical_gpu_verified"] != false || got["pod_uid"] != first.Evidence.Pod.UID {
		t.Fatalf("promoted simulated device or lost Pod: %+v", got)
	}
	for _, name := range []string{"pod", "node", "claim reservation", "job", "generation", "rv", "unplaced", "unallocated", "slice"} {
		t.Run(name, func(t *testing.T) {
			second, changed := first, inv
			switch name {
			case "pod":
				changed.PodClaim.Pod.UID = "foreign"
			case "node":
				changed.PodClaim.NodeName = "foreign"
			case "claim reservation":
				changed.PodClaim.Claim.PodUID = "foreign"
			case "job":
				second.Evidence.Job.UID = "foreign"
			case "generation":
				second.Evidence.DesiredGeneration++
			case "rv":
				second.Evidence.Pod.ResourceVersion = "changed"
			case "unplaced":
				second.Evidence.PodNodeName = ""
			case "unallocated":
				changed.PodClaim.Claim.Device.Device = ""
			case "slice":
				changed.Slice.UID = ""
			}
			if got, err := singleWorkloadDRAEvidence(pin, first, second, changed); err == nil || got != nil {
				t.Fatalf("accepted %s: %+v", name, got)
			}
		})
	}
}
