package postgres

import "testing"

func goodReleaseCommitFence() (ReleaseCommitFence, releaseCommitFenceSnapshot) {
	expectation := ReleaseCommitFence{
		TenantID: "tenant-a", WorkloadID: "workload-a", AllocationID: "allocation-a", ProviderID: "provider-a",
		CreateOperationID: "create-a", DeleteOperationID: "delete-a",
		TargetResourceID: "tenant-a/job-a", TargetResourceUID: "job-uid-a",
		AllocationGeneration: 1, DeleteGeneration: 2,
		WorkloadVersion: 7, AllocationVersion: 3, CreateOperationVersion: 2, DeleteOperationVersion: 4,
	}
	observed := releaseCommitFenceSnapshot{
		WorkloadGeneration: 2, WorkloadVersion: 7, DesiredState: "DELETE_REQUESTED", WorkloadPhase: "RELEASING", LatestAllocationID: "allocation-a",
		AllocationGeneration: 1, AllocationVersion: 3, AllocationPhase: "RELEASING",
		CreateGeneration: 1, CreateVersion: 2, CreateKind: "CREATE", CreateStatus: "SUCCEEDED",
		CreateResourceID: "tenant-a/job-a", CreateResourceUID: "job-uid-a",
		DeleteGeneration: 2, DeleteVersion: 4, DeleteKind: "DELETE", DeleteStatus: "SUCCEEDED",
		DeleteTargetID: "tenant-a/job-a", DeleteTargetUID: "job-uid-a",
	}
	return expectation, observed
}

func TestReleaseCommitFenceRejectsInvalidInput(t *testing.T) {
	w, _ := goodReleaseCommitFence()
	for _, tc := range []struct {
		name string
		edit func(*ReleaseCommitFence)
	}{
		{"missing_tenant", func(x *ReleaseCommitFence) { x.TenantID = "" }},
		{"space_in_identity", func(x *ReleaseCommitFence) { x.AllocationID = " allocation-a" }},
		{"missing_create", func(x *ReleaseCommitFence) { x.CreateOperationID = "" }},
		{"missing_delete", func(x *ReleaseCommitFence) { x.DeleteOperationID = "" }},
		{"missing_uid", func(x *ReleaseCommitFence) { x.TargetResourceUID = "" }},
		{"equal_generations", func(x *ReleaseCommitFence) { x.DeleteGeneration = x.AllocationGeneration }},
		{"no_workload_version", func(x *ReleaseCommitFence) { x.WorkloadVersion = 0 }},
		{"no_allocation_version", func(x *ReleaseCommitFence) { x.AllocationVersion = 0 }},
		{"no_create_version", func(x *ReleaseCommitFence) { x.CreateOperationVersion = 0 }},
		{"no_delete_version", func(x *ReleaseCommitFence) { x.DeleteOperationVersion = 0 }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			p := w
			tc.edit(&p)
			if validReleaseCommitFence(p) {
				t.Fatal("accepted invalid release preflight input")
			}
		})
	}
}

func TestReleaseCommitFenceSnapshotIsNecessaryNotReleaseAuthority(t *testing.T) {
	w, s := goodReleaseCommitFence()
	if !s.matches(w) {
		t.Fatal("valid strict current-state snapshot rejected")
	}
	for _, tc := range []struct {
		name string
		edit func(*releaseCommitFenceSnapshot)
	}{
		{"newer_desired_generation", func(x *releaseCommitFenceSnapshot) { x.WorkloadGeneration++ }},
		{"workload_not_delete", func(x *releaseCommitFenceSnapshot) { x.DesiredState = "ACTIVE" }},
		{"workload_not_releasing", func(x *releaseCommitFenceSnapshot) { x.WorkloadPhase = "ALLOCATED" }},
		{"successor_allocation", func(x *releaseCommitFenceSnapshot) { x.LatestAllocationID = "successor" }},
		{"allocation_not_releasing", func(x *releaseCommitFenceSnapshot) { x.AllocationPhase = "BOUND" }},
		{"changed_allocation_generation", func(x *releaseCommitFenceSnapshot) { x.AllocationGeneration++ }},
		{"old_create_not_successful", func(x *releaseCommitFenceSnapshot) { x.CreateStatus = "UNKNOWN" }},
		{"old_create_different_uid", func(x *releaseCommitFenceSnapshot) { x.CreateResourceUID = "other-uid" }},
		{"old_create_different_id", func(x *releaseCommitFenceSnapshot) { x.CreateResourceID = "other-id" }},
		{"old_create_generation", func(x *releaseCommitFenceSnapshot) { x.CreateGeneration++ }},
		{"delete_not_successful", func(x *releaseCommitFenceSnapshot) { x.DeleteStatus = "UNKNOWN" }},
		{"delete_wrong_kind", func(x *releaseCommitFenceSnapshot) { x.DeleteKind = "CREATE" }},
		{"delete_wrong_uid", func(x *releaseCommitFenceSnapshot) { x.DeleteTargetUID = "successor-uid" }},
		{"delete_wrong_resource", func(x *releaseCommitFenceSnapshot) { x.DeleteTargetID = "other-resource" }},
		{"delete_wrong_generation", func(x *releaseCommitFenceSnapshot) { x.DeleteGeneration++ }},
		{"stale_workload_version", func(x *releaseCommitFenceSnapshot) { x.WorkloadVersion++ }},
		{"stale_allocation_version", func(x *releaseCommitFenceSnapshot) { x.AllocationVersion++ }},
		{"stale_create_version", func(x *releaseCommitFenceSnapshot) { x.CreateVersion++ }},
		{"stale_delete_version", func(x *releaseCommitFenceSnapshot) { x.DeleteVersion++ }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			bad := s
			tc.edit(&bad)
			if bad.matches(w) {
				t.Fatal("unsafe release candidate matched")
			}
		})
	}
}
