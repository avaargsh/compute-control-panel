package postgres

import (
	"context"
	"errors"
	"testing"

	"github.com/avaargsh/compute-control-panel/internal/provider"
)

func correlatedReleaseFixture(p ReleaseCommitFence) provider.ReleaseEvidenceCorrelation {
	return provider.ReleaseEvidenceCorrelation{
		TenantID: p.TenantID, WorkloadID: p.WorkloadID,
		AllocationID: p.AllocationID, ProviderID: p.ProviderID,
		DeleteOperationID: p.DeleteOperationID,
		Job: provider.KubernetesResourceRef{
			Namespace: "tenant-a", Name: "job-a",
			UID: p.TargetResourceUID, ResourceVersion: "job-rv-12",
		},
		Pod: provider.KubernetesResourceRef{
			Namespace: "tenant-a", Name: "pod-a",
			UID: "pod-uid-a", ResourceVersion: "pod-rv-34",
		},
		Claim: provider.KubernetesResourceRef{
			Namespace: "tenant-a", Name: "claim-a",
			UID: "claim-uid-a", ResourceVersion: "claim-rv-56",
		},
		Device: provider.DRAAllocatedDevice{
			Request: "gpu", Driver: "example.com", Pool: "pool-a", Device: "device-a",
		},
	}
}

func TestCorrelatedReleaseIdentityMustMatchDurableFence(t *testing.T) {
	p, _ := goodReleaseCommitFence()
	ok := correlatedReleaseFixture(p)
	if !correlatedReleaseMatchesFence(p, ok) {
		t.Fatal("matching structural release identities rejected")
	}
	tests := []struct {
		name string
		edit func(*provider.ReleaseEvidenceCorrelation)
	}{
		{"wrong tenant", func(c *provider.ReleaseEvidenceCorrelation) { c.TenantID = "tenant-b" }},
		{"wrong workload", func(c *provider.ReleaseEvidenceCorrelation) { c.WorkloadID = "other" }},
		{"wrong allocation", func(c *provider.ReleaseEvidenceCorrelation) { c.AllocationID = "other" }},
		{"wrong provider", func(c *provider.ReleaseEvidenceCorrelation) { c.ProviderID = "other" }},
		{"wrong DELETE", func(c *provider.ReleaseEvidenceCorrelation) { c.DeleteOperationID = "other" }},
		{"successor Job UID", func(c *provider.ReleaseEvidenceCorrelation) { c.Job.UID = "new-job-uid" }},
		{"successor Job name", func(c *provider.ReleaseEvidenceCorrelation) { c.Job.Name = "successor" }},
		{"successor Job namespace", func(c *provider.ReleaseEvidenceCorrelation) { c.Job.Namespace = "tenant-b" }},
		{"missing Job resourceVersion", func(c *provider.ReleaseEvidenceCorrelation) { c.Job.ResourceVersion = "" }},
		{"missing Pod UID", func(c *provider.ReleaseEvidenceCorrelation) { c.Pod.UID = "" }},
		{"Pod in other namespace", func(c *provider.ReleaseEvidenceCorrelation) { c.Pod.Namespace = "tenant-b" }},
		{"missing Pod resourceVersion", func(c *provider.ReleaseEvidenceCorrelation) { c.Pod.ResourceVersion = "" }},
		{"missing Claim UID", func(c *provider.ReleaseEvidenceCorrelation) { c.Claim.UID = "" }},
		{"Claim in other namespace", func(c *provider.ReleaseEvidenceCorrelation) { c.Claim.Namespace = "tenant-b" }},
		{"missing Claim resourceVersion", func(c *provider.ReleaseEvidenceCorrelation) { c.Claim.ResourceVersion = "" }},
		{"missing device driver", func(c *provider.ReleaseEvidenceCorrelation) { c.Device.Driver = "" }},
		{"missing device pool", func(c *provider.ReleaseEvidenceCorrelation) { c.Device.Pool = "" }},
		{"missing device", func(c *provider.ReleaseEvidenceCorrelation) { c.Device.Device = "" }},
		{"missing device request", func(c *provider.ReleaseEvidenceCorrelation) { c.Device.Request = "" }},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			bad := ok
			tc.edit(&bad)
			if correlatedReleaseMatchesFence(p, bad) {
				t.Fatalf("accepted identity splice: %s", tc.name)
			}
		})
	}
}

func TestCorrelatedReleasePreflightNeedsCallerTransaction(t *testing.T) {
	p, _ := goodReleaseCommitFence()
	ok := correlatedReleaseFixture(p)
	if err := ValidateCorrelatedReleasePreflight(context.Background(), nil, p, ok); !errors.Is(err, ErrReleaseCommitFence) {
		t.Fatalf("missing durable transaction was accepted: %v", err)
	}
	if err := ValidateCorrelatedReleasePreflight(nil, nil, p, ok); !errors.Is(err, ErrReleaseCommitFence) {
		t.Fatalf("missing context was accepted: %v", err)
	}
}
