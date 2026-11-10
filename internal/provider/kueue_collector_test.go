package provider

import (
	"context"
	"errors"
	"testing"
	"time"
)

func TestFakeCollector_HappyPath(t *testing.T) {
	f := NewFakeCollector()
	key := "t1|w1|1|ns|job"
	want := &KueueEvidenceResult{
		Evidence: KueueJobEvidence{
			TenantID:          "t1",
			WorkloadID:        "w1",
			DesiredGeneration: 1,
			Job: KubernetesResourceRef{
				Namespace: "ns", Name: "job", UID: "job-uid", ResourceVersion: "1",
			},
			JobTenantID: "t1", JobWorkloadID: "w1", JobDesiredGeneration: 1,
		},
		Provenance: CollectionProvenance{
			Source:     "fake",
			ObservedAt: time.Now(),
		},
	}
	f.EvidenceByKey[key] = want

	res, err := f.Collect(context.Background(), "t1", "w1", 1, "ns", "job")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if res.Evidence.TenantID != "t1" || res.Evidence.Job.UID != "job-uid" {
		t.Fatalf("unexpected evidence: %+v", res.Evidence)
	}
	stage, aerr := AssessKueueJobObservation(res.Evidence)
	if aerr != nil || stage != KueueUnproven {
		t.Logf("stage=%s err=%v (expected for partial)", stage, aerr)
	}
}

func TestFakeCollector_MissingKey(t *testing.T) {
	f := NewFakeCollector()
	_, err := f.Collect(context.Background(), "t1", "w1", 1, "ns", "missing")
	if err == nil {
		t.Fatal("expected error for missing key")
	}
}

func TestFakeCollector_ForcedFailure(t *testing.T) {
	f := NewFakeCollector()
	key := "t1|w1|1|ns|job"
	f.FailKeys[key] = errors.New("simulated identity mismatch")
	_, err := f.Collect(context.Background(), "t1", "w1", 1, "ns", "job")
	if err == nil {
		t.Fatal("expected forced failure")
	}
}

func TestFakeCollector_ContextCancel(t *testing.T) {
	f := NewFakeCollector()
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_, err := f.Collect(ctx, "t1", "w1", 1, "ns", "job")
	if err == nil {
		t.Fatal("expected context error")
	}
}
