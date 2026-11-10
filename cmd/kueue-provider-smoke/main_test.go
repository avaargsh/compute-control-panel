package main

import (
	"context"
	"testing"

	"github.com/avaargsh/compute-control-panel/internal/operation"
)

func TestDisposableLifecycleSmokeRequestIsFixedAndValid(t *testing.T) {
	req := lifecycleCreateRequest()
	if err := req.Validate(); err != nil {
		t.Fatalf("invalid lifecycle request: %v", err)
	}
	if req.Kind != operation.Create ||
		req.TenantID != "tenant-a" || req.WorkloadID != "workload-a" ||
		req.ProviderID != "kueue" || req.DesiredGeneration != 7 ||
		req.OperationID == "" || req.IdempotencyKey == "" {
		t.Fatalf("unexpected lifecycle identity: %+v", req)
	}
}

func TestDisposableLifecycleGuardRequiresExactOptIn(t *testing.T) {
	for _, env := range []map[string]string{
		{},
		{"CCP_DISPOSABLE_KIND_SMOKE": "true"},
		{"CCP_DISPOSABLE_KIND_SMOKE": "0"},
	} {
		if disposableAllowed(func(k string) string { return env[k] }) {
			t.Fatalf("accepted unsafe smoke environment: %+v", env)
		}
	}
	if !disposableAllowed(func(k string) string {
		if k == "CCP_DISPOSABLE_KIND_SMOKE" {
			return "1"
		}
		return ""
	}) {
		t.Fatal("explicit disposable test opt-in rejected")
	}
}

func TestStableJobObservationRetriesOnlyUnprovenSnapshot(t *testing.T) {
	calls := 0
	got, err := stableJobObservation(context.Background(), func(context.Context) (operation.Observation, error) {
		calls++
		if calls == 1 {
			return operation.Observation{
				Result: operation.ObservationConflict, ReasonCode: "KUEUE_OBSERVATION_UNPROVEN",
			}, nil
		}
		return operation.Observation{Result: operation.ResourcePresent, ResourceUID: "trusted-uid"}, nil
	})
	if err != nil || got.Result != operation.ResourcePresent || got.ResourceUID != "trusted-uid" || calls != 2 {
		t.Fatalf("snapshot recovery: %+v calls=%d err=%v", got, calls, err)
	}

	calls = 0
	got, err = stableJobObservation(context.Background(), func(context.Context) (operation.Observation, error) {
		calls++
		return operation.Observation{Result: operation.ObservationConflict, ReasonCode: "JOB_UID_REPLACED"}, nil
	})
	if err != nil || calls != 1 || got.ReasonCode != "JOB_UID_REPLACED" {
		t.Fatalf("unsafe UID conflict was retried or swallowed: %+v calls=%d err=%v", got, calls, err)
	}
}
