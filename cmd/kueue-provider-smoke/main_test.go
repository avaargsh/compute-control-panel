package main

import (
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
