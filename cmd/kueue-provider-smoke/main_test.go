package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
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

func TestDurableRequestsStayDistinctFromMemorySmoke(t *testing.T) {
	create := durableCreateRequest()
	if err := create.Validate(); err != nil {
		t.Fatalf("invalid durable create: %v", err)
	}
	if create.OperationID == lifecycleCreateRequest().OperationID || create.Kind != operation.Create {
		t.Fatalf("durable create collided with the memory smoke: %+v", create)
	}
	remove := durableDeleteRequest(smokeNamespace+"/ccp-example", "uid-1")
	if err := remove.Validate(); err != nil {
		t.Fatalf("invalid durable delete: %v", err)
	}
	if remove.OperationID == create.OperationID || remove.TargetResourceUID != "uid-1" || remove.Kind != operation.Delete {
		t.Fatalf("unexpected durable delete: %+v", remove)
	}
}

func TestDurablePhaseArgs(t *testing.T) {
	phase, subject, hold, err := parseDurableArgs([]string{"durable", "create", "--hold"})
	if err != nil || phase != "create" || subject != "" || !hold {
		t.Fatalf("create hold: phase=%s subject=%s hold=%t err=%v", phase, subject, hold, err)
	}
	phase, subject, hold, err = parseDurableArgs([]string{"durable", "inspect", "delete"})
	if err != nil || phase != "inspect" || subject != "delete" || hold {
		t.Fatalf("inspect delete: phase=%s subject=%s hold=%t err=%v", phase, subject, hold, err)
	}
	phase, subject, hold, err = parseDurableArgs([]string{"durable", "replay", "create"})
	if err != nil || phase != "replay" || subject != "create" || hold {
		t.Fatalf("replay create: phase=%s subject=%s hold=%t err=%v", phase, subject, hold, err)
	}
	if _, _, _, err := parseDurableArgs([]string{"durable", "fail-closed"}); err != nil {
		t.Fatalf("fail-closed: %v", err)
	}
	for _, args := range [][]string{
		{"memory"},
		{"durable"},
		{"durable", "inspect"},
		{"durable", "create", "--hold", "extra"},
		{"durable", "replay", "update"},
	} {
		if _, _, _, err := parseDurableArgs(args); err == nil {
			t.Fatalf("accepted durable args %q", args)
		}
	}
}

func TestEmptyBindingDSNFailsClosedWithoutAPI(t *testing.T) {
	var out bytes.Buffer
	err := runDurable([]string{"durable", "fail-closed"}, func(k string) string {
		switch k {
		case "CCP_DISPOSABLE_KIND_SMOKE":
			return "1"
		case "POD_NAMESPACE":
			return smokeNamespace
		default:
			return ""
		}
	}, &out)
	if !errors.Is(err, errEmptyBindingDSN) {
		t.Fatalf("empty DSN err=%v", err)
	}
	var got failClosedReport
	if err := json.Unmarshal(out.Bytes(), &got); err != nil {
		t.Fatalf("report: %v", err)
	}
	if !got.FailedClosed || got.Reason != "empty_dsn" || got.CreateApplied || got.DeleteApplied || got.InspectPresent {
		t.Fatalf("empty DSN report: %+v", got)
	}
}

func TestAdmissionRequestsStayDistinctFromOtherSmokeJobs(t *testing.T) {
	create := admissionCreateRequest()
	if err := create.Validate(); err != nil {
		t.Fatalf("invalid admission create: %v", err)
	}
	if create.Kind != operation.Create || create.OperationID == "" ||
		create.OperationID == lifecycleCreateRequest().OperationID ||
		create.OperationID == durableCreateRequest().OperationID ||
		create.IdempotencyKey == lifecycleCreateRequest().IdempotencyKey ||
		create.IdempotencyKey == durableCreateRequest().IdempotencyKey ||
		create.RequestDigest == lifecycleCreateRequest().RequestDigest ||
		create.RequestDigest == durableCreateRequest().RequestDigest {
		t.Fatalf("admission create collided with another smoke job: %+v", create)
	}
	remove := admissionDeleteRequest(smokeNamespace+"/ccp-example", "uid-admission")
	if err := remove.Validate(); err != nil {
		t.Fatalf("invalid admission delete: %v", err)
	}
	if remove.Kind != operation.Delete || remove.OperationID == create.OperationID ||
		remove.TargetResourceUID != "uid-admission" || remove.TargetResourceID == "" {
		t.Fatalf("unexpected admission delete: %+v", remove)
	}
}

func TestAdmissionPhaseArgs(t *testing.T) {
	phase, subject, err := parseAdmissionArgs([]string{"admission", "create"})
	if err != nil || phase != "create" || subject != "" {
		t.Fatalf("create: phase=%s subject=%s err=%v", phase, subject, err)
	}
	phase, subject, err = parseAdmissionArgs([]string{"admission", "inspect", "delete"})
	if err != nil || phase != "inspect" || subject != "delete" {
		t.Fatalf("inspect delete: phase=%s subject=%s err=%v", phase, subject, err)
	}
	phase, subject, err = parseAdmissionArgs([]string{"admission", "inspect", "create"})
	if err != nil || phase != "inspect" || subject != "create" {
		t.Fatalf("inspect create: phase=%s subject=%s err=%v", phase, subject, err)
	}
	for _, args := range [][]string{
		{"admission"},
		{"admission", "replay"},
		{"admission", "inspect"},
		{"admission", "inspect", "update"},
		{"admission", "create", "--hold"},
		{"durable", "create"},
	} {
		if _, _, err := parseAdmissionArgs(args); err == nil {
			t.Fatalf("accepted admission args %q", args)
		}
	}
}

func TestDurablePhasesRejectMissingOptIn(t *testing.T) {
	err := runDurable([]string{"durable", "create"}, func(string) string { return "" }, io.Discard)
	if err == nil || errors.Is(err, errEmptyBindingDSN) {
		t.Fatalf("missing opt-in err=%v", err)
	}
}
