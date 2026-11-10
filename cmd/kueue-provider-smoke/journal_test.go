package main

import (
	"bytes"
	"encoding/json"
	"errors"
	"testing"

	"github.com/avaargsh/compute-control-panel/internal/operation"
)

func TestJournalSmokePhasesAreExplicitAndDisposableOnly(t *testing.T) {
	for _, tc := range []struct {
		args  []string
		phase string
		hold  bool
	}{
		{[]string{"journal", "create", "--hold"}, "create", true},
		{[]string{"journal", "recover"}, "recover", false},
		{[]string{"journal", "delete", "--hold"}, "delete", true},
		{[]string{"journal", "recover-delete"}, "recover-delete", false},
		{[]string{"journal", "successor", "--hold"}, "successor", true},
		{[]string{"journal", "recover-successor"}, "recover-successor", false},
		{[]string{"journal", "fail-closed"}, "fail-closed", false},
	} {
		phase, hold, err := parseJournalArgs(tc.args)
		if err != nil || phase != tc.phase || hold != tc.hold {
			t.Fatalf("args=%q phase=%q hold=%v err=%v", tc.args, phase, hold, err)
		}
	}
	for _, args := range [][]string{
		{"journal"}, {"journal", "create"}, {"journal", "create", "--hold", "extra"},
		{"journal", "recover", "--hold"}, {"journal", "delete"}, {"journal", "delete", "--hold", "extra"},
		{"journal", "recover-delete", "--hold"}, {"journal", "successor"},
		{"journal", "recover-successor", "--hold"}, {"journal", "fail-closed", "delete"},
		{"durable", "create"},
	} {
		if _, _, err := parseJournalArgs(args); err == nil {
			t.Fatalf("unsafe journal command accepted: %q", args)
		}
	}
	if err := runJournal([]string{"journal", "recover"}, func(string) string { return "" }, nil); err == nil {
		t.Fatal("journal recovery without disposable opt-in or binding DSN accepted")
	}
	if err := runJournal([]string{"journal", "recover-delete"}, func(string) string { return "" }, nil); err == nil {
		t.Fatal("journal DELETE recovery without disposable opt-in accepted")
	}
}

func TestJournalDeleteFailClosedStopsBeforeAdapterWhenDSNEmpty(t *testing.T) {
	var out bytes.Buffer
	err := runJournal([]string{"journal", "fail-closed"}, func(k string) string {
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
	var got journalFailClosedReport
	if err := json.Unmarshal(out.Bytes(), &got); err != nil {
		t.Fatalf("report: %v", err)
	}
	if got.Phase != "journal-fail-closed" || !got.FailedClosed || got.Reason != "empty_dsn" ||
		got.DeleteApplied || got.ReleaseAttested {
		t.Fatalf("empty DSN report: %+v", got)
	}
}

func TestJournalSmokeUsesSeparateImmutableKueueOperation(t *testing.T) {
	req := journalCreateRequest()
	if err := req.Validate(); err != nil {
		t.Fatal(err)
	}
	if req.Kind != operation.Create || req.DesiredGeneration != 1 ||
		req.OperationID == durableCreateRequest().OperationID ||
		req.WorkloadID == durableCreateRequest().WorkloadID ||
		req.IdempotencyKey == durableCreateRequest().IdempotencyKey {
		t.Fatalf("journal fixture must not alias the separate binding smoke: %+v", req)
	}
	remove := journalDeleteRequest(smokeNamespace+"/ccp-example", "uid-original")
	if err := remove.Validate(); err != nil {
		t.Fatal(err)
	}
	successor := journalSuccessorRequest(remove.TargetResourceID, remove.TargetResourceUID)
	if err := successor.Validate(); err != nil {
		t.Fatal(err)
	}
	if remove.Kind != operation.Delete || successor.Kind != operation.Delete ||
		remove.OperationID == req.OperationID || successor.OperationID == remove.OperationID ||
		remove.OperationID == durableDeleteRequest(remove.TargetResourceID, remove.TargetResourceUID).OperationID ||
		remove.IdempotencyKey == req.IdempotencyKey || successor.IdempotencyKey == remove.IdempotencyKey ||
		remove.RequestDigest == req.RequestDigest || successor.RequestDigest == remove.RequestDigest ||
		remove.TenantID != req.TenantID || remove.WorkloadID != req.WorkloadID ||
		remove.ProviderID != req.ProviderID || remove.DesiredGeneration != req.DesiredGeneration ||
		remove.TargetResourceUID != "uid-original" || successor.TargetResourceUID != remove.TargetResourceUID {
		t.Fatalf("journal DELETE fixture alias or identity drift: delete=%+v successor=%+v", remove, successor)
	}
}
