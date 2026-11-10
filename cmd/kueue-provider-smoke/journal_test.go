package main

import (
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
	} {
		phase, hold, err := parseJournalArgs(tc.args)
		if err != nil || phase != tc.phase || hold != tc.hold {
			t.Fatalf("args=%q phase=%q hold=%v err=%v", tc.args, phase, hold, err)
		}
	}
	for _, args := range [][]string{
		{"journal"}, {"journal", "create"}, {"journal", "create", "--hold", "extra"},
		{"journal", "recover", "--hold"}, {"journal", "delete"}, {"durable", "create"},
	} {
		if _, _, err := parseJournalArgs(args); err == nil {
			t.Fatalf("unsafe journal command accepted: %q", args)
		}
	}
	if err := runJournal([]string{"journal", "recover"}, func(string) string { return "" }, nil); err == nil {
		t.Fatal("journal recovery without disposable opt-in or binding DSN accepted")
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
}
