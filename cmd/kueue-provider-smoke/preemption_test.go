package main

import (
	"errors"
	"io"
	"testing"

	"github.com/avaargsh/compute-control-panel/internal/operation"
	"github.com/avaargsh/compute-control-panel/internal/provider"
)

type providerRequestIdentity struct {
	id, key, digest string
}

func identityOf(req provider.Request) providerRequestIdentity {
	return providerRequestIdentity{req.OperationID, req.IdempotencyKey, req.RequestDigest}
}

func (a providerRequestIdentity) same(b providerRequestIdentity) bool {
	return a.id == b.id || a.key == b.key || a.digest == b.digest
}

func TestPreemptionRequestsStayDistinct(t *testing.T) {
	low := preemptLowRequest()
	high := preemptHighRequest()
	if err := low.Validate(); err != nil {
		t.Fatalf("invalid low create: %v", err)
	}
	if err := high.Validate(); err != nil {
		t.Fatalf("invalid high create: %v", err)
	}
	others := []providerRequestIdentity{
		identityOf(admissionCreateRequest()),
		identityOf(durableCreateRequest()),
		identityOf(journalCreateRequest()),
		identityOf(lifecycleCreateRequest()),
	}
	mine := []providerRequestIdentity{
		identityOf(low), identityOf(high),
		identityOf(preemptLowDelete(smokeNamespace+"/ccp-example", "uid-low")),
		identityOf(preemptHighDelete(smokeNamespace+"/ccp-example", "uid-high")),
		identityOf(preemptLowSuccessorRequest(smokeNamespace+"/ccp-example", "uid-low")),
		identityOf(preemptHighSuccessorRequest(smokeNamespace+"/ccp-example", "uid-high")),
	}
	for i, left := range mine {
		for _, right := range mine[i+1:] {
			if left.same(right) {
				t.Fatalf("preemption requests collided: %+v %+v", left, right)
			}
		}
		for _, right := range others {
			if left.same(right) {
				t.Fatalf("preemption request collided with another smoke job: %+v %+v", left, right)
			}
		}
	}
	if low.Kind != operation.Create || high.ProviderID != preemptProviderID || low.WorkloadID == high.WorkloadID {
		t.Fatalf("low and high requests were not distinct queued creates: %+v %+v", low, high)
	}
	remove := preemptLowDelete(smokeNamespace+"/ccp-example", "uid-low")
	if err := remove.Validate(); err != nil || remove.Kind != operation.Delete || remove.TargetResourceUID != "uid-low" ||
		remove.OperationID == low.OperationID || remove.WorkloadID != low.WorkloadID {
		t.Fatalf("unexpected low delete: %+v err=%v", remove, err)
	}
	if len(preemptCapability) != len("sha256:")+64 {
		t.Fatalf("capability digest length %d", len(preemptCapability))
	}
}

func TestPreemptionPhaseArgs(t *testing.T) {
	phase, hold, err := parsePreemptionArgs([]string{"preemption", "submit-low"})
	if err != nil || phase != "submit-low" || hold {
		t.Fatalf("submit-low: %s hold=%t err=%v", phase, hold, err)
	}
	phase, hold, err = parsePreemptionArgs([]string{"preemption", "delete", "--hold"})
	if err != nil || phase != "delete" || !hold {
		t.Fatalf("delete: %s hold=%t err=%v", phase, hold, err)
	}
	phase, hold, err = parsePreemptionArgs([]string{"preemption", "recover-successor"})
	if err != nil || phase != "recover-successor" || hold {
		t.Fatalf("recover-successor: %s hold=%t err=%v", phase, hold, err)
	}
	for _, args := range [][]string{
		{"preemption"},
		{"preemption", "create"},
		{"admission", "create"},
		{"preemption", "delete"},
		{"preemption", "submit-low", "--hold"},
	} {
		if _, _, err := parsePreemptionArgs(args); err == nil {
			t.Fatalf("accepted preemption args %q", args)
		}
	}
}

func TestPreemptionRequiresDisposableOptIn(t *testing.T) {
	err := runPreemption([]string{"preemption", "submit-low"}, func(string) string { return "" }, io.Discard)
	if err == nil || errors.Is(err, errEmptyBindingDSN) {
		t.Fatalf("missing opt-in err=%v", err)
	}
}
