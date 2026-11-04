package provider

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/avaargsh/compute-control-panel/internal/operation"
)

func fakeRequest() Request {
	return Request{
		OperationID: "op-fake-1", TenantID: "tenant-1", WorkloadID: "workload-1",
		ProviderID: "fake", IdempotencyKey: "idempotency-1",
		RequestDigest:     "sha256:" + strings.Repeat("a", 64),
		DesiredGeneration: 1, Kind: operation.Create,
	}
}

func TestFakeProviderLostACKInspectableAndIdempotent(t *testing.T) {
	fake := NewFakeProvider()
	ctx := context.Background()
	req := fakeRequest()
	fake.LoseNextAcknowledgement()
	if _, err := fake.Apply(ctx, req); !errors.Is(err, ErrAcknowledgementLost) {
		t.Fatalf("expected simulated lost acknowledgement: %v", err)
	}
	observed, err := fake.Inspect(ctx, req)
	if err != nil || observed.Result != operation.ResourcePresent ||
		observed.OperationID != req.OperationID || observed.ResourceOperationID != req.OperationID ||
		observed.ResourceUID == "" || observed.ResourceGeneration != req.DesiredGeneration ||
		observed.ResourceVersion == "" || observed.ObservedAt.IsZero() {
		t.Fatalf("lost-ACK operation must be inspectable: %+v err=%v", observed, err)
	}
	receipt, err := fake.Apply(ctx, req)
	if err != nil || receipt.OperationID != req.OperationID || receipt.RequestDigest != req.RequestDigest {
		t.Fatalf("identical replay was not idempotent: %+v %v", receipt, err)
	}
	again, err := fake.Inspect(ctx, req)
	if err != nil || again.ResourceUID != observed.ResourceUID {
		t.Fatalf("replay changed resource UID: %+v %v", again, err)
	}
	conflicting := req
	conflicting.RequestDigest = "sha256:" + strings.Repeat("b", 64)
	if _, err := fake.Apply(ctx, conflicting); !errors.Is(err, ErrOperationConflict) {
		t.Fatalf("digest conflict silently reused operation: %v", err)
	}
}

func TestFakeProviderNeverInfersNoEffectFromMissingResource(t *testing.T) {
	fake := NewFakeProvider()
	req := fakeRequest()
	evidence, err := fake.Inspect(context.Background(), req)
	if err != nil || evidence.Result == operation.OperationNotAppliedFinal {
		t.Fatalf("missing record cannot prove no-effect: %+v %v", evidence, err)
	}
	if err := fake.DeclareNotAppliedFinal(req); err != nil {
		t.Fatal(err)
	}
	final, err := fake.Inspect(context.Background(), req)
	if err != nil || final.Result != operation.OperationNotAppliedFinal ||
		final.ResourceVersion == "" || final.ObservedAt.IsZero() {
		t.Fatalf("explicit final no-effect evidence unavailable: %+v %v", final, err)
	}
	// The authoritative NOT_APPLIED_FINAL is what permits a retry with
	// the very same operation ID, idempotency key, and digest.
	if _, err := fake.Apply(context.Background(), req); err != nil {
		t.Fatalf("same-key retry after final no-effect proof failed: %v", err)
	}
	applied, err := fake.Inspect(context.Background(), req)
	if err != nil || applied.Result != operation.ResourcePresent ||
		applied.ResourceOperationID != req.OperationID {
		t.Fatalf("safe retry was not attributable to original operation: %+v %v", applied, err)
	}
}

func TestFakeProviderObservationDrivesExistingDomainReconciliation(t *testing.T) {
	ctx := context.Background()
	req := fakeRequest()
	fake := NewFakeProvider()
	now := time.Now()
	op := operation.Operation{
		ID: req.OperationID, TenantID: req.TenantID, WorkloadID: req.WorkloadID,
		ProviderID: req.ProviderID, IdempotencyKey: req.IdempotencyKey,
		RequestDigest: req.RequestDigest, Generation: req.DesiredGeneration,
		Version: 1, Kind: operation.Create, Status: operation.Pending,
	}
	claimed, err := op.Claim(1, "worker-1", now, time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	inFlight, err := claimed.Start(claimed.Version, "worker-1", claimed.FenceToken, now.Add(time.Second))
	if err != nil {
		t.Fatal(err)
	}
	fake.LoseNextAcknowledgement()
	_, _ = fake.Apply(ctx, req)
	unknown, err := inFlight.MarkUnknown(inFlight.Version, "worker-1", inFlight.FenceToken, now.Add(2*time.Second))
	if err != nil {
		t.Fatal(err)
	}
	reconciling, err := unknown.BeginReconciliation(unknown.Version, "", 0, "worker-2", now.Add(3*time.Second), time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	evidence, err := fake.Inspect(ctx, req)
	if err != nil {
		t.Fatal(err)
	}
	verified, err := reconciling.ApplyObservation(reconciling.Version, "worker-2", reconciling.FenceToken,
		time.Now().Add(10*time.Second), req.DesiredGeneration, evidence)
	if err != nil || verified.Status != operation.Succeeded || verified.ProviderResourceUID == "" {
		t.Fatalf("recovered provider result: %+v %v", verified, err)
	}
}
