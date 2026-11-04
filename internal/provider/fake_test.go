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

func TestFakeProviderTargetBoundUpdateDeleteAndLostReleaseACK(t *testing.T) {
	ctx := context.Background()
	fake := NewFakeProvider()
	create := fakeRequest()
	if _, err := fake.Apply(ctx, create); err != nil {
		t.Fatal(err)
	}
	created, err := fake.Inspect(ctx, create)
	if err != nil || created.Result != operation.ResourcePresent {
		t.Fatalf("CREATE inspection: %+v %v", created, err)
	}

	update := create
	update.OperationID = "op-fake-update"
	update.IdempotencyKey = "idempotency-update"
	update.DesiredGeneration = 2
	update.Kind = operation.Update
	update.TargetResourceID = created.ResourceID
	update.TargetResourceUID = created.ResourceUID
	if _, err := fake.Apply(ctx, update); err != nil {
		t.Fatalf("target-bound UPDATE: %v", err)
	}
	updated, err := fake.Inspect(ctx, update)
	if err != nil || updated.Result != operation.ResourcePresent ||
		updated.ResourceID != created.ResourceID || updated.ResourceUID != created.ResourceUID ||
		updated.ResourceOperationID != update.OperationID ||
		updated.ResourceGeneration != update.DesiredGeneration {
		t.Fatalf("UPDATE lost exact target identity: %+v %v", updated, err)
	}

	remove := update
	remove.OperationID = "op-fake-delete"
	remove.IdempotencyKey = "idempotency-delete"
	remove.DesiredGeneration = 3
	remove.Kind = operation.Delete
	fake.LoseNextAcknowledgement()
	if _, err := fake.Apply(ctx, remove); !errors.Is(err, ErrAcknowledgementLost) {
		t.Fatalf("DELETE lost ACK must be ambiguous: %v", err)
	}
	released, err := fake.Inspect(ctx, remove)
	if err != nil || released.Result != operation.TargetAbsent ||
		released.TargetResourceID != created.ResourceID ||
		released.TargetResourceUID != created.ResourceUID ||
		released.ResourceID != "" || released.ResourceUID != "" {
		t.Fatalf("exact deleted UID must be inspectable: %+v %v", released, err)
	}
	if receipt, err := fake.Apply(ctx, remove); err != nil || receipt.OperationID != remove.OperationID {
		t.Fatalf("same operation DELETE replay is not idempotent: %+v %v", receipt, err)
	}
	// Historical CREATE must not falsely present a released resource as live.
	old, err := fake.Inspect(ctx, create)
	if err != nil || old.Result == operation.ResourcePresent ||
		old.Result == operation.OperationNotAppliedFinal {
		t.Fatalf("stale CREATE inspection reported false live resource: %+v %v", old, err)
	}
}

func TestFakeProviderCannotDeleteForeignStaleOrAlreadyReclaimedUID(t *testing.T) {
	ctx := context.Background()
	fake := NewFakeProvider()
	create := fakeRequest()
	if _, err := fake.Apply(ctx, create); err != nil {
		t.Fatal(err)
	}
	created, err := fake.Inspect(ctx, create)
	if err != nil {
		t.Fatal(err)
	}
	remove := create
	remove.Kind = operation.Delete
	remove.OperationID = "op-delete"
	remove.IdempotencyKey = "delete-key"
	remove.TargetResourceID = created.ResourceID
	remove.TargetResourceUID = created.ResourceUID

	for name, mutate := range map[string]func(*Request){
		"stale uid":        func(r *Request) { r.TargetResourceUID = "uid-replaced" },
		"foreign tenant":   func(r *Request) { r.TenantID = "tenant-other" },
		"foreign workload": func(r *Request) { r.WorkloadID = "workload-other" },
		"foreign provider": func(r *Request) { r.ProviderID = "provider-other" },
	} {
		t.Run(name, func(t *testing.T) {
			bad := remove
			mutate(&bad)
			if _, err := fake.Apply(ctx, bad); !errors.Is(err, ErrOperationConflict) {
				t.Fatalf("unauthorized target mutation accepted: %v", err)
			}
		})
	}
	if _, err := fake.Apply(ctx, remove); err != nil {
		t.Fatalf("valid delete: %v", err)
	}
	second := remove
	second.OperationID = "op-other-delete"
	second.IdempotencyKey = "another-delete-key"
	if _, err := fake.Apply(ctx, second); !errors.Is(err, ErrOperationConflict) {
		t.Fatalf("new DELETE operation cannot infer release from missing target: %v", err)
	}
	if obs, err := fake.Inspect(ctx, second); err != nil ||
		obs.Result == operation.TargetAbsent || obs.Result == operation.OperationNotAppliedFinal {
		t.Fatalf("unrecorded delete inferred finality: %+v %v", obs, err)
	}
}

func TestFakeProviderRejectsStaleGenerationMutation(t *testing.T) {
	ctx := context.Background()
	fake := NewFakeProvider()
	create := fakeRequest()
	if _, err := fake.Apply(ctx, create); err != nil {
		t.Fatal(err)
	}
	created, err := fake.Inspect(ctx, create)
	if err != nil {
		t.Fatal(err)
	}

	current := create
	current.OperationID = "op-update-generation-3"
	current.IdempotencyKey = "update-gen-3"
	current.DesiredGeneration = 3
	current.Kind = operation.Update
	current.TargetResourceID = created.ResourceID
	current.TargetResourceUID = created.ResourceUID
	if _, err := fake.Apply(ctx, current); err != nil {
		t.Fatalf("new generation UPDATE rejected: %v", err)
	}

	for _, kind := range []operation.Kind{operation.Update, operation.Delete} {
		t.Run(string(kind), func(t *testing.T) {
			stale := current
			stale.OperationID = "op-stale-" + string(kind)
			stale.IdempotencyKey = "stale-" + string(kind)
			stale.DesiredGeneration = 2
			stale.Kind = kind
			if _, err := fake.Apply(ctx, stale); !errors.Is(err, ErrOperationConflict) {
				t.Fatalf("stale %s mutated newer generation: %v", kind, err)
			}
		})
	}
	stillLive, err := fake.Inspect(ctx, current)
	if err != nil || stillLive.Result != operation.ResourcePresent ||
		stillLive.ResourceUID != created.ResourceUID {
		t.Fatalf("stale mutation removed or obscured live resource: %+v %v", stillLive, err)
	}

	release := current
	release.OperationID = "op-delete-generation-4"
	release.IdempotencyKey = "delete-gen-4"
	release.DesiredGeneration = 4
	release.Kind = operation.Delete
	if _, err := fake.Apply(ctx, release); err != nil {
		t.Fatalf("current generation release failed: %v", err)
	}
	obs, err := fake.Inspect(ctx, release)
	if err != nil || obs.Result != operation.TargetAbsent ||
		obs.TargetResourceUID != created.ResourceUID {
		t.Fatalf("current generation release not inspectable: %+v %v", obs, err)
	}
}
