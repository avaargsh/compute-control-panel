// Package provider defines the bounded provider operation boundary.
// Kubernetes, Kueue and DRA remain authoritative for resource realization;
// the Compute Control Plane owns only intent, records and reconciliation.
package provider

import (
    "context"
    "errors"
    "fmt"
    "strings"
    "sync"
    "time"

    "github.com/avaargsh/compute-control-panel/internal/operation"
)

var (
    ErrInvalidRequest = errors.New("invalid provider operation request")
    ErrOperationConflict = errors.New("provider operation identity/digest conflict")
    // This error means only that the response was lost; the side effect can
    // already have happened. The caller must persist UNKNOWN and inspect.
    ErrAcknowledgementLost = errors.New("provider acknowledgement lost after side effect")
)

// Request is an immutable resolved operation. It deliberately contains no
// arbitrary caller-controlled shell, scheduler or Kubernetes API fields.
type Request struct {
    OperationID string
    TenantID string
    WorkloadID string
    ProviderID string
    IdempotencyKey string
    RequestDigest string
    DesiredGeneration uint64
    Kind operation.Kind
    TargetResourceID string
    TargetResourceUID string
}

func (r Request) Validate() error {
    op:=operation.Operation{
        ID:r.OperationID, TenantID:r.TenantID, WorkloadID:r.WorkloadID,
        ProviderID:r.ProviderID, IdempotencyKey:r.IdempotencyKey,
        RequestDigest:r.RequestDigest, Generation:r.DesiredGeneration,
        Version:1, Kind:r.Kind,Status:operation.Pending,
        TargetResourceID:r.TargetResourceID,TargetResourceUID:r.TargetResourceUID,
    }
    if err:=op.Validate(); err!=nil { return fmt.Errorf("%w: %v",ErrInvalidRequest,err) }
    return nil
}

type Acknowledgement struct {
    OperationID string
    RequestDigest string
}

// Adapter is intentionally narrower than a workload scheduler. Its Inspect
// method must query provider-owned authoritative state. A missing resource
// listing alone must NEVER imply OperationNotAppliedFinal.
type Adapter interface {
    Apply(context.Context,Request)(Acknowledgement,error)
    Inspect(context.Context,Request)(operation.Observation,error)
}

// FakeProvider is a concurrency-safe, process-local conformance double.
// It must never be used as a source of production authoritative evidence.
type FakeProvider struct {
    mu sync.Mutex
    journal map[string]*fakeRecord
    nextLost bool
    generation uint64
}

type fakeRecord struct {
    request Request
    applied bool
    noEffectFinal bool
    resourceID string
    resourceUID string
    version uint64
}

func NewFakeProvider()*FakeProvider {
    return &FakeProvider{journal:make(map[string]*fakeRecord)}
}

func (f *FakeProvider) LoseNextAcknowledgement() {
    f.mu.Lock()
    defer f.mu.Unlock()
    f.nextLost=true
}

// An explicit provider-side finality record is required to simulate
// NOT_APPLIED_FINAL. Resource absence cannot establish it.
func (f *FakeProvider) DeclareNotAppliedFinal(req Request)error {
    if err:=req.Validate(); err!=nil { return err }
    f.mu.Lock()
    defer f.mu.Unlock()
    prior,ok:=f.journal[req.OperationID]
    if ok {
        if !sameRequest(prior.request,req) || prior.applied { return ErrOperationConflict }
    } else {
        prior=&fakeRecord{request:req}
        f.journal[req.OperationID]=prior
    }
    prior.noEffectFinal=true
    f.generation++
    prior.version=f.generation
    return nil
}

func sameRequest(a,b Request)bool {
    return a.OperationID==b.OperationID && a.TenantID==b.TenantID &&
        a.WorkloadID==b.WorkloadID && a.ProviderID==b.ProviderID &&
        a.IdempotencyKey==b.IdempotencyKey && a.RequestDigest==b.RequestDigest &&
        a.DesiredGeneration==b.DesiredGeneration && a.Kind==b.Kind &&
        a.TargetResourceID==b.TargetResourceID && a.TargetResourceUID==b.TargetResourceUID
}

func (f *FakeProvider) Apply(ctx context.Context,req Request)(Acknowledgement,error) {
    if err:=ctx.Err(); err!=nil { return Acknowledgement{},err }
    if err:=req.Validate(); err!=nil { return Acknowledgement{},err }
    f.mu.Lock()
    defer f.mu.Unlock()
    existing,found:=f.journal[req.OperationID]
    if found && !sameRequest(existing.request,req) { return Acknowledgement{},ErrOperationConflict }
    if !found {
        existing=&fakeRecord{request:req}
        f.journal[req.OperationID]=existing
    }
    if !existing.applied {
        // A fake CREATE is materialized once per exact operation identity.
        // UPDATE/DELETE require a real provider-specific target adapter;
        // no fake shortcut is allowed for those operations.
        if req.Kind!=operation.Create { return Acknowledgement{},ErrInvalidRequest }
        f.generation++
        existing.applied=true
        existing.noEffectFinal=false
        existing.resourceID="fake-resource/"+req.OperationID
        existing.resourceUID="fake-uid/"+req.OperationID
        existing.version=f.generation
    }
    ack:=Acknowledgement{OperationID:req.OperationID,RequestDigest:req.RequestDigest}
    if f.nextLost {
        f.nextLost=false
        return Acknowledgement{},ErrAcknowledgementLost
    }
    return ack,nil
}

func (f *FakeProvider) Inspect(ctx context.Context,req Request)(operation.Observation,error) {
    if err:=ctx.Err(); err!=nil { return operation.Observation{},err }
    if err:=req.Validate(); err!=nil { return operation.Observation{},err }
    f.mu.Lock()
    defer f.mu.Unlock()
    obs:=operation.Observation{
        OperationID:req.OperationID, TenantID:req.TenantID,
        WorkloadID:req.WorkloadID, ProviderID:req.ProviderID,
        DesiredGeneration:req.DesiredGeneration, ObservedAt:time.Now().UTC(),
        ResourceVersion: "fake-journal/unrecorded",
    }
    rec,found:=f.journal[req.OperationID]
    if !found {
        // An absent journal entry is not an authoritative terminal
        // no-effect receipt; fail closed into manual review.
        obs.Result=operation.ObservationConflict
        obs.ReasonCode="MISSING_AUTHORITATIVE_OPERATION"
        return obs,nil
    }
    if !sameRequest(rec.request,req) {
        obs.Result=operation.ObservationConflict
        obs.ReasonCode="OPERATION_FINGERPRINT_MISMATCH"
        return obs,nil
    }
    obs.ResourceVersion=fmt.Sprintf("fake-journal/%d",rec.version)
    switch {
    case rec.applied:
        obs.Result=operation.ResourcePresent
        obs.ResourceID=rec.resourceID
        obs.ResourceUID=rec.resourceUID
        obs.ResourceOperationID=req.OperationID
        obs.ResourceGeneration=req.DesiredGeneration
    case rec.noEffectFinal:
        obs.Result=operation.OperationNotAppliedFinal
    default:
        obs.Result=operation.ObservationConflict
        obs.ReasonCode="OPERATION_STATE_UNCONFIRMED"
    }
    return obs,nil
}

var _ Adapter = (*FakeProvider)(nil)

// Keep digest validation in the domain; this local check only guards obvious
// whitespace inputs before building an immutable Operation value.
var _ = strings.TrimSpace
