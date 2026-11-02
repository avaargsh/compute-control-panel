package operation

import (
    "errors"
    "testing"
)
func fixture(kind Kind) Operation {
    return Operation{ID:"op-1",TenantID:"t-1",WorkloadID:"w-1",ProviderID:"p-1",
        Generation:7,Version:1,Kind:kind,Status:Pending}
}
func TestUnknownRequiresInspection(t *testing.T) {
    op:=fixture(Create)
    if _,err:=op.Begin(99); !errors.Is(err,ErrStale) {t.Fatalf("stale accepted: %v",err)}
    op,_=op.Begin(1)
    op,_=op.LostAck(2)
    if _,err:=op.Begin(3); !errors.Is(err,ErrTransition) {t.Fatalf("blind retry: %v",err)}
    if _,err:=op.Verify(3,Observation{OperationID:"op-1",Generation:6,Existence:Exists,ProviderResourceID:"pod-1"}); !errors.Is(err,ErrObservation) {
        t.Fatalf("stale generation: %v",err)
    }
    if _,err:=op.Verify(3,Observation{OperationID:"op-2",Generation:7,Existence:Exists,ProviderResourceID:"pod-1"}); !errors.Is(err,ErrObservation) {
        t.Fatalf("wrong operation: %v",err)
    }
    if _,err:=op.Verify(3,Observation{OperationID:"op-1",Generation:7,Existence:Exists}); !errors.Is(err,ErrObservation) {
        t.Fatalf("missing provider ID: %v",err)
    }
    verified,err:=op.Verify(3,Observation{OperationID:"op-1",Generation:7,Existence:Exists,ProviderResourceID:"pod-1"})
    if err!=nil||verified.Status!=Verified||verified.Version!=4 {t.Fatalf("verified: %+v %v",verified,err)}
}
func TestDeleteNeedsObservedAbsence(t *testing.T) {
    op,_:=fixture(Delete).Begin(1)
    if _,err:=op.Verify(2,Observation{OperationID:"op-1",Generation:7,Existence:Exists,ProviderResourceID:"pod-1"}); !errors.Is(err,ErrObservation) {
        t.Fatalf("delete ACK mistaken for release: %v",err)
    }
    got,err:=op.Verify(2,Observation{OperationID:"op-1",Generation:7,Existence:Absent})
    if err!=nil||got.Status!=Verified {t.Fatalf("release: %+v %v",got,err)}
}
func TestRejectInvalidOperation(t *testing.T) {
    op:=fixture(Create)
    op.TenantID=""
    if !errors.Is(op.Validate(),ErrInvalid) {t.Fatal("missing tenant accepted")}
}
