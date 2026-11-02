package operation

import (
    "errors"
    "strings"
    "testing"
    "time"
)

func fixture(kind Kind) Operation {
    op := Operation{
        ID: "op-1", TenantID: "tenant-1", WorkloadID: "workload-1",
        ProviderID: "provider-1", IdempotencyKey: "compute-key-1",
        RequestDigest: "sha256:" + strings.Repeat("a", 64),
        Generation: 7, Version: 1, Kind: kind, Status: Pending,
    }
    if kind == Delete {
        op.TargetResourceID = "job-1"
        op.TargetResourceUID = "uid-1"
    }
    return op
}

func claimAndStart(t *testing.T, op Operation, now time.Time) Operation {
    t.Helper()
    var err error
    op, err = op.Claim(1, "controller-1", now, time.Minute)
    if err != nil { t.Fatal(err) }
    op, err = op.Start(op.Version, "controller-1", op.FenceToken, now.Add(time.Second))
    if err != nil { t.Fatal(err) }
    return op
}

func reconcile(t *testing.T, op Operation, now time.Time) Operation {
    t.Helper()
    var got Operation
    var err error
    if op.Status == InFlight {
        got, err = op.BeginReconciliation(op.Version, op.LeaseOwner, op.FenceToken, "reconciler-1", now, time.Minute)
    } else {
        got, err = op.BeginReconciliation(op.Version, "", 0, "reconciler-1", now, time.Minute)
    }
    if err != nil { t.Fatal(err) }
    return got
}

func TestCreateRequiresCorrelatedProviderObservation(t *testing.T) {
    now := time.Unix(100, 0).UTC()
    op := claimAndStart(t, fixture(Create), now)
    op = reconcile(t, op, now.Add(2*time.Second))
    verified, err := op.ApplyObservation(op.Version, "reconciler-1", op.FenceToken, now.Add(3*time.Second), 7, Observation{
        OperationID: "op-1", TenantID: "tenant-1", WorkloadID: "workload-1",
        ProviderID: "provider-1", DesiredGeneration: 7,
        Result: ResourcePresent, ResourceID: "job-1", ResourceUID: "uid-1",
        ResourceOperationID: "op-1", ResourceGeneration: 7,
        ResourceVersion: "rv-10", ObservedAt: now.Add(3*time.Second),
    })
    if err != nil { t.Fatal(err) }
    if verified.Status != Succeeded || verified.ProviderResourceID != "job-1" ||
        verified.ProviderResourceUID != "uid-1" { t.Fatalf("unexpected operation: %+v", verified) }
}

func TestLostAckRequiresReconcileAndSameKeyRetry(t *testing.T) {
    now := time.Unix(200, 0).UTC()
    op := claimAndStart(t, fixture(Create), now)
    var err error
    op, err = op.MarkUnknown(op.Version, "controller-1", op.FenceToken, now.Add(2*time.Second))
    if err != nil || op.Status != Unknown { t.Fatalf("lost ACK: %+v %v", op, err) }
    if _, err := op.Claim(op.Version, "controller-2", now.Add(3*time.Second), time.Minute); !errors.Is(err, ErrTransition) {
        t.Fatalf("UNKNOWN blindly retried: %v", err)
    }
    op = reconcile(t, op, now.Add(3*time.Second))
    op, err = op.ApplyObservation(op.Version, "reconciler-1", op.FenceToken, now.Add(4*time.Second), 7, Observation{
        OperationID: "op-1", TenantID: "tenant-1", WorkloadID: "workload-1",
        ProviderID: "provider-1", DesiredGeneration: 7,
        Result: OperationNotAppliedFinal, ResourceVersion: "op-rv-4",
        ObservedAt: now.Add(4*time.Second),
    })
    if err != nil || op.Status != Pending { t.Fatalf("authoritative not-applied: %+v %v", op, err) }
    if op.ID != "op-1" || op.IdempotencyKey != "compute-key-1" ||
        op.RequestDigest != "sha256:"+strings.Repeat("a",64) {
        t.Fatal("retry changed provider operation identity")
    }
}

func TestDeleteRequiresExactTargetUIDAbsence(t *testing.T) {
    now := time.Unix(300, 0).UTC()
    op := claimAndStart(t, fixture(Delete), now)
    op = reconcile(t, op, now.Add(2*time.Second))
    base := Observation{
        OperationID: "op-1", TenantID: "tenant-1", WorkloadID: "workload-1",
        ProviderID: "provider-1", DesiredGeneration: 7,
        Result: TargetAbsent, TargetResourceID: "job-1",
        TargetResourceUID: "uid-other", ResourceVersion: "rv-20",
        ObservedAt: now.Add(3*time.Second),
    }
    if _, err := op.ApplyObservation(op.Version, "reconciler-1", op.FenceToken, now.Add(3*time.Second), 7, base); !errors.Is(err, ErrObservation) {
        t.Fatalf("wrong UID proved release: %v", err)
    }
    base.TargetResourceUID = "uid-1"
    released, err := op.ApplyObservation(op.Version, "reconciler-1", op.FenceToken, now.Add(3*time.Second), 7, base)
    if err != nil || released.Status != Succeeded { t.Fatalf("exact UID absence: %+v %v", released, err) }
}

func TestLeaseExpiryAndFencingRejectStaleController(t *testing.T) {
    now := time.Unix(400, 0).UTC()
    op, err := fixture(Create).Claim(1, "controller-1", now, time.Second)
    if err != nil { t.Fatal(err) }
    expired, err := op.ExpireLease(2, now.Add(2*time.Second))
    if err != nil || expired.Status != Unknown { t.Fatalf("expiry: %+v %v", expired, err) }
    reconciling, err := expired.BeginReconciliation(3, "", 0, "controller-2", now.Add(3*time.Second), time.Minute)
    if err != nil { t.Fatal(err) }
    if _, err := reconciling.ApplyObservation(4, "controller-1", op.FenceToken, now.Add(4*time.Second), 7, Observation{}); !errors.Is(err, ErrStaleLease) {
        t.Fatalf("stale fence committed: %v", err)
    }
}

func TestStaleGenerationCreateSchedulesCompensation(t *testing.T) {
    now := time.Unix(500, 0).UTC()
    op := claimAndStart(t, fixture(Create), now)
    op = reconcile(t, op, now.Add(2*time.Second))
    superseded, err := op.ApplyObservation(op.Version, "reconciler-1", op.FenceToken, now.Add(3*time.Second), 8, Observation{
        OperationID: "op-1", TenantID: "tenant-1", WorkloadID: "workload-1",
        ProviderID: "provider-1", DesiredGeneration: 7,
        Result: ResourcePresent, ResourceID: "job-old", ResourceUID: "uid-old",
        ResourceOperationID: "op-1", ResourceGeneration: 7,
        ResourceVersion: "rv-30", ObservedAt: now.Add(3*time.Second),
    })
    if err != nil || superseded.Status != Superseded ||
        !superseded.CleanupRequired || !superseded.ReconcileRequired {
        t.Fatalf("stale create not compensated: %+v %v", superseded, err)
    }
}

func TestWrongProviderOrOperationEvidenceRejected(t *testing.T) {
    now := time.Unix(600, 0).UTC()
    op := claimAndStart(t, fixture(Create), now)
    op = reconcile(t, op, now.Add(2*time.Second))
    observation := Observation{
        OperationID: "other-op", TenantID: "tenant-1", WorkloadID: "workload-1",
        ProviderID: "provider-1", DesiredGeneration: 7, Result: ResourcePresent,
        ResourceID: "job-1", ResourceUID: "uid-1",
        ResourceOperationID: "other-op", ResourceGeneration: 7,
        ResourceVersion: "rv-40", ObservedAt: now.Add(3*time.Second),
    }
    if _, err := op.ApplyObservation(op.Version, "reconciler-1", op.FenceToken, now.Add(3*time.Second), 7, observation); !errors.Is(err, ErrObservation) {
        t.Fatalf("wrong operation accepted: %v", err)
    }
    observation.OperationID = "op-1"
    observation.ProviderID = "foreign-provider"
    if _, err := op.ApplyObservation(op.Version, "reconciler-1", op.FenceToken, now.Add(3*time.Second), 7, observation); !errors.Is(err, ErrObservation) {
        t.Fatalf("wrong provider accepted: %v", err)
    }
}

func TestConflictingObservationRequiresManualReview(t *testing.T) {
    now := time.Unix(700, 0).UTC()
    op := claimAndStart(t, fixture(Create), now)
    op = reconcile(t, op, now.Add(2*time.Second))
    got, err := op.ApplyObservation(op.Version, "reconciler-1", op.FenceToken, now.Add(3*time.Second), 7, Observation{
        OperationID: "op-1", TenantID: "tenant-1", WorkloadID: "workload-1",
        ProviderID: "provider-1", DesiredGeneration: 7, Result: ObservationConflict,
        ReasonCode: "RESOURCE_LABEL_MISMATCH", ResourceVersion: "rv-50",
        ObservedAt: now.Add(3*time.Second),
    })
    if err != nil || got.Status != ManualReview || got.ManualReviewCode != "RESOURCE_LABEL_MISMATCH" {
        t.Fatalf("conflict not quarantined: %+v %v", got, err)
    }
}

func TestDeleteOperationRequiresTargetIdentity(t *testing.T) {
    op := fixture(Delete)
    op.TargetResourceUID = ""
    if !errors.Is(op.Validate(), ErrInvalid) { t.Fatal("delete without UID accepted") }
}

func TestStaleGenerationUpdateReconcilesWithoutDeletingTarget(t *testing.T) {
    now := time.Unix(800, 0).UTC()
    op := fixture(Update)
    op.TargetResourceID = "job-existing"
    op.TargetResourceUID = "uid-existing"
    op = claimAndStart(t, op, now)
    op = reconcile(t, op, now.Add(2*time.Second))
    superseded, err := op.ApplyObservation(op.Version, "reconciler-1", op.FenceToken, now.Add(3*time.Second), 8, Observation{
        OperationID: "op-1", TenantID: "tenant-1", WorkloadID: "workload-1",
        ProviderID: "provider-1", DesiredGeneration: 7,
        Result: ResourcePresent, ResourceID: "job-existing", ResourceUID: "uid-existing",
        ResourceOperationID: "op-1", ResourceGeneration: 7,
        ResourceVersion: "rv-60", ObservedAt: now.Add(3*time.Second),
    })
    if err != nil || superseded.Status != Superseded ||
        superseded.CleanupRequired || !superseded.ReconcileRequired {
        t.Fatalf("stale update scheduled unsafe deletion: %+v %v", superseded, err)
    }
    if superseded.ProviderResourceID != "job-existing" || superseded.ProviderResourceUID != "uid-existing" {
        t.Fatalf("stale update lost provider identity: %+v", superseded)
    }
}

func TestUpdateOperationRequiresExactTargetIdentity(t *testing.T) {
    op := fixture(Update)
    op.TargetResourceID = ""
    op.TargetResourceUID = ""
    if !errors.Is(op.Validate(), ErrInvalid) { t.Fatal("update without target UID accepted") }
}
