package postgres

import (
    "context"
    "database/sql"
    "errors"
    "strings"
    "sync"
    "testing"
    "time"

    "github.com/avaargsh/compute-control-panel/internal/operation"
)

// Integration tests deliberately reopen Repository instances: journal state
// must reside in PostgreSQL, not in the worker that last held the lease.
func operationFixture(t *testing.T, repo *Repository, ctx context.Context, tenant string) operation.Operation {
    t.Helper()
    payload := []byte(`{"resources":{"cpu":"2"},"api_version":"compute.platform/v1"}`)
    wID := "workload-op-" + randomSuffix(t)
    _, created, err := repo.CreateWorkload(ctx, createParams(
        tenant, "workload-key-"+randomSuffix(t), testDigest(payload), payload,
        wID, "event-workload-"+randomSuffix(t),
    ))
    if err != nil || !created { t.Fatalf("create parent workload: %v, created=%v", err, created) }
    return operation.Operation{
        ID: "op-"+randomSuffix(t),
        TenantID: tenant,
        WorkloadID: wID,
        ProviderID: "provider-fake-1",
        IdempotencyKey: "op-key-"+randomSuffix(t),
        RequestDigest: "sha256:"+strings.Repeat("a",64),
        Generation: 1, Version: 1, Kind: operation.Create, Status: operation.Pending,
    }
}

func TestDurableOperationLostACKAndReconciliationAfterRestart(t *testing.T) {
    db := openIntegrationDB(t)
    ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
    defer cancel()
    writer, _ := NewRepository(db)
    tenant := "tenant-operation-"+randomSuffix(t)
    initial := operationFixture(t, writer, ctx, tenant)
    created, wasCreated, err := writer.CreateOperation(ctx, initial, "event-operation-"+randomSuffix(t), "corr-operation-1")
    if err != nil || !wasCreated || created.Status != operation.Pending { t.Fatalf("create operation: %+v, %t, %v", created, wasCreated, err) }

    claimed, err := writer.ClaimOperation(ctx, tenant, initial.ID, 1, "worker-a", time.Minute)
    if err != nil || claimed.Status != operation.Leased || claimed.FenceToken != 1 { t.Fatalf("claim: %+v, %v", claimed, err) }
    inFlight, err := writer.StartOperation(ctx, tenant, initial.ID, claimed.Version, "worker-a", claimed.FenceToken)
    if err != nil || inFlight.Status != operation.InFlight { t.Fatalf("start: %+v %v", inFlight, err) }
    unknown, err := writer.MarkOperationUnknown(ctx, tenant, initial.ID, inFlight.Version, "worker-a", inFlight.FenceToken)
    if err != nil || unknown.Status != operation.Unknown { t.Fatalf("ACK lost: %+v %v", unknown, err) }

    // The new repository acts like a process restart, sharing only durable DB.
    reader, _ := NewRepository(db)
    recovered, err := reader.GetOperation(ctx, tenant, initial.ID)
    if err != nil || recovered.Status != operation.Unknown || recovered.Version != unknown.Version {
        t.Fatalf("restart lost UNKNOWN state: %+v, %v", recovered, err)
    }
    if _, err := reader.ClaimOperation(ctx, tenant, initial.ID, recovered.Version, "worker-b", time.Minute); !errors.Is(err, operation.ErrTransition) {
        t.Fatalf("UNKNOWN was blindly retried: %v", err)
    }
    reconciling, err := reader.BeginOperationReconciliation(ctx, tenant, initial.ID, recovered.Version, "", 0, "reconciler-b", time.Minute)
    if err != nil || reconciling.Status != operation.Reconciling || reconciling.FenceToken <= unknown.FenceToken {
        t.Fatalf("UNKNOWN was not fenced and reconciled: %+v, %v", reconciling, err)
    }
    if _, err := writer.StartOperation(ctx, tenant, initial.ID, inFlight.Version, "worker-a", inFlight.FenceToken); !errors.Is(err, operation.ErrStale) {
        t.Fatalf("stale worker committed after restart/reconciliation: %v", err)
    }
    if _, err := reader.GetOperation(ctx, "other-tenant", initial.ID); !errors.Is(err, sql.ErrNoRows) {
        t.Fatalf("cross-tenant observation leaked: %v", err)
    }
}

func TestConcurrentDurableClaimExactlyOneWinner(t *testing.T) {
    db := openIntegrationDB(t)
    ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
    defer cancel()
    repo, _ := NewRepository(db)
    tenant := "tenant-claim-"+randomSuffix(t)
    op := operationFixture(t, repo, ctx, tenant)
    _, _, err := repo.CreateOperation(ctx, op, "event-claim-"+randomSuffix(t), "corr-claim")
    if err != nil { t.Fatal(err) }

    const workers = 10
    results := make([]operation.Operation, workers)
    errs := make([]error, workers)
    owners := make([]string, workers)
    for i := range owners { owners[i] = "worker-"+randomSuffix(t) }
    var wg sync.WaitGroup
    for i := 0; i < workers; i++ {
        wg.Add(1)
        go func(index int) {
            defer wg.Done()
            results[index], errs[index] = repo.ClaimOperation(ctx, tenant, op.ID, 1, owners[index], time.Minute)
        }(i)
    }
    wg.Wait()
    successes := 0
    for i, e := range errs {
        if e == nil {
            successes++
            if results[i].Version != 2 || results[i].FenceToken != 1 { t.Fatalf("bad winner: %+v", results[i]) }
        } else if !errors.Is(e, operation.ErrStale) {
            t.Fatalf("unexpected loser error %d: %v", i, e)
        }
    }
    if successes != 1 { t.Fatalf("claim winners: %d, want one", successes) }
}

func TestOperationCreateIdempotencyAndAtomicOutbox(t *testing.T) {
    db := openIntegrationDB(t)
    ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
    defer cancel()
    repo, _ := NewRepository(db)
    tenant := "tenant-op-idem-"+randomSuffix(t)
    op := operationFixture(t, repo, ctx, tenant)
    eventID := "event-"+randomSuffix(t)
    created, ok, err := repo.CreateOperation(ctx, op, eventID, "corr-1")
    if err != nil || !ok { t.Fatalf("operation create: %v, %t", err,ok) }
    replay := op
    replay.ID = "op-replay-"+randomSuffix(t)
    duplicate, replayCreated, err := repo.CreateOperation(ctx, replay, "event-unused-"+randomSuffix(t), "corr-2")
    if err != nil || replayCreated || duplicate.ID != created.ID { t.Fatalf("idempotency replay: %+v %t %v",duplicate,replayCreated,err) }
    changed := replay
    changed.RequestDigest = "sha256:"+strings.Repeat("b",64)
    if _, _, err := repo.CreateOperation(ctx,changed,"event-unused-"+randomSuffix(t),"corr-3"); !errors.Is(err,ErrIdempotencyConflict) {
        t.Fatalf("changed digest accepted: %v",err)
    }
    var eventCount int
    if err := db.QueryRowContext(ctx,`SELECT count(*) FROM compute_outbox_events WHERE tenant_id=$1 AND aggregate_type='compute_operation' AND aggregate_id=$2`,
        tenant,created.ID).Scan(&eventCount); err != nil || eventCount != 1 {
        t.Fatalf("non-atomic create/outbox: count=%d err=%v",eventCount,err)
    }

    second := op
    second.ID = "op-second-"+randomSuffix(t)
    second.IdempotencyKey = "key-second-"+randomSuffix(t)
    // A duplicate global outbox event ID must roll back the operation row.
    if _,_,err := repo.CreateOperation(ctx,second,eventID,"corr-4"); err == nil {
        t.Fatal("duplicate event ID did not abort transaction")
    }
    if _,err := repo.GetOperation(ctx,tenant,second.ID); !errors.Is(err,sql.ErrNoRows) {
        t.Fatalf("operation survived failed outbox insert: %v",err)
    }
}

func TestOperationLeaseExpiryAdvancesFence(t *testing.T) {
    db := openIntegrationDB(t)
    ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
    defer cancel()
    repo, _ := NewRepository(db)
    tenant := "tenant-op-expire-"+randomSuffix(t)
    op := operationFixture(t,repo,ctx,tenant)
    if _,_,err := repo.CreateOperation(ctx,op,"event-"+randomSuffix(t),"corr-expire"); err != nil { t.Fatal(err) }
    claimed, err := repo.ClaimOperation(ctx,tenant,op.ID,1,"worker-old",30*time.Millisecond)
    if err != nil {t.Fatal(err)}
    if _,err := repo.ExpireOperationLease(ctx,tenant,op.ID,claimed.Version); !errors.Is(err,operation.ErrLeaseActive) {
        t.Fatalf("lease expired too early: %v",err)
    }
    time.Sleep(80*time.Millisecond)
    expired, err := repo.ExpireOperationLease(ctx,tenant,op.ID,claimed.Version)
    if err != nil || expired.Status != operation.Unknown || expired.FenceToken != claimed.FenceToken+1 {
        t.Fatalf("lease expiry failed to fence: %+v %v",expired,err)
    }
}


func TestOldGenerationOperationCannotDispatchButCanReplay(t *testing.T) {
    db:=openIntegrationDB(t)
    ctx,cancel:=context.WithTimeout(context.Background(),15*time.Second)
    defer cancel()
    repo,_:=NewRepository(db)
    tenant:="tenant-stale-"+randomSuffix(t)
    op:=operationFixture(t,repo,ctx,tenant)
    created,ok,err:=repo.CreateOperation(ctx,op,"event-"+randomSuffix(t),"corr-stale")
    if err!=nil || !ok {t.Fatalf("initial operation: %+v %t %v",created,ok,err)}
    _,err=db.ExecContext(ctx,`UPDATE compute_workloads SET desired_generation=2,
        version=version+1 WHERE tenant_id=$1 AND workload_id=$2`,tenant,op.WorkloadID)
    if err!=nil {t.Fatal(err)}
    replay:=op
    replay.ID="op-unused-"+randomSuffix(t)
    previous,createdAgain,err:=repo.CreateOperation(ctx,replay,"event-unused-"+randomSuffix(t),"corr-replay")
    if err!=nil || createdAgain || previous.ID!=created.ID {
        t.Fatalf("historical idempotency replay rejected after generation update: %+v %t %v",previous,createdAgain,err)
    }
    if _,err:=repo.ClaimOperation(ctx,tenant,op.ID,1,"obsolete-worker",time.Minute); !errors.Is(err,operation.ErrStale) {
        t.Fatalf("stale generation operation was dispatchable: %v",err)
    }
}
