package postgres

import (
	"context"
	"testing"
	"time"

	"github.com/avaargsh/compute-control-panel/internal/operation"
)

// A session-local operation must not become a valid lease: recovery needs a
// committed, versioned and fenced operation in CCP's permanent journal.
func TestReconciliationLeaseCannotBeDivertedToTempOperation(t *testing.T) {
	db := openIntegrationDB(t)
	db.SetMaxOpenConns(1)
	db.SetMaxIdleConns(1)
	repo, err := NewRepository(db)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	initial := operationFixture(t, repo, ctx, "tenant-lease-shadow-"+randomSuffix(t))
	if _, _, err := repo.CreateOperation(ctx, initial, "event-"+randomSuffix(t), "corr-lease"); err != nil {
		t.Fatal(err)
	}
	leased, err := repo.ClaimOperation(ctx, initial.TenantID, initial.ID, 1, "worker", time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	started, err := repo.StartOperation(ctx, initial.TenantID, initial.ID, leased.Version, "worker", leased.FenceToken)
	if err != nil {
		t.Fatal(err)
	}
	unknown, err := repo.MarkOperationUnknown(ctx, initial.TenantID, initial.ID, started.Version, "worker", started.FenceToken)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := db.ExecContext(ctx, `CREATE TEMP TABLE compute_operations
        (LIKE public.compute_operations INCLUDING DEFAULTS) ON COMMIT PRESERVE ROWS`); err != nil {
		t.Fatal(err)
	}
	if _, err := db.ExecContext(ctx, `INSERT INTO pg_temp.compute_operations
        SELECT * FROM public.compute_operations WHERE tenant_id=$1 AND compute_operation_id=$2`, unknown.TenantID, unknown.ID); err != nil {
		t.Fatal(err)
	}
	claimed, err := repo.BeginOperationReconciliation(ctx, unknown.TenantID, unknown.ID,
		unknown.Version, "", 0, "reconciler", time.Minute)
	if err != nil || claimed.Status != operation.Reconciling {
		t.Fatalf("reconciliation claim: %+v err=%v", claimed, err)
	}
	var realStatus, tempStatus operation.Status
	var realVersion, tempVersion int64
	if err := db.QueryRowContext(ctx, `SELECT status,version FROM public.compute_operations
        WHERE tenant_id=$1 AND compute_operation_id=$2`, unknown.TenantID, unknown.ID).Scan(&realStatus, &realVersion); err != nil {
		t.Fatal(err)
	}
	if err := db.QueryRowContext(ctx, `SELECT status,version FROM pg_temp.compute_operations
        WHERE tenant_id=$1 AND compute_operation_id=$2`, unknown.TenantID, unknown.ID).Scan(&tempStatus, &tempVersion); err != nil {
		t.Fatal(err)
	}
	if realStatus != operation.Reconciling || uint64(realVersion) != claimed.Version ||
		tempStatus != operation.Unknown || uint64(tempVersion) != unknown.Version {
		t.Fatalf("recovery claimed nonauthoritative lease: permanent=%s/%d temp=%s/%d returned=%s/%d",
			realStatus, realVersion, tempStatus, tempVersion, claimed.Status, claimed.Version)
	}
}

// An unqualified GetOperation can return a forged version, even when the real
// operation remained unchanged after a restart.
func TestGetOperationIgnoresTempShadowVersion(t *testing.T) {
	db := openIntegrationDB(t)
	db.SetMaxOpenConns(1)
	db.SetMaxIdleConns(1)
	repo, err := NewRepository(db)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	initial := operationFixture(t, repo, ctx, "tenant-read-shadow-"+randomSuffix(t))
	if _, _, err := repo.CreateOperation(ctx, initial, "event-"+randomSuffix(t), "corr-read"); err != nil {
		t.Fatal(err)
	}
	if _, err := db.ExecContext(ctx, `CREATE TEMP TABLE compute_operations
        (LIKE public.compute_operations INCLUDING DEFAULTS) ON COMMIT PRESERVE ROWS`); err != nil {
		t.Fatal(err)
	}
	if _, err := db.ExecContext(ctx, `INSERT INTO pg_temp.compute_operations
        SELECT * FROM public.compute_operations WHERE tenant_id=$1 AND compute_operation_id=$2`, initial.TenantID, initial.ID); err != nil {
		t.Fatal(err)
	}
	if _, err := db.ExecContext(ctx, `UPDATE pg_temp.compute_operations SET version=version+7
        WHERE tenant_id=$1 AND compute_operation_id=$2`, initial.TenantID, initial.ID); err != nil {
		t.Fatal(err)
	}
	got, err := repo.GetOperation(ctx, initial.TenantID, initial.ID)
	if err != nil || got.Version != initial.Version || got.Status != operation.Pending {
		t.Fatalf("temporary journal masked authoritative version: got=%+v err=%v", got, err)
	}
}

// A successful CreateOperation must have an associated permanent Outbox event;
// pg_temp may not absorb it while the permanent journal operation is committed.
func TestCreateOperationCannotDivertOutboxToTempTable(t *testing.T) {
	db := openIntegrationDB(t)
	db.SetMaxOpenConns(1)
	db.SetMaxIdleConns(1)
	repo, err := NewRepository(db)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	initial := operationFixture(t, repo, ctx, "tenant-outbox-shadow-"+randomSuffix(t))
	if _, err := db.ExecContext(ctx, `CREATE TEMP TABLE compute_outbox_events
        (LIKE public.compute_outbox_events INCLUDING DEFAULTS) ON COMMIT PRESERVE ROWS`); err != nil {
		t.Fatal(err)
	}
	eventID := "event-shadow-" + randomSuffix(t)
	got, created, err := repo.CreateOperation(ctx, initial, eventID, "corr-outbox")
	if err != nil || !created || got.Status != operation.Pending {
		t.Fatalf("operation create: created=%v op=%+v err=%v", created, got, err)
	}
	var permanent, temp int
	if err := db.QueryRowContext(ctx, `SELECT count(*) FROM public.compute_outbox_events WHERE event_id=$1`, eventID).Scan(&permanent); err != nil {
		t.Fatal(err)
	}
	if err := db.QueryRowContext(ctx, `SELECT count(*) FROM pg_temp.compute_outbox_events WHERE event_id=$1`, eventID).Scan(&temp); err != nil {
		t.Fatal(err)
	}
	if permanent != 1 || temp != 0 {
		t.Fatalf("committed operation lost durable Outbox: permanent=%d temp=%d", permanent, temp)
	}
}
