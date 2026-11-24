package postgres

import (
	"context"
	"database/sql"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgconn"
)

func TestResolveReleaseCommitFenceDerivesAndLocksCurrentRows(t *testing.T) {
	db := openIntegrationDB(t)
	repo, err := NewRepository(db)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	suffix := randomSuffix(t)
	params := acceptanceFixture(t, "tenant-release-resolver-"+suffix,
		"idempotency-w-resolver-"+suffix, "idempotency-create-resolver-"+suffix,
		"event-create-resolver-"+suffix)
	accepted, created, err := repo.AcceptWorkload(ctx, params)
	if err != nil || !created {
		t.Fatalf("accept: created=%t err=%v", created, err)
	}
	tenant, workload := params.Workload.TenantID, accepted.Workload.WorkloadID
	allocation, providerID, createID := accepted.Allocation.AllocationID, accepted.Allocation.ProviderID, accepted.Operation.ID
	deleteID := "operation-delete-resolver-" + suffix
	resourceID, resourceUID := "tenant-a/job-"+suffix, "job-uid-"+suffix

	updateOne := func(query string, args ...any) {
		t.Helper()
		result, err := db.ExecContext(ctx, query, args...)
		if err != nil {
			t.Fatalf("prepare release fixture: %v", err)
		}
		rows, err := result.RowsAffected()
		if err != nil || rows != 1 {
			t.Fatalf("fixture affected=%d err=%v", rows, err)
		}
	}
	updateOne(`UPDATE public.compute_operations SET status='SUCCEEDED',
		provider_resource_id=$3, provider_resource_uid=$4, version=version+1
		WHERE tenant_id=$1 AND compute_operation_id=$2`, tenant, createID, resourceID, resourceUID)
	updateOne(`UPDATE public.compute_workloads SET desired_generation=2,
		desired_state='DELETE_REQUESTED', observed_phase='RELEASING', version=version+1
		WHERE tenant_id=$1 AND workload_id=$2`, tenant, workload)
	updateOne(`UPDATE public.compute_allocations SET phase='RELEASING', version=version+1
		WHERE tenant_id=$1 AND allocation_id=$2`, tenant, allocation)
	updateOne(`INSERT INTO public.compute_operations(
		compute_operation_id,tenant_id,workload_id,allocation_id,provider_id,
		operation_kind,desired_generation,provider_request_digest,idempotency_key,
		status,target_resource_id,target_resource_uid)
		VALUES($1,$2,$3,NULL,$4,'DELETE',2,$5,$6,'SUCCEEDED',$7,$8)`,
		deleteID, tenant, workload, providerID,
		"sha256:"+strings.Repeat("d", 64), "idempotency-delete-resolver-"+suffix,
		resourceID, resourceUID)

	tx, err := db.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelReadCommitted})
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback()
	got, err := ResolveReleaseCommitFence(ctx, tx, tenant, workload, allocation, createID, deleteID)
	if err != nil {
		t.Fatalf("resolve durable release fence: %v", err)
	}
	if got.TenantID != tenant || got.WorkloadID != workload || got.AllocationID != allocation ||
		got.ProviderID != providerID || got.CreateOperationID != createID || got.DeleteOperationID != deleteID ||
		got.TargetResourceID != resourceID || got.TargetResourceUID != resourceUID ||
		got.AllocationGeneration != 1 || got.DeleteGeneration != 2 ||
		got.WorkloadVersion != 2 || got.AllocationVersion != 2 ||
		got.CreateOperationVersion != 2 || got.DeleteOperationVersion != 1 {
		t.Fatalf("resolved fence does not match current durable rows: %+v", got)
	}
	wrongTenantTx, err := db.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelReadCommitted})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := ResolveReleaseCommitFence(ctx, wrongTenantTx, "tenant-other",
		workload, allocation, createID, deleteID); !errors.Is(err, ErrReleaseCommitFence) {
		t.Fatalf("wrong tenant resolved another tenant's fence: %v", err)
	}
	if err := wrongTenantTx.Rollback(); err != nil {
		t.Fatal(err)
	}

	// The resolver must retain locks on all four authoritative rows in the
	// caller's transaction; a separate transaction using NOWAIT must fail.
	rows := []struct {
		name  string
		query string
		id    string
	}{
		{"workload", `SELECT version FROM public.compute_workloads WHERE tenant_id=$1 AND workload_id=$2 FOR UPDATE NOWAIT`, workload},
		{"allocation", `SELECT version FROM public.compute_allocations WHERE tenant_id=$1 AND allocation_id=$2 FOR UPDATE NOWAIT`, allocation},
		{"create operation", `SELECT version FROM public.compute_operations WHERE tenant_id=$1 AND compute_operation_id=$2 FOR UPDATE NOWAIT`, createID},
		{"delete operation", `SELECT version FROM public.compute_operations WHERE tenant_id=$1 AND compute_operation_id=$2 FOR UPDATE NOWAIT`, deleteID},
	}
	for _, row := range rows {
		t.Run(row.name, func(t *testing.T) {
			contender, err := db.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelReadCommitted})
			if err != nil {
				t.Fatal(err)
			}
			defer contender.Rollback()
			var version int64
			err = contender.QueryRowContext(ctx, row.query, tenant, row.id).Scan(&version)
			var pgErr *pgconn.PgError
			if !errors.As(err, &pgErr) || pgErr.Code != "55P03" {
				t.Fatalf("resolver did not retain %s lock: version=%d err=%v", row.name, version, err)
			}
		})
	}
}

func TestResolveReleaseCommitFenceRejectsMissingTransactionAndMalformedScope(t *testing.T) {
	if _, err := ResolveReleaseCommitFence(context.Background(), nil,
		"tenant-a", "workload-a", "allocation-a", "create-a", "delete-a"); !errors.Is(err, ErrReleaseCommitFence) {
		t.Fatalf("missing transaction was accepted: %v", err)
	}
	if _, err := ResolveReleaseCommitFence(context.Background(), nil,
		"", "workload-a", "allocation-a", "create-a", "delete-a"); !errors.Is(err, ErrReleaseCommitFence) {
		t.Fatalf("missing tenant was accepted: %v", err)
	}
}
