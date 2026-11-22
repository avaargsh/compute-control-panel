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

// This uses the existing PostgreSQL 16 integration harness. Without a real
// migrated RLS-aware database, the existing openIntegrationDB helper skips.
// A pure snapshot test alone is NOT a DB lock / CAS acceptance test.
func TestReleaseCommitFenceMatchesLivePostgresGenerationAndUID(t *testing.T) {
	db := openIntegrationDB(t)
	repo, err := NewRepository(db)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	suffix := randomSuffix(t)
	params := acceptanceFixture(t, "tenant-release-gate-"+suffix,
		"idempotency-w-"+suffix, "idempotency-create-"+suffix,
		"event-create-release-gate-"+suffix)
	accepted, created, err := repo.AcceptWorkload(ctx, params)
	if err != nil || !created {
		t.Fatalf("acceptance: created=%t err=%v", created, err)
	}
	tenant := params.Workload.TenantID
	workload := accepted.Workload.WorkloadID
	allocation := accepted.Allocation.AllocationID
	provider := accepted.Allocation.ProviderID
	createID := accepted.Operation.ID
	deleteID := "operation-delete-release-" + suffix
	resourceID := "tenant-a/job-" + suffix
	resourceUID := "job-uid-" + suffix

	exec := func(query string, args ...any) {
		t.Helper()
		result, err := db.ExecContext(ctx, query, args...)
		if err != nil {
			t.Fatalf("prepare release fixture: %v", err)
		}
		n, err := result.RowsAffected()
		if err != nil || n != 1 {
			t.Fatalf("fixture affected=%d err=%v", n, err)
		}
	}
	exec(`UPDATE public.compute_operations
        SET status='SUCCEEDED', provider_resource_id=$3, provider_resource_uid=$4, version=version+1
        WHERE tenant_id=$1 AND compute_operation_id=$2`, tenant, createID, resourceID, resourceUID)
	exec(`UPDATE public.compute_workloads
        SET desired_generation=2, desired_state='DELETE_REQUESTED', observed_phase='RELEASING',
            version=version+1
        WHERE tenant_id=$1 AND workload_id=$2`, tenant, workload)
	exec(`UPDATE public.compute_allocations
        SET phase='RELEASING', version=version+1
        WHERE tenant_id=$1 AND allocation_id=$2`, tenant, allocation)
	exec(`INSERT INTO public.compute_operations(
         compute_operation_id, tenant_id, workload_id, allocation_id, provider_id,
         operation_kind, desired_generation, provider_request_digest, idempotency_key,
         status, target_resource_id, target_resource_uid)
         VALUES($1,$2,$3,$4,$5,'DELETE',2,$6,$7,'SUCCEEDED',$8,$9)`,
		// A DELETE for Generation 2 cannot point its FK at Allocation Generation 1.
		// The source CREATE and exact target UID carry historical linkage.
		deleteID, tenant, workload, nil, provider,
		"sha256:"+strings.Repeat("b", 64), "idempotency-delete-"+suffix, resourceID, resourceUID)

	want := ReleaseCommitFence{
		TenantID: tenant, WorkloadID: workload, AllocationID: allocation, ProviderID: provider,
		CreateOperationID: createID, DeleteOperationID: deleteID,
		TargetResourceID: resourceID, TargetResourceUID: resourceUID,
		AllocationGeneration: 1, DeleteGeneration: 2,
		WorkloadVersion: 2, AllocationVersion: 2,
		CreateOperationVersion: 2, DeleteOperationVersion: 1,
	}
	check := func(expected ReleaseCommitFence) error {
		t.Helper()
		tx, err := db.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelReadCommitted})
		if err != nil {
			t.Fatalf("begin: %v", err)
		}
		defer tx.Rollback()
		return ValidateReleaseCommitFence(ctx, tx, expected)
	}
	if err := check(want); err != nil {
		t.Fatalf("valid candidate rejected: %v", err)
	}
	t.Run("wrong tenant rejected", func(t *testing.T) {
		x := want
		x.TenantID = "tenant-other"
		if !errors.Is(check(x), ErrReleaseCommitFence) {
			t.Fatal("wrong tenant passed")
		}
	})
	t.Run("wrong historical CREATE rejected", func(t *testing.T) {
		x := want
		x.CreateOperationID = deleteID
		if !errors.Is(check(x), ErrReleaseCommitFence) {
			t.Fatal("wrong create passed")
		}
	})
	t.Run("stale caller version rejected", func(t *testing.T) {
		x := want
		x.WorkloadVersion--
		if !errors.Is(check(x), ErrReleaseCommitFence) {
			t.Fatal("stale version passed")
		}
	})
	t.Run("successor operation UID rejected", func(t *testing.T) {
		exec(`UPDATE public.compute_operations SET target_resource_uid='successor-uid', version=version+1
            WHERE tenant_id=$1 AND compute_operation_id=$2`, tenant, deleteID)
		if !errors.Is(check(want), ErrReleaseCommitFence) {
			t.Fatal("new UID passed")
		}
		exec(`UPDATE public.compute_operations SET target_resource_uid=$3, version=version-1
            WHERE tenant_id=$1 AND compute_operation_id=$2`, tenant, deleteID, resourceUID)
	})
	t.Run("new desired generation rejects stale DELETE", func(t *testing.T) {
		exec(`UPDATE public.compute_workloads SET desired_generation=3, version=version+1
            WHERE tenant_id=$1 AND workload_id=$2`, tenant, workload)
		if !errors.Is(check(want), ErrReleaseCommitFence) {
			t.Fatal("stale DELETE was accepted")
		}
		exec(`UPDATE public.compute_workloads SET desired_generation=2, version=version-1
            WHERE tenant_id=$1 AND workload_id=$2`, tenant, workload)
	})
	t.Run("partial release phase rejected", func(t *testing.T) {
		exec(`UPDATE public.compute_allocations SET phase='BOUND', version=version+1
            WHERE tenant_id=$1 AND allocation_id=$2`, tenant, allocation)
		if !errors.Is(check(want), ErrReleaseCommitFence) {
			t.Fatal("non-releasing allocation passed")
		}
		exec(`UPDATE public.compute_allocations SET phase='RELEASING', version=version-1
            WHERE tenant_id=$1 AND allocation_id=$2`, tenant, allocation)
	})
	if err := check(want); err != nil {
		t.Fatalf("fixture not restored: %v", err)
	}
	// A matching snapshot is meaningful only if the caller's transaction
	// prevents concurrent writers from changing ANY of the four source rows.
	// PostgreSQL NOWAIT returns SQLSTATE 55P03 if a distinct transaction
	// cannot take an incompatible row lock. This checks actual row locking,
	// not merely snapshot fields or a single-connection pool timeout.
	t.Run("same transaction pins workload allocation create and delete rows", func(t *testing.T) {
		holder, err := db.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelReadCommitted})
		if err != nil {
			t.Fatal(err)
		}
		defer holder.Rollback()
		if err := ValidateReleaseCommitFence(ctx, holder, want); err != nil {
			t.Fatalf("failed to establish locked preflight: %v", err)
		}
		rows := []struct {
			name  string
			query string
			id    string
		}{
			{"workload", `SELECT version FROM public.compute_workloads
				WHERE tenant_id=$1 AND workload_id=$2 FOR UPDATE NOWAIT`, workload},
			{"allocation", `SELECT version FROM public.compute_allocations
				WHERE tenant_id=$1 AND allocation_id=$2 FOR UPDATE NOWAIT`, allocation},
			{"create operation", `SELECT version FROM public.compute_operations
				WHERE tenant_id=$1 AND compute_operation_id=$2 FOR UPDATE NOWAIT`, createID},
			{"delete operation", `SELECT version FROM public.compute_operations
				WHERE tenant_id=$1 AND compute_operation_id=$2 FOR UPDATE NOWAIT`, deleteID},
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
					t.Fatalf("independent transaction was not blocked by %s lock: version=%d err=%v", row.name, version, err)
				}
			})
		}
		if err := holder.Rollback(); err != nil {
			t.Fatalf("release preflight locks: %v", err)
		}
		// Positive control: the same row becomes lockable once the caller
		// releases its transaction. No protected release state is written.
		after, err := db.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelReadCommitted})
		if err != nil {
			t.Fatal(err)
		}
		defer after.Rollback()
		var version int64
		if err := after.QueryRowContext(ctx, rows[0].query, tenant, workload).Scan(&version); err != nil {
			t.Fatalf("workload lock remained unavailable after rollback: %v", err)
		}
	})
	var allocationPhase, workloadPhase string
	if err := db.QueryRowContext(ctx, `SELECT a.phase,w.observed_phase
        FROM public.compute_allocations a
        JOIN public.compute_workloads w ON w.tenant_id=a.tenant_id AND w.workload_id=a.workload_id
        WHERE a.tenant_id=$1 AND a.allocation_id=$2`, tenant, allocation).Scan(&allocationPhase, &workloadPhase); err != nil {
		t.Fatal(err)
	}
	if allocationPhase != "RELEASING" || workloadPhase != "RELEASING" {
		t.Fatalf("read-only fence unexpectedly changed state: alloc=%s workload=%s", allocationPhase, workloadPhase)
	}
}
