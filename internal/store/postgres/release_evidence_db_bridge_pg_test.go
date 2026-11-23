package postgres

import (
	"context"
	"database/sql"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/avaargsh/compute-control-panel/internal/provider"
)

// This is an actual PostgreSQL 16 integration test, not a fake transaction.
// The test harness skips only when DATABASE_URL is absent. A matching
// correlation is still caller-supplied and therefore NOT release authority.
func TestCorrelatedReleasePreflightChecksCurrentPostgresRows(t *testing.T) {
	db := openIntegrationDB(t)
	repo, err := NewRepository(db)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	suffix := randomSuffix(t)
	params := acceptanceFixture(t, "tenant-release-bridge-"+suffix,
		"idempotency-w-bridge-"+suffix, "idempotency-c-bridge-"+suffix,
		"event-create-bridge-"+suffix)
	accepted, created, err := repo.AcceptWorkload(ctx, params)
	if err != nil || !created {
		t.Fatalf("accept: created=%t err=%v", created, err)
	}
	tenant := params.Workload.TenantID
	workload := accepted.Workload.WorkloadID
	allocation := accepted.Allocation.AllocationID
	createID := accepted.Operation.ID
	providerID := accepted.Allocation.ProviderID
	deleteID := "delete-bridge-" + suffix
	resourceID, resourceUID := tenant+"/job-a", "job-uid-"+suffix

	updateOne := func(sqlText string, args ...any) {
		t.Helper()
		result, err := db.ExecContext(ctx, sqlText, args...)
		if err != nil {
			t.Fatalf("prepare current-state fixture: %v", err)
		}
		n, err := result.RowsAffected()
		if err != nil || n != 1 {
			t.Fatalf("fixture affected=%d err=%v", n, err)
		}
	}
	updateOne(`UPDATE public.compute_operations
		SET status='SUCCEEDED', provider_resource_id=$3, provider_resource_uid=$4,
			version=version+1 WHERE tenant_id=$1 AND compute_operation_id=$2`,
		tenant, createID, resourceID, resourceUID)
	updateOne(`UPDATE public.compute_workloads SET desired_generation=2,
		desired_state='DELETE_REQUESTED', observed_phase='RELEASING',
		version=version+1 WHERE tenant_id=$1 AND workload_id=$2`, tenant, workload)
	updateOne(`UPDATE public.compute_allocations SET phase='RELEASING',
		version=version+1 WHERE tenant_id=$1 AND allocation_id=$2`, tenant, allocation)
	updateOne(`INSERT INTO public.compute_operations(
		compute_operation_id,tenant_id,workload_id,allocation_id,provider_id,
		operation_kind,desired_generation,provider_request_digest,
		idempotency_key,status,target_resource_id,target_resource_uid)
		VALUES ($1,$2,$3,NULL,$4,'DELETE',2,$5,$6,'SUCCEEDED',$7,$8)`,
		deleteID, tenant, workload, providerID,
		"sha256:"+strings.Repeat("c", 64), "idem-delete-bridge-"+suffix,
		resourceID, resourceUID)

	fence := ReleaseCommitFence{
		TenantID: tenant, WorkloadID: workload, AllocationID: allocation,
		ProviderID: providerID, CreateOperationID: createID, DeleteOperationID: deleteID,
		TargetResourceID: resourceID, TargetResourceUID: resourceUID,
		AllocationGeneration: 1, DeleteGeneration: 2, WorkloadVersion: 2,
		AllocationVersion: 2, CreateOperationVersion: 2, DeleteOperationVersion: 1,
	}
	corr := correlatedReleaseFixture(fence)
	probe := func(f ReleaseCommitFence, c provider.ReleaseEvidenceCorrelation) error {
		t.Helper()
		tx, err := db.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelReadCommitted})
		if err != nil {
			t.Fatal(err)
		}
		defer tx.Rollback()
		return ValidateCorrelatedReleasePreflight(ctx, tx, f, c)
	}
	if err := probe(fence, corr); err != nil {
		t.Fatalf("current locked Generation/UID identity mismatch: %v", err)
	}
	t.Run("different tenant", func(t *testing.T) {
		bad := corr
		bad.TenantID = "tenant-other"
		if !errors.Is(probe(fence, bad), ErrReleaseCommitFence) {
			t.Fatal("wrong tenant correlation crossed durable fence")
		}
	})
	t.Run("successor Job UID", func(t *testing.T) {
		bad := corr
		bad.Job.UID = "new-uid"
		if !errors.Is(probe(fence, bad), ErrReleaseCommitFence) {
			t.Fatal("successor Job UID crossed durable fence")
		}
	})
	t.Run("stale current workload version", func(t *testing.T) {
		stale := fence
		stale.WorkloadVersion--
		if !errors.Is(probe(stale, corr), ErrReleaseCommitFence) {
			t.Fatal("stale durable Workload version crossed fence")
		}
	})
	t.Run("newer desired generation", func(t *testing.T) {
		updateOne(`UPDATE public.compute_workloads SET desired_generation=3,
			version=version+1 WHERE tenant_id=$1 AND workload_id=$2`, tenant, workload)
		if !errors.Is(probe(fence, corr), ErrReleaseCommitFence) {
			t.Fatal("superseded DELETE remained eligible")
		}
	})
	var phase, desiredState string
	if err := db.QueryRowContext(ctx, `SELECT a.phase,w.desired_state
		FROM public.compute_allocations a
		JOIN public.compute_workloads w ON w.tenant_id=a.tenant_id
			AND w.workload_id=a.workload_id
		WHERE a.tenant_id=$1 AND a.allocation_id=$2`,
		tenant, allocation).Scan(&phase, &desiredState); err != nil {
		t.Fatal(err)
	}
	if phase != "RELEASING" || desiredState != "DELETE_REQUESTED" {
		t.Fatalf("read-only preflight changed protected state: allocation=%q workload=%q", phase, desiredState)
	}
}
