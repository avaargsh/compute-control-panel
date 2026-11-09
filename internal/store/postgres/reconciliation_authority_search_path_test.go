package postgres

import (
	"testing"

	"github.com/avaargsh/compute-control-panel/internal/operation"
)

// A successful inspection must persist authoritative evidence. A session-local
// relation must not intercept the evidence INSERT while the real ComputeOperation
// is transitioned to SUCCEEDED and a permanent Outbox event is emitted.
func TestReconcileOperationWritesPermanentObservationDespiteTempShadow(t *testing.T) {
	repo, db, ctx, current := setupReconciliation(t)
	db.SetMaxOpenConns(1)
	db.SetMaxIdleConns(1)

	if _, err := db.ExecContext(ctx, `CREATE TEMP TABLE compute_resource_observations
		(LIKE public.compute_resource_observations INCLUDING DEFAULTS)
		ON COMMIT PRESERVE ROWS`); err != nil {
		t.Fatal(err)
	}
	evidence := inspectionEvidence(current, trustedObservation(current, operation.ResourcePresent))
	next, err := repo.ReconcileOperation(ctx, current.TenantID, current.ID, current.Version,
		"reconciler", current.FenceToken, evidence)
	if err != nil || next.Status != operation.Succeeded {
		t.Fatalf("inspection could not resolve operation: next=%+v, err=%v", next, err)
	}

	var durable, shadow int
	if err := db.QueryRowContext(ctx,
		"SELECT count(*) FROM public.compute_resource_observations WHERE observation_id=$1",
		evidence.ObservationID).Scan(&durable); err != nil {
		t.Fatal(err)
	}
	if err := db.QueryRowContext(ctx,
		"SELECT count(*) FROM pg_temp.compute_resource_observations WHERE observation_id=$1",
		evidence.ObservationID).Scan(&shadow); err != nil {
		t.Fatal(err)
	}
	if durable != 1 || shadow != 0 {
		t.Fatalf("successful inspection diverted authoritative evidence: permanent=%d temp=%d", durable, shadow)
	}
}

// A stale generation must be read from CCP's permanent Workload, never from
// a caller/session-local table that falsely claims the Workload advanced.
func TestReconcileOperationIgnoresTempWorkloadGeneration(t *testing.T) {
	repo, db, ctx, current := setupReconciliation(t)
	db.SetMaxOpenConns(1)
	db.SetMaxIdleConns(1)

	if _, err := db.ExecContext(ctx, `CREATE TEMP TABLE compute_workloads (
		tenant_id text NOT NULL, workload_id text NOT NULL, desired_generation bigint NOT NULL
	) ON COMMIT PRESERVE ROWS`); err != nil {
		t.Fatal(err)
	}
	if _, err := db.ExecContext(ctx, `INSERT INTO pg_temp.compute_workloads
		(tenant_id, workload_id, desired_generation) VALUES ($1,$2,$3)`,
		current.TenantID, current.WorkloadID, int64(current.Generation+1)); err != nil {
		t.Fatal(err)
	}
	var authoritative int64
	if err := db.QueryRowContext(ctx,
		`SELECT desired_generation FROM public.compute_workloads WHERE tenant_id=$1 AND workload_id=$2`,
		current.TenantID, current.WorkloadID).Scan(&authoritative); err != nil {
		t.Fatal(err)
	}
	if authoritative != int64(current.Generation) {
		t.Fatalf("unexpected actual Workload generation: %d", authoritative)
	}

	evidence := inspectionEvidence(current, trustedObservation(current, operation.ResourcePresent))
	next, err := repo.ReconcileOperation(ctx, current.TenantID, current.ID, current.Version,
		"reconciler", current.FenceToken, evidence)
	if err != nil || next.Status != operation.Succeeded || next.CleanupRequired || next.ReconcileRequired {
		t.Fatalf("TEMP generation incorrectly superseded authoritative Workload: next=%+v err=%v", next, err)
	}
}
