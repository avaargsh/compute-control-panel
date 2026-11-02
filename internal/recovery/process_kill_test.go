package recovery_test

import (
	"bufio"
	"bytes"
	"context"
	"database/sql"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"testing"
	"time"

	"github.com/avaargsh/compute-control-panel/internal/operation"
	"github.com/avaargsh/compute-control-panel/internal/provider"
	"github.com/avaargsh/compute-control-panel/internal/recovery"
	"github.com/avaargsh/compute-control-panel/internal/store/postgres"
)

// The helper runs as a genuine OS subprocess, obtains and commits a recovery
// lease, signals the parent, and intentionally never completes inspection.
// The parent SIGKILLs it and proves PostgreSQL's lease survives the crash.
func TestRecoveryLeaseHolderSubprocess(t *testing.T) {
	if os.Getenv("COMPUTE_RECOVERY_KILL_HELPER") != "1" {
		t.Skip("subprocess-only lease holder")
	}
	db, err := sql.Open("pgx", os.Getenv("DATABASE_URL"))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	repo, err := postgres.NewRepository(db)
	if err != nil {
		t.Fatal(err)
	}
	tenantID := os.Getenv("COMPUTE_RECOVERY_TENANT")
	operationID := os.Getenv("COMPUTE_RECOVERY_OPERATION")
	current, err := repo.GetOperation(context.Background(), tenantID, operationID)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := repo.BeginOperationReconciliation(
		context.Background(), tenantID, operationID, current.Version, "", 0,
		"killed-worker", 100*time.Millisecond,
	); err != nil {
		t.Fatal(err)
	}
	fmt.Println("LEASE_CLAIMED")
	time.Sleep(30 * time.Second)
	t.Fatal("helper was not killed")
}

func TestSIGKILLRecoveryLeaseIsFencedBeforeInspection(t *testing.T) {
	db, repo, unknown := setupUnknown(t)
	ctx := context.Background()
	fake := provider.NewFakeProvider()
	req := provider.Request{
		OperationID:       unknown.ID,
		TenantID:          unknown.TenantID,
		WorkloadID:        unknown.WorkloadID,
		ProviderID:        unknown.ProviderID,
		IdempotencyKey:    unknown.IdempotencyKey,
		RequestDigest:     unknown.RequestDigest,
		DesiredGeneration: unknown.Generation,
		Kind:              unknown.Kind,
	}
	fake.LoseNextAcknowledgement()
	if _, err := fake.Apply(ctx, req); !errors.Is(err, provider.ErrAcknowledgementLost) {
		t.Fatalf("fake side effect must precede lost ACK: %v", err)
	}
	processCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	cmd := exec.CommandContext(processCtx, os.Args[0], "-test.run=^TestRecoveryLeaseHolderSubprocess$")
	cmd.Env = append(os.Environ(),
		"COMPUTE_RECOVERY_KILL_HELPER=1",
		"COMPUTE_RECOVERY_TENANT="+unknown.TenantID,
		"COMPUTE_RECOVERY_OPERATION="+unknown.ID,
	)
	var stderr bytes.Buffer
	cmd.Stderr = &stderr
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		t.Fatal(err)
	}
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	defer func() { _ = cmd.Process.Kill() }()
	scanner := bufio.NewScanner(stdout)
	if !scanner.Scan() || scanner.Text() != "LEASE_CLAIMED" {
		_ = cmd.Wait()
		t.Fatalf("subprocess did not persist lease: output=%q stderr=%q", scanner.Text(), stderr.String())
	}
	leased, err := repo.GetOperation(ctx, unknown.TenantID, unknown.ID)
	if err != nil || leased.Status != operation.Reconciling || leased.FenceToken <= unknown.FenceToken {
		t.Fatalf("child did not commit fenced lease: %+v %v", leased, err)
	}
	if err := cmd.Process.Kill(); err != nil {
		t.Fatal(err)
	}
	_ = cmd.Wait()
	afterKill, err := repo.GetOperation(ctx, unknown.TenantID, unknown.ID)
	if err != nil || afterKill.Status != operation.Reconciling || afterKill.FenceToken != leased.FenceToken {
		t.Fatalf("SIGKILL lost or rewrote durable lease: %+v %v", afterKill, err)
	}
	// The stale owner must not be bypassed while its committed lease is active.
	if _, err := repo.BeginOperationReconciliation(
		ctx, unknown.TenantID, unknown.ID, afterKill.Version, "", 0,
		"new-worker-early", time.Minute,
	); !errors.Is(err, operation.ErrTransition) {
		t.Fatalf("active recovery lease was stolen: %v", err)
	}
	time.Sleep(220 * time.Millisecond)
	worker, err := recovery.NewWorker(repo, adapterFor(inspectOnly{Adapter: fake}))
	if err != nil {
		t.Fatal(err)
	}
	recovered, err := worker.RecoverUnknown(
		ctx, unknown.TenantID, unknown.ID, "after-sigkill", time.Minute,
	)
	if err != nil || recovered.Status != operation.Succeeded ||
		recovered.ProviderResourceUID == "" || recovered.FenceToken <= leased.FenceToken {
		t.Fatalf("inspect-first SIGKILL recovery failed: %+v %v", recovered, err)
	}
	var observations int
	if err := db.QueryRowContext(ctx,
		"SELECT count(*) FROM compute_resource_observations WHERE tenant_id=$1 AND compute_operation_id=$2",
		unknown.TenantID, unknown.ID,
	).Scan(&observations); err != nil || observations != 1 {
		t.Fatalf("recovered operation had %d observation rows: %v", observations, err)
	}
}
