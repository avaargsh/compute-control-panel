package recovery_test

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"errors"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/avaargsh/compute-control-panel/internal/operation"
	"github.com/avaargsh/compute-control-panel/internal/provider"
	"github.com/avaargsh/compute-control-panel/internal/recovery"
	"github.com/avaargsh/compute-control-panel/internal/store/postgres"
	_ "github.com/jackc/pgx/v5/stdlib"
)

func uniqueID(t *testing.T) string {
	t.Helper()
	var n [12]byte
	if _, err := rand.Read(n[:]); err != nil {
		t.Fatal(err)
	}
	return hex.EncodeToString(n[:])
}

func setupUnknown(t *testing.T) (*sql.DB, *postgres.Repository, operation.Operation) {
	t.Helper()
	dsn := os.Getenv("DATABASE_URL")
	if dsn == "" {
		t.Skip("DATABASE_URL not configured")
	}
	db, err := sql.Open("pgx", dsn)
	if err != nil {
		t.Fatal(err)
	}
	db.SetMaxOpenConns(8)
	t.Cleanup(func() { _ = db.Close() })
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := db.PingContext(ctx); err != nil {
		t.Fatal(err)
	}
	repo, err := postgres.NewRepository(db)
	if err != nil {
		t.Fatal(err)
	}
	request := []byte(`{"resources":{"cpu":"2"}}`)
	hash := sha256.Sum256(request)
	digest := "sha256:" + hex.EncodeToString(hash[:])
	tenant := "tenant-recovery-" + uniqueID(t)
	wID := "workload-" + uniqueID(t)
	_, created, err := repo.CreateWorkload(ctx, postgres.CreateWorkloadParams{
		WorkloadID: wID, TenantID: tenant, ProjectID: "project-1",
		OwnerService: "agent-runtime", OwnerResourceType: "run",
		OwnerResourceID:   "run-" + uniqueID(t),
		NormalizedRequest: request, RequestDigest: digest,
		IdempotencyKey: "workload-idem-" + uniqueID(t),
		EventID:        "workload-event-" + uniqueID(t), CorrelationID: "corr-1",
	})
	if err != nil || !created {
		t.Fatalf("create workload: %t %v", created, err)
	}
	op := operation.Operation{
		ID: "operation-" + uniqueID(t), TenantID: tenant,
		WorkloadID: wID, ProviderID: "provider-fake-1",
		Kind: operation.Create, Status: operation.Pending,
		Generation: 1, Version: 1, RequestDigest: "sha256:" + strings.Repeat("a", 64),
		IdempotencyKey: "provider-idem-" + uniqueID(t),
	}
	if _, _, err := repo.CreateOperation(ctx, op, "operation-event-"+uniqueID(t), "corr-2"); err != nil {
		t.Fatal(err)
	}
	claimed, err := repo.ClaimOperation(ctx, tenant, op.ID, 1, "worker-1", time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	started, err := repo.StartOperation(ctx, tenant, op.ID, claimed.Version, "worker-1", claimed.FenceToken)
	if err != nil {
		t.Fatal(err)
	}
	unknown, err := repo.MarkOperationUnknown(ctx, tenant, op.ID, started.Version, "worker-1", started.FenceToken)
	if err != nil {
		t.Fatal(err)
	}
	return db, repo, unknown
}

func adapterFor(fake provider.Adapter) map[string]recovery.RegisteredProvider {
	return map[string]recovery.RegisteredProvider{
		"provider-fake-1": {Adapter: fake, Source: "fake-integration", AdapterVersion: "v0-test"},
	}
}

func TestRecoverUnknownInspectsWithoutDispatchAndCommitsEvidence(t *testing.T) {
	db, repo, op := setupUnknown(t)
	fake := provider.NewFakeProvider()
	ctx := context.Background()
	req := provider.Request{
		OperationID: op.ID, TenantID: op.TenantID, WorkloadID: op.WorkloadID,
		ProviderID: op.ProviderID, IdempotencyKey: op.IdempotencyKey,
		RequestDigest: op.RequestDigest, DesiredGeneration: op.Generation, Kind: op.Kind,
	}
	fake.LoseNextAcknowledgement()
	if _, err := fake.Apply(ctx, req); !errors.Is(err, provider.ErrAcknowledgementLost) {
		t.Fatalf("fake must apply before losing ACK: %v", err)
	}
	// Recreated worker and repository have no in-memory operation state.
	restarted, err := postgres.NewRepository(db)
	if err != nil {
		t.Fatal(err)
	}
	worker, err := recovery.NewWorker(restarted, adapterFor(fake))
	if err != nil {
		t.Fatal(err)
	}
	result, err := worker.RecoverUnknown(ctx, op.TenantID, op.ID, "reconciler", time.Minute)
	if err != nil || result.Status != operation.Succeeded || result.ProviderResourceUID == "" {
		t.Fatalf("inspect-first recovery: %+v %v", result, err)
	}
	var evidenceCount, eventCount int
	if err := db.QueryRowContext(ctx, "SELECT count(*) FROM compute_resource_observations WHERE tenant_id=$1 AND compute_operation_id=$2", op.TenantID, op.ID).Scan(&evidenceCount); err != nil {
		t.Fatal(err)
	}
	if err := db.QueryRowContext(ctx, "SELECT count(*) FROM compute_outbox_events WHERE tenant_id=$1 AND aggregate_type='compute_operation' AND aggregate_id=$2", op.TenantID, op.ID).Scan(&eventCount); err != nil {
		t.Fatal(err)
	}
	if evidenceCount != 1 || eventCount != 2 {
		t.Fatalf("recovery did not atomically persist inspection and event: evidence=%d event=%d", evidenceCount, eventCount)
	}
	if _, err := worker.RecoverUnknown(ctx, op.TenantID, op.ID, "other-worker", time.Minute); !errors.Is(err, operation.ErrTransition) {
		t.Fatalf("final operation was retried: %v", err)
	}
	if _, err := worker.RecoverUnknown(ctx, "foreign-tenant", op.ID, "worker", time.Minute); !errors.Is(err, sql.ErrNoRows) {
		t.Fatalf("cross-tenant recovery leaked: %v", err)
	}
}

func TestRecoverUnknownRefusesUnregisteredProviderBeforeLease(t *testing.T) {
	_, repo, op := setupUnknown(t)
	worker, err := recovery.NewWorker(repo, map[string]recovery.RegisteredProvider{})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := worker.RecoverUnknown(context.Background(), op.TenantID, op.ID, "worker", time.Minute); !errors.Is(err, recovery.ErrUnregisteredProvider) {
		t.Fatalf("unregistered provider recovery accepted: %v", err)
	}
	persisted, err := repo.GetOperation(context.Background(), op.TenantID, op.ID)
	if err != nil || persisted.Status != operation.Unknown {
		t.Fatalf("unregistered provider mutated operation: %+v %v", persisted, err)
	}
}

type brokenInspector struct{}

func (brokenInspector) Apply(context.Context, provider.Request) (provider.Acknowledgement, error) {
	panic("recover must never call Apply")
}
func (brokenInspector) Inspect(context.Context, provider.Request) (operation.Observation, error) {
	return operation.Observation{}, errors.New("provider unavailable")
}

func TestInspectFailureRetainsFencedReconciliationAndDoesNotDispatch(t *testing.T) {
	_, repo, op := setupUnknown(t)
	worker, err := recovery.NewWorker(repo, adapterFor(brokenInspector{}))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := worker.RecoverUnknown(context.Background(), op.TenantID, op.ID, "worker", time.Minute); err == nil {
		t.Fatal("unavailable provider was accepted")
	}
	after, err := repo.GetOperation(context.Background(), op.TenantID, op.ID)
	if err != nil || after.Status != operation.Reconciling || after.FenceToken <= op.FenceToken {
		t.Fatalf("failed inspect discarded fenced recovery lease: %+v %v", after, err)
	}
}
