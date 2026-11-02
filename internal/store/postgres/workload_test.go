package postgres

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"sync"
	"testing"
	"time"

	"database/sql"

	_ "github.com/jackc/pgx/v5/stdlib"
)

func TestCreateWorkloadConcurrentIdempotentAndOutboxAtomic(t *testing.T) {
	db := openIntegrationDB(t)
	repo, err := NewRepository(db)
	if err != nil {
		t.Fatal(err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	request := json.RawMessage(`{"api_version":"compute.platform/v1","project_id":"project-int-1","resources":{"cpu":"2"}}`)
	digest := testDigest(request)
	tenantID := "tenant_it_" + randomSuffix(t)
	idempotencyKey := "idem_" + randomSuffix(t)

	const workers = 12
	results := make([]Workload, workers)
	created := make([]bool, workers)
	errs := make([]error, workers)
	var wg sync.WaitGroup
	wg.Add(workers)
	for i := 0; i < workers; i++ {
		i := i
		go func() {
			defer wg.Done()
			params := createParams(tenantID, idempotencyKey, digest, request, randomSuffix(t), randomSuffix(t))
			results[i], created[i], errs[i] = repo.CreateWorkload(ctx, params)
		}()
	}
	wg.Wait()

	createdCount := 0
	stableWorkloadID := ""
	for i := range results {
		if errs[i] != nil {
			t.Fatalf("concurrent CreateWorkload[%d]: %v", i, errs[i])
		}
		if created[i] {
			createdCount++
		}
		if stableWorkloadID == "" {
			stableWorkloadID = results[i].WorkloadID
		}
		if results[i].WorkloadID != stableWorkloadID {
			t.Fatalf("idempotent callers received different workload IDs: %q vs %q", stableWorkloadID, results[i].WorkloadID)
		}
	}
	if createdCount != 1 {
		t.Fatalf("created=true count = %d, want exactly one", createdCount)
	}

	var workloadCount, outboxCount int
	if err := db.QueryRowContext(ctx,
		`SELECT count(*) FROM compute_workloads WHERE tenant_id=$1 AND idempotency_key=$2`,
		tenantID, idempotencyKey).Scan(&workloadCount); err != nil {
		t.Fatal(err)
	}
	if err := db.QueryRowContext(ctx,
		`SELECT count(*) FROM compute_outbox_events WHERE tenant_id=$1 AND aggregate_id=$2`,
		tenantID, stableWorkloadID).Scan(&outboxCount); err != nil {
		t.Fatal(err)
	}
	if workloadCount != 1 || outboxCount != 1 {
		t.Fatalf("durable rows workload=%d outbox=%d, want 1/1", workloadCount, outboxCount)
	}

	changedRequest := json.RawMessage(`{"api_version":"compute.platform/v1","project_id":"project-int-1","resources":{"cpu":"4"}}`)
	changedDigest := testDigest(changedRequest)
	params := createParams(tenantID, idempotencyKey, changedDigest, changedRequest, randomSuffix(t), randomSuffix(t))
	if _, _, err := repo.CreateWorkload(ctx, params); !errors.Is(err, ErrIdempotencyConflict) {
		t.Fatalf("same key with changed digest error = %v, want ErrIdempotencyConflict", err)
	}
	if err := db.QueryRowContext(ctx,
		`SELECT count(*) FROM compute_workloads WHERE tenant_id=$1 AND idempotency_key=$2`,
		tenantID, idempotencyKey).Scan(&workloadCount); err != nil {
		t.Fatal(err)
	}
	if workloadCount != 1 {
		t.Fatalf("changed-digest request created another workload: count=%d", workloadCount)
	}
}

func TestCreateWorkloadRejectsInvalidParams(t *testing.T) {
	request := json.RawMessage(`{"project_id":"project-1"}`)
	valid := createParams("tenant-1", "idem-1", testDigest(request), request, "workload-1", "event-1")
	valid.ProjectID = "project-1"
	differentDigest := testDigest(json.RawMessage(`{"project_id":"other-project"}`))
	cases := []struct {
		name   string
		mutate func(*CreateWorkloadParams)
	}{
		{name: "missing tenant", mutate: func(p *CreateWorkloadParams) { p.TenantID = "" }},
		{name: "profile revision without profile", mutate: func(p *CreateWorkloadParams) { p.ComputeProfileRevision = "2" }},
		{name: "invalid JSON", mutate: func(p *CreateWorkloadParams) { p.NormalizedRequest = json.RawMessage(`{"broken"`) }},
		{name: "non-object JSON", mutate: func(p *CreateWorkloadParams) { p.NormalizedRequest = json.RawMessage(`[]`) }},
		{name: "invalid digest", mutate: func(p *CreateWorkloadParams) { p.RequestDigest = "sha256:BAD" }},
		{name: "digest does not bind request bytes", mutate: func(p *CreateWorkloadParams) { p.RequestDigest = differentDigest }},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			params := valid
			tc.mutate(&params)
			if err := validateCreateWorkload(params); !errors.Is(err, ErrInvalidRequest) {
				t.Fatalf("validation error = %v, want ErrInvalidRequest", err)
			}
		})
	}
}

func openIntegrationDB(t *testing.T) *sql.DB {
	t.Helper()
	dsn := os.Getenv("DATABASE_URL")
	if dsn == "" {
		t.Skip("DATABASE_URL is not configured; PostgreSQL integration test skipped")
	}
	db, err := sql.Open("pgx", dsn)
	if err != nil {
		t.Fatalf("open PostgreSQL: %v", err)
	}
	db.SetMaxOpenConns(16)
	db.SetMaxIdleConns(8)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := db.PingContext(ctx); err != nil {
		db.Close()
		t.Fatalf("ping PostgreSQL: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })
	return db
}

func createParams(tenantID, idempotencyKey, digest string, request json.RawMessage, workloadID, eventID string) CreateWorkloadParams {
	return CreateWorkloadParams{
		WorkloadID:        workloadID,
		TenantID:          tenantID,
		ProjectID:         "project-int-1",
		OwnerService:      "agent-runtime",
		OwnerResourceType: "run",
		OwnerResourceID:   "run-int-1",
		NormalizedRequest: request,
		RequestDigest:     digest,
		IdempotencyKey:    idempotencyKey,
		EventID:           eventID,
		CorrelationID:     "corr-int-1",
	}
}

func testDigest(request []byte) string {
	digest := sha256.Sum256(request)
	return "sha256:" + hex.EncodeToString(digest[:])
}

func randomSuffix(t *testing.T) string {
	t.Helper()
	var bytes [12]byte
	if _, err := rand.Read(bytes[:]); err != nil {
		t.Fatal(err)
	}
	return fmt.Sprintf("%x", bytes[:])
}

func TestCreateWorkloadIdempotencyRejectsChangedOwnership(t *testing.T) {
	db := openIntegrationDB(t)
	repo, err := NewRepository(db)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	request := json.RawMessage(`{"api_version":"compute.platform/v1","project_id":"project-int-1","resources":{"cpu":"2"}}`)
	p := createParams("tenant-ownership-"+randomSuffix(t), "idem-"+randomSuffix(t),
		testDigest(request), request, "workload-"+randomSuffix(t), "event-"+randomSuffix(t))
	original, created, err := repo.CreateWorkload(ctx, p)
	if err != nil || !created {
		t.Fatalf("initial create: %+v %v", original, err)
	}

	cases := []struct {
		name   string
		mutate func(*CreateWorkloadParams)
	}{
		{"project", func(p *CreateWorkloadParams) {
			p.ProjectID = "project-foreign"
			p.NormalizedRequest = json.RawMessage(`{"api_version":"compute.platform/v1","project_id":"project-foreign","resources":{"cpu":"2"}}`)
			p.RequestDigest = testDigest(p.NormalizedRequest)
		}},
		{"owner service", func(p *CreateWorkloadParams) { p.OwnerService = "sandbox-provider" }},
		{"owner resource type", func(p *CreateWorkloadParams) { p.OwnerResourceType = "sandbox" }},
		{"owner resource id", func(p *CreateWorkloadParams) { p.OwnerResourceID = "run-foreign" }},
		{"compute profile", func(p *CreateWorkloadParams) {
			p.ComputeProfileID = "profile-1"
			p.ComputeProfileRevision = "revision-1"
		}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			changed := p
			tc.mutate(&changed)
			changed.WorkloadID = "workload-replay-" + randomSuffix(t)
			changed.EventID = "event-replay-" + randomSuffix(t)
			if _, created, err := repo.CreateWorkload(ctx, changed); !errors.Is(err, ErrIdempotencyConflict) || created {
				t.Fatalf("changed ownership replay: created=%t err=%v, want ErrIdempotencyConflict", created, err)
			}
		})
	}
	replay := p
	replay.WorkloadID = "workload-replay-" + randomSuffix(t)
	replay.EventID = "event-replay-" + randomSuffix(t)
	got, created, err := repo.CreateWorkload(ctx, replay)
	if err != nil || created || got.WorkloadID != original.WorkloadID {
		t.Fatalf("exact replay mismatch: created=%t got=%+v err=%v", created, got, err)
	}
	var count int
	if err := db.QueryRowContext(ctx,
		`SELECT COUNT(*) FROM compute_outbox_events WHERE tenant_id=$1
			AND aggregate_type='compute_workload' AND aggregate_id=$2`,
		p.TenantID, original.WorkloadID).Scan(&count); err != nil {
		t.Fatal(err)
	}
	if count != 1 {
		t.Fatalf("changed ownership emitted %d events, want 1", count)
	}
}

func TestCreateWorkloadRejectsNormalizedRequestProjectMismatch(t *testing.T) {
	request := json.RawMessage(`{"api_version":"compute.platform/v1","project_id":"project-body","resources":{"cpu":"2"}}`)
	params := createParams("tenant-project-mismatch", "idem-project-mismatch",
		testDigest(request), request, "workload-project-mismatch", "event-project-mismatch")
	params.ProjectID = "project-metadata"
	if err := validateCreateWorkload(params); !errors.Is(err, ErrInvalidRequest) {
		t.Fatalf("request body project_id mismatch accepted: %v", err)
	}
}

func TestCreateWorkloadRejectsDuplicateNormalizedJSONKeys(t *testing.T) {
	cases := []struct {
		name    string
		request json.RawMessage
	}{
		{
			name:    "duplicate project id",
			request: json.RawMessage(`{"api_version":"compute.platform/v1","project_id":"project-a","project_id":"project-b","resources":{"cpu":"2"}}`),
		},
		{
			name:    "nested duplicate resource key",
			request: json.RawMessage(`{"api_version":"compute.platform/v1","project_id":"project-a","resources":{"cpu":"2","cpu":"8"}}`),
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			params := createParams("tenant-duplicate-json", "idem-"+tc.name,
				testDigest(tc.request), tc.request, "workload-"+tc.name, "event-"+tc.name)
			if err := validateCreateWorkload(params); !errors.Is(err, ErrInvalidRequest) {
				t.Fatalf("ambiguous normalized request accepted: %v", err)
			}
		})
	}
}
