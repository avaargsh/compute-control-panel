package httpapi

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/avaargsh/compute-control-panel/internal/operation"
	"github.com/avaargsh/compute-control-panel/internal/store/postgres"
)

type testWorkloadReadAuthorizer struct {
	err   error
	calls int
}

func (a *testWorkloadReadAuthorizer) AuthorizeRead(context.Context, WorkloadPrincipal, postgres.Workload) error {
	a.calls++
	return a.err
}

type testWorkloadReadRepository struct {
	details postgres.WorkloadDetails
	err     error
	tenantID string
	workloadID string
	calls   int
}

func (r *testWorkloadReadRepository) GetWorkloadDetails(_ context.Context, tenantID, workloadID string) (postgres.WorkloadDetails, error) {
	r.calls++
	r.tenantID = tenantID
	r.workloadID = workloadID
	return r.details, r.err
}

func newReadFixture() (WorkloadPrincipal, *testWorkloadReadAuthorizer, *testWorkloadReadRepository) {
	principal := WorkloadPrincipal{
		TenantID: "tenant-read-001",
		Subject: "spiffe://example.org/ns/agent-runtime/sa/runtime",
		Service: "agent-runtime",
		ProjectIDs: []string{"project-read-001"},
	}
	now := time.Date(2026, 11, 4, 8, 0, 0, 0, time.UTC)
	authorizer := &testWorkloadReadAuthorizer{}
	repository := &testWorkloadReadRepository{
		details: postgres.WorkloadDetails{
			Workload: postgres.Workload{
				WorkloadID: "workload-read-001",
				TenantID: "tenant-read-001",
				ProjectID: "project-read-001",
				DesiredGeneration: 2,
				Version: 4,
				DesiredState: "ACTIVE",
				ObservedPhase: "PROVISIONING",
				LatestAllocationID: sql.NullString{String: "allocation-read-002", Valid: true},
				CreatedAt: now.Add(-time.Hour),
				UpdatedAt: now,
			},
			Allocations: []postgres.Allocation{
				{AllocationID: "allocation-read-001", TenantID: "tenant-read-001", WorkloadID: "workload-read-001", ProviderID: "provider-old", PoolID: "pool-old", DesiredGeneration: 1, Phase: "RELEASED", Version: 3},
				{AllocationID: "allocation-read-002", TenantID: "tenant-read-001", WorkloadID: "workload-read-001", ProviderID: "provider-current", PoolID: "pool-current", DesiredGeneration: 2, Phase: "PENDING", Version: 1},
			},
			Operations: []operation.Operation{
				{ID: "operation-read-001", TenantID: "tenant-read-001", WorkloadID: "workload-read-001", AllocationID: "allocation-read-001", ProviderID: "provider-old", IdempotencyKey: "idem-old", RequestDigest: "sha256:" + strings.Repeat("a", 64), Generation: 1, Version: 3, Kind: operation.Create, Status: operation.Succeeded, ProviderResourceID: "provider-resource-old", ProviderResourceUID: "provider-uid-secret"},
				{ID: "operation-read-002", TenantID: "tenant-read-001", WorkloadID: "workload-read-001", AllocationID: "allocation-read-002", ProviderID: "provider-current", IdempotencyKey: "idem-current", RequestDigest: "sha256:" + strings.Repeat("b", 64), Generation: 2, Version: 1, Kind: operation.Create, Status: operation.Pending},
			},
		},
	}
	return principal, authorizer, repository
}

func newWorkloadReadTestHandler(t *testing.T, principal WorkloadPrincipal, authErr error, authorizer *testWorkloadReadAuthorizer, repository *testWorkloadReadRepository) http.Handler {
	t.Helper()
	handler, err := NewWorkloadReadHandler(WorkloadReadDependencies{
		Authenticator: testPrincipalAuthenticator{principal: principal, err: authErr},
		Authorizer: authorizer,
		Repository: repository,
	})
	if err != nil {
		t.Fatalf("NewWorkloadReadHandler: %v", err)
	}
	return handler
}

func performWorkloadRead(handler http.Handler, method, path string) *httptest.ResponseRecorder {
	request := httptest.NewRequest(method, path, nil)
	recorder := httptest.NewRecorder()
	handler.ServeHTTP(recorder, request)
	return recorder
}

func TestGetWorkloadRequiresAuthenticatedPrincipal(t *testing.T) {
	principal, authorizer, repository := newReadFixture()
	handler := newWorkloadReadTestHandler(t, principal, errors.New("invalid certificate"), authorizer, repository)

	recorder := performWorkloadRead(handler, http.MethodGet, "/api/v1/workloads/workload-read-001")
	if recorder.Code != http.StatusUnauthorized {
		t.Fatalf("status = %d, want 401; body=%s", recorder.Code, recorder.Body.String())
	}
	if repository.calls != 0 || authorizer.calls != 0 {
		t.Fatalf("unauthenticated request reached read dependencies: repo=%d authorizer=%d", repository.calls, authorizer.calls)
	}
}

func TestGetWorkloadIsTenantScopedAndHidesOutOfProjectResources(t *testing.T) {
	principal, authorizer, repository := newReadFixture()
	principal.TenantID = "attacker-tenant"
	handler := newWorkloadReadTestHandler(t, principal, nil, authorizer, repository)

	recorder := performWorkloadRead(handler, http.MethodGet, "/api/v1/workloads/workload-read-001")
	if recorder.Code != http.StatusNotFound {
		t.Fatalf("status = %d, want 404; body=%s", recorder.Code, recorder.Body.String())
	}
	if repository.tenantID != "attacker-tenant" || authorizer.calls != 0 {
		t.Fatalf("read did not use authenticated tenant or leaked project existence: tenant=%q authorizer=%d", repository.tenantID, authorizer.calls)
	}
}

func TestGetWorkloadRequiresProjectGrant(t *testing.T) {
	principal, authorizer, repository := newReadFixture()
	principal.ProjectIDs = nil
	handler := newWorkloadReadTestHandler(t, principal, nil, authorizer, repository)

	recorder := performWorkloadRead(handler, http.MethodGet, "/api/v1/workloads/workload-read-001")
	if recorder.Code != http.StatusNotFound {
		t.Fatalf("status = %d, want 404; body=%s", recorder.Code, recorder.Body.String())
	}
	if repository.calls != 1 || authorizer.calls != 0 {
		t.Fatalf("out-of-project history reached authorizer: repo=%d authorizer=%d", repository.calls, authorizer.calls)
	}
}

func TestGetWorkloadRequiresReadAuthorization(t *testing.T) {
	principal, authorizer, repository := newReadFixture()
	authorizer.err = errors.New("read denied")
	handler := newWorkloadReadTestHandler(t, principal, nil, authorizer, repository)

	recorder := performWorkloadRead(handler, http.MethodGet, "/api/v1/workloads/workload-read-001")
	if recorder.Code != http.StatusForbidden {
		t.Fatalf("status = %d, want 403; body=%s", recorder.Code, recorder.Body.String())
	}
	if repository.calls != 1 || authorizer.calls != 1 {
		t.Fatalf("authorization calls repo=%d authorizer=%d", repository.calls, authorizer.calls)
	}
	if strings.Contains(recorder.Body.String(), "read denied") {
		t.Fatal("read authorizer internal error leaked to response")
	}
}

func TestGetWorkloadReturnsAllocationAndOperationHistoryWithoutProviderSecrets(t *testing.T) {
	principal, authorizer, repository := newReadFixture()
	handler := newWorkloadReadTestHandler(t, principal, nil, authorizer, repository)

	recorder := performWorkloadRead(handler, http.MethodGet, "/api/v1/workloads/workload-read-001")
	if recorder.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200; body=%s", recorder.Code, recorder.Body.String())
	}
	if recorder.Header().Get("Cache-Control") != "no-store" {
		t.Fatalf("Cache-Control = %q", recorder.Header().Get("Cache-Control"))
	}
	if repository.tenantID != principal.TenantID || repository.workloadID != "workload-read-001" || authorizer.calls != 1 {
		t.Fatalf("unexpected read scope: tenant=%q workload=%q authorizer=%d", repository.tenantID, repository.workloadID, authorizer.calls)
	}
	var response struct {
		Workload struct {
			WorkloadID string `json:"workload_id"`
			ProjectID string `json:"project_id"`
			DesiredGeneration int64 `json:"desired_generation"`
			ObservedPhase string `json:"observed_phase"`
			LatestAllocationID string `json:"latest_allocation_id"`
		} `json:"workload"`
		Allocations []struct {
			AllocationID string `json:"allocation_id"`
			ProviderID string `json:"provider_id"`
			Phase string `json:"phase"`
		} `json:"allocations"`
		Operations []struct {
			OperationID string `json:"compute_operation_id"`
			AllocationID string `json:"allocation_id"`
			Status string `json:"status"`
		} `json:"operations"`
	}
	if err := json.Unmarshal(recorder.Body.Bytes(), &response); err != nil {
		t.Fatal(err)
	}
	if response.Workload.WorkloadID != "workload-read-001" ||
		response.Workload.ProjectID != "project-read-001" ||
		response.Workload.DesiredGeneration != 2 ||
		response.Workload.ObservedPhase != "PROVISIONING" ||
		response.Workload.LatestAllocationID != "allocation-read-002" {
		t.Fatalf("unexpected Workload projection: %+v", response.Workload)
	}
	if len(response.Allocations) != 2 || response.Allocations[1].AllocationID != "allocation-read-002" ||
		response.Allocations[1].ProviderID != "provider-current" || response.Allocations[1].Phase != "PENDING" {
		t.Fatalf("unexpected Allocation history: %+v", response.Allocations)
	}
	if len(response.Operations) != 2 || response.Operations[1].OperationID != "operation-read-002" ||
		response.Operations[1].AllocationID != "allocation-read-002" || response.Operations[1].Status != "PENDING" {
		t.Fatalf("unexpected Operation history: %+v", response.Operations)
	}
	for _, forbidden := range []string{"tenant-read-001", "provider_resource_uid", "provider-uid-secret", "lease_owner", "fence_token", "request_digest"} {
		if strings.Contains(recorder.Body.String(), forbidden) {
			t.Fatalf("response leaked internal field %q: %s", forbidden, recorder.Body.String())
		}
	}
}

func TestGetWorkloadMapsMissingRowsToNotFound(t *testing.T) {
	principal, authorizer, repository := newReadFixture()
	repository.err = sql.ErrNoRows
	handler := newWorkloadReadTestHandler(t, principal, nil, authorizer, repository)

	recorder := performWorkloadRead(handler, http.MethodGet, "/api/v1/workloads/missing")
	if recorder.Code != http.StatusNotFound {
		t.Fatalf("status = %d, want 404; body=%s", recorder.Code, recorder.Body.String())
	}
	if authorizer.calls != 0 {
		t.Fatalf("missing Workload reached read authorizer: %d calls", authorizer.calls)
	}
}

func TestGetWorkloadMethodAndPathBoundary(t *testing.T) {
	principal, authorizer, repository := newReadFixture()
	handler := newWorkloadReadTestHandler(t, principal, nil, authorizer, repository)

	recorder := performWorkloadRead(handler, http.MethodPost, "/api/v1/workloads/workload-read-001")
	if recorder.Code != http.StatusMethodNotAllowed || recorder.Header().Get("Allow") != http.MethodGet {
		t.Fatalf("POST status/Allow = %d/%q, want 405/GET", recorder.Code, recorder.Header().Get("Allow"))
	}
	recorder = performWorkloadRead(handler, http.MethodGet, "/api/v1/workloads/workload-read-001/extra")
	if recorder.Code != http.StatusNotFound {
		t.Fatalf("malformed path status = %d, want 404", recorder.Code)
	}
}
