package httpapi

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/avaargsh/compute-control-panel/internal/operation"
	"github.com/avaargsh/compute-control-panel/internal/store/postgres"
)

type testPrincipalAuthenticator struct {
	principal WorkloadPrincipal
	err       error
}

func (a testPrincipalAuthenticator) Authenticate(context.Context, *http.Request) (WorkloadPrincipal, error) {
	return a.principal, a.err
}

type testWorkloadAuthorizer struct {
	err   error
	calls int
}

func (a *testWorkloadAuthorizer) AuthorizeCreate(context.Context, WorkloadPrincipal, WorkloadCreateRequest, string) error {
	a.calls++
	return a.err
}

type testProviderSelector struct {
	selection WorkloadProviderSelection
	err       error
	calls     int
}

func (s *testProviderSelector) SelectProvider(context.Context, WorkloadPrincipal, WorkloadCreateRequest) (WorkloadProviderSelection, error) {
	s.calls++
	return s.selection, s.err
}

type testAcceptanceRepository struct {
	result       postgres.WorkloadAcceptance
	lookupFound  bool
	lookupErr    error
	lookupParams postgres.AcceptanceLookup
	lookupCalls  int
	created      bool
	err          error
	params       postgres.AcceptWorkloadParams
	calls        int
}

func (r *testAcceptanceRepository) GetAcceptanceByIdempotency(_ context.Context, lookup postgres.AcceptanceLookup) (postgres.WorkloadAcceptance, bool, error) {
	r.lookupCalls++
	r.lookupParams = lookup
	return r.result, r.lookupFound, r.lookupErr
}

func (r *testAcceptanceRepository) AcceptWorkload(_ context.Context, params postgres.AcceptWorkloadParams) (postgres.WorkloadAcceptance, bool, error) {
	r.calls++
	r.params = params
	return r.result, r.created, r.err
}

type sequentialWorkloadIDs struct{ next int }

func (g *sequentialWorkloadIDs) NewID(prefix string) (string, error) {
	g.next++
	return prefix + "-" + string(rune('a'+g.next-1)), nil
}

func newAcceptanceTestHandler(t *testing.T, auth testPrincipalAuthenticator, authorizer *testWorkloadAuthorizer, selector *testProviderSelector, repo *testAcceptanceRepository) http.Handler {
	t.Helper()
	handler, err := NewWorkloadCreateHandler(WorkloadCreateDependencies{
		Authenticator:    auth,
		Authorizer:       authorizer,
		ProviderSelector: selector,
		Repository:       repo,
		IDs:              &sequentialWorkloadIDs{},
	})
	if err != nil {
		t.Fatalf("NewWorkloadCreateHandler: %v", err)
	}
	return handler
}

func validCreateBody() string {
	return `{
		"api_version": "compute.platform/v1",
		"project_id": "project-001",
		"owner_ref": {
			"service": "agent-runtime",
			"resource_type": "run",
			"resource_id": "run-001"
		},
		"compute_profile_ref": {"id": "gpu-inference", "revision": "3"},
		"constraints": {"pool_selector": {"capability": "gpu.h100"}, "max_duration_seconds": 7200}
	}`
}

func newAcceptanceFixture() (WorkloadPrincipal, *testWorkloadAuthorizer, *testProviderSelector, *testAcceptanceRepository) {
	principal := WorkloadPrincipal{
		TenantID:   "tenant-001",
		Subject:    "spiffe://cluster.local/ns/agent-runtime/sa/runtime",
		Service:    "agent-runtime",
		ProjectIDs: []string{"project-001"},
	}
	authorizer := &testWorkloadAuthorizer{}
	selector := &testProviderSelector{selection: WorkloadProviderSelection{ProviderID: "provider-server-selected", PoolID: "pool-server-selected"}}
	repo := &testAcceptanceRepository{
		created: true,
		result: postgres.WorkloadAcceptance{
			Workload: postgres.Workload{
				WorkloadID:        "workload-stable",
				TenantID:          "tenant-001",
				ProjectID:         "project-001",
				DesiredGeneration: 1,
				DesiredState:      "ACTIVE",
				ObservedPhase:     "ACCEPTED",
			},
			Allocation: postgres.Allocation{
				AllocationID: "allocation-stable",
				Phase:        "PENDING",
			},
			Operation: operation.Operation{ID: "operation-stable"},
		},
	}
	return principal, authorizer, selector, repo
}

func performCreate(handler http.Handler, body string, idempotencyKey string) *httptest.ResponseRecorder {
	req := httptest.NewRequest(http.MethodPost, "/api/v1/workloads", strings.NewReader(body))
	if idempotencyKey != "" {
		req.Header.Set("Idempotency-Key", idempotencyKey)
	}
	recorder := httptest.NewRecorder()
	handler.ServeHTTP(recorder, req)
	return recorder
}

func TestCreateWorkloadRequiresAuthenticatedPrincipal(t *testing.T) {
	principal, authorizer, selector, repo := newAcceptanceFixture()
	handler := newAcceptanceTestHandler(t, testPrincipalAuthenticator{principal: principal, err: errors.New("invalid service token")}, authorizer, selector, repo)

	recorder := performCreate(handler, validCreateBody(), "request-001")
	if recorder.Code != http.StatusUnauthorized {
		t.Fatalf("status = %d, want 401; body=%s", recorder.Code, recorder.Body.String())
	}
	if repo.calls != 0 || selector.calls != 0 || authorizer.calls != 0 {
		t.Fatalf("unauthenticated request reached downstream dependencies: repo=%d selector=%d authorizer=%d", repo.calls, selector.calls, authorizer.calls)
	}
}

func TestCreateWorkloadRejectsProjectOutsidePrincipalScope(t *testing.T) {
	principal, authorizer, selector, repo := newAcceptanceFixture()
	principal.ProjectIDs = nil
	handler := newAcceptanceTestHandler(t, testPrincipalAuthenticator{principal: principal}, authorizer, selector, repo)

	recorder := performCreate(handler, validCreateBody(), "request-001")
	if recorder.Code != http.StatusForbidden {
		t.Fatalf("status = %d, want 403; body=%s", recorder.Code, recorder.Body.String())
	}
	if selector.calls != 0 || repo.calls != 0 {
		t.Fatalf("unauthorized project reached selector/repository: selector=%d repo=%d", selector.calls, repo.calls)
	}
}

func TestCreateWorkloadRejectsOwnerServiceMismatch(t *testing.T) {
	principal, authorizer, selector, repo := newAcceptanceFixture()
	principal.Service = "sandbox-provider"
	handler := newAcceptanceTestHandler(t, testPrincipalAuthenticator{principal: principal}, authorizer, selector, repo)

	recorder := performCreate(handler, validCreateBody(), "request-001")
	if recorder.Code != http.StatusForbidden {
		t.Fatalf("status = %d, want 403; body=%s", recorder.Code, recorder.Body.String())
	}
	if selector.calls != 0 || repo.calls != 0 {
		t.Fatalf("owner mismatch reached selector/repository: selector=%d repo=%d", selector.calls, repo.calls)
	}
}

func TestCreateWorkloadRejectsDuplicateJSONKeys(t *testing.T) {
	principal, authorizer, selector, repo := newAcceptanceFixture()
	body := strings.Replace(validCreateBody(), `"project_id": "project-001",`, `"project_id": "project-001", "project_id": "project-001",`, 1)
	handler := newAcceptanceTestHandler(t, testPrincipalAuthenticator{principal: principal}, authorizer, selector, repo)

	recorder := performCreate(handler, body, "request-001")
	if recorder.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want 400; body=%s", recorder.Code, recorder.Body.String())
	}
	if repo.calls != 0 {
		t.Fatalf("duplicate-key body reached repository: %d calls", repo.calls)
	}
}

func TestCreateWorkloadRejectsClientSelectedProvider(t *testing.T) {
	principal, authorizer, selector, repo := newAcceptanceFixture()
	body := strings.Replace(validCreateBody(), `"compute_profile_ref":`, `"provider_id": "client-choice", "compute_profile_ref":`, 1)
	handler := newAcceptanceTestHandler(t, testPrincipalAuthenticator{principal: principal}, authorizer, selector, repo)

	recorder := performCreate(handler, body, "request-001")
	if recorder.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want 400; body=%s", recorder.Code, recorder.Body.String())
	}
	if selector.calls != 0 || repo.calls != 0 {
		t.Fatalf("client provider selection was not rejected before downstream calls")
	}
}

func TestCreateWorkloadPersistsAuthorizedAtomicAcceptance(t *testing.T) {
	principal, authorizer, selector, repo := newAcceptanceFixture()
	handler := newAcceptanceTestHandler(t, testPrincipalAuthenticator{principal: principal}, authorizer, selector, repo)

	recorder := performCreate(handler, validCreateBody(), "request-001")
	if recorder.Code != http.StatusAccepted {
		t.Fatalf("status = %d, want 202; body=%s", recorder.Code, recorder.Body.String())
	}
	if authorizer.calls != 1 || selector.calls != 1 || repo.calls != 1 {
		t.Fatalf("dependency calls = authorizer:%d selector:%d repo:%d", authorizer.calls, selector.calls, repo.calls)
	}
	p := repo.params
	if p.Workload.TenantID != principal.TenantID || p.Workload.ProjectID != "project-001" {
		t.Fatalf("tenant/project were not taken from authorized principal/request scope: %+v", p.Workload)
	}
	if p.ProviderID != "provider-server-selected" || p.PoolID != "pool-server-selected" {
		t.Fatalf("provider selection must be server-side: provider=%q pool=%q", p.ProviderID, p.PoolID)
	}
	if p.Operation.AllocationID != p.AllocationID || p.Operation.ProviderID != p.ProviderID || p.Operation.Status != operation.Pending {
		t.Fatalf("CREATE operation is not bound to initial Allocation: %+v", p.Operation)
	}
	if p.Operation.Generation != 1 || p.Operation.Kind != operation.Create || p.Workload.IdempotencyKey != "request-001" {
		t.Fatalf("invalid initial operation/Workload intent: workload=%+v operation=%+v", p.Workload, p.Operation)
	}
	if !json.Valid(p.Workload.NormalizedRequest) || p.Workload.RequestDigest == "" {
		t.Fatalf("normalized request or digest missing")
	}
	var response map[string]any
	if err := json.Unmarshal(recorder.Body.Bytes(), &response); err != nil {
		t.Fatal(err)
	}
	if response["observed_phase"] != "ACCEPTED" || response["allocation_phase"] != "PENDING" ||
		response["allocation_id"] != "allocation-stable" || response["compute_operation_id"] != "operation-stable" {
		t.Fatalf("response lost acceptance/Allocation state distinction: %+v", response)
	}
	if recorder.Header().Get("Location") != "/api/v1/workloads/workload-stable" {
		t.Fatalf("Location = %q", recorder.Header().Get("Location"))
	}
}

func TestCreateWorkloadIdempotentReplaySkipsProviderSelection(t *testing.T) {
	principal, authorizer, selector, repo := newAcceptanceFixture()
	repo.lookupFound = true
	selector.err = errors.New("capacity changed after original acceptance")
	handler := newAcceptanceTestHandler(t, testPrincipalAuthenticator{principal: principal}, authorizer, selector, repo)

	recorder := performCreate(handler, validCreateBody(), "request-001")
	if recorder.Code != http.StatusAccepted {
		t.Fatalf("status = %d, want 202; body=%s", recorder.Code, recorder.Body.String())
	}
	if repo.lookupCalls != 1 || repo.calls != 0 || selector.calls != 0 {
		t.Fatalf("replay must use durable acceptance without provider resolution: lookup=%d accept=%d selector=%d", repo.lookupCalls, repo.calls, selector.calls)
	}
	if authorizer.calls != 1 {
		t.Fatalf("authorization must still be rechecked on replay: calls=%d", authorizer.calls)
	}
	if repo.lookupParams.TenantID != principal.TenantID ||
		repo.lookupParams.IdempotencyKey != "request-001" ||
		repo.lookupParams.ProjectID != "project-001" ||
		repo.lookupParams.OperationIdempotencyKey == "" ||
		repo.lookupParams.OperationRequestDigest == "" {
		t.Fatalf("incomplete durable acceptance lookup: %+v", repo.lookupParams)
	}
	var response map[string]any
	if err := json.Unmarshal(recorder.Body.Bytes(), &response); err != nil {
		t.Fatal(err)
	}
	if response["workload_id"] != "workload-stable" ||
		response["allocation_id"] != "allocation-stable" ||
		response["compute_operation_id"] != "operation-stable" {
		t.Fatalf("idempotent replay did not return original identities: %+v", response)
	}
}

func TestCreateWorkloadIdempotencyConflictIsCheckedBeforeProviderSelection(t *testing.T) {
	principal, authorizer, selector, repo := newAcceptanceFixture()
	repo.lookupErr = postgres.ErrIdempotencyConflict
	handler := newAcceptanceTestHandler(t, testPrincipalAuthenticator{principal: principal}, authorizer, selector, repo)

	recorder := performCreate(handler, validCreateBody(), "request-001")
	if recorder.Code != http.StatusConflict {
		t.Fatalf("status = %d, want 409; body=%s", recorder.Code, recorder.Body.String())
	}
	if selector.calls != 0 || repo.calls != 0 {
		t.Fatalf("idempotency conflict reached provider selection or acceptance write")
	}
}

func TestCreateWorkloadAuthorizesBeforeProviderSelection(t *testing.T) {
	principal, authorizer, selector, repo := newAcceptanceFixture()
	authorizer.err = errors.New("Guard Permit rejected")
	handler := newAcceptanceTestHandler(t, testPrincipalAuthenticator{principal: principal}, authorizer, selector, repo)

	recorder := performCreate(handler, validCreateBody(), "request-001")
	if recorder.Code != http.StatusForbidden {
		t.Fatalf("status = %d, want 403; body=%s", recorder.Code, recorder.Body.String())
	}
	if selector.calls != 0 || repo.calls != 0 {
		t.Fatalf("unauthorized mutation reached provider selection/repository")
	}
}

func TestCreateWorkloadMapsIdempotencyConflict(t *testing.T) {
	principal, authorizer, selector, repo := newAcceptanceFixture()
	repo.err = postgres.ErrIdempotencyConflict
	handler := newAcceptanceTestHandler(t, testPrincipalAuthenticator{principal: principal}, authorizer, selector, repo)

	recorder := performCreate(handler, validCreateBody(), "request-001")
	if recorder.Code != http.StatusConflict {
		t.Fatalf("status = %d, want 409; body=%s", recorder.Code, recorder.Body.String())
	}
}

func TestCreateWorkloadRequiresIdempotencyKey(t *testing.T) {
	principal, authorizer, selector, repo := newAcceptanceFixture()
	handler := newAcceptanceTestHandler(t, testPrincipalAuthenticator{principal: principal}, authorizer, selector, repo)

	recorder := performCreate(handler, validCreateBody(), "")
	if recorder.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want 400; body=%s", recorder.Code, recorder.Body.String())
	}
	if repo.calls != 0 {
		t.Fatalf("missing idempotency key reached repository")
	}
}

func TestCreateWorkloadRejectsProviderResolutionFailure(t *testing.T) {
	principal, authorizer, selector, repo := newAcceptanceFixture()
	selector.err = errors.New("no eligible provider")
	handler := newAcceptanceTestHandler(t, testPrincipalAuthenticator{principal: principal}, authorizer, selector, repo)

	recorder := performCreate(handler, validCreateBody(), "request-001")
	if recorder.Code != http.StatusServiceUnavailable {
		t.Fatalf("status = %d, want 503; body=%s", recorder.Code, recorder.Body.String())
	}
	if repo.calls != 0 {
		t.Fatalf("provider-resolution failure reached repository")
	}
}

func TestCreateWorkloadMethodBoundary(t *testing.T) {
	principal, authorizer, selector, repo := newAcceptanceFixture()
	handler := newAcceptanceTestHandler(t, testPrincipalAuthenticator{principal: principal}, authorizer, selector, repo)

	req := httptest.NewRequest(http.MethodGet, "/api/v1/workloads", nil)
	recorder := httptest.NewRecorder()
	handler.ServeHTTP(recorder, req)
	if recorder.Code != http.StatusMethodNotAllowed || recorder.Header().Get("Allow") != http.MethodPost {
		t.Fatalf("GET status/Allow = %d/%q, want 405/POST", recorder.Code, recorder.Header().Get("Allow"))
	}
}
