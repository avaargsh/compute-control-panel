package httpapi

import (
	"context"
	"database/sql"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/avaargsh/compute-control-panel/internal/store/postgres"
)

type providerManagementRepositoryStub struct {
	record        postgres.ProviderRecord
	created       bool
	registerErr   error
	readErr       error
	drainErr      error
	registerCalls int
	readCalls     int
	drainCalls    int
	lastRegister  postgres.ProviderRegistration
	lastID        string
	lastVersion   int64
	lastReason    string
}

func (s *providerManagementRepositoryStub) RegisterProvider(_ context.Context, p postgres.ProviderRegistration) (postgres.ProviderRecord, bool, error) {
	s.registerCalls++
	s.lastRegister = p
	return s.record, s.created, s.registerErr
}
func (s *providerManagementRepositoryStub) GetProvider(_ context.Context, id string) (postgres.ProviderRecord, error) {
	s.readCalls++
	s.lastID = id
	return s.record, s.readErr
}
func (s *providerManagementRepositoryStub) BeginProviderDrain(_ context.Context, id string, version int64, reason string) (postgres.ProviderRecord, error) {
	s.drainCalls++
	s.lastID, s.lastVersion, s.lastReason = id, version, reason
	return s.record, s.drainErr
}

type providerManagementAuthorizerStub struct {
	err    error
	calls  int
	action string
	id     string
}

func (s *providerManagementAuthorizerStub) AuthorizeProviderManage(_ context.Context, _ WorkloadPrincipal, action, id string) error {
	s.calls++
	s.action, s.id = action, id
	return s.err
}

func providerManagementFixture(t *testing.T) (http.Handler, *providerManagementRepositoryStub, *providerManagementAuthorizerStub) {
	t.Helper()
	repo := &providerManagementRepositoryStub{
		created: true,
		record: postgres.ProviderRecord{
			ProviderRegistration: postgres.ProviderRegistration{
				ProviderID: "provider-001", ClusterScope: "cluster-a", AdapterSource: "kubernetes",
				AdapterVersion: "v1", CapabilityDigest: "sha256:" + strings.Repeat("a", 64),
			},
			State: postgres.ProviderRegistered, Version: 1,
		},
	}
	policy := &providerManagementAuthorizerStub{}
	h, err := NewProviderManagementHandler(ProviderManagementDependencies{
		Authenticator: testPrincipalAuthenticator{principal: WorkloadPrincipal{
			TenantID: "tenant-001", Subject: "spiffe://example.org/ns/admin/sa/manager", Service: "compute-admin",
		}},
		Authorizer: policy,
		Repository: repo,
	})
	if err != nil {
		t.Fatal(err)
	}
	return h, repo, policy
}

func providerManagementRequest(handler http.Handler, method, path, body string) *httptest.ResponseRecorder {
	req := httptest.NewRequest(method, path, strings.NewReader(body))
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)
	return rec
}

func providerRegistrationJSON() string {
	return `{"provider_id":"provider-001","cluster_scope":"cluster-a","adapter_source":"kubernetes","adapter_version":"v1","capability_digest":"sha256:` + strings.Repeat("a", 64) + `"}`
}

func TestProviderManageRequiresIndependentAuthorizationBeforeRepository(t *testing.T) {
	h, repo, policy := providerManagementFixture(t)
	policy.err = errors.New("not a provider administrator")
	for _, tc := range []struct{ method, path, body, action string }{
		{http.MethodPost, "/api/v1/manage/providers", providerRegistrationJSON(), "register"},
		{http.MethodGet, "/api/v1/manage/providers/provider-001", "", "read"},
		{http.MethodPost, "/api/v1/manage/providers/provider-001/drain", `{"expected_version":1,"reason_code":"MAINTENANCE"}`, "drain"},
	} {
		resp := providerManagementRequest(h, tc.method, tc.path, tc.body)
		if resp.Code != http.StatusForbidden || policy.action != tc.action {
			t.Fatalf("%s %s: status=%d action=%s", tc.method, tc.path, resp.Code, policy.action)
		}
	}
	if repo.registerCalls != 0 || repo.readCalls != 0 || repo.drainCalls != 0 {
		t.Fatal("unauthorized provider management reached repository")
	}
	if policy.calls != 3 {
		t.Fatalf("policy calls=%d, want 3", policy.calls)
	}
}

func TestProviderManageRequiresAuthenticatedService(t *testing.T) {
	repo := &providerManagementRepositoryStub{}
	policy := &providerManagementAuthorizerStub{}
	h, err := NewProviderManagementHandler(ProviderManagementDependencies{
		Authenticator: testPrincipalAuthenticator{err: errors.New("no verified identity")},
		Authorizer:    policy, Repository: repo,
	})
	if err != nil {
		t.Fatal(err)
	}
	resp := providerManagementRequest(h, http.MethodPost, "/api/v1/manage/providers", providerRegistrationJSON())
	if resp.Code != http.StatusUnauthorized || policy.calls != 0 || repo.registerCalls != 0 {
		t.Fatalf("unauthenticated create reached downstream: status=%d", resp.Code)
	}
}

func TestProviderManageRegistrationReplayAndRead(t *testing.T) {
	h, repo, policy := providerManagementFixture(t)
	resp := providerManagementRequest(h, http.MethodPost, "/api/v1/manage/providers", providerRegistrationJSON())
	if resp.Code != http.StatusCreated || resp.Header().Get("Location") != "/api/v1/manage/providers/provider-001" || resp.Header().Get("Cache-Control") != "no-store" {
		t.Fatalf("registration status=%d headers=%v body=%s", resp.Code, resp.Header(), resp.Body.String())
	}
	if repo.registerCalls != 1 || repo.lastRegister.ProviderID != "provider-001" || policy.action != "register" {
		t.Fatal("registration did not use existing authoritative repository")
	}
	repo.created = false
	replay := providerManagementRequest(h, http.MethodPost, "/api/v1/manage/providers", providerRegistrationJSON())
	if replay.Code != http.StatusOK || repo.registerCalls != 2 {
		t.Fatalf("registration replay status=%d calls=%d", replay.Code, repo.registerCalls)
	}
	read := providerManagementRequest(h, http.MethodGet, "/api/v1/manage/providers/provider-001", "")
	if read.Code != http.StatusOK || repo.readCalls != 1 || policy.action != "read" || repo.lastID != "provider-001" {
		t.Fatalf("read status=%d calls=%d action=%s", read.Code, repo.readCalls, policy.action)
	}
	for _, forbidden := range []string{"READY", "gpu_capacity", "lease_owner", "fencing_token"} {
		if strings.Contains(read.Body.String(), forbidden) {
			t.Fatalf("unverified or internal field exposed: %s", forbidden)
		}
	}
}

func TestProviderManageDrainCASAndErrorMapping(t *testing.T) {
	h, repo, policy := providerManagementFixture(t)
	repo.record.State, repo.record.Version = postgres.ProviderDraining, 2
	body := `{"expected_version":1,"reason_code":"PLANNED_MAINTENANCE"}`
	resp := providerManagementRequest(h, http.MethodPost, "/api/v1/manage/providers/provider-001/drain", body)
	if resp.Code != http.StatusOK || repo.lastVersion != 1 || repo.lastReason != "PLANNED_MAINTENANCE" || policy.action != "drain" {
		t.Fatalf("drain status=%d args=%d,%s action=%s", resp.Code, repo.lastVersion, repo.lastReason, policy.action)
	}
	repo.drainErr = postgres.ErrProviderStale
	resp = providerManagementRequest(h, http.MethodPost, "/api/v1/manage/providers/provider-001/drain", body)
	if resp.Code != http.StatusConflict {
		t.Fatalf("stale CAS status=%d", resp.Code)
	}
	repo.drainErr = postgres.ErrProviderTransition
	resp = providerManagementRequest(h, http.MethodPost, "/api/v1/manage/providers/provider-001/drain", body)
	if resp.Code != http.StatusConflict {
		t.Fatalf("forbidden transition status=%d", resp.Code)
	}
	repo.drainErr = sql.ErrNoRows
	resp = providerManagementRequest(h, http.MethodPost, "/api/v1/manage/providers/provider-001/drain", body)
	if resp.Code != http.StatusNotFound {
		t.Fatalf("missing provider status=%d", resp.Code)
	}
}

func TestProviderManageRejectsMalformedAndUnsafeRequests(t *testing.T) {
	h, repo, _ := providerManagementFixture(t)
	bodies := []string{
		`{}`,
		`{"provider_id":"bad/path","cluster_scope":"c","adapter_source":"kubernetes","adapter_version":"v1","capability_digest":"sha256:` + strings.Repeat("a", 64) + `"}`,
		strings.Replace(providerRegistrationJSON(), `"provider_id":`, `"provider_id":"provider-001","provider_id":`, 1),
		strings.Replace(providerRegistrationJSON(), `"cluster_scope":"cluster-a"`, `"cluster_scope":null`, 1),
		strings.Replace(providerRegistrationJSON(), `"adapter_version":`, `"ready":true,"adapter_version":`, 1),
	}
	for _, body := range bodies {
		resp := providerManagementRequest(h, http.MethodPost, "/api/v1/manage/providers", body)
		if resp.Code != http.StatusBadRequest {
			t.Fatalf("unsafe body accepted: status=%d body=%s", resp.Code, body)
		}
	}
	if repo.registerCalls != 0 {
		t.Fatalf("invalid requests reached repository: %d", repo.registerCalls)
	}
	for _, body := range []string{`{}`, `{"expected_version":0,"reason_code":"MAINTENANCE"}`, `{"expected_version":1,"reason_code":"invalid space"}`, `{"expected_version":1,"reason_code":"MAINTENANCE","force":true}`, `{"expected_version":1,"reason_code":null}`} {
		resp := providerManagementRequest(h, http.MethodPost, "/api/v1/manage/providers/provider-001/drain", body)
		if resp.Code != http.StatusBadRequest {
			t.Fatalf("invalid drain body accepted: status=%d body=%s", resp.Code, body)
		}
	}
	if repo.drainCalls != 0 {
		t.Fatal("invalid drain reached repository")
	}
	for _, tc := range []struct{ method, path string }{
		{http.MethodDelete, "/api/v1/manage/providers/provider-001"},
		{http.MethodPatch, "/api/v1/manage/providers/provider-001"},
		{http.MethodPost, "/api/v1/manage/providers/provider-001/ready"},
	} {
		resp := providerManagementRequest(h, tc.method, tc.path, "")
		if resp.Code != http.StatusMethodNotAllowed && resp.Code != http.StatusNotFound {
			t.Fatalf("unsafe lifecycle route status=%d path=%s", resp.Code, tc.path)
		}
	}
}

func TestProviderManageConstructorFailsClosed(t *testing.T) {
	if _, err := NewProviderManagementHandler(ProviderManagementDependencies{}); err == nil {
		t.Fatal("nil dependencies must deny registration of Manage routes")
	}
}

func TestProviderManageUnsupportedRoutesRemainNoStore(t *testing.T) {
	h, _, _ := providerManagementFixture(t)
	for _, tc := range []struct{ method, path string }{
		{http.MethodDelete, "/api/v1/manage/providers/provider-001"},
		{http.MethodPost, "/api/v1/manage/providers/provider-001/ready"},
		{http.MethodGet, "/api/v1/manage/providers"},
	} {
		response := providerManagementRequest(h, tc.method, tc.path, "")
		if response.Code != http.StatusMethodNotAllowed && response.Code != http.StatusNotFound {
			t.Fatalf("unexpected route %s %s returned %d", tc.method, tc.path, response.Code)
		}
		if response.Header().Get("Cache-Control") != "no-store" {
			t.Fatalf("%s %s did not disable caching", tc.method, tc.path)
		}
	}
}

// A Go 1.22+ ServeMux GET route also matches HEAD, unless the handler
// explicitly rejects it. Management reads must not grow an implicit method.
func TestProviderManageRejectsImplicitHeadRead(t *testing.T) {
	h, repo, policy := providerManagementFixture(t)
	response := providerManagementRequest(h, http.MethodHead, "/api/v1/manage/providers/provider-001", "")
	if response.Code != http.StatusMethodNotAllowed || response.Header().Get("Allow") != "GET" {
		t.Fatalf("HEAD status=%d allow=%q, want 405 and Allow GET", response.Code, response.Header().Get("Allow"))
	}
	if response.Header().Get("Cache-Control") != "no-store" {
		t.Fatal("HEAD response must disable caching")
	}
	if repo.readCalls != 0 || policy.calls != 0 {
		t.Fatalf("implicit HEAD reached privileged read: repository=%d authorizer=%d", repo.readCalls, policy.calls)
	}
}

func TestProviderManageRejectsCaseFoldedJSONFieldAliases(t *testing.T) {
	h, repo, policy := providerManagementFixture(t)
	// encoding/json accepts case-insensitive struct-field matches.
	// Reject aliases rather than allowing a second spelling to overwrite
	// authority-bearing Provider identity or expected CAS version.
	registration := strings.Replace(
		providerRegistrationJSON(),
		`"provider_id":"provider-001",`,
		`"provider_id":"provider-001","Provider_ID":"provider-002",`,
		1,
	)
	if response := providerManagementRequest(h, http.MethodPost, "/api/v1/manage/providers", registration); response.Code != http.StatusBadRequest {
		t.Fatalf("ambiguous Provider ID returned status %d: %s", response.Code, response.Body.String())
	}
	drain := `{"expected_version":1,"Expected_Version":2,"reason_code":"MAINTENANCE"}`
	if response := providerManagementRequest(h, http.MethodPost, "/api/v1/manage/providers/provider-001/drain", drain); response.Code != http.StatusBadRequest {
		t.Fatalf("ambiguous expected version returned status %d: %s", response.Code, response.Body.String())
	}
	if repo.registerCalls != 0 || repo.drainCalls != 0 || policy.calls != 0 {
		t.Fatalf("case-folded field aliases reached privileged calls: reg=%d drain=%d auth=%d", repo.registerCalls, repo.drainCalls, policy.calls)
	}
}

func TestProviderManageConstructorRejectsTypedNilDependencies(t *testing.T) {
	tests := []struct {
		name string
		deps ProviderManagementDependencies
	}{
		{
			name: "nil authenticator",
			deps: ProviderManagementDependencies{
				Authenticator: (*SPIFFEWorkloadAuthenticator)(nil),
				Authorizer:    &providerManagementAuthorizerStub{},
				Repository:    &providerManagementRepositoryStub{},
			},
		},
		{
			name: "nil authorizer",
			deps: ProviderManagementDependencies{
				Authenticator: testPrincipalAuthenticator{},
				Authorizer:    (*ExactProviderManagementAuthorizer)(nil),
				Repository:    &providerManagementRepositoryStub{},
			},
		},
		{
			name: "nil repository",
			deps: ProviderManagementDependencies{
				Authenticator: testPrincipalAuthenticator{},
				Authorizer:    &providerManagementAuthorizerStub{},
				Repository:    (*postgres.Repository)(nil),
			},
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if handler, err := NewProviderManagementHandler(tc.deps); err == nil || handler != nil {
				t.Fatalf("typed-nil dependency accepted: handler=%v err=%v", handler, err)
			}
		})
	}
}
