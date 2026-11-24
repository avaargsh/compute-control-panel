package httpapi

import (
	"bytes"
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"
	"time"

	"github.com/avaargsh/compute-control-panel/internal/hostidentity"
	"github.com/avaargsh/compute-control-panel/internal/store/postgres"
)

const hostSessionIngressSubject = "spiffe://example.org/ns/compute-host/sa/host-agent"

type hostSessionIngressFakeAuthenticator struct {
	principal WorkloadPrincipal
}

func (a hostSessionIngressFakeAuthenticator) Authenticate(context.Context, *http.Request) (WorkloadPrincipal, error) {
	return a.principal, nil
}

type hostSessionIngressRepositoryStub struct {
	startRequest     postgres.AuthenticatedHostSessionStartRequest
	heartbeatRequest postgres.AuthenticatedHostHeartbeatRequest
	fence            hostidentity.SessionFence
	disposition      hostidentity.Disposition
	recordedAt       time.Time
	startErr         error
	heartbeatErr     error
	startCalls       int
	heartbeatCalls   int
}

func (s *hostSessionIngressRepositoryStub) StartAuthenticatedHostSession(
	_ context.Context, request postgres.AuthenticatedHostSessionStartRequest,
) (hostidentity.SessionFence, error) {
	s.startCalls++
	s.startRequest = request
	return s.fence, s.startErr
}

func (s *hostSessionIngressRepositoryStub) RecordAuthenticatedHostHeartbeat(
	_ context.Context, request postgres.AuthenticatedHostHeartbeatRequest,
) (hostidentity.Disposition, time.Time, error) {
	s.heartbeatCalls++
	s.heartbeatRequest = request
	return s.disposition, s.recordedAt, s.heartbeatErr
}

func hostSessionIngressAuthenticator(t *testing.T, service string) *SPIFFEWorkloadAuthenticator {
	t.Helper()
	principal := WorkloadPrincipal{
		TenantID: "tenant-host-ingress", Subject: hostSessionIngressSubject, Service: service,
	}
	auth, err := NewSPIFFEWorkloadAuthenticator("example.org", &testWorkloadIdentityResolver{
		expected: hostSessionIngressSubject, principal: principal,
	})
	if err != nil {
		t.Fatal(err)
	}
	return auth
}

func hostSessionIngressTLS(subject string) *tls.ConnectionState {
	uri, _ := url.Parse(subject)
	leaf := &x509.Certificate{Raw: []byte("verified-host-agent-leaf"), URIs: []*url.URL{uri}}
	return &tls.ConnectionState{
		PeerCertificates: []*x509.Certificate{leaf},
		VerifiedChains:   [][]*x509.Certificate{{leaf}},
	}
}

func TestNewHostSessionIngressHandlerRequiresSPIFFEAuthenticatorAndRepository(t *testing.T) {
	repo := &hostSessionIngressRepositoryStub{}
	if _, err := NewHostSessionIngressHandler(HostSessionIngressDependencies{Repository: repo}); err == nil {
		t.Fatal("missing SPIFFE authenticator allowed HostSession ingress construction")
	}
	if _, err := NewHostSessionIngressHandler(HostSessionIngressDependencies{
		Authenticator: hostSessionIngressAuthenticator(t, "compute-host-agent"),
	}); err == nil {
		t.Fatal("missing HostSession repository allowed ingress construction")
	}
	if _, err := NewHostSessionIngressHandler(HostSessionIngressDependencies{
		Authenticator: hostSessionIngressFakeAuthenticator{principal: WorkloadPrincipal{
			TenantID: "tenant-host-ingress", Subject: hostSessionIngressSubject, Service: "compute-host-agent",
		}},
		Repository: repo,
	}); err == nil {
		t.Fatal("arbitrary authenticator was allowed to impersonate the TLS peer")
	}
}

func TestHostSessionIngressStartsAndRecordsOnlyAuthenticatedHostAgent(t *testing.T) {
	recordedAt := time.Date(2026, 11, 24, 5, 0, 0, 0, time.UTC)
	binding := hostidentity.Binding{
		HostID: "host-001", ProviderID: "provider-001", ClusterScope: "cluster-001",
		NodeUID: "node-uid-001", Subject: hostSessionIngressSubject,
	}
	repo := &hostSessionIngressRepositoryStub{
		fence: hostidentity.SessionFence{
			Binding: binding, Epoch: 1, SessionID: "hs_session_001",
			IssuedAt: recordedAt, ExpiresAt: recordedAt.Add(time.Minute),
		},
		disposition: hostidentity.Accept, recordedAt: recordedAt,
	}
	handler, err := NewHostSessionIngressHandler(HostSessionIngressDependencies{
		Authenticator: hostSessionIngressAuthenticator(t, "compute-host-agent"),
		Repository: repo,
	})
	if err != nil {
		t.Fatal(err)
	}

	start := httptest.NewRequest(http.MethodPost, hostSessionStartPath,
		strings.NewReader(`{"expected_epoch":0,"ttl_seconds":60}`))
	start.Header.Set("Idempotency-Key", "host-start-request-001")
	start.TLS = hostSessionIngressTLS(hostSessionIngressSubject)
	startResponse := httptest.NewRecorder()
	handler.ServeHTTP(startResponse, start)
	if startResponse.Code != http.StatusCreated || repo.startCalls != 1 {
		t.Fatalf("HostSession start: HTTP %d calls=%d body=%s", startResponse.Code, repo.startCalls, startResponse.Body.String())
	}
	if repo.startRequest.TenantID != "tenant-host-ingress" ||
		repo.startRequest.Service != "compute-host-agent" ||
		repo.startRequest.Subject != hostSessionIngressSubject ||
		repo.startRequest.ExpectedEpoch != 0 || repo.startRequest.RequestID != "host-start-request-001" ||
		repo.startRequest.TTL != time.Minute {
		t.Fatalf("start did not use verified identity and normalized request: %+v", repo.startRequest)
	}
	var startResponseBody map[string]any
	if err := json.Unmarshal(startResponse.Body.Bytes(), &startResponseBody); err != nil {
		t.Fatal(err)
	}
	if startResponseBody["session_id"] != "hs_session_001" || startResponseBody["epoch"] != float64(1) {
		t.Fatalf("unexpected session response: %v", startResponseBody)
	}

	heartbeatBody := `{"epoch":1,"session_id":"hs_session_001","sequence":1,"agent_version":"1.2.3"}`
	heartbeat := httptest.NewRequest(http.MethodPost, hostHeartbeatPath, strings.NewReader(heartbeatBody))
	heartbeat.Header.Set("Content-Type", "application/json")
	heartbeat.TLS = hostSessionIngressTLS(hostSessionIngressSubject)
	heartbeatResponse := httptest.NewRecorder()
	handler.ServeHTTP(heartbeatResponse, heartbeat)
	if heartbeatResponse.Code != http.StatusAccepted || repo.heartbeatCalls != 1 {
		t.Fatalf("heartbeat: HTTP %d calls=%d body=%s", heartbeatResponse.Code, repo.heartbeatCalls, heartbeatResponse.Body.String())
	}
	if repo.heartbeatRequest.TenantID != "tenant-host-ingress" ||
		repo.heartbeatRequest.Service != "compute-host-agent" ||
		repo.heartbeatRequest.Subject != hostSessionIngressSubject ||
		repo.heartbeatRequest.Epoch != 1 || repo.heartbeatRequest.SessionID != "hs_session_001" ||
		repo.heartbeatRequest.Sequence != 1 ||
		!strings.HasPrefix(repo.heartbeatRequest.Digest, "sha256:") ||
		len(repo.heartbeatRequest.Digest) != 71 {
		t.Fatalf("heartbeat did not bind verified identity and server digest: %+v", repo.heartbeatRequest)
	}
	if strings.Contains(heartbeatBody, repo.heartbeatRequest.Digest) {
		t.Fatal("server accepted a digest from the HTTP body instead of computing it")
	}

	repo.disposition = hostidentity.Replay
	replay := httptest.NewRequest(http.MethodPost, hostHeartbeatPath, strings.NewReader(heartbeatBody))
	replay.Header.Set("Content-Type", "application/json")
	replay.TLS = hostSessionIngressTLS(hostSessionIngressSubject)
	replayResponse := httptest.NewRecorder()
	handler.ServeHTTP(replayResponse, replay)
	if replayResponse.Code != http.StatusOK || repo.heartbeatCalls != 2 {
		t.Fatalf("exact heartbeat replay: HTTP %d calls=%d body=%s", replayResponse.Code, repo.heartbeatCalls, replayResponse.Body.String())
	}
}

func TestHostSessionIngressRejectsUnauthenticatedAndNonHostPrincipals(t *testing.T) {
	repo := &hostSessionIngressRepositoryStub{}
	handler, err := NewHostSessionIngressHandler(HostSessionIngressDependencies{
		Authenticator: hostSessionIngressAuthenticator(t, "agent-runtime"),
		Repository: repo,
	})
	if err != nil {
		t.Fatal(err)
	}
	request := httptest.NewRequest(http.MethodPost, hostSessionStartPath,
		strings.NewReader(`{"expected_epoch":0,"ttl_seconds":30}`))
	request.Header.Set("Idempotency-Key", "host-start-request")
	recorder := httptest.NewRecorder()
	handler.ServeHTTP(recorder, request)
	if recorder.Code != http.StatusUnauthorized || repo.startCalls != 0 {
		t.Fatalf("missing verified TLS reached HostSession repository: HTTP %d calls=%d", recorder.Code, repo.startCalls)
	}

	request = httptest.NewRequest(http.MethodPost, hostSessionStartPath,
		strings.NewReader(`{"expected_epoch":0,"ttl_seconds":30}`))
	request.Header.Set("Idempotency-Key", "host-start-request")
	request.TLS = hostSessionIngressTLS(hostSessionIngressSubject)
	recorder = httptest.NewRecorder()
	handler.ServeHTTP(recorder, request)
	if recorder.Code != http.StatusForbidden || repo.startCalls != 0 {
		t.Fatalf("non-host service identity was allowed: HTTP %d calls=%d", recorder.Code, repo.startCalls)
	}
}

func TestHostSessionIngressRejectsAmbiguousHeartbeatJSONAndInvalidSessionFields(t *testing.T) {
	repo := &hostSessionIngressRepositoryStub{}
	handler, err := NewHostSessionIngressHandler(HostSessionIngressDependencies{
		Authenticator: hostSessionIngressAuthenticator(t, "compute-host-agent"),
		Repository: repo,
	})
	if err != nil {
		t.Fatal(err)
	}
	for _, body := range []string{
		`{"epoch":1,"session_id":"hs_1","sequence":1,"agent_version":"1","sequence":2}`,
		`{"epoch":1,"session_id":"hs_1","sequence":1,"agent_version":"1","unknown":true}`,
		`{"epoch":1,"session_id":"hs_1","sequence":1,"agent_version":null}`,
		`{"epoch":0,"session_id":"hs_1","sequence":1,"agent_version":"1"}`,
		`{"epoch":1,"session_id":"hs_1","sequence":0,"agent_version":"1"}`,
		`{"epoch":1,"session_id":"not-a-server-session","sequence":1,"agent_version":"1"}`,
	} {
		request := httptest.NewRequest(http.MethodPost, hostHeartbeatPath, bytes.NewBufferString(body))
		request.Header.Set("Content-Type", "application/json")
		request.TLS = hostSessionIngressTLS(hostSessionIngressSubject)
		recorder := httptest.NewRecorder()
		handler.ServeHTTP(recorder, request)
		if recorder.Code != http.StatusBadRequest || repo.heartbeatCalls != 0 {
			t.Fatalf("invalid heartbeat body %q: HTTP %d calls=%d", body, recorder.Code, repo.heartbeatCalls)
		}
	}
}

func TestHostSessionIngressMapsRepositoryFencesWithoutLeakingErrors(t *testing.T) {
	repo := &hostSessionIngressRepositoryStub{startErr: postgres.ErrHostSessionUnauthorized}
	handler, err := NewHostSessionIngressHandler(HostSessionIngressDependencies{
		Authenticator: hostSessionIngressAuthenticator(t, "compute-host-agent"),
		Repository: repo,
	})
	if err != nil {
		t.Fatal(err)
	}
	request := httptest.NewRequest(http.MethodPost, hostSessionStartPath,
		strings.NewReader(`{"expected_epoch":0,"ttl_seconds":30}`))
	request.Header.Set("Idempotency-Key", "host-start-request")
	request.TLS = hostSessionIngressTLS(hostSessionIngressSubject)
	recorder := httptest.NewRecorder()
	handler.ServeHTTP(recorder, request)
	if recorder.Code != http.StatusForbidden || strings.Contains(recorder.Body.String(), "SQL") {
		t.Fatalf("authorization failure leaked storage detail: HTTP %d %s", recorder.Code, recorder.Body.String())
	}

	repo.startErr = errors.New("database unavailable")
	request = httptest.NewRequest(http.MethodPost, hostSessionStartPath,
		strings.NewReader(`{"expected_epoch":0,"ttl_seconds":30}`))
	request.Header.Set("Idempotency-Key", "host-start-request")
	request.TLS = hostSessionIngressTLS(hostSessionIngressSubject)
	recorder = httptest.NewRecorder()
	handler.ServeHTTP(recorder, request)
	if recorder.Code != http.StatusServiceUnavailable {
		t.Fatalf("repository outage must fail closed with 503, got %d", recorder.Code)
	}
}
