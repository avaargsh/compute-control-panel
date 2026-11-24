package httpapi

import (
	"bytes"
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/json"
	"errors"
	"math/big"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"
	"time"

	"github.com/avaargsh/compute-control-panel/internal/hostidentity"
	"github.com/avaargsh/compute-control-panel/internal/store/postgres"
)

const hostSessionIngressSubject = "spiffe://example.org/host/node-a"

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

type hostSessionIngressFixture struct {
	roots *x509.CertPool
	ca    *x509.Certificate
	key   ed25519.PrivateKey
}

func newHostSessionIngressFixture(t *testing.T) hostSessionIngressFixture {
	t.Helper()
	pub, key, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	now := time.Now()
	ca := &x509.Certificate{
		SerialNumber: big.NewInt(1), Subject: pkix.Name{CommonName: "host test CA"},
		NotBefore: now.Add(-time.Hour), NotAfter: now.Add(time.Hour),
		IsCA: true, BasicConstraintsValid: true, KeyUsage: x509.KeyUsageCertSign,
	}
	raw, err := x509.CreateCertificate(rand.Reader, ca, ca, pub, key)
	if err != nil {
		t.Fatal(err)
	}
	root, err := x509.ParseCertificate(raw)
	if err != nil {
		t.Fatal(err)
	}
	roots := x509.NewCertPool()
	roots.AddCert(root)
	return hostSessionIngressFixture{roots: roots, ca: root, key: key}
}

func (f hostSessionIngressFixture) tlsState(t *testing.T, subject string) *tls.ConnectionState {
	t.Helper()
	pub, _, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	uri, err := url.Parse(subject)
	if err != nil {
		t.Fatal(err)
	}
	now := time.Now()
	template := &x509.Certificate{
		SerialNumber: big.NewInt(2), NotBefore: now.Add(-time.Minute), NotAfter: now.Add(time.Minute),
		URIs: []*url.URL{uri}, KeyUsage: x509.KeyUsageDigitalSignature,
		ExtKeyUsage: []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth},
	}
	raw, err := x509.CreateCertificate(rand.Reader, template, f.ca, pub, f.key)
	if err != nil {
		t.Fatal(err)
	}
	leaf, err := x509.ParseCertificate(raw)
	if err != nil {
		t.Fatal(err)
	}
	chains, err := leaf.Verify(x509.VerifyOptions{Roots: f.roots, KeyUsages: []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth}})
	if err != nil {
		t.Fatal(err)
	}
	return &tls.ConnectionState{PeerCertificates: []*x509.Certificate{leaf}, VerifiedChains: chains}
}

func TestNewHostSessionIngressHandlerRequiresHostRootsAndRepository(t *testing.T) {
	f := newHostSessionIngressFixture(t)
	repo := &hostSessionIngressRepositoryStub{}
	for _, deps := range []HostSessionIngressDependencies{
		{Repository: repo},
		{HostTrustRoots: f.roots, HostTrustDomain: "example.org"},
		{HostTrustRoots: f.roots, Repository: repo},
	} {
		if _, err := NewHostSessionIngressHandler(deps); err == nil {
			t.Fatal("incomplete host ingress dependencies accepted")
		}
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
	f := newHostSessionIngressFixture(t)
	handler, err := NewHostSessionIngressHandler(HostSessionIngressDependencies{
		HostTrustRoots: f.roots, HostTrustDomain: "example.org", Repository: repo,
	})
	if err != nil {
		t.Fatal(err)
	}

	start := httptest.NewRequest(http.MethodPost, hostSessionStartPath,
		strings.NewReader(`{"expected_epoch":0,"ttl_seconds":60}`))
	start.Header.Set("Content-Type", "application/json")
	start.Header.Set("Idempotency-Key", "host-start-request-001")
	start.TLS = f.tlsState(t, hostSessionIngressSubject)
	startResponse := httptest.NewRecorder()
	handler.ServeHTTP(startResponse, start)
	if startResponse.Code != http.StatusCreated || repo.startCalls != 1 {
		t.Fatalf("HostSession start: HTTP %d calls=%d body=%s", startResponse.Code, repo.startCalls, startResponse.Body.String())
	}
	if repo.startRequest.Subject != hostSessionIngressSubject ||
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
	heartbeat.TLS = f.tlsState(t, hostSessionIngressSubject)
	heartbeatResponse := httptest.NewRecorder()
	handler.ServeHTTP(heartbeatResponse, heartbeat)
	if heartbeatResponse.Code != http.StatusAccepted || repo.heartbeatCalls != 1 {
		t.Fatalf("heartbeat: HTTP %d calls=%d body=%s", heartbeatResponse.Code, repo.heartbeatCalls, heartbeatResponse.Body.String())
	}
	if repo.heartbeatRequest.Subject != hostSessionIngressSubject ||
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
	replay.TLS = f.tlsState(t, hostSessionIngressSubject)
	replayResponse := httptest.NewRecorder()
	handler.ServeHTTP(replayResponse, replay)
	if replayResponse.Code != http.StatusOK || repo.heartbeatCalls != 2 {
		t.Fatalf("exact heartbeat replay: HTTP %d calls=%d body=%s", replayResponse.Code, repo.heartbeatCalls, replayResponse.Body.String())
	}
}

func TestHostSessionIngressRejectsUnauthenticatedAndNonHostPrincipals(t *testing.T) {
	repo := &hostSessionIngressRepositoryStub{}
	f := newHostSessionIngressFixture(t)
	handler, err := NewHostSessionIngressHandler(HostSessionIngressDependencies{
		HostTrustRoots: f.roots, HostTrustDomain: "example.org", Repository: repo,
	})
	if err != nil {
		t.Fatal(err)
	}
	request := httptest.NewRequest(http.MethodPost, hostSessionStartPath,
		strings.NewReader(`{"expected_epoch":0,"ttl_seconds":30}`))
	request.Header.Set("Content-Type", "application/json")
	request.Header.Set("Idempotency-Key", "host-start-request")
	recorder := httptest.NewRecorder()
	handler.ServeHTTP(recorder, request)
	if recorder.Code != http.StatusUnauthorized || repo.startCalls != 0 {
		t.Fatalf("missing verified TLS reached HostSession repository: HTTP %d calls=%d", recorder.Code, repo.startCalls)
	}

	request = httptest.NewRequest(http.MethodPost, hostSessionStartPath,
		strings.NewReader(`{"expected_epoch":0,"ttl_seconds":30}`))
	request.Header.Set("Content-Type", "application/json")
	request.Header.Set("Idempotency-Key", "host-start-request")
	request.TLS = f.tlsState(t, "spiffe://example.org/ns/compute-host/sa/host-agent")
	recorder = httptest.NewRecorder()
	handler.ServeHTTP(recorder, request)
	if recorder.Code != http.StatusUnauthorized || repo.startCalls != 0 {
		t.Fatalf("non-host service identity was allowed: HTTP %d calls=%d", recorder.Code, repo.startCalls)
	}
}

func TestHostSessionIngressRejectsAmbiguousHeartbeatJSONAndInvalidSessionFields(t *testing.T) {
	repo := &hostSessionIngressRepositoryStub{}
	f := newHostSessionIngressFixture(t)
	handler, err := NewHostSessionIngressHandler(HostSessionIngressDependencies{
		HostTrustRoots: f.roots, HostTrustDomain: "example.org", Repository: repo,
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
		request.TLS = f.tlsState(t, hostSessionIngressSubject)
		recorder := httptest.NewRecorder()
		handler.ServeHTTP(recorder, request)
		if recorder.Code != http.StatusBadRequest || repo.heartbeatCalls != 0 {
			t.Fatalf("invalid heartbeat body %q: HTTP %d calls=%d", body, recorder.Code, repo.heartbeatCalls)
		}
	}
}

func TestHostSessionIngressMapsRepositoryFencesWithoutLeakingErrors(t *testing.T) {
	repo := &hostSessionIngressRepositoryStub{startErr: postgres.ErrHostSessionUnauthorized}
	f := newHostSessionIngressFixture(t)
	handler, err := NewHostSessionIngressHandler(HostSessionIngressDependencies{
		HostTrustRoots: f.roots, HostTrustDomain: "example.org", Repository: repo,
	})
	if err != nil {
		t.Fatal(err)
	}
	request := httptest.NewRequest(http.MethodPost, hostSessionStartPath,
		strings.NewReader(`{"expected_epoch":0,"ttl_seconds":30}`))
	request.Header.Set("Content-Type", "application/json")
	request.Header.Set("Idempotency-Key", "host-start-request")
	request.TLS = f.tlsState(t, hostSessionIngressSubject)
	recorder := httptest.NewRecorder()
	handler.ServeHTTP(recorder, request)
	if recorder.Code != http.StatusForbidden || strings.Contains(recorder.Body.String(), "SQL") {
		t.Fatalf("authorization failure leaked storage detail: HTTP %d %s", recorder.Code, recorder.Body.String())
	}

	repo.startErr = errors.New("database unavailable")
	request = httptest.NewRequest(http.MethodPost, hostSessionStartPath,
		strings.NewReader(`{"expected_epoch":0,"ttl_seconds":30}`))
	request.Header.Set("Content-Type", "application/json")
	request.Header.Set("Idempotency-Key", "host-start-request")
	request.TLS = f.tlsState(t, hostSessionIngressSubject)
	recorder = httptest.NewRecorder()
	handler.ServeHTTP(recorder, request)
	if recorder.Code != http.StatusServiceUnavailable {
		t.Fatalf("repository outage must fail closed with 503, got %d", recorder.Code)
	}
}
