package httpapi

import (
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

const hostSessionIngressSubject = "spiffe://ccp.example/host/machine-001"

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

type hostCredentialAuthority struct {
	roots *x509.CertPool
	ca    *x509.Certificate
	key   ed25519.PrivateKey
}

func newHostCredentialAuthority(t *testing.T) *hostCredentialAuthority {
	t.Helper()
	now := time.Now().UTC()
	publicKey, privateKey, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	caTemplate := &x509.Certificate{
		SerialNumber: big.NewInt(1),
		Subject: pkix.Name{CommonName: "CCP host-only test CA"},
		NotBefore: now.Add(-time.Hour), NotAfter: now.Add(time.Hour),
		IsCA: true, BasicConstraintsValid: true,
		KeyUsage: x509.KeyUsageCertSign | x509.KeyUsageDigitalSignature,
	}
	caDER, err := x509.CreateCertificate(rand.Reader, caTemplate, caTemplate, publicKey, privateKey)
	if err != nil {
		t.Fatal(err)
	}
	ca, err := x509.ParseCertificate(caDER)
	if err != nil {
		t.Fatal(err)
	}
	roots := x509.NewCertPool()
	roots.AddCert(ca)
	return &hostCredentialAuthority{roots: roots, ca: ca, key: privateKey}
}

func (a *hostCredentialAuthority) issue(t *testing.T, subject string, serial int64) tls.Certificate {
	t.Helper()
	now := time.Now().UTC()
	publicKey, privateKey, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	uri, err := url.Parse(subject)
	if err != nil {
		t.Fatal(err)
	}
	leafTemplate := &x509.Certificate{
		SerialNumber: big.NewInt(serial),
		NotBefore: now.Add(-time.Hour), NotAfter: now.Add(time.Hour),
		KeyUsage: x509.KeyUsageDigitalSignature,
		ExtKeyUsage: []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth},
		URIs: []*url.URL{uri},
	}
	leafDER, err := x509.CreateCertificate(rand.Reader, leafTemplate, a.ca, publicKey, a.key)
	if err != nil {
		t.Fatal(err)
	}
	leaf, err := x509.ParseCertificate(leafDER)
	if err != nil {
		t.Fatal(err)
	}
	return tls.Certificate{
		Certificate: [][]byte{leafDER, a.ca.Raw},
		PrivateKey: privateKey,
		Leaf: leaf,
	}
}

func startHostSessionIngressTLSServer(
	t *testing.T, handler http.Handler, roots *x509.CertPool, clientCert *tls.Certificate,
) (*httptest.Server, *http.Client) {
	t.Helper()
	server := httptest.NewUnstartedServer(handler)
	server.TLS = &tls.Config{
		MinVersion: tls.VersionTLS13,
		ClientAuth: tls.RequireAndVerifyClientCert,
		ClientCAs: roots,
	}
	server.StartTLS()
	t.Cleanup(server.Close)

	client := server.Client()
	if clientCert != nil {
		transport, ok := client.Transport.(*http.Transport)
		if !ok {
			t.Fatal("httptest client transport is not *http.Transport")
		}
		transport = transport.Clone()
		transport.TLSClientConfig = transport.TLSClientConfig.Clone()
		transport.TLSClientConfig.Certificates = []tls.Certificate{*clientCert}
		client.Transport = transport
	}
	return server, client
}

func newHostSessionIngressHandler(
	t *testing.T, roots *x509.CertPool, repo *hostSessionIngressRepositoryStub,
) http.Handler {
	t.Helper()
	handler, err := NewHostSessionIngressHandler(HostSessionIngressDependencies{
		HostTrustRoots: roots, HostTrustDomain: "ccp.example", Repository: repo,
	})
	if err != nil {
		t.Fatal(err)
	}
	return handler
}

func TestNewHostSessionIngressHandlerRequiresDedicatedTrustAndRepository(t *testing.T) {
	authority := newHostCredentialAuthority(t)
	repo := &hostSessionIngressRepositoryStub{}
	if _, err := NewHostSessionIngressHandler(HostSessionIngressDependencies{
		HostTrustDomain: "ccp.example", Repository: repo,
	}); err == nil {
		t.Fatal("missing host trust roots allowed ingress construction")
	}
	if _, err := NewHostSessionIngressHandler(HostSessionIngressDependencies{
		HostTrustRoots: authority.roots, HostTrustDomain: "ccp.example",
	}); err == nil {
		t.Fatal("missing HostSession repository allowed ingress construction")
	}
}

func TestHostSessionIngressStartsAndRecordsOnlyDedicatedHostCredential(t *testing.T) {
	authority := newHostCredentialAuthority(t)
	clientCert := authority.issue(t, hostSessionIngressSubject, 2)
	recordedAt := time.Now().UTC().Truncate(time.Microsecond)
	binding := hostidentity.Binding{
		HostID: "machine-001", ProviderID: "provider-001", ClusterScope: "cluster-001",
		NodeUID: "node-uid-001", Subject: hostSessionIngressSubject,
	}
	repo := &hostSessionIngressRepositoryStub{
		fence: hostidentity.SessionFence{
			Binding: binding, Epoch: 1, SessionID: "hs_session_001",
			IssuedAt: recordedAt, ExpiresAt: recordedAt.Add(time.Minute),
		},
		disposition: hostidentity.Accept, recordedAt: recordedAt,
	}
	handler := newHostSessionIngressHandler(t, authority.roots, repo)
	server, client := startHostSessionIngressTLSServer(t, handler, authority.roots, &clientCert)

	start, err := http.NewRequest(http.MethodPost, server.URL+hostSessionStartPath,
		strings.NewReader(`{"expected_epoch":0,"ttl_seconds":60}`))
	if err != nil {
		t.Fatal(err)
	}
	start.Header.Set("Content-Type", "application/json")
	start.Header.Set("Idempotency-Key", "host-start-request-001")
	startResponse, err := client.Do(start)
	if err != nil {
		t.Fatal(err)
	}
	defer startResponse.Body.Close()
	if startResponse.StatusCode != http.StatusCreated || repo.startCalls != 1 {
		body, _ := io.ReadAll(startResponse.Body)
		t.Fatalf("HostSession start: HTTP %d calls=%d body=%s", startResponse.StatusCode, repo.startCalls, body)
	}
	if repo.startRequest.Subject != hostSessionIngressSubject ||
		repo.startRequest.ExpectedEpoch != 0 || repo.startRequest.RequestID != "host-start-request-001" ||
		repo.startRequest.TTL != time.Minute {
		t.Fatalf("start did not use verified host certificate subject and normalized request: %+v", repo.startRequest)
	}
	var startResponseBody map[string]any
	if err := json.NewDecoder(startResponse.Body).Decode(&startResponseBody); err != nil {
		t.Fatal(err)
	}
	if startResponseBody["session_id"] != "hs_session_001" || startResponseBody["epoch"] != float64(1) {
		t.Fatalf("unexpected session response: %v", startResponseBody)
	}

	heartbeatBody := `{"epoch":1,"session_id":"hs_session_001","sequence":1,"agent_version":"1.2.3"}`
	heartbeat, err := http.NewRequest(http.MethodPost, server.URL+hostHeartbeatPath, strings.NewReader(heartbeatBody))
	if err != nil {
		t.Fatal(err)
	}
	heartbeat.Header.Set("Content-Type", "application/json")
	heartbeatResponse, err := client.Do(heartbeat)
	if err != nil {
		t.Fatal(err)
	}
	defer heartbeatResponse.Body.Close()
	if heartbeatResponse.StatusCode != http.StatusAccepted || repo.heartbeatCalls != 1 {
		body, _ := io.ReadAll(heartbeatResponse.Body)
		t.Fatalf("heartbeat: HTTP %d calls=%d body=%s", heartbeatResponse.StatusCode, repo.heartbeatCalls, body)
	}
	if repo.heartbeatRequest.Subject != hostSessionIngressSubject ||
		repo.heartbeatRequest.Epoch != 1 || repo.heartbeatRequest.SessionID != "hs_session_001" ||
		repo.heartbeatRequest.Sequence != 1 ||
		!strings.HasPrefix(repo.heartbeatRequest.Digest, "sha256:") ||
		len(repo.heartbeatRequest.Digest) != 71 {
		t.Fatalf("heartbeat did not use verified host identity and server digest: %+v", repo.heartbeatRequest)
	}
	if strings.Contains(heartbeatBody, repo.heartbeatRequest.Digest) {
		t.Fatal("server accepted a digest from the HTTP body instead of computing it")
	}

	repo.disposition = hostidentity.Replay
	replay, err := http.NewRequest(http.MethodPost, server.URL+hostHeartbeatPath, strings.NewReader(heartbeatBody))
	if err != nil {
		t.Fatal(err)
	}
	replay.Header.Set("Content-Type", "application/json")
	replayResponse, err := client.Do(replay)
	if err != nil {
		t.Fatal(err)
	}
	defer replayResponse.Body.Close()
	if replayResponse.StatusCode != http.StatusOK || repo.heartbeatCalls != 2 {
		t.Fatalf("exact heartbeat replay: HTTP %d calls=%d", replayResponse.StatusCode, repo.heartbeatCalls)
	}
}

func TestHostSessionIngressRejectsMissingOrNonHostCredentials(t *testing.T) {
	authority := newHostCredentialAuthority(t)
	repo := &hostSessionIngressRepositoryStub{}
	handler := newHostSessionIngressHandler(t, authority.roots, repo)
	server, noCredentialClient := startHostSessionIngressTLSServer(t, handler, authority.roots, nil)

	request, err := http.NewRequest(http.MethodPost, server.URL+hostSessionStartPath,
		strings.NewReader(`{"expected_epoch":0,"ttl_seconds":30}`))
	if err != nil {
		t.Fatal(err)
	}
	request.Header.Set("Content-Type", "application/json")
	request.Header.Set("Idempotency-Key", "host-start-request")
	if _, err := noCredentialClient.Do(request); err == nil {
		t.Fatal("mTLS listener accepted a client without a host credential")
	}
	if repo.startCalls != 0 {
		t.Fatalf("unauthenticated client reached repository: %d calls", repo.startCalls)
	}

	workloadCert := authority.issue(t, "spiffe://ccp.example/workload/machine-001", 3)
	server2, workloadClient := startHostSessionIngressTLSServer(t, handler, authority.roots, &workloadCert)
	request, err = http.NewRequest(http.MethodPost, server2.URL+hostSessionStartPath,
		strings.NewReader(`{"expected_epoch":0,"ttl_seconds":30}`))
	if err != nil {
		t.Fatal(err)
	}
	request.Header.Set("Content-Type", "application/json")
	request.Header.Set("Idempotency-Key", "host-start-request")
	response, err := workloadClient.Do(request)
	if err != nil {
		t.Fatal(err)
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusUnauthorized || repo.startCalls != 0 {
		t.Fatalf("workload namespace credential was accepted as a host: HTTP %d calls=%d", response.StatusCode, repo.startCalls)
	}
}

func TestHostSessionIngressRejectsAmbiguousJSONAndInvalidSessionFields(t *testing.T) {
	authority := newHostCredentialAuthority(t)
	clientCert := authority.issue(t, hostSessionIngressSubject, 4)
	repo := &hostSessionIngressRepositoryStub{}
	handler := newHostSessionIngressHandler(t, authority.roots, repo)
	server, client := startHostSessionIngressTLSServer(t, handler, authority.roots, &clientCert)

	for _, body := range []string{
		`{"epoch":1,"session_id":"hs_1","sequence":1,"agent_version":"1","sequence":2}`,
		`{"epoch":1,"session_id":"hs_1","sequence":1,"agent_version":"1","unknown":true}`,
		`{"epoch":1,"session_id":"hs_1","sequence":1,"agent_version":null}`,
		`{"epoch":0,"session_id":"hs_1","sequence":1,"agent_version":"1"}`,
		`{"epoch":1,"session_id":"hs_1","sequence":0,"agent_version":"1"}`,
		`{"epoch":1,"session_id":"not-a-server-session","sequence":1,"agent_version":"1"}`,
	} {
		request, err := http.NewRequest(http.MethodPost, server.URL+hostHeartbeatPath, strings.NewReader(body))
		if err != nil {
			t.Fatal(err)
		}
		request.Header.Set("Content-Type", "application/json")
		response, err := client.Do(request)
		if err != nil {
			t.Fatal(err)
		}
		response.Body.Close()
		if response.StatusCode != http.StatusBadRequest || repo.heartbeatCalls != 0 {
			t.Fatalf("invalid heartbeat body %q: HTTP %d calls=%d", body, response.StatusCode, repo.heartbeatCalls)
		}
	}
}

func TestHostSessionIngressMapsRepositoryFencesWithoutLeakingErrors(t *testing.T) {
	authority := newHostCredentialAuthority(t)
	clientCert := authority.issue(t, hostSessionIngressSubject, 5)
	repo := &hostSessionIngressRepositoryStub{startErr: postgres.ErrHostSessionUnauthorized}
	handler := newHostSessionIngressHandler(t, authority.roots, repo)
	server, client := startHostSessionIngressTLSServer(t, handler, authority.roots, &clientCert)

	request, err := http.NewRequest(http.MethodPost, server.URL+hostSessionStartPath,
		strings.NewReader(`{"expected_epoch":0,"ttl_seconds":30}`))
	if err != nil {
		t.Fatal(err)
	}
	request.Header.Set("Content-Type", "application/json")
	request.Header.Set("Idempotency-Key", "host-start-request")
	response, err := client.Do(request)
	if err != nil {
		t.Fatal(err)
	}
	body, _ := io.ReadAll(response.Body)
	response.Body.Close()
	if response.StatusCode != http.StatusForbidden || strings.Contains(string(body), "SQL") {
		t.Fatalf("authorization failure leaked storage detail: HTTP %d %s", response.StatusCode, body)
	}

	repo.startErr = errors.New("database unavailable")
	request, err = http.NewRequest(http.MethodPost, server.URL+hostSessionStartPath,
		strings.NewReader(`{"expected_epoch":0,"ttl_seconds":30}`))
	if err != nil {
		t.Fatal(err)
	}
	request.Header.Set("Content-Type", "application/json")
	request.Header.Set("Idempotency-Key", "host-start-request")
	response, err = client.Do(request)
	if err != nil {
		t.Fatal(err)
	}
	response.Body.Close()
	if response.StatusCode != http.StatusServiceUnavailable {
		t.Fatalf("repository outage must fail closed with 503, got %d", response.StatusCode)
	}
}
