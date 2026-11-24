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
	"io"
	"math/big"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/avaargsh/compute-control-panel/internal/hostidentity"
	"github.com/avaargsh/compute-control-panel/internal/store/postgres"
)

const hostSessionIngressSubject = "spiffe://ccp.example/host/machine-001"

type hostSessionIngressRepositoryStub struct {
	mu               sync.Mutex
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
	s.mu.Lock()
	defer s.mu.Unlock()
	s.startCalls++
	s.startRequest = request
	return s.fence, s.startErr
}

func (s *hostSessionIngressRepositoryStub) RecordAuthenticatedHostHeartbeat(
	_ context.Context, request postgres.AuthenticatedHostHeartbeatRequest,
) (hostidentity.Disposition, time.Time, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.heartbeatCalls++
	s.heartbeatRequest = request
	return s.disposition, s.recordedAt, s.heartbeatErr
}

func (s *hostSessionIngressRepositoryStub) observedStart() (int, postgres.AuthenticatedHostSessionStartRequest) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.startCalls, s.startRequest
}

func (s *hostSessionIngressRepositoryStub) observedHeartbeat() (int, postgres.AuthenticatedHostHeartbeatRequest) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.heartbeatCalls, s.heartbeatRequest
}

func (s *hostSessionIngressRepositoryStub) setDisposition(disposition hostidentity.Disposition) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.disposition = disposition
}

func (s *hostSessionIngressRepositoryStub) setStartError(err error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.startErr = err
}

type hostCredentialAuthority struct {
	roots  *x509.CertPool
	ca     *x509.Certificate
	key    ed25519.PrivateKey
	crlDER []byte
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
		Subject:      pkix.Name{CommonName: "CCP host-only test CA"},
		NotBefore:    now.Add(-time.Hour), NotAfter: now.Add(time.Hour),
		IsCA: true, BasicConstraintsValid: true,
		KeyUsage:     x509.KeyUsageCertSign | x509.KeyUsageCRLSign | x509.KeyUsageDigitalSignature,
		SubjectKeyId: []byte{1, 2, 3, 4},
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
	crlDER, err := x509.CreateRevocationList(rand.Reader, &x509.RevocationList{
		Number: big.NewInt(1), ThisUpdate: now.Add(-time.Minute), NextUpdate: now.Add(24 * time.Hour),
	}, ca, privateKey)
	if err != nil {
		t.Fatal(err)
	}
	return &hostCredentialAuthority{roots: roots, ca: ca, key: privateKey, crlDER: crlDER}
}

func (a *hostCredentialAuthority) revocationList(
	t *testing.T, thisUpdate, nextUpdate time.Time, revoked ...int64,
) []byte {
	t.Helper()
	entries := make([]x509.RevocationListEntry, 0, len(revoked))
	for _, serial := range revoked {
		entries = append(entries, x509.RevocationListEntry{
			SerialNumber: big.NewInt(serial), RevocationTime: time.Now().UTC().Add(-time.Minute),
		})
	}
	der, err := x509.CreateRevocationList(rand.Reader, &x509.RevocationList{
		Number: big.NewInt(2), ThisUpdate: thisUpdate, NextUpdate: nextUpdate,
		RevokedCertificateEntries: entries,
	}, a.ca, a.key)
	if err != nil {
		t.Fatal(err)
	}
	return der
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
		NotBefore:    now.Add(-time.Hour), NotAfter: now.Add(time.Hour),
		KeyUsage:    x509.KeyUsageDigitalSignature,
		ExtKeyUsage: []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth},
		URIs:        []*url.URL{uri},
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
		PrivateKey:  privateKey,
		Leaf:        leaf,
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
		ClientCAs:  roots,
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
	t *testing.T, authority *hostCredentialAuthority, repo *hostSessionIngressRepositoryStub,
) http.Handler {
	t.Helper()
	return newHostSessionIngressHandlerWithPolicy(t, authority, [][]byte{authority.crlDER}, time.Now, repo)
}

func newHostSessionIngressHandlerWithPolicy(
	t *testing.T, authority *hostCredentialAuthority, crls [][]byte,
	now func() time.Time, repo *hostSessionIngressRepositoryStub,
) http.Handler {
	t.Helper()
	handler, err := NewHostSessionIngressHandler(HostSessionIngressDependencies{
		HostTrustRoots: authority.roots, HostTrustDomain: "ccp.example",
		HostRevocationCRLs: crls, HostRevocationIssuerCertificates: []*x509.Certificate{authority.ca},
		Now: now, Repository: repo,
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
		HostRevocationCRLs: [][]byte{authority.crlDER}, Now: time.Now,
	}); err == nil {
		t.Fatal("missing HostSession repository allowed ingress construction")
	}
	if _, err := NewHostSessionIngressHandler(HostSessionIngressDependencies{
		HostTrustRoots: authority.roots, HostTrustDomain: "ccp.example",
		Repository: repo, Now: time.Now,
	}); err == nil {
		t.Fatal("missing issuer CRL allowed ingress construction")
	}
	if _, err := NewHostSessionIngressHandler(HostSessionIngressDependencies{
		HostTrustRoots: authority.roots, HostTrustDomain: "ccp.example",
		HostRevocationCRLs: [][]byte{authority.crlDER}, Repository: repo,
	}); err == nil {
		t.Fatal("missing verifier clock allowed ingress construction")
	}
}

// A verified client certificate is not enough when the negotiated transport is legacy TLS.
func TestHostSessionIngressRejectsLegacyTLSBeforeRepository(t *testing.T) {
	authority := newHostCredentialAuthority(t)
	clientCert := authority.issue(t, hostSessionIngressSubject, 91)
	leaf, err := x509.ParseCertificate(clientCert.Certificate[0])
	if err != nil {
		t.Fatal(err)
	}
	issuer, err := x509.ParseCertificate(clientCert.Certificate[1])
	if err != nil {
		t.Fatal(err)
	}

	repo := &hostSessionIngressRepositoryStub{}
	handler := newHostSessionIngressHandler(t, authority, repo)
	request := httptest.NewRequest(http.MethodPost, hostSessionStartPath,
		strings.NewReader(`{"expected_epoch":0,"ttl_seconds":30}`))
	request.Header.Set("Content-Type", "application/json")
	request.Header.Set("Idempotency-Key", "legacy-tls-host-start")
	request.TLS = &tls.ConnectionState{
		Version:          tls.VersionTLS12,
		PeerCertificates: []*x509.Certificate{leaf},
		VerifiedChains:   [][]*x509.Certificate{{leaf, issuer}},
	}
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, request)

	if calls, _ := repo.observedStart(); response.Code != http.StatusUnauthorized || calls != 0 {
		t.Fatalf("legacy TLS reached HostSession repository: HTTP %d calls=%d", response.Code, calls)
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
	handler := newHostSessionIngressHandler(t, authority, repo)
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
	startCalls, startRequest := repo.observedStart()
	if startResponse.StatusCode != http.StatusCreated || startCalls != 1 {
		body, _ := io.ReadAll(startResponse.Body)
		t.Fatalf("HostSession start: HTTP %d calls=%d body=%s", startResponse.StatusCode, startCalls, body)
	}
	if startRequest.Subject != hostSessionIngressSubject ||
		startRequest.ExpectedEpoch != 0 || startRequest.RequestID != "host-start-request-001" ||
		startRequest.TTL != time.Minute {
		t.Fatalf("start did not use verified host certificate subject and normalized request: %+v", startRequest)
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
	heartbeatCalls, heartbeatRequest := repo.observedHeartbeat()
	if heartbeatResponse.StatusCode != http.StatusAccepted || heartbeatCalls != 1 {
		body, _ := io.ReadAll(heartbeatResponse.Body)
		t.Fatalf("heartbeat: HTTP %d calls=%d body=%s", heartbeatResponse.StatusCode, heartbeatCalls, body)
	}
	if heartbeatRequest.Subject != hostSessionIngressSubject ||
		heartbeatRequest.Epoch != 1 || heartbeatRequest.SessionID != "hs_session_001" ||
		heartbeatRequest.Sequence != 1 ||
		!strings.HasPrefix(heartbeatRequest.Digest, "sha256:") ||
		len(heartbeatRequest.Digest) != 71 {
		t.Fatalf("heartbeat did not use verified host identity and server digest: %+v", heartbeatRequest)
	}
	if strings.Contains(heartbeatBody, heartbeatRequest.Digest) {
		t.Fatal("server accepted a digest from the HTTP body instead of computing it")
	}

	repo.setDisposition(hostidentity.Replay)
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
	heartbeatCalls, replayRequest := repo.observedHeartbeat()
	if replayResponse.StatusCode != http.StatusOK || heartbeatCalls != 2 || replayRequest != heartbeatRequest {
		t.Fatalf("exact heartbeat replay: HTTP %d calls=%d", replayResponse.StatusCode, heartbeatCalls)
	}
}

func TestHostSessionIngressRejectsCredentialAfterAtomicCRLRotation(t *testing.T) {
	authority := newHostCredentialAuthority(t)
	clientCert := authority.issue(t, hostSessionIngressSubject, 79)
	now := time.Now().UTC()
	revocations, err := hostidentity.NewHostCredentialRevocationListStore([][]byte{authority.crlDER}, []*x509.Certificate{authority.ca}, now)
	if err != nil {
		t.Fatalf("create revocation store: %v", err)
	}
	issuedAt := now.Truncate(time.Microsecond)
	binding := hostidentity.Binding{
		HostID: "machine-001", ProviderID: "provider-001", ClusterScope: "cluster-001",
		NodeUID: "node-uid-001", Subject: hostSessionIngressSubject,
	}
	repo := &hostSessionIngressRepositoryStub{
		fence: hostidentity.SessionFence{
			Binding: binding, Epoch: 1, SessionID: "hs_session_001",
			IssuedAt: issuedAt, ExpiresAt: issuedAt.Add(time.Minute),
		},
	}
	handler, err := NewHostSessionIngressHandler(HostSessionIngressDependencies{
		HostTrustRoots: authority.roots, HostTrustDomain: "ccp.example",
		HostRevocationListStore: revocations, Now: time.Now, Repository: repo,
	})
	if err != nil {
		t.Fatalf("create host ingress with revocation store: %v", err)
	}
	server, client := startHostSessionIngressTLSServer(t, handler, authority.roots, &clientCert)
	start := func(requestID string) int {
		t.Helper()
		request, err := http.NewRequest(http.MethodPost, server.URL+hostSessionStartPath,
			strings.NewReader(`{"expected_epoch":0,"ttl_seconds":60}`))
		if err != nil {
			t.Fatal(err)
		}
		request.Header.Set("Content-Type", "application/json")
		request.Header.Set("Idempotency-Key", requestID)
		response, err := client.Do(request)
		if err != nil {
			t.Fatal(err)
		}
		defer response.Body.Close()
		return response.StatusCode
	}

	if status := start("before-crl-rotation"); status != http.StatusCreated {
		t.Fatalf("valid certificate rejected before rotation: HTTP %d", status)
	}
	if calls, _ := repo.observedStart(); calls != 1 {
		t.Fatalf("expected one repository call before rotation, got %d", calls)
	}

	rotated := authority.revocationList(t, now.Add(-time.Minute), now.Add(time.Hour), 79)
	if err := revocations.Replace([][]byte{rotated}, time.Now().UTC()); err != nil {
		t.Fatalf("install newer CRL: %v", err)
	}
	if status := start("after-crl-rotation"); status != http.StatusUnauthorized {
		t.Fatalf("newly revoked certificate accepted after atomic rotation: HTTP %d", status)
	}
	if calls, _ := repo.observedStart(); calls != 1 {
		t.Fatalf("revoked certificate reached repository after rotation: calls=%d", calls)
	}
}

func TestHostSessionIngressRejectsRevokedCertificateBeforeRepository(t *testing.T) {
	authority := newHostCredentialAuthority(t)
	clientCert := authority.issue(t, hostSessionIngressSubject, 77)
	now := time.Now().UTC()
	crl := authority.revocationList(t, now.Add(-time.Minute), now.Add(time.Hour), 77)
	repo := &hostSessionIngressRepositoryStub{}
	handler := newHostSessionIngressHandlerWithPolicy(t, authority, [][]byte{crl}, time.Now, repo)
	server, client := startHostSessionIngressTLSServer(t, handler, authority.roots, &clientCert)

	request, err := http.NewRequest(http.MethodPost, server.URL+hostSessionStartPath,
		strings.NewReader(`{"expected_epoch":0,"ttl_seconds":30}`))
	if err != nil {
		t.Fatal(err)
	}
	request.Header.Set("Content-Type", "application/json")
	request.Header.Set("Idempotency-Key", "revoked-host-start")
	response, err := client.Do(request)
	if err != nil {
		t.Fatal(err)
	}
	response.Body.Close()
	if calls, _ := repo.observedStart(); response.StatusCode != http.StatusUnauthorized || calls != 0 {
		t.Fatalf("revoked host credential reached repository: HTTP %d calls=%d", response.StatusCode, calls)
	}
}

func TestHostSessionIngressUsesInjectedClockForCertificateExpiry(t *testing.T) {
	authority := newHostCredentialAuthority(t)
	clientCert := authority.issue(t, hostSessionIngressSubject, 78)
	repo := &hostSessionIngressRepositoryStub{}
	future := clientCert.Leaf.NotAfter.Add(time.Second)
	handler := newHostSessionIngressHandlerWithPolicy(
		t, authority, [][]byte{authority.crlDER}, func() time.Time { return future }, repo,
	)
	server, client := startHostSessionIngressTLSServer(t, handler, authority.roots, &clientCert)

	request, err := http.NewRequest(http.MethodPost, server.URL+hostSessionStartPath,
		strings.NewReader(`{"expected_epoch":0,"ttl_seconds":30}`))
	if err != nil {
		t.Fatal(err)
	}
	request.Header.Set("Content-Type", "application/json")
	request.Header.Set("Idempotency-Key", "expired-host-start")
	response, err := client.Do(request)
	if err != nil {
		t.Fatal(err)
	}
	response.Body.Close()
	if calls, _ := repo.observedStart(); response.StatusCode != http.StatusUnauthorized || calls != 0 {
		t.Fatalf("certificate expired at injected verifier clock was accepted: HTTP %d calls=%d", response.StatusCode, calls)
	}
}

func TestHostSessionIngressRejectsMissingOrNonHostCredentials(t *testing.T) {
	authority := newHostCredentialAuthority(t)
	repo := &hostSessionIngressRepositoryStub{}
	handler := newHostSessionIngressHandler(t, authority, repo)
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
	if startCalls, _ := repo.observedStart(); startCalls != 0 {
		t.Fatalf("unauthenticated client reached repository: %d calls", startCalls)
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
	if startCalls, _ := repo.observedStart(); response.StatusCode != http.StatusUnauthorized || startCalls != 0 {
		t.Fatalf("workload namespace credential was accepted as a host: HTTP %d calls=%d", response.StatusCode, startCalls)
	}
}

func TestHostSessionIngressRejectsAmbiguousJSONAndInvalidSessionFields(t *testing.T) {
	authority := newHostCredentialAuthority(t)
	clientCert := authority.issue(t, hostSessionIngressSubject, 4)
	repo := &hostSessionIngressRepositoryStub{}
	handler := newHostSessionIngressHandler(t, authority, repo)
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
		if heartbeatCalls, _ := repo.observedHeartbeat(); response.StatusCode != http.StatusBadRequest || heartbeatCalls != 0 {
			t.Fatalf("invalid heartbeat body %q: HTTP %d calls=%d", body, response.StatusCode, heartbeatCalls)
		}
	}
}

func TestHostSessionIngressMapsRepositoryFencesWithoutLeakingErrors(t *testing.T) {
	authority := newHostCredentialAuthority(t)
	clientCert := authority.issue(t, hostSessionIngressSubject, 5)
	repo := &hostSessionIngressRepositoryStub{startErr: postgres.ErrHostSessionUnauthorized}
	handler := newHostSessionIngressHandler(t, authority, repo)
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

	repo.setStartError(errors.New("database unavailable"))
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
