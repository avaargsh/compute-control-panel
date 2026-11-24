package httpapi

import (
	"bytes"
	"context"
	"crypto/ed25519"
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

	"github.com/avaargsh/compute-control-panel/internal/provider"
)

const draReceiptTestSubject = "spiffe://example.org/ns/dra/sa/driver"

type draReceiptTestAuthenticator struct {
	principal WorkloadPrincipal
	err       error
}

func (a draReceiptTestAuthenticator) Authenticate(_ context.Context, _ *http.Request) (WorkloadPrincipal, error) {
	return a.principal, a.err
}

type draReceiptTestResolver struct {
	verifierCalls int
	calls         int
	err           error
	verifier      *provider.DRAUnprepareVerifier
	serverNow     time.Time
	candidate     provider.ReleaseEvidenceCandidate
}

func (r *draReceiptTestResolver) ResolveRegisteredDRAUnprepareVerifier(_ context.Context, _ string, _ provider.DRAUnprepareEvent) (*provider.DRAUnprepareVerifier, time.Time, error) {
	r.verifierCalls++
	return r.verifier, r.serverNow, nil
}

func (r *draReceiptTestResolver) ResolveDRAReceiptEvidence(_ context.Context, _ WorkloadPrincipal, _ provider.DRAUnprepareEvent) (provider.ReleaseEvidenceCandidate, error) {
	r.calls++
	return r.candidate, r.err
}

type draReceiptTestJournal struct {
	calls   int
	created bool
	err     error
}

func (j *draReceiptTestJournal) VerifyAndRecordRestricted(_ context.Context, _ *provider.DRAUnprepareVerifier, _ provider.ReleaseEvidenceCandidate, _ provider.SignedDRAUnprepare, _ time.Time) (bool, error) {
	j.calls++
	return j.created, j.err
}

func draReceiptTestPrincipal() WorkloadPrincipal {
	return WorkloadPrincipal{Subject: draReceiptTestSubject, TenantID: "platform", Service: "dra-node-driver"}
}

func draReceiptTestTLS() *tls.ConnectionState {
	leaf := &x509.Certificate{Raw: []byte("verified-client-cert"), URIs: []*url.URL{{
		Scheme: "spiffe", Host: "example.org", Path: "/ns/dra/sa/driver",
	}}}
	return &tls.ConnectionState{Version: tls.VersionTLS13, PeerCertificates: []*x509.Certificate{leaf}, VerifiedChains: [][]*x509.Certificate{{leaf}}}
}

func draReceiptSPIFFEAuthenticator(t *testing.T) WorkloadAuthenticator {
	t.Helper()
	auth, err := NewSPIFFEWorkloadAuthenticator("example.org", &testWorkloadIdentityResolver{
		expected: draReceiptTestSubject, principal: draReceiptTestPrincipal(),
	})
	if err != nil {
		t.Fatal(err)
	}
	return auth
}

func draReceiptTestPrivateKey() ed25519.PrivateKey {
	return ed25519.NewKeyFromSeed(bytes.Repeat([]byte{1}, ed25519.SeedSize))
}

func draReceiptTestSign(event provider.DRAUnprepareEvent) []byte {
	payload, err := json.Marshal(event)
	if err != nil {
		panic(err)
	}
	payload = append([]byte("CCP:DRA:NodeUnprepareResources:v1\n"), payload...)
	return ed25519.Sign(draReceiptTestPrivateKey(), payload)
}

func draReceiptTestSigned() provider.SignedDRAUnprepare {
	event := provider.DRAUnprepareEvent{
		Version: "ccp.dra.node-unprepare.v1", ClusterID: "cluster-a",
		SourceID: "source-a", KeyID: "key-a", EventID: "event-a",
		RPC: "NodeUnprepareResources", Result: "SUCCESS",
		Driver: "gpu.example.com", NodeName: "node-a",
		ClaimNamespace: "tenant-a", ClaimName: "claim-a", ClaimUID: "claim-uid-a",
		PodUID: "pod-uid-a", AllocatedClaimResourceVersion: "11",
		AllocatedDevice: provider.DRAAllocatedDevice{
			Request: "gpu", Driver: "gpu.example.com", Pool: "pool-a", Device: "gpu-a",
		},
		CompletedAt: time.Now().UTC(),
	}
	return provider.SignedDRAUnprepare{Event: event, Signature: draReceiptTestSign(event)}
}

func draReceiptTestBody(t *testing.T, signed provider.SignedDRAUnprepare) []byte {
	t.Helper()
	body, err := json.Marshal(map[string]any{"event": signed.Event, "signature": signed.Signature})
	if err != nil {
		t.Fatal(err)
	}
	return body
}

func draReceiptTestHandler(t *testing.T, authenticator WorkloadAuthenticator) (http.Handler, *draReceiptTestResolver, *draReceiptTestJournal) {
	t.Helper()
	policy, err := NewExactDRAReceiptIngestAuthorizer([]DRAReceiptIngestGrant{{
		Subject: draReceiptTestSubject, TenantID: "platform", Service: "dra-node-driver",
		ClusterID: "cluster-a", SourceID: "source-a", Driver: "gpu.example.com", NodeName: "node-a",
	}})
	if err != nil {
		t.Fatal(err)
	}
	verifier, err := provider.NewDRAUnprepareVerifier(provider.DRAUnprepareAuthority{
		ClusterID: "cluster-a", SourceID: "source-a", KeyID: "key-a",
		Driver: "gpu.example.com", PublicKey: draReceiptTestPrivateKey().Public().(ed25519.PublicKey),
	}, time.Minute, 10*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	resolver := &draReceiptTestResolver{verifier: verifier, serverNow: time.Now().UTC()}
	resolver.candidate.Create.ProviderID = "provider-a"
	resolver.candidate.Delete.ProviderID = "provider-a"
	journal := &draReceiptTestJournal{created: true}
	h, err := NewDRAReceiptIngestHandler(DRAReceiptIngestDependencies{
		Authenticator: authenticator, Authorizer: policy, VerifierResolver: &RegisteredDRAReceiptVerifierResolver{source: resolver}, Resolver: resolver, Journal: journal,
	})
	if err != nil {
		t.Fatal(err)
	}
	return h, resolver, journal
}

func TestDRAReceiptIngestRefusesMissingDependenciesAndInvalidGrants(t *testing.T) {
	if _, err := NewDRAReceiptIngestHandler(DRAReceiptIngestDependencies{}); err == nil {
		t.Fatal("nil dependencies allowed an internal receipt endpoint")
	}
	if _, err := NewExactDRAReceiptIngestAuthorizer(nil); err == nil {
		t.Fatal("missing grants silently allowed")
	}
	if _, err := NewExactDRAReceiptIngestAuthorizer([]DRAReceiptIngestGrant{{
		Subject: draReceiptTestSubject, TenantID: "platform", Service: "agent-runtime",
		ClusterID: "cluster-a", SourceID: "source-a", Driver: "gpu.example.com", NodeName: "node-a",
	}}); err == nil {
		t.Fatal("non-driver workload role allowed as a DRA receipt source")
	}
}

// A generic caller-provided authenticator could assert any Subject despite a
// verified peer TLS chain. The internal ingress must only accept the SPIFFE
// authenticator that binds certificate SAN to authoritative directory Subject.
func TestDRAReceiptIngestConstructorRefusesArbitraryAuthenticator(t *testing.T) {
	policy, err := NewExactDRAReceiptIngestAuthorizer([]DRAReceiptIngestGrant{{
		Subject: draReceiptTestSubject, TenantID: "platform", Service: "dra-node-driver",
		ClusterID: "cluster-a", SourceID: "source-a", Driver: "gpu.example.com", NodeName: "node-a",
	}})
	if err != nil {
		t.Fatal(err)
	}
	h, err := NewDRAReceiptIngestHandler(DRAReceiptIngestDependencies{
		Authenticator: draReceiptTestAuthenticator{principal: draReceiptTestPrincipal()},
		Authorizer:    policy, VerifierResolver: &RegisteredDRAReceiptVerifierResolver{source: &draReceiptTestResolver{}}, Resolver: &draReceiptTestResolver{},
		Journal: &draReceiptTestJournal{},
	})
	if err == nil || h != nil {
		t.Fatal("injected authenticator could impersonate the TLS peer for DRA receipts")
	}
}

func TestDRAReceiptIngestRequiresVerifiedSPIFFEIdentityAndExactSource(t *testing.T) {
	identityResolver := &testWorkloadIdentityResolver{expected: draReceiptTestSubject, principal: draReceiptTestPrincipal()}
	auth, err := NewSPIFFEWorkloadAuthenticator("example.org", identityResolver)
	if err != nil {
		t.Fatal(err)
	}
	h, resolver, journal := draReceiptTestHandler(t, auth)
	body := draReceiptTestBody(t, draReceiptTestSigned())
	req := httptest.NewRequest(http.MethodPost, "/internal/v1/dra/node-unprepare-receipts", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	if rec.Code != http.StatusUnauthorized || resolver.calls != 0 || journal.calls != 0 {
		t.Fatalf("unauthenticated receipt reached resolver/journal: HTTP %d calls %d/%d", rec.Code, resolver.calls, journal.calls)
	}

	spiffeURI, err := url.Parse(draReceiptTestSubject)
	if err != nil {
		t.Fatal(err)
	}
	leaf := &x509.Certificate{Raw: []byte("verified-client-cert"), URIs: []*url.URL{spiffeURI}}
	peer := &tls.ConnectionState{Version: tls.VersionTLS13, PeerCertificates: []*x509.Certificate{leaf}, VerifiedChains: [][]*x509.Certificate{{leaf}}}
	req = httptest.NewRequest(http.MethodPost, "/internal/v1/dra/node-unprepare-receipts", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	req.TLS = peer
	rec = httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	if rec.Code != http.StatusAccepted || resolver.calls != 1 || journal.calls != 1 {
		t.Fatalf("verified identity refused: HTTP %d calls %d/%d", rec.Code, resolver.calls, journal.calls)
	}

	wrong := draReceiptTestSigned()
	wrong.Event.SourceID = "another-node-source"
	req = httptest.NewRequest(http.MethodPost, "/internal/v1/dra/node-unprepare-receipts", bytes.NewReader(draReceiptTestBody(t, wrong)))
	req.Header.Set("Content-Type", "application/json")
	req.TLS = peer
	rec = httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	if rec.Code != http.StatusForbidden || resolver.calls != 1 || journal.calls != 1 {
		t.Fatalf("cross-source principal bypassed policy: HTTP %d calls %d/%d", rec.Code, resolver.calls, journal.calls)
	}
}

func TestDRAReceiptIngestRejectsHistoricalEvidenceFromDifferentProvider(t *testing.T) {
	for _, tc := range []struct {
		name   string
		mutate func(*provider.ReleaseEvidenceCandidate)
	}{
		{name: "CREATE provider differs", mutate: func(c *provider.ReleaseEvidenceCandidate) { c.Create.ProviderID = "provider-b" }},
		{name: "DELETE provider differs", mutate: func(c *provider.ReleaseEvidenceCandidate) { c.Delete.ProviderID = "provider-b" }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			h, resolver, journal := draReceiptTestHandler(t, draReceiptSPIFFEAuthenticator(t))
			tc.mutate(&resolver.candidate)
			req := httptest.NewRequest(http.MethodPost, draReceiptIngestPath,
				bytes.NewReader(draReceiptTestBody(t, draReceiptTestSigned())))
			req.Header.Set("Content-Type", "application/json")
			req.TLS = draReceiptTestTLS()
			rec := httptest.NewRecorder()
			h.ServeHTTP(rec, req)
			if rec.Code != http.StatusForbidden || resolver.calls != 1 || journal.calls != 0 {
				t.Fatalf("cross-Provider historical evidence reached journal: HTTP %d evidence=%d journal=%d",
					rec.Code, resolver.calls, journal.calls)
			}
		})
	}
}

func TestDRAReceiptIngestRejectsInvalidSignatureBeforeHistoricalEvidenceResolution(t *testing.T) {
	h, resolver, journal := draReceiptTestHandler(t, draReceiptSPIFFEAuthenticator(t))
	signed := draReceiptTestSigned()
	signed.Signature[0] ^= 0xff
	req := httptest.NewRequest(http.MethodPost, draReceiptIngestPath, bytes.NewReader(draReceiptTestBody(t, signed)))
	req.Header.Set("Content-Type", "application/json")
	req.TLS = draReceiptTestTLS()
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	if rec.Code != http.StatusForbidden || resolver.verifierCalls != 1 || resolver.calls != 0 || journal.calls != 0 {
		t.Fatalf("invalid signature reached historical evidence resolver/journal: HTTP %d verifier=%d evidence=%d journal=%d",
			rec.Code, resolver.verifierCalls, resolver.calls, journal.calls)
	}
}

func TestDRAReceiptIngestUsesRegistryDatabaseClockBeforeHistoricalEvidenceResolution(t *testing.T) {
	h, resolver, journal := draReceiptTestHandler(t, draReceiptSPIFFEAuthenticator(t))
	signed := draReceiptTestSigned()
	// The receipt is fresh relative to the process clock but stale relative
	// to the authoritative PostgreSQL clock returned by the source resolver.
	resolver.serverNow = signed.Event.CompletedAt.Add(2 * time.Minute)
	req := httptest.NewRequest(http.MethodPost, draReceiptIngestPath, bytes.NewReader(draReceiptTestBody(t, signed)))
	req.Header.Set("Content-Type", "application/json")
	req.TLS = draReceiptTestTLS()
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	if rec.Code != http.StatusForbidden || resolver.verifierCalls != 1 || resolver.calls != 0 || journal.calls != 0 {
		t.Fatalf("stale receipt reached historical evidence resolver/journal: HTTP %d verifier=%d evidence=%d journal=%d",
			rec.Code, resolver.verifierCalls, resolver.calls, journal.calls)
	}
}

func TestDRAReceiptIngestRejectsMissingRegistryDatabaseClock(t *testing.T) {
	h, resolver, journal := draReceiptTestHandler(t, draReceiptSPIFFEAuthenticator(t))
	resolver.serverNow = time.Time{}
	req := httptest.NewRequest(http.MethodPost, draReceiptIngestPath, bytes.NewReader(draReceiptTestBody(t, draReceiptTestSigned())))
	req.Header.Set("Content-Type", "application/json")
	req.TLS = draReceiptTestTLS()
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	if rec.Code != http.StatusServiceUnavailable || resolver.verifierCalls != 1 || resolver.calls != 0 || journal.calls != 0 {
		t.Fatalf("missing registry clock did not fail closed: HTTP %d verifier=%d evidence=%d journal=%d",
			rec.Code, resolver.verifierCalls, resolver.calls, journal.calls)
	}
}

func TestDRAReceiptIngestRejectsClientEvidenceAndAmbiguousJSON(t *testing.T) {
	h, resolver, journal := draReceiptTestHandler(t, draReceiptSPIFFEAuthenticator(t))
	payload := string(draReceiptTestBody(t, draReceiptTestSigned()))
	for _, tc := range []struct{ name, body string }{
		{"caller-supplied allocation evidence", strings.TrimSuffix(payload, "}") + ",\"candidate\":{\"allocation_id\":\"forged\"}}"},
		{"case-folded source identity alias", strings.Replace(payload, "\"source_id\"", "\"Source_ID\"", 1)},
		{"duplicate source key", strings.Replace(payload, "\"source_id\":\"source-a\"", "\"source_id\":\"source-a\",\"source_id\":\"source-b\"", 1)},
		{"null signature", strings.Replace(payload, "\"signature\":\"", "\"signature\":null,\"ignored\":\"", 1)},
	} {
		t.Run(tc.name, func(t *testing.T) {
			req := httptest.NewRequest(http.MethodPost, "/internal/v1/dra/node-unprepare-receipts", strings.NewReader(tc.body))
			req.Header.Set("Content-Type", "application/json")
			req.TLS = draReceiptTestTLS()
			rec := httptest.NewRecorder()
			h.ServeHTTP(rec, req)
			if rec.Code != http.StatusBadRequest || resolver.calls != 0 || journal.calls != 0 {
				t.Fatalf("unsafe JSON accepted: HTTP %d calls %d/%d", rec.Code, resolver.calls, journal.calls)
			}
		})
	}
}

func TestDRAReceiptIngestRejectsLegacyTLSAndAmbiguousContentType(t *testing.T) {
	h, resolver, journal := draReceiptTestHandler(t, draReceiptSPIFFEAuthenticator(t))
	body := draReceiptTestBody(t, draReceiptTestSigned())

	legacyTLS := httptest.NewRequest(http.MethodPost, draReceiptIngestPath, bytes.NewReader(body))
	legacyTLS.Header.Set("Content-Type", "application/json")
	legacy := draReceiptTestTLS()
	legacy.Version = tls.VersionTLS12
	legacyTLS.TLS = legacy
	legacyResponse := httptest.NewRecorder()
	h.ServeHTTP(legacyResponse, legacyTLS)
	if legacyResponse.Code != http.StatusUnauthorized || resolver.calls != 0 || journal.calls != 0 {
		t.Fatalf("legacy TLS reached receipt resolution: HTTP %d calls=%d/%d",
			legacyResponse.Code, resolver.calls, journal.calls)
	}

	ambiguous := httptest.NewRequest(http.MethodPost, draReceiptIngestPath, bytes.NewReader(body))
	ambiguous.Header.Add("Content-Type", "application/json")
	ambiguous.Header.Add("Content-Type", "text/plain")
	ambiguous.TLS = draReceiptTestTLS()
	ambiguousResponse := httptest.NewRecorder()
	h.ServeHTTP(ambiguousResponse, ambiguous)
	if ambiguousResponse.Code != http.StatusUnsupportedMediaType || resolver.calls != 0 || journal.calls != 0 {
		t.Fatalf("ambiguous Content-Type reached receipt resolution: HTTP %d calls=%d/%d",
			ambiguousResponse.Code, resolver.calls, journal.calls)
	}
}

func TestDRAReceiptIngestRecordsOnlyEvidenceAndFailsClosedOnDependencies(t *testing.T) {
	h, resolver, journal := draReceiptTestHandler(t, draReceiptSPIFFEAuthenticator(t))
	signed := draReceiptTestSigned()
	for i, created := range []bool{true, false} {
		journal.created = created
		req := httptest.NewRequest(http.MethodPost, "/internal/v1/dra/node-unprepare-receipts", bytes.NewReader(draReceiptTestBody(t, signed)))
		req.Header.Set("Content-Type", "application/json")
		req.TLS = draReceiptTestTLS()
		rec := httptest.NewRecorder()
		h.ServeHTTP(rec, req)
		want := http.StatusAccepted
		if !created {
			want = http.StatusOK
		}
		if rec.Code != want || rec.Header().Get("Cache-Control") != "no-store" {
			t.Fatalf("receipt %d response HTTP %d", i, rec.Code)
		}
		if strings.Contains(strings.ToUpper(rec.Body.String()), "RELEASED") || strings.Contains(strings.ToUpper(rec.Body.String()), "FINAL") {
			t.Fatalf("HTTP response falsely claims release: %q", rec.Body.String())
		}
	}
	if journal.calls != 2 {
		t.Fatalf("journal calls=%d, want 2", journal.calls)
	}
	resolver.err = errors.New("trusted evidence unavailable")
	req := httptest.NewRequest(http.MethodPost, "/internal/v1/dra/node-unprepare-receipts", bytes.NewReader(draReceiptTestBody(t, signed)))
	req.Header.Set("Content-Type", "application/json")
	req.TLS = draReceiptTestTLS()
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	if rec.Code != http.StatusServiceUnavailable || journal.calls != 2 {
		t.Fatalf("resolver failure allowed journal write: HTTP %d calls=%d", rec.Code, journal.calls)
	}
	resolver.err = nil
	journal.err = provider.ErrDRAReceiptJournalRole
	req = httptest.NewRequest(http.MethodPost, "/internal/v1/dra/node-unprepare-receipts", bytes.NewReader(draReceiptTestBody(t, signed)))
	req.Header.Set("Content-Type", "application/json")
	req.TLS = draReceiptTestTLS()
	rec = httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	if rec.Code != http.StatusServiceUnavailable || journal.calls != 3 {
		t.Fatalf("unsafe journal failed open: HTTP %d calls=%d", rec.Code, journal.calls)
	}
}
