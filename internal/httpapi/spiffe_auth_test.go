package httpapi

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"net/http/httptest"
	"net/url"
	"testing"
)

type testWorkloadIdentityResolver struct {
	expected  string
	principal WorkloadPrincipal
	err       error
	calls     int
}

func (r *testWorkloadIdentityResolver) ResolveWorkloadPrincipal(_ context.Context, spiffeID string) (WorkloadPrincipal, error) {
	r.calls++
	if r.expected != "" && spiffeID != r.expected {
		return WorkloadPrincipal{}, errors.New("unexpected SPIFFE identity")
	}
	return r.principal, r.err
}

func TestNewSPIFFEWorkloadAuthenticatorRequiresTrustDomainAndResolver(t *testing.T) {
	if _, err := NewSPIFFEWorkloadAuthenticator("", &testWorkloadIdentityResolver{}); err == nil {
		t.Fatal("empty trust domain must fail")
	}
	if _, err := NewSPIFFEWorkloadAuthenticator("example.org", nil); err == nil {
		t.Fatal("nil resolver must fail")
	}
}

func TestSPIFFEWorkloadAuthenticatorRequiresVerifiedTLSChain(t *testing.T) {
	resolver := &testWorkloadIdentityResolver{}
	auth, err := NewSPIFFEWorkloadAuthenticator("example.org", resolver)
	if err != nil {
		t.Fatal(err)
	}
	request := httptest.NewRequest("GET", "/test", nil)
	if _, err := auth.Authenticate(context.Background(), request); !errors.Is(err, ErrWorkloadAuthentication) {
		t.Fatalf("missing TLS error = %v", err)
	}
	request.TLS = &tls.ConnectionState{PeerCertificates: []*x509.Certificate{{Raw: []byte("leaf")}}}
	if _, err := auth.Authenticate(context.Background(), request); !errors.Is(err, ErrWorkloadAuthentication) {
		t.Fatalf("unverified peer error = %v", err)
	}
	if resolver.calls != 0 {
		t.Fatalf("unverified TLS reached identity resolver: %d calls", resolver.calls)
	}
}

func TestSPIFFEWorkloadAuthenticatorRejectsMissingAmbiguousAndUntrustedIDs(t *testing.T) {
	resolver := &testWorkloadIdentityResolver{}
	auth, err := NewSPIFFEWorkloadAuthenticator("example.org", resolver)
	if err != nil {
		t.Fatal(err)
	}
	cases := []struct {
		name string
		ids []string
	}{
		{name: "no identity"},
		{name: "wrong trust domain", ids: []string{"spiffe://other.example/ns/agent/sa/runtime"}},
		{name: "multiple identities", ids: []string{"spiffe://example.org/ns/agent/sa/runtime", "spiffe://example.org/ns/sandbox/sa/provider"}},
		{name: "parent path", ids: []string{"spiffe://example.org/ns/../sa/runtime"}},
		{name: "empty path segment", ids: []string{"spiffe://example.org/ns//sa/runtime"}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			request := httptest.NewRequest("GET", "/test", nil)
			uris := make([]*url.URL, 0, len(tc.ids))
			for _, value := range tc.ids {
				parsed, err := url.Parse(value)
				if err != nil {
					t.Fatal(err)
				}
				uris = append(uris, parsed)
			}
			leaf := &x509.Certificate{Raw: []byte("verified-leaf"), URIs: uris}
			request.TLS = &tls.ConnectionState{
				PeerCertificates: []*x509.Certificate{leaf},
				VerifiedChains:   [][]*x509.Certificate{{leaf}},
			}
			if _, err := auth.Authenticate(context.Background(), request); !errors.Is(err, ErrWorkloadAuthentication) {
				t.Fatalf("authentication error = %v", err)
			}
		})
	}
	if resolver.calls != 0 {
		t.Fatalf("invalid SPIFFE identities reached resolver: %d calls", resolver.calls)
	}
}

func TestSPIFFEWorkloadAuthenticatorResolvesTrustedPrincipal(t *testing.T) {
	const identity = "spiffe://example.org/ns/agent-runtime/sa/runtime"
	resolver := &testWorkloadIdentityResolver{
		expected: identity,
		principal: WorkloadPrincipal{
			TenantID:   "tenant-001",
			Subject:    identity,
			Service:    "agent-runtime",
			ProjectIDs: []string{"project-001"},
		},
	}
	auth, err := NewSPIFFEWorkloadAuthenticator("example.org", resolver)
	if err != nil {
		t.Fatal(err)
	}
	request := httptest.NewRequest("GET", "/test", nil)
	leaf := &x509.Certificate{Raw: []byte("verified-leaf")}
	leaf.URIs = []*url.URL{{Scheme: "spiffe", Host: "example.org", Path: "/ns/agent-runtime/sa/runtime"}}
	request.TLS = &tls.ConnectionState{
		PeerCertificates: []*x509.Certificate{leaf},
		VerifiedChains:   [][]*x509.Certificate{{leaf}},
	}
	principal, err := auth.Authenticate(context.Background(), request)
	if err != nil {
		t.Fatal(err)
	}
	if principal.TenantID != "tenant-001" || principal.Service != "agent-runtime" ||
		principal.Subject != identity || len(principal.ProjectIDs) != 1 || principal.ProjectIDs[0] != "project-001" {
		t.Fatalf("unexpected resolved principal: %+v", principal)
	}
	if resolver.calls != 1 {
		t.Fatalf("resolver calls = %d, want 1", resolver.calls)
	}
	resolver.principal.ProjectIDs[0] = "mutated-after-return"
	if principal.ProjectIDs[0] != "project-001" {
		t.Fatal("principal project grants alias resolver-owned memory")
	}
}

func TestSPIFFEWorkloadAuthenticatorRejectsPrincipalSubjectMismatch(t *testing.T) {
	resolver := &testWorkloadIdentityResolver{
		principal: WorkloadPrincipal{
			TenantID: "tenant-001",
			Subject:  "spiffe://example.org/ns/other/sa/service",
			Service:  "agent-runtime",
		},
	}
	auth, err := NewSPIFFEWorkloadAuthenticator("example.org", resolver)
	if err != nil {
		t.Fatal(err)
	}
	request := httptest.NewRequest("GET", "/test", nil)
	leaf := &x509.Certificate{Raw: []byte("verified-leaf"), URIs: []*url.URL{{Scheme: "spiffe", Host: "example.org", Path: "/ns/agent-runtime/sa/runtime"}}}
	request.TLS = &tls.ConnectionState{
		PeerCertificates: []*x509.Certificate{leaf},
		VerifiedChains:   [][]*x509.Certificate{{leaf}},
	}
	if _, err := auth.Authenticate(context.Background(), request); !errors.Is(err, ErrWorkloadAuthentication) {
		t.Fatalf("principal mismatch error = %v", err)
	}
}
