package httpapi

import (
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"errors"
	"math/big"
	"net/http"
	"net/http/httptest"
	"net/url"
	"testing"
	"time"
)

func providerAdminGrant() ProviderManagementGrant {
	return ProviderManagementGrant{
		Subject:    "spiffe://example.org/ns/admin/sa/manager",
		TenantID:   "tenant-001",
		Service:    "compute-admin",
		ProviderID: "provider-001",
		Action:     "read",
	}
}

func TestExactProviderManagementAuthorizerRejectsAmbiguousConfiguration(t *testing.T) {
	valid := providerAdminGrant()
	for _, tc := range []struct {
		name string
		edit func(*ProviderManagementGrant)
	}{
		{"empty subject", func(g *ProviderManagementGrant) { g.Subject = "" }},
		{"non-SPIFFE subject", func(g *ProviderManagementGrant) { g.Subject = "manager" }},
		{"wildcard subject", func(g *ProviderManagementGrant) { g.Subject = "spiffe://example.org/*" }},
		{"empty tenant", func(g *ProviderManagementGrant) { g.TenantID = "" }},
		{"empty service", func(g *ProviderManagementGrant) { g.Service = "" }},
		{"wildcard provider", func(g *ProviderManagementGrant) { g.ProviderID = "*" }},
		{"unsupported action", func(g *ProviderManagementGrant) { g.Action = "retire" }},
		{"case-folded action", func(g *ProviderManagementGrant) { g.Action = "READ" }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			g := valid
			tc.edit(&g)
			if _, err := NewExactProviderManagementAuthorizer([]ProviderManagementGrant{g}); err == nil {
				t.Fatal("invalid management grant was accepted")
			}
		})
	}
	if _, err := NewExactProviderManagementAuthorizer(nil); err == nil {
		t.Fatal("nil grant set must fail closed")
	}
	if _, err := NewExactProviderManagementAuthorizer([]ProviderManagementGrant{valid, valid}); err == nil {
		t.Fatal("duplicate management grant must be rejected")
	}
}

func TestExactProviderManagementAuthorizerBindsSubjectTenantServiceProviderAndAction(t *testing.T) {
	grant := providerAdminGrant()
	authz, err := NewExactProviderManagementAuthorizer([]ProviderManagementGrant{grant})
	if err != nil {
		t.Fatal(err)
	}
	principal := WorkloadPrincipal{
		Subject: grant.Subject, TenantID: grant.TenantID,
		Service: grant.Service, ProjectIDs: []string{"project-admin"},
	}
	if err := authz.AuthorizeProviderManage(context.Background(), principal, "read", grant.ProviderID); err != nil {
		t.Fatalf("exact grant denied: %v", err)
	}
	cases := []struct {
		name       string
		p          WorkloadPrincipal
		action     string
		providerID string
	}{
		{"different subject", WorkloadPrincipal{Subject: "spiffe://example.org/ns/admin/sa/other", TenantID: grant.TenantID, Service: grant.Service}, "read", grant.ProviderID},
		{"different tenant", WorkloadPrincipal{Subject: grant.Subject, TenantID: "tenant-other", Service: grant.Service}, "read", grant.ProviderID},
		{"different service", WorkloadPrincipal{Subject: grant.Subject, TenantID: grant.TenantID, Service: "agent-runtime"}, "read", grant.ProviderID},
		{"different provider", principal, "read", "provider-other"},
		{"different action", principal, "drain", grant.ProviderID},
		{"project grants not admin", WorkloadPrincipal{Subject: "spiffe://example.org/ns/agent-runtime/sa/worker", TenantID: grant.TenantID, Service: "agent-runtime", ProjectIDs: []string{"project-admin"}}, "read", grant.ProviderID},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if err := authz.AuthorizeProviderManage(context.Background(), tc.p, tc.action, tc.providerID); !errors.Is(err, ErrProviderManagementDenied) {
				t.Fatalf("authorization error=%v, want deny", err)
			}
		})
	}
	// Constructor must not retain caller-owned mutable grants.
	grant.Subject = "spiffe://example.org/ns/admin/sa/new"
	if err := authz.AuthorizeProviderManage(context.Background(), principal, "read", "provider-001"); err != nil {
		t.Fatalf("caller mutation changed policy: %v", err)
	}
}

func newProviderManagementTLSTestClient(t *testing.T, handler http.Handler, spiffeID string) (*httptest.Server, *http.Client) {
	t.Helper()
	_, caKey, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	now := time.Now()
	caTmpl := &x509.Certificate{
		SerialNumber: big.NewInt(1), Subject: pkix.Name{CommonName: "CCP test client CA"},
		NotBefore: now.Add(-time.Hour), NotAfter: now.Add(time.Hour),
		IsCA: true, BasicConstraintsValid: true,
		KeyUsage: x509.KeyUsageCertSign | x509.KeyUsageDigitalSignature,
	}
	caDER, err := x509.CreateCertificate(rand.Reader, caTmpl, caTmpl, caKey.Public(), caKey)
	if err != nil {
		t.Fatal(err)
	}
	ca, err := x509.ParseCertificate(caDER)
	if err != nil {
		t.Fatal(err)
	}
	_, leafKey, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	uri, err := url.Parse(spiffeID)
	if err != nil {
		t.Fatal(err)
	}
	leafTmpl := &x509.Certificate{
		SerialNumber: big.NewInt(2), NotBefore: now.Add(-time.Hour), NotAfter: now.Add(time.Hour),
		KeyUsage:    x509.KeyUsageDigitalSignature,
		ExtKeyUsage: []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth},
		URIs:        []*url.URL{uri},
	}
	leafDER, err := x509.CreateCertificate(rand.Reader, leafTmpl, ca, leafKey.Public(), caKey)
	if err != nil {
		t.Fatal(err)
	}
	clientCert := tls.Certificate{Certificate: [][]byte{leafDER}, PrivateKey: leafKey}
	pool := x509.NewCertPool()
	pool.AddCert(ca)
	server := httptest.NewUnstartedServer(handler)
	server.TLS = &tls.Config{
		MinVersion: tls.VersionTLS12, ClientAuth: tls.RequireAndVerifyClientCert,
		ClientCAs: pool,
	}
	server.StartTLS()
	t.Cleanup(server.Close)
	client := server.Client()
	client.Transport.(*http.Transport).TLSClientConfig.Certificates = []tls.Certificate{clientCert}
	return server, client
}

func TestProviderManageSPIFFEVerifiedTLSAndExactAdminGrant(t *testing.T) {
	grant := providerAdminGrant()
	for _, tc := range []struct {
		name      string
		grant     ProviderManagementGrant
		status    int
		repoCalls int
	}{
		{"matching grant", grant, http.StatusOK, 1},
		{"nonmatching provider", func() ProviderManagementGrant { g := grant; g.ProviderID = "provider-other"; return g }(), http.StatusForbidden, 0},
	} {
		t.Run(tc.name, func(t *testing.T) {
			repo := &providerManagementRepositoryStub{}
			authz, err := NewExactProviderManagementAuthorizer([]ProviderManagementGrant{tc.grant})
			if err != nil {
				t.Fatal(err)
			}
			resolver := &testWorkloadIdentityResolver{
				expected: grant.Subject,
				principal: WorkloadPrincipal{
					Subject: grant.Subject, TenantID: grant.TenantID, Service: grant.Service,
				},
			}
			auth, err := NewSPIFFEWorkloadAuthenticator("example.org", resolver)
			if err != nil {
				t.Fatal(err)
			}
			h, err := NewProviderManagementHandler(ProviderManagementDependencies{
				Authenticator: auth, Authorizer: authz, Repository: repo,
			})
			if err != nil {
				t.Fatal(err)
			}
			server, client := newProviderManagementTLSTestClient(t, h, grant.Subject)
			response, err := client.Get(server.URL + "/api/v1/manage/providers/provider-001")
			if err != nil {
				t.Fatal(err)
			}
			defer response.Body.Close()
			if response.StatusCode != tc.status || repo.readCalls != tc.repoCalls || resolver.calls != 1 {
				t.Fatalf("status=%d repo=%d resolver=%d", response.StatusCode, repo.readCalls, resolver.calls)
			}
		})
	}
}
