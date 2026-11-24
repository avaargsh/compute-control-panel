package hostidentity

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"
	"time"
)

func TestHostSPIFFEAuthenticatorRejectsOversizedIdentityBeforeResolver(t *testing.T) {
	now := time.Date(2026, 11, 24, 6, 0, 0, 0, time.UTC)
	subject := "spiffe://ccp.example/host/" + strings.Repeat("x", 1025)
	resolver := &hostAuthTestResolver{binding: Binding{
		HostID: "host-a", ProviderID: "provider-a", ClusterScope: "cluster-a",
		NodeUID: "node-a", Subject: subject,
	}}
	auth, err := NewHostSPIFFEAuthenticator("ccp.example", resolver, func() time.Time { return now })
	if err != nil {
		t.Fatal(err)
	}
	u, err := url.Parse(subject)
	if err != nil {
		t.Fatal(err)
	}
	cert := &x509.Certificate{
		Raw: []byte("leaf"), URIs: []*url.URL{u},
		NotBefore: now.Add(-time.Minute), NotAfter: now.Add(time.Minute),
	}
	req := httptest.NewRequest("GET", "https://ccp.example/", nil)
	req.TLS = &tls.ConnectionState{
		PeerCertificates: []*x509.Certificate{cert},
		VerifiedChains: [][]*x509.Certificate{{cert}},
	}
	if _, err := auth.Authenticate(context.Background(), req); !errors.Is(err, ErrHostAuthentication) {
		t.Fatalf("oversized identity accepted: %v", err)
	}
	if resolver.calls != 0 {
		t.Fatalf("oversized identity reached resolver %d times", resolver.calls)
	}
}

func TestNewHostSPIFFEAuthenticatorRejectsInvalidConfiguration(t *testing.T) {
	now := func() time.Time { return time.Date(2026, 11, 24, 6, 0, 0, 0, time.UTC) }
	resolver := &hostAuthTestResolver{}
	var typedNil *hostAuthTestResolver
	cases := []struct {
		name string
		domain string
		resolver HostIdentityResolver
		clock func() time.Time
	}{
		{name: "empty trust domain", domain: "", resolver: resolver, clock: now},
		{name: "path in trust domain", domain: "ccp.example/host", resolver: resolver, clock: now},
		{name: "typed nil resolver", domain: "ccp.example", resolver: typedNil, clock: now},
		{name: "nil clock", domain: "ccp.example", resolver: resolver},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if _, err := NewHostSPIFFEAuthenticator(tc.domain, tc.resolver, tc.clock); err == nil {
				t.Fatal("invalid authenticator configuration was accepted")
			}
		})
	}
}
