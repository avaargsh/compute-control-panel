package hostidentity

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"net/http/httptest"
	"net/url"
	"testing"
	"time"
)

type hostAuthTestResolver struct {
	binding Binding
	err     error
	calls   int
	subject string
}

func (r *hostAuthTestResolver) ResolveHostBinding(_ context.Context, subject string) (Binding, error) {
	r.calls++
	r.subject = subject
	if r.err != nil {
		return Binding{}, r.err
	}
	return r.binding, nil
}

func hostAuthURI(t *testing.T, raw string) *url.URL {
	t.Helper()
	u, err := url.Parse(raw)
	if err != nil {
		t.Fatal(err)
	}
	return u
}

func hostAuthRequest(now time.Time, uris ...*url.URL) *httpRequestFixture {
	cert := &x509.Certificate{
		Raw:       []byte("verified-leaf"),
		URIs:      uris,
		NotBefore: now.Add(-time.Minute),
		NotAfter:  now.Add(time.Minute),
	}
	return &httpRequestFixture{tls: &tls.ConnectionState{
		PeerCertificates: []*x509.Certificate{cert},
		VerifiedChains:   [][]*x509.Certificate{{cert}},
	}}
}

type httpRequestFixture struct {
	tls *tls.ConnectionState
}

func (f *httpRequestFixture) request() *httpRequest {
	return &httpRequest{TLS: f.tls}
}

// These aliases keep the fixture focused on the verified TLS state.
type httpRequest = struct {
	TLS *tls.ConnectionState
}

func TestHostSPIFFEAuthenticatorResolvesVerifiedHostBinding(t *testing.T) {
	now := time.Date(2026, 11, 24, 6, 0, 0, 0, time.UTC)
	subject := "spiffe://ccp.example/host/node-a"
	binding := Binding{HostID: "host-a", ProviderID: "provider-a", ClusterScope: "cluster-a", NodeUID: "node-uid-a", Subject: subject}
	resolver := &hostAuthTestResolver{binding: binding}
	auth, err := NewHostSPIFFEAuthenticator("CCP.example", resolver, func() time.Time { return now })
	if err != nil {
		t.Fatal(err)
	}
	req := httptest.NewRequest("GET", "https://ccp.example/host/heartbeat", nil)
	cert := &x509.Certificate{Raw: []byte("verified-leaf"), URIs: []*url.URL{hostAuthURI(t, subject)}, NotBefore: now.Add(-time.Minute), NotAfter: now.Add(time.Minute)}
	req.TLS = &tls.ConnectionState{PeerCertificates: []*x509.Certificate{cert}, VerifiedChains: [][]*x509.Certificate{{cert}}}

	got, err := auth.Authenticate(context.Background(), req)
	if err != nil || got != binding {
		t.Fatalf("Authenticate() = %+v, %v; want %+v", got, err, binding)
	}
	if resolver.calls != 1 || resolver.subject != subject {
		t.Fatalf("resolver calls=%d subject=%q", resolver.calls, resolver.subject)
	}
}

func TestHostSPIFFEAuthenticatorRejectsUnverifiedTLS(t *testing.T) {
	now := time.Date(2026, 11, 24, 6, 0, 0, 0, time.UTC)
	subject := "spiffe://ccp.example/host/node-a"
	resolver := &hostAuthTestResolver{binding: Binding{HostID: "host-a", ProviderID: "provider-a", ClusterScope: "cluster-a", NodeUID: "node-a", Subject: subject}}
	auth, err := NewHostSPIFFEAuthenticator("ccp.example", resolver, func() time.Time { return now })
	if err != nil {
		t.Fatal(err)
	}

	cert := &x509.Certificate{Raw: []byte("leaf"), URIs: []*url.URL{hostAuthURI(t, subject)}, NotBefore: now.Add(-time.Minute), NotAfter: now.Add(time.Minute)}
	cases := []struct {
		name  string
		state *tls.ConnectionState
	}{
		{name: "no TLS"},
		{name: "peer only", state: &tls.ConnectionState{PeerCertificates: []*x509.Certificate{cert}}},
		{name: "verified chain only", state: &tls.ConnectionState{VerifiedChains: [][]*x509.Certificate{{cert}}}},
		{name: "different verified leaf", state: &tls.ConnectionState{PeerCertificates: []*x509.Certificate{cert}, VerifiedChains: [][]*x509.Certificate{{{Raw: []byte("other")}}}}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			req := httptest.NewRequest("GET", "https://ccp.example/", nil)
			req.TLS = tc.state
			if _, err := auth.Authenticate(context.Background(), req); !errors.Is(err, ErrHostAuthentication) {
				t.Fatalf("Authenticate() error=%v, want ErrHostAuthentication", err)
			}
		})
	}
	if resolver.calls != 0 {
		t.Fatalf("unverified TLS reached resolver %d times", resolver.calls)
	}
}

func TestHostSPIFFEAuthenticatorRequiresOneCanonicalHostIdentity(t *testing.T) {
	now := time.Date(2026, 11, 24, 6, 0, 0, 0, time.UTC)
	valid := "spiffe://ccp.example/host/node-a"
	cases := []struct {
		name string
		uris []string
	}{
		{name: "missing", uris: nil},
		{name: "wrong trust domain", uris: []string{"spiffe://other.example/host/node-a"}},
		{name: "workload class", uris: []string{"spiffe://ccp.example/workload/worker-a"}},
		{name: "duplicate host identities", uris: []string{valid, "spiffe://ccp.example/host/node-b"}},
		{name: "path traversal", uris: []string{"spiffe://ccp.example/host/../node-a"}},
		{name: "empty path segment", uris: []string{"spiffe://ccp.example/host//node-a"}},
		{name: "query", uris: []string{"spiffe://ccp.example/host/node-a?x=1"}},
		{name: "empty query", uris: []string{"spiffe://ccp.example/host/node-a?"}},
		{name: "fragment", uris: []string{"spiffe://ccp.example/host/node-a#fragment"}},
		{name: "encoded path", uris: []string{"spiffe://ccp.example/host/%2Fnode-a"}},
		{name: "userinfo", uris: []string{"spiffe://user@ccp.example/host/node-a"}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			var uris []*url.URL
			for _, raw := range tc.uris {
				uris = append(uris, hostAuthURI(t, raw))
			}
			resolver := &hostAuthTestResolver{binding: Binding{HostID: "host-a", ProviderID: "provider-a", ClusterScope: "cluster-a", NodeUID: "node-a", Subject: valid}}
			auth, err := NewHostSPIFFEAuthenticator("ccp.example", resolver, func() time.Time { return now })
			if err != nil {
				t.Fatal(err)
			}
			req := httptest.NewRequest("GET", "https://ccp.example/", nil)
			cert := &x509.Certificate{Raw: []byte("leaf"), URIs: uris, NotBefore: now.Add(-time.Minute), NotAfter: now.Add(time.Minute)}
			req.TLS = &tls.ConnectionState{PeerCertificates: []*x509.Certificate{cert}, VerifiedChains: [][]*x509.Certificate{{cert}}}
			if _, err := auth.Authenticate(context.Background(), req); !errors.Is(err, ErrHostAuthentication) {
				t.Fatalf("Authenticate() error=%v, want ErrHostAuthentication", err)
			}
			if resolver.calls != 0 {
				t.Fatalf("invalid URI reached resolver %d times", resolver.calls)
			}
		})
	}
}

func TestHostSPIFFEAuthenticatorChecksCertificateValidityWindow(t *testing.T) {
	now := time.Date(2026, 11, 24, 6, 0, 0, 0, time.UTC)
	subject := "spiffe://ccp.example/host/node-a"
	for _, tc := range []struct {
		name                string
		notBefore, notAfter time.Time
	}{
		{name: "expired", notBefore: now.Add(-2 * time.Hour), notAfter: now},
		{name: "not yet valid", notBefore: now.Add(time.Second), notAfter: now.Add(time.Hour)},
		{name: "zero validity", notBefore: time.Time{}, notAfter: now.Add(time.Hour)},
	} {
		t.Run(tc.name, func(t *testing.T) {
			resolver := &hostAuthTestResolver{binding: Binding{HostID: "host-a", ProviderID: "provider-a", ClusterScope: "cluster-a", NodeUID: "node-a", Subject: subject}}
			auth, err := NewHostSPIFFEAuthenticator("ccp.example", resolver, func() time.Time { return now })
			if err != nil {
				t.Fatal(err)
			}
			req := httptest.NewRequest("GET", "https://ccp.example/", nil)
			cert := &x509.Certificate{Raw: []byte("leaf"), URIs: []*url.URL{hostAuthURI(t, subject)}, NotBefore: tc.notBefore, NotAfter: tc.notAfter}
			req.TLS = &tls.ConnectionState{PeerCertificates: []*x509.Certificate{cert}, VerifiedChains: [][]*x509.Certificate{{cert}}}
			if _, err := auth.Authenticate(context.Background(), req); !errors.Is(err, ErrHostAuthentication) {
				t.Fatalf("Authenticate() error=%v, want ErrHostAuthentication", err)
			}
			if resolver.calls != 0 {
				t.Fatalf("invalid certificate reached resolver %d times", resolver.calls)
			}
		})
	}
}

func TestHostSPIFFEAuthenticatorFailsClosedOnResolverAndBindingMismatch(t *testing.T) {
	now := time.Date(2026, 11, 24, 6, 0, 0, 0, time.UTC)
	subject := "spiffe://ccp.example/host/node-a"
	validBinding := Binding{HostID: "host-a", ProviderID: "provider-a", ClusterScope: "cluster-a", NodeUID: "node-a", Subject: subject}
	cases := []struct {
		name     string
		resolver *hostAuthTestResolver
	}{
		{name: "authority unavailable", resolver: &hostAuthTestResolver{err: errors.New("authority unavailable")}},
		{name: "subject mismatch", resolver: &hostAuthTestResolver{binding: func() Binding { b := validBinding; b.Subject = "spiffe://ccp.example/host/other"; return b }()}},
		{name: "missing provider", resolver: &hostAuthTestResolver{binding: Binding{HostID: "host-a", ClusterScope: "cluster-a", NodeUID: "node-a", Subject: subject}}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			auth, err := NewHostSPIFFEAuthenticator("ccp.example", tc.resolver, func() time.Time { return now })
			if err != nil {
				t.Fatal(err)
			}
			req := httptest.NewRequest("GET", "https://ccp.example/", nil)
			cert := &x509.Certificate{Raw: []byte("leaf"), URIs: []*url.URL{hostAuthURI(t, subject)}, NotBefore: now.Add(-time.Minute), NotAfter: now.Add(time.Minute)}
			req.TLS = &tls.ConnectionState{PeerCertificates: []*x509.Certificate{cert}, VerifiedChains: [][]*x509.Certificate{{cert}}}
			if _, err := auth.Authenticate(context.Background(), req); !errors.Is(err, ErrHostAuthentication) {
				t.Fatalf("Authenticate() error=%v, want ErrHostAuthentication", err)
			}
		})
	}
}
