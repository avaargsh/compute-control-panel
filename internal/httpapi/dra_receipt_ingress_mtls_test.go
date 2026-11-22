package httpapi

import (
	"bytes"
	"crypto/ed25519"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"io"
	"math/big"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"
	"time"
)

// This is an actual TLS handshake between an in-memory HTTP server and real
// ephemeral X.509 certificates. The evidence resolver and journal remain
// test doubles: success here proves transport/identity isolation ONLY, not
// NodeUnprepareResources execution, PostgreSQL ingest, or GPU deallocation.
func draReceiptMTLSTestCA(t *testing.T, label string) (*x509.Certificate, ed25519.PrivateKey, *x509.CertPool) {
	t.Helper()
	publicKey, privateKey, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	template := &x509.Certificate{}
	template.SerialNumber = big.NewInt(91001)
	template.Subject = pkix.Name{CommonName: label}
	template.NotBefore = time.Now().Add(-time.Hour)
	template.NotAfter = time.Now().Add(time.Hour)
	template.IsCA = true
	template.BasicConstraintsValid = true
	template.KeyUsage = x509.KeyUsageCertSign | x509.KeyUsageDigitalSignature
	encoded, err := x509.CreateCertificate(rand.Reader, template, template, publicKey, privateKey)
	if err != nil {
		t.Fatal(err)
	}
	ca, err := x509.ParseCertificate(encoded)
	if err != nil {
		t.Fatal(err)
	}
	pool := x509.NewCertPool()
	pool.AddCert(ca)
	return ca, privateKey, pool
}

func draReceiptMTLSTestLeaf(
	t *testing.T, issuer *x509.Certificate, issuerKey ed25519.PrivateKey,
	serial int64, server bool, identities ...string,
) tls.Certificate {
	t.Helper()
	publicKey, privateKey, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	template := &x509.Certificate{}
	template.SerialNumber = big.NewInt(serial)
	template.NotBefore = time.Now().Add(-time.Hour)
	template.NotAfter = time.Now().Add(time.Hour)
	template.KeyUsage = x509.KeyUsageDigitalSignature
	if server {
		template.Subject = pkix.Name{CommonName: "loopback-test-server"}
		template.ExtKeyUsage = []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth}
		template.IPAddresses = []net.IP{net.ParseIP("127.0.0.1"), net.ParseIP("::1")}
	} else {
		template.Subject = pkix.Name{CommonName: "loopback-test-driver"}
		template.ExtKeyUsage = []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth}
		for _, identity := range identities {
			uri, parseErr := url.Parse(identity)
			if parseErr != nil {
				t.Fatal(parseErr)
			}
			template.URIs = append(template.URIs, uri)
		}
	}
	encoded, err := x509.CreateCertificate(rand.Reader, template, issuer, publicKey, issuerKey)
	if err != nil {
		t.Fatal(err)
	}
	return tls.Certificate{
		Certificate: [][]byte{encoded, issuer.Raw},
		PrivateKey:  privateKey,
	}
}

func TestDRAReceiptIngestRealMTLSHandshakeAndIdentityBinding(t *testing.T) {
	trustedCA, trustedCAKey, trustedRoots := draReceiptMTLSTestCA(t, "dra-ingress-trusted-ca")
	foreignCA, foreignCAKey, _ := draReceiptMTLSTestCA(t, "dra-ingress-foreign-ca")
	serverCert := draReceiptMTLSTestLeaf(t, trustedCA, trustedCAKey, 91002, true)
	driverCert := draReceiptMTLSTestLeaf(t, trustedCA, trustedCAKey, 91003, false, draReceiptTestSubject)
	foreignCert := draReceiptMTLSTestLeaf(t, foreignCA, foreignCAKey, 91004, false, draReceiptTestSubject)
	wrongSubjectCert := draReceiptMTLSTestLeaf(t, trustedCA, trustedCAKey, 91005, false,
		"spiffe://example.org/ns/dra/sa/unknown")
	wrongDomainCert := draReceiptMTLSTestLeaf(t, trustedCA, trustedCAKey, 91006, false,
		"spiffe://other.example.org/ns/dra/sa/driver")
	ambiguousCert := draReceiptMTLSTestLeaf(t, trustedCA, trustedCAKey, 91007, false,
		draReceiptTestSubject, "spiffe://example.org/ns/dra/sa/second")

	auth, err := NewSPIFFEWorkloadAuthenticator("example.org",
		&testWorkloadIdentityResolver{expected: draReceiptTestSubject, principal: draReceiptTestPrincipal()})
	if err != nil {
		t.Fatal(err)
	}
	handler, resolver, journal := draReceiptTestHandler(t, auth)
	server := httptest.NewUnstartedServer(handler)
	server.TLS = &tls.Config{}
	server.TLS.MinVersion = tls.VersionTLS13
	server.TLS.ClientAuth = tls.RequireAndVerifyClientCert
	server.TLS.ClientCAs = trustedRoots
	server.TLS.Certificates = []tls.Certificate{serverCert}
	server.StartTLS()
	defer server.Close()

	send := func(certs []tls.Certificate, body []byte) (int, string, error) {
		transport := &http.Transport{}
		transport.TLSClientConfig = &tls.Config{
			MinVersion:   tls.VersionTLS13,
			RootCAs:      trustedRoots,
			Certificates: certs,
		}
		defer transport.CloseIdleConnections()
		client := &http.Client{Transport: transport, Timeout: 5 * time.Second}
		request, err := http.NewRequest(http.MethodPost, server.URL+draReceiptIngestPath, bytes.NewReader(body))
		if err != nil {
			t.Fatal(err)
		}
		request.Header.Set("Content-Type", "application/json")
		response, err := client.Do(request)
		if err != nil {
			return 0, "", err
		}
		defer response.Body.Close()
		received, err := io.ReadAll(io.LimitReader(response.Body, 4<<10))
		if err != nil {
			return 0, "", err
		}
		return response.StatusCode, string(received), nil
	}
	validBody := draReceiptTestBody(t, draReceiptTestSigned())
	for _, tc := range []struct {
		name               string
		certificates       []tls.Certificate
		wantHandshakeError bool
		wantHTTP           int
	}{
		{name: "no client certificate", wantHandshakeError: true},
		{name: "self-signed by unknown CA", certificates: []tls.Certificate{foreignCert}, wantHandshakeError: true},
		{name: "wrong SPIFFE identity", certificates: []tls.Certificate{wrongSubjectCert}, wantHTTP: http.StatusUnauthorized},
		{name: "wrong SPIFFE trust domain", certificates: []tls.Certificate{wrongDomainCert}, wantHTTP: http.StatusUnauthorized},
		{name: "multiple SPIFFE identities", certificates: []tls.Certificate{ambiguousCert}, wantHTTP: http.StatusUnauthorized},
		{name: "valid source certificate", certificates: []tls.Certificate{driverCert}, wantHTTP: http.StatusAccepted},
	} {
		t.Run(tc.name, func(t *testing.T) {
			beforeResolver, beforeJournal := resolver.calls, journal.calls
			status, body, err := send(tc.certificates, validBody)
			if tc.wantHandshakeError {
				if err == nil {
					t.Fatalf("client TLS handshake succeeded unexpectedly: HTTP %d %s", status, body)
				}
				if resolver.calls != beforeResolver || journal.calls != beforeJournal {
					t.Fatalf("untrusted TLS reached business logic: resolver=%d journal=%d", resolver.calls, journal.calls)
				}
				return
			}
			if err != nil || status != tc.wantHTTP {
				t.Fatalf("real mTLS result HTTP %d (want %d), body=%q err=%v", status, tc.wantHTTP, body, err)
			}
			if tc.wantHTTP == http.StatusUnauthorized &&
				(resolver.calls != beforeResolver || journal.calls != beforeJournal) {
				t.Fatal("invalid SPIFFE identity reached resolver or journal")
			}
			if tc.wantHTTP == http.StatusAccepted {
				if resolver.calls != beforeResolver+1 || journal.calls != beforeJournal+1 ||
					!strings.Contains(body, "NEW_EVIDENCE_ONLY") {
					t.Fatalf("valid SPIFFE source did not return evidence-only result: %q", body)
				}
			}
		})
	}

	// Even a trusted certificate cannot authorize a different driver node or
	// source. Exact authorization runs after TLS and before evidence resolution.
	changed := draReceiptTestSigned()
	changed.Event.NodeName = "forged-other-node"
	beforeResolver, beforeJournal := resolver.calls, journal.calls
	status, body, err := send([]tls.Certificate{driverCert}, draReceiptTestBody(t, changed))
	if err != nil || status != http.StatusForbidden || resolver.calls != beforeResolver ||
		journal.calls != beforeJournal {
		t.Fatalf("signed cross-node receipt bypassed authorization: HTTP %d %s err=%v", status, body, err)
	}
}
