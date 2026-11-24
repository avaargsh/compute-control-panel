package hostidentity

import (
	"crypto/ed25519"
	"crypto/rand"
	"crypto/x509"
	"crypto/x509/pkix"
	"math/big"
	"net/url"
	"testing"
	"time"
)

func TestVerifyHostCredential(t *testing.T) {
	now := time.Date(2026, 11, 24, 0, 0, 0, 0, time.UTC)
	pub, key, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	ca := &x509.Certificate{
		SerialNumber: big.NewInt(1), Subject: pkix.Name{CommonName: "host fixture CA"},
		NotBefore: now.Add(-time.Hour), NotAfter: now.Add(time.Hour),
		IsCA: true, BasicConstraintsValid: true,
		KeyUsage: x509.KeyUsageCertSign | x509.KeyUsageCRLSign, SubjectKeyId: []byte{1, 2, 3, 4},
	}
	rawCA, err := x509.CreateCertificate(rand.Reader, ca, ca, pub, key)
	if err != nil {
		t.Fatal(err)
	}
	root, err := x509.ParseCertificate(rawCA)
	if err != nil {
		t.Fatal(err)
	}
	roots := x509.NewCertPool()
	roots.AddCert(root)
	crlDER, err := x509.CreateRevocationList(rand.Reader, &x509.RevocationList{
		Number: big.NewInt(1), ThisUpdate: now.Add(-time.Minute), NextUpdate: now.Add(time.Hour),
	}, root, key)
	if err != nil {
		t.Fatal(err)
	}
	revocationLists, err := ParseHostCredentialRevocationLists([][]byte{crlDER})
	if err != nil {
		t.Fatal(err)
	}
	leafPub, _, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct {
		name string
		uris []string
		eku  x509.ExtKeyUsage
		when time.Time
		ok   bool
	}{
		{"host", []string{"spiffe://ccp.example/host/machine-1"}, x509.ExtKeyUsageClientAuth, now, true},
		{"workload namespace", []string{"spiffe://ccp.example/workload/machine-1"}, x509.ExtKeyUsageClientAuth, now, false},
		{"foreign domain", []string{"spiffe://other.example/host/machine-1"}, x509.ExtKeyUsageClientAuth, now, false},
		{"ambiguous", []string{"spiffe://ccp.example/host/a", "spiffe://ccp.example/host/b"}, x509.ExtKeyUsageClientAuth, now, false},
		{"encoded alias", []string{"spiffe://ccp.example/host/%61"}, x509.ExtKeyUsageClientAuth, now, false},
		{"dot segment", []string{"spiffe://ccp.example/host/../a"}, x509.ExtKeyUsageClientAuth, now, false},
		{"query", []string{"spiffe://ccp.example/host/a?x=y"}, x509.ExtKeyUsageClientAuth, now, false},
		{"missing URI", nil, x509.ExtKeyUsageClientAuth, now, false},
		{"server only", []string{"spiffe://ccp.example/host/a"}, x509.ExtKeyUsageServerAuth, now, false},
		{"any EKU", []string{"spiffe://ccp.example/host/a"}, x509.ExtKeyUsageAny, now, false},
		{"expired", []string{"spiffe://ccp.example/host/a"}, x509.ExtKeyUsageClientAuth, now.Add(2 * time.Minute), false},
		{"zero clock", []string{"spiffe://ccp.example/host/a"}, x509.ExtKeyUsageClientAuth, time.Time{}, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			leaf := &x509.Certificate{
				SerialNumber: big.NewInt(2), NotBefore: now.Add(-time.Minute), NotAfter: now.Add(time.Minute),
				KeyUsage: x509.KeyUsageDigitalSignature, ExtKeyUsage: []x509.ExtKeyUsage{tc.eku},
			}
			for _, value := range tc.uris {
				uri, err := url.Parse(value)
				if err != nil {
					t.Fatal(err)
				}
				leaf.URIs = append(leaf.URIs, uri)
			}
			raw, err := x509.CreateCertificate(rand.Reader, leaf, root, leafPub, key)
			if err != nil {
				t.Fatal(err)
			}
			subject, err := VerifyHostCredential([][]byte{raw}, roots, "ccp.example", revocationLists, tc.when)
			if tc.ok {
				if err != nil || subject != tc.uris[0] {
					t.Fatalf("valid host rejected: %q, %v", subject, err)
				}
				for _, untrusted := range []*x509.CertPool{nil, x509.NewCertPool()} {
					if subject, err := VerifyHostCredential([][]byte{raw}, untrusted, "ccp.example", revocationLists, now); err == nil || subject != "" {
						t.Fatal("accepted without explicit trusted roots")
					}
				}
			} else if err == nil || subject != "" {
				t.Fatalf("invalid credential accepted: %q, %v", subject, err)
			}
		})
	}
	if subject, err := VerifyHostCredential(nil, roots, "ccp.example", revocationLists, now); err == nil || subject != "" {
		t.Fatal("accepted missing certificate")
	}
}
