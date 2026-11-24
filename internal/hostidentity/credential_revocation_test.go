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

type hostCredentialRevocationFixture struct {
	now     time.Time
	roots   *x509.CertPool
	issuer  *x509.Certificate
	key     ed25519.PrivateKey
	leafDER []byte
}

func newHostCredentialRevocationFixture(t *testing.T) hostCredentialRevocationFixture {
	t.Helper()
	now := time.Date(2026, 11, 24, 0, 0, 0, 0, time.UTC)
	publicKey, privateKey, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	caTemplate := &x509.Certificate{
		SerialNumber: big.NewInt(1),
		Subject:      pkix.Name{CommonName: "host revocation fixture CA"},
		NotBefore:    now.Add(-time.Hour), NotAfter: now.Add(24 * time.Hour),
		IsCA: true, BasicConstraintsValid: true,
		KeyUsage:    x509.KeyUsageCertSign | x509.KeyUsageCRLSign | x509.KeyUsageDigitalSignature,
		SubjectKeyId: []byte{1, 2, 3, 4},
	}
	caDER, err := x509.CreateCertificate(rand.Reader, caTemplate, caTemplate, publicKey, privateKey)
	if err != nil {
		t.Fatal(err)
	}
	issuer, err := x509.ParseCertificate(caDER)
	if err != nil {
		t.Fatal(err)
	}
	roots := x509.NewCertPool()
	roots.AddCert(issuer)

	leafPublic, _, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	uri, err := url.Parse("spiffe://ccp.example/host/machine-42")
	if err != nil {
		t.Fatal(err)
	}
	leafTemplate := &x509.Certificate{
		SerialNumber: big.NewInt(42),
		NotBefore:    now.Add(-time.Minute), NotAfter: now.Add(time.Hour),
		KeyUsage:     x509.KeyUsageDigitalSignature,
		ExtKeyUsage: []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth},
		URIs:        []*url.URL{uri},
	}
	leafDER, err := x509.CreateCertificate(rand.Reader, leafTemplate, issuer, leafPublic, privateKey)
	if err != nil {
		t.Fatal(err)
	}
	return hostCredentialRevocationFixture{
		now: now, roots: roots, issuer: issuer, key: privateKey, leafDER: leafDER,
	}
}

func (f hostCredentialRevocationFixture) crl(t *testing.T, issuer *x509.Certificate, key ed25519.PrivateKey, thisUpdate, nextUpdate time.Time, revoked ...int64) []byte {
	t.Helper()
	entries := make([]x509.RevocationListEntry, 0, len(revoked))
	for _, serial := range revoked {
		entries = append(entries, x509.RevocationListEntry{
			SerialNumber:   big.NewInt(serial),
			RevocationTime: f.now.Add(-time.Minute),
		})
	}
	der, err := x509.CreateRevocationList(rand.Reader, &x509.RevocationList{
		Number:                    big.NewInt(1),
		ThisUpdate:                thisUpdate,
		NextUpdate:                nextUpdate,
		RevokedCertificateEntries: entries,
	}, issuer, key)
	if err != nil {
		t.Fatal(err)
	}
	return der
}

func TestVerifyHostCredentialRequiresCurrentIssuerCRL(t *testing.T) {
	f := newHostCredentialRevocationFixture(t)
	validCRL := f.crl(t, f.issuer, f.key, f.now.Add(-time.Minute), f.now.Add(time.Hour))
	expiredCRL := f.crl(t, f.issuer, f.key, f.now.Add(-time.Hour), f.now)
	futureCRL := f.crl(t, f.issuer, f.key, f.now.Add(time.Minute), f.now.Add(time.Hour))
	revokedCRL := f.crl(t, f.issuer, f.key, f.now.Add(-time.Minute), f.now.Add(time.Hour), 42)

	otherPublic, otherKey, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	otherTemplate := &x509.Certificate{
		SerialNumber: big.NewInt(9),
		Subject:      f.issuer.Subject,
		NotBefore:    f.now.Add(-time.Hour), NotAfter: f.now.Add(24 * time.Hour),
		IsCA: true, BasicConstraintsValid: true,
		KeyUsage:    x509.KeyUsageCertSign | x509.KeyUsageCRLSign,
		SubjectKeyId: []byte{9, 8, 7, 6},
	}
	otherDER, err := x509.CreateCertificate(rand.Reader, otherTemplate, otherTemplate, otherPublic, otherKey)
	if err != nil {
		t.Fatal(err)
	}
	otherIssuer, err := x509.ParseCertificate(otherDER)
	if err != nil {
		t.Fatal(err)
	}
	wrongSignerCRL := f.crl(t, otherIssuer, otherKey, f.now.Add(-time.Minute), f.now.Add(time.Hour))

	for _, tc := range []struct {
		name string
		crls [][]byte
		want bool
	}{
		{name: "current issuer CRL", crls: [][]byte{validCRL}, want: true},
		{name: "missing CRL", want: false},
		{name: "expired CRL", crls: [][]byte{expiredCRL}, want: false},
		{name: "not-yet-valid CRL", crls: [][]byte{futureCRL}, want: false},
		{name: "revoked serial", crls: [][]byte{revokedCRL}, want: false},
		{name: "wrong signing CA with same issuer name", crls: [][]byte{wrongSignerCRL}, want: false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			lists, err := ParseHostCredentialRevocationLists(tc.crls)
			if err != nil {
				if tc.want {
					t.Fatalf("parse valid CRL: %v", err)
				}
				return
			}
			_, err = VerifyHostCredential([][]byte{f.leafDER}, f.roots, "ccp.example", lists, f.now)
			if tc.want && err != nil {
				t.Fatalf("valid credential rejected: %v", err)
			}
			if !tc.want && err == nil {
				t.Fatal("credential accepted without a valid, current, non-revoking issuer CRL")
			}
		})
	}
}

func TestParseHostCredentialRevocationListsRejectsAmbiguousOrMalformedInput(t *testing.T) {
	f := newHostCredentialRevocationFixture(t)
	crl := f.crl(t, f.issuer, f.key, f.now.Add(-time.Minute), f.now.Add(time.Hour))
	for _, tc := range []struct {
		name string
		raw  [][]byte
	}{
		{name: "missing", raw: nil},
		{name: "empty", raw: [][]byte{{}}},
		{name: "malformed", raw: [][]byte{[]byte("not a CRL")}},
		{name: "duplicate issuer", raw: [][]byte{crl, crl}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if lists, err := ParseHostCredentialRevocationLists(tc.raw); err == nil || lists != nil {
				t.Fatalf("invalid CRL input accepted: lists=%v err=%v", lists, err)
			}
		})
	}
}
