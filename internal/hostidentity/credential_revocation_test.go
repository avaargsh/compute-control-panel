package hostidentity

import (
	"crypto/ed25519"
	"crypto/rand"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/asn1"
	"math/big"
	"net/url"
	"sync"
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
		KeyUsage:     x509.KeyUsageCertSign | x509.KeyUsageCRLSign | x509.KeyUsageDigitalSignature,
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
		KeyUsage:    x509.KeyUsageDigitalSignature,
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
	return f.crlNumber(t, issuer, key, 1, thisUpdate, nextUpdate, revoked...)
}

func (f hostCredentialRevocationFixture) crlNumber(t *testing.T, issuer *x509.Certificate, key ed25519.PrivateKey, number int64, thisUpdate, nextUpdate time.Time, revoked ...int64) []byte {
	t.Helper()
	entries := make([]x509.RevocationListEntry, 0, len(revoked))
	for _, serial := range revoked {
		entries = append(entries, x509.RevocationListEntry{
			SerialNumber:   big.NewInt(serial),
			RevocationTime: f.now.Add(-time.Minute),
		})
	}
	der, err := x509.CreateRevocationList(rand.Reader, &x509.RevocationList{
		Number:                    big.NewInt(number),
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
		KeyUsage:     x509.KeyUsageCertSign | x509.KeyUsageCRLSign,
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

func TestParseHostCredentialRevocationListsRejectsUnsupportedCRLScopes(t *testing.T) {
	f := newHostCredentialRevocationFixture(t)
	for _, tc := range []struct {
		name     string
		oid      asn1.ObjectIdentifier
		value    []byte
		critical bool
	}{
		{
			name: "delta CRL indicator", oid: asn1.ObjectIdentifier{2, 5, 29, 27},
			value: []byte{0x02, 0x01, 0x01}, critical: true,
		},
		{
			name: "indirect CRL distribution point", oid: asn1.ObjectIdentifier{2, 5, 29, 28},
			value: []byte{0x30, 0x00}, critical: true,
		},
		{
			name: "unknown critical extension", oid: asn1.ObjectIdentifier{1, 2, 3, 4},
			value: []byte{0x05, 0x00}, critical: true,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			der, err := x509.CreateRevocationList(rand.Reader, &x509.RevocationList{
				Number: big.NewInt(2), ThisUpdate: f.now.Add(-time.Minute), NextUpdate: f.now.Add(time.Hour),
				ExtraExtensions: []pkix.Extension{{Id: tc.oid, Critical: tc.critical, Value: tc.value}},
			}, f.issuer, f.key)
			if err != nil {
				t.Fatal(err)
			}
			if lists, err := ParseHostCredentialRevocationLists([][]byte{der}); err == nil || lists != nil {
				t.Fatalf("unsupported CRL scope accepted: lists=%v err=%v", lists, err)
			}
		})
	}
}

func TestHostCredentialRevocationListStoreRotatesAndRejectsNewlyRevokedSerial(t *testing.T) {
	f := newHostCredentialRevocationFixture(t)
	initial := f.crlNumber(t, f.issuer, f.key, 1, f.now.Add(-time.Minute), f.now.Add(time.Hour))
	revoked := f.crlNumber(t, f.issuer, f.key, 2, f.now.Add(-time.Minute), f.now.Add(time.Hour), 42)

	store, err := NewHostCredentialRevocationListStore([][]byte{initial}, []*x509.Certificate{f.issuer}, f.now)
	if err != nil {
		t.Fatalf("create revocation store: %v", err)
	}
	if _, err := store.VerifyHostCredential([][]byte{f.leafDER}, f.roots, "ccp.example", f.now); err != nil {
		t.Fatalf("initial valid CRL rejected: %v", err)
	}
	if err := store.Replace([][]byte{revoked}, f.now); err != nil {
		t.Fatalf("replace with newer current CRL: %v", err)
	}
	if _, err := store.VerifyHostCredential([][]byte{f.leafDER}, f.roots, "ccp.example", f.now); err == nil {
		t.Fatal("credential remained accepted after the replacement CRL revoked its serial")
	}
}

func TestHostCredentialRevocationListStoreKeepsLastGoodSnapshotOnRejectedUpdate(t *testing.T) {
	f := newHostCredentialRevocationFixture(t)
	initial := f.crlNumber(t, f.issuer, f.key, 2, f.now.Add(-time.Minute), f.now.Add(time.Hour))
	older := f.crlNumber(t, f.issuer, f.key, 1, f.now.Add(-time.Minute), f.now.Add(time.Hour))
	sameNumberDifferent := f.crlNumber(t, f.issuer, f.key, 2, f.now.Add(-time.Minute), f.now.Add(time.Hour), 99)
	expired := f.crlNumber(t, f.issuer, f.key, 3, f.now.Add(-time.Hour), f.now)
	wrongPublic, wrongKey, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	wrongTemplate := &x509.Certificate{
		SerialNumber: big.NewInt(9), Subject: f.issuer.Subject,
		NotBefore: f.now.Add(-time.Hour), NotAfter: f.now.Add(24 * time.Hour),
		IsCA: true, BasicConstraintsValid: true,
		KeyUsage:     x509.KeyUsageCertSign | x509.KeyUsageCRLSign,
		SubjectKeyId: []byte{9, 8, 7, 6},
	}
	wrongDER, err := x509.CreateCertificate(rand.Reader, wrongTemplate, wrongTemplate, wrongPublic, wrongKey)
	if err != nil {
		t.Fatal(err)
	}
	wrongIssuer, err := x509.ParseCertificate(wrongDER)
	if err != nil {
		t.Fatal(err)
	}
	wrongSigner := f.crlNumber(t, wrongIssuer, wrongKey, 3, f.now.Add(-time.Minute), f.now.Add(time.Hour))
	store, err := NewHostCredentialRevocationListStore([][]byte{initial}, []*x509.Certificate{f.issuer}, f.now)
	if err != nil {
		t.Fatalf("create revocation store: %v", err)
	}

	for _, tc := range []struct {
		name string
		crls [][]byte
	}{
		{name: "malformed DER", crls: [][]byte{[]byte("not a CRL")}},
		{name: "older CRL number", crls: [][]byte{older}},
		{name: "same CRL number with different bytes", crls: [][]byte{sameNumberDifferent}},
		{name: "expired CRL", crls: [][]byte{expired}},
		{name: "wrong signing key with same issuer name", crls: [][]byte{wrongSigner}},
		{name: "missing issuer CRL", crls: nil},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if err := store.Replace(tc.crls, f.now); err == nil {
				t.Fatal("invalid, stale or regressing CRL update was accepted")
			}
			if _, err := store.VerifyHostCredential([][]byte{f.leafDER}, f.roots, "ccp.example", f.now); err != nil {
				t.Fatalf("rejected update displaced the last known-good CRL snapshot: %v", err)
			}
		})
	}
}

func TestHostCredentialRevocationListStoreConcurrentReplacementCannotRollBack(t *testing.T) {
	f := newHostCredentialRevocationFixture(t)
	initial := f.crlNumber(t, f.issuer, f.key, 1, f.now.Add(-time.Minute), f.now.Add(time.Hour))
	intermediate := f.crlNumber(t, f.issuer, f.key, 2, f.now.Add(-time.Minute), f.now.Add(time.Hour))
	latestRevoked := f.crlNumber(t, f.issuer, f.key, 3, f.now.Add(-time.Minute), f.now.Add(time.Hour), 42)
	store, err := NewHostCredentialRevocationListStore([][]byte{initial}, []*x509.Certificate{f.issuer}, f.now)
	if err != nil {
		t.Fatalf("create revocation store: %v", err)
	}

	start := make(chan struct{})
	var wg sync.WaitGroup
	var intermediateErr error
	wg.Add(1)
	go func() {
		defer wg.Done()
		<-start
		intermediateErr = store.Replace([][]byte{intermediate}, f.now)
	}()
	close(start)
	if err := store.Replace([][]byte{latestRevoked}, f.now); err != nil {
		t.Fatalf("install latest CRL: %v", err)
	}
	wg.Wait()
	if intermediateErr != nil && intermediateErr != ErrHostCredential {
		t.Fatalf("concurrent older update returned unexpected error: %v", intermediateErr)
	}
	if _, err := store.VerifyHostCredential([][]byte{f.leafDER}, f.roots, "ccp.example", f.now); err == nil {
		t.Fatal("concurrent lower-numbered CRL rolled back the latest revocation")
	}
}

func TestHostCredentialRevocationListStoreRejectsExpiredSnapshot(t *testing.T) {
	f := newHostCredentialRevocationFixture(t)
	initial := f.crlNumber(t, f.issuer, f.key, 1, f.now.Add(-time.Minute), f.now.Add(time.Hour))
	store, err := NewHostCredentialRevocationListStore([][]byte{initial}, []*x509.Certificate{f.issuer}, f.now)
	if err != nil {
		t.Fatalf("create revocation store: %v", err)
	}
	if _, err := store.VerifyHostCredential([][]byte{f.leafDER}, f.roots, "ccp.example", f.now.Add(time.Hour)); err == nil {
		t.Fatal("expired CRL snapshot allowed a host credential")
	}
}

func TestVerifyHostCredentialRequiresCRLForEveryNonRootCertificate(t *testing.T) {
	f := newHostCredentialRevocationFixture(t)
	intermediatePublic, intermediateKey, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	intermediateTemplate := &x509.Certificate{
		SerialNumber: big.NewInt(10),
		Subject:      pkix.Name{CommonName: "host intermediate CA"},
		NotBefore:    f.now.Add(-time.Hour), NotAfter: f.now.Add(24 * time.Hour),
		IsCA: true, BasicConstraintsValid: true,
		KeyUsage:       x509.KeyUsageCertSign | x509.KeyUsageCRLSign,
		SubjectKeyId:   []byte{5, 6, 7, 8},
		AuthorityKeyId: f.issuer.SubjectKeyId,
	}
	intermediateDER, err := x509.CreateCertificate(rand.Reader, intermediateTemplate, f.issuer, intermediatePublic, f.key)
	if err != nil {
		t.Fatal(err)
	}
	intermediate, err := x509.ParseCertificate(intermediateDER)
	if err != nil {
		t.Fatal(err)
	}
	leafPublic, _, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	uri, err := url.Parse("spiffe://ccp.example/host/intermediate-machine")
	if err != nil {
		t.Fatal(err)
	}
	leafTemplate := &x509.Certificate{
		SerialNumber: big.NewInt(77),
		NotBefore:    f.now.Add(-time.Minute), NotAfter: f.now.Add(time.Hour),
		KeyUsage:       x509.KeyUsageDigitalSignature,
		ExtKeyUsage:    []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth},
		URIs:           []*url.URL{uri},
		AuthorityKeyId: intermediate.SubjectKeyId,
	}
	leafDER, err := x509.CreateCertificate(rand.Reader, leafTemplate, intermediate, leafPublic, intermediateKey)
	if err != nil {
		t.Fatal(err)
	}

	rootCRL := f.crl(t, f.issuer, f.key, f.now.Add(-time.Minute), f.now.Add(time.Hour))
	intermediateCRL := f.crl(t, intermediate, intermediateKey, f.now.Add(-time.Minute), f.now.Add(time.Hour))
	revokedLeafCRL := f.crl(t, intermediate, intermediateKey, f.now.Add(-time.Minute), f.now.Add(time.Hour), 77)
	revokedIntermediateCRL := f.crl(t, f.issuer, f.key, f.now.Add(-time.Minute), f.now.Add(time.Hour), 10)

	for _, tc := range []struct {
		name string
		crls [][]byte
		want bool
	}{
		{name: "missing intermediate issuer CRL", crls: [][]byte{rootCRL}, want: false},
		{name: "complete issuer CRL coverage", crls: [][]byte{rootCRL, intermediateCRL}, want: true},
		{name: "revoked leaf serial", crls: [][]byte{rootCRL, revokedLeafCRL}, want: false},
		{name: "revoked intermediate serial", crls: [][]byte{revokedIntermediateCRL, intermediateCRL}, want: false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			lists, err := ParseHostCredentialRevocationLists(tc.crls)
			if err != nil {
				t.Fatal(err)
			}
			_, err = VerifyHostCredential([][]byte{leafDER, intermediateDER}, f.roots, "ccp.example", lists, f.now)
			if tc.want && err != nil {
				t.Fatalf("complete verified path CRL coverage rejected: %v", err)
			}
			if !tc.want && err == nil {
				t.Fatal("credential accepted without complete, non-revoking issuer CRL coverage")
			}
		})
	}
}
