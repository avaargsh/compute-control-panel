package hostidentity

import (
	"crypto/x509"
	"encoding/asn1"
	"errors"
	"regexp"
	"strings"
	"time"
)

var ErrHostCredential = errors.New("host credential verification failed")

var hostTrustLabel = regexp.MustCompile(`^[a-z0-9]([a-z0-9-]{0,61}[a-z0-9])?$`)
var hostMachineID = regexp.MustCompile(`^[a-zA-Z0-9][a-zA-Z0-9._-]{0,255}$`)

// VerifyHostCredential validates presented DER certificates against an explicit,
// host-only trust bundle and the v1 spiffe://domain/host/machine-id profile.
// It is not proof of private-key possession, machine attestation, authorization
// or revocation. A future direct mTLS transport must prove possession and
// resolve the returned subject against the current independent host authority.
// No caller-provided certificate bytes alone may authorize a session write.
func VerifyHostCredential(chain [][]byte, roots *x509.CertPool, trustDomain string, now time.Time) (string, error) {
	if roots == nil || now.IsZero() || len(chain) == 0 || len(chain) > 8 || len(trustDomain) > 253 {
		return "", ErrHostCredential
	}
	for _, label := range strings.Split(trustDomain, ".") {
		if !hostTrustLabel.MatchString(label) {
			return "", ErrHostCredential
		}
	}
	intermediates := x509.NewCertPool()
	var leaf *x509.Certificate
	for i, der := range chain {
		if len(der) == 0 || len(der) > 65536 {
			return "", ErrHostCredential
		}
		cert, err := x509.ParseCertificate(der)
		if err != nil || now.Before(cert.NotBefore) || !now.Before(cert.NotAfter) {
			return "", ErrHostCredential
		}
		if i == 0 {
			leaf = cert
		} else {
			intermediates.AddCert(cert)
		}
	}
	if leaf.IsCA || leaf.KeyUsage&x509.KeyUsageDigitalSignature == 0 {
		return "", ErrHostCredential
	}
	clientAuth := false
	for _, eku := range leaf.ExtKeyUsage {
		if eku == x509.ExtKeyUsageAny {
			return "", ErrHostCredential
		}
		clientAuth = clientAuth || eku == x509.ExtKeyUsageClientAuth
	}
	if !clientAuth {
		return "", ErrHostCredential
	}
	if _, err := leaf.Verify(x509.VerifyOptions{Roots: roots, Intermediates: intermediates, CurrentTime: now, KeyUsages: []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth}}); err != nil {
		return "", ErrHostCredential
	}
	// Inspect the original SAN encoding to reject aliases that URL parsing may
	// normalize, and reject additional SAN identities of every type.
	var subject string
	for _, ext := range leaf.Extensions {
		if !ext.Id.Equal(asn1.ObjectIdentifier{2, 5, 29, 17}) {
			continue
		}
		var names []asn1.RawValue
		rest, err := asn1.Unmarshal(ext.Value, &names)
		if err != nil || len(rest) != 0 || len(names) != 1 || names[0].Class != asn1.ClassContextSpecific || names[0].Tag != 6 || names[0].IsCompound || subject != "" {
			return "", ErrHostCredential
		}
		subject = string(names[0].Bytes)
	}
	prefix := "spiffe://" + trustDomain + "/host/"
	if !strings.HasPrefix(subject, prefix) || !hostMachineID.MatchString(strings.TrimPrefix(subject, prefix)) || len(leaf.URIs) != 1 || leaf.URIs[0].String() != subject {
		return "", ErrHostCredential
	}
	return subject, nil
}
