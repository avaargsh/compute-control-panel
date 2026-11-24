package hostidentity

import (
	"bytes"
	"crypto/x509"
	"encoding/asn1"
	"errors"
	"regexp"
	"strings"
	"time"
)

var ErrHostCredential = errors.New("host credential verification failed")

var hostTrustLabel = regexp.MustCompile(`^[a-z0-9]([a-z0-9-]{0,61}[a-z0-9])?$`)
var hostMachineID = regexp.MustCompile("^[a-zA-Z0-9][a-zA-Z0-9._-]{0,255}$")

var deltaCRLIndicatorOID = asn1.ObjectIdentifier{2, 5, 29, 27}
var issuingDistributionPointOID = asn1.ObjectIdentifier{2, 5, 29, 28}
var certificateIssuerOID = asn1.ObjectIdentifier{2, 5, 29, 29}

// ParseHostCredentialRevocationLists parses a bounded, explicit CRL bundle.
// One CRL per issuer is accepted; delta and indirect CRLs are unsupported.
func ParseHostCredentialRevocationLists(encoded [][]byte) ([]*x509.RevocationList, error) {
	if len(encoded) == 0 || len(encoded) > 8 {
		return nil, ErrHostCredential
	}
	lists := make([]*x509.RevocationList, 0, len(encoded))
	issuers := make(map[string]struct{}, len(encoded))
	for _, der := range encoded {
		if len(der) == 0 || len(der) > 1<<20 {
			return nil, ErrHostCredential
		}
		list, err := x509.ParseRevocationList(append([]byte(nil), der...))
		if err != nil || list.Number == nil || list.Number.Sign() <= 0 ||
			list.ThisUpdate.IsZero() || list.NextUpdate.IsZero() ||
			!list.ThisUpdate.Before(list.NextUpdate) || len(list.RawIssuer) == 0 ||
			len(list.AuthorityKeyId) == 0 {
			return nil, ErrHostCredential
		}
		for _, ext := range list.Extensions {
			if ext.Critical || ext.Id.Equal(deltaCRLIndicatorOID) || ext.Id.Equal(issuingDistributionPointOID) {
				return nil, ErrHostCredential
			}
		}
		for _, entry := range list.RevokedCertificateEntries {
			if entry.SerialNumber == nil || entry.SerialNumber.Sign() <= 0 {
				return nil, ErrHostCredential
			}
			for _, ext := range entry.Extensions {
				if ext.Critical || ext.Id.Equal(certificateIssuerOID) {
					return nil, ErrHostCredential
				}
			}
		}
		issuer := string(list.RawIssuer)
		if _, exists := issuers[issuer]; exists {
			return nil, ErrHostCredential
		}
		issuers[issuer] = struct{}{}
		lists = append(lists, list)
	}
	return lists, nil
}

func verifyHostCredentialRevocation(chains [][]*x509.Certificate, lists []*x509.RevocationList, now time.Time) error {
	if len(chains) == 0 || len(lists) == 0 {
		return ErrHostCredential
	}
	for _, chain := range chains {
		if len(chain) < 2 {
			return ErrHostCredential
		}
		for i := 0; i < len(chain)-1; i++ {
			certificate, issuer := chain[i], chain[i+1]
			if certificate == nil || certificate.SerialNumber == nil || issuer == nil ||
				!issuer.IsCA || issuer.KeyUsage&x509.KeyUsageCRLSign == 0 || len(issuer.SubjectKeyId) == 0 {
				return ErrHostCredential
			}
			var matched *x509.RevocationList
			for _, list := range lists {
				if bytes.Equal(list.RawIssuer, issuer.RawSubject) {
					if matched != nil {
						return ErrHostCredential
					}
					matched = list
				}
			}
			if matched == nil || !bytes.Equal(matched.AuthorityKeyId, issuer.SubjectKeyId) ||
				now.Before(matched.ThisUpdate) || !now.Before(matched.NextUpdate) ||
				matched.CheckSignatureFrom(issuer) != nil {
				return ErrHostCredential
			}
			for _, entry := range matched.RevokedCertificateEntries {
				if entry.SerialNumber.Cmp(certificate.SerialNumber) == 0 {
					return ErrHostCredential
				}
			}
		}
	}
	return nil
}

// VerifyHostCredential validates presented DER certificates against an explicit,
// host-only trust bundle and the v1 spiffe://domain/host/machine-id profile.
// It is not proof of private-key possession, machine attestation, authorization
// or revocation. A future direct mTLS transport must prove possession and
// resolve the returned subject against the current independent host authority.
// No caller-provided certificate bytes alone may authorize a session write.
func VerifyHostCredential(chain [][]byte, roots *x509.CertPool, trustDomain string, revocationLists []*x509.RevocationList, now time.Time) (string, error) {
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
	verifiedChains, err := leaf.Verify(x509.VerifyOptions{
		Roots: roots, Intermediates: intermediates, CurrentTime: now,
		KeyUsages: []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth},
	})
	if err != nil || verifyHostCredentialRevocation(verifiedChains, revocationLists, now) != nil {
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
