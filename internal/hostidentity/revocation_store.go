package hostidentity

import (
	"bytes"
	"crypto/x509"
	"sync"
	"sync/atomic"
	"time"
)

type hostCredentialRevocationSnapshot struct {
	lists []*x509.RevocationList
}

// HostCredentialRevocationListStore atomically publishes immutable CRL
// snapshots. Callers never receive the stored CRL or issuer pointers, so
// request-time verification can safely read while another goroutine replaces it.
type HostCredentialRevocationListStore struct {
	mu      sync.Mutex
	issuers []*x509.Certificate
	current atomic.Pointer[hostCredentialRevocationSnapshot]
}

// NewHostCredentialRevocationListStore constructs a ready store from a bounded,
// current CRL bundle and the exact issuer certificates expected to sign it.
// Issuer certificates are copied from DER and retained as an immutable set.
func NewHostCredentialRevocationListStore(
	encoded [][]byte, issuerCertificates []*x509.Certificate, now time.Time,
) (*HostCredentialRevocationListStore, error) {
	lists, err := ParseHostCredentialRevocationLists(encoded)
	if err != nil {
		return nil, ErrHostCredential
	}
	issuers, err := parseHostCredentialRevocationIssuers(issuerCertificates)
	if err != nil || !hostCredentialRevocationListsValid(lists, issuers, now) {
		return nil, ErrHostCredential
	}
	store := &HostCredentialRevocationListStore{issuers: issuers}
	store.current.Store(&hostCredentialRevocationSnapshot{lists: lists})
	return store, nil
}

// ValidateCurrent verifies that the store is initialized and every CRL in its
// current snapshot remains current and signed by its configured issuer.
func (s *HostCredentialRevocationListStore) ValidateCurrent(now time.Time) error {
	if s == nil || now.IsZero() {
		return ErrHostCredential
	}
	snapshot := s.current.Load()
	if snapshot == nil || !hostCredentialRevocationListsValid(snapshot.lists, s.issuers, now) {
		return ErrHostCredential
	}
	return nil
}

// Replace parses and validates a complete candidate bundle before publishing
// it. The exact configured issuer set must remain covered; CRL numbers and
// ThisUpdate cannot regress, and an equal number is accepted only for
// byte-identical DER. The last known-good snapshot remains active on every
// rejected update. Changing issuers requires a separately reviewed trust-bundle
// rotation and construction of a new store.
func (s *HostCredentialRevocationListStore) Replace(encoded [][]byte, now time.Time) error {
	if s == nil {
		return ErrHostCredential
	}
	lists, err := ParseHostCredentialRevocationLists(encoded)
	if err != nil || !hostCredentialRevocationListsValid(lists, s.issuers, now) {
		return ErrHostCredential
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	previous := s.current.Load()
	if previous == nil || !hostCredentialRevocationListsAdvance(previous.lists, lists) {
		return ErrHostCredential
	}
	s.current.Store(&hostCredentialRevocationSnapshot{lists: lists})
	return nil
}

// VerifyHostCredential verifies against one complete immutable CRL snapshot.
// The existing verifier still checks the actual presented path, issuer
// signature, AKI, cRLSign usage, issuer coverage and revoked serial before
// returning a subject.
func (s *HostCredentialRevocationListStore) VerifyHostCredential(
	chain [][]byte, roots *x509.CertPool, trustDomain string, now time.Time,
) (string, error) {
	if s == nil || now.IsZero() {
		return "", ErrHostCredential
	}
	snapshot := s.current.Load()
	if snapshot == nil || !hostCredentialRevocationListsCurrent(snapshot.lists, now) {
		return "", ErrHostCredential
	}
	return VerifyHostCredential(chain, roots, trustDomain, snapshot.lists, now)
}

func parseHostCredentialRevocationIssuers(encoded []*x509.Certificate) ([]*x509.Certificate, error) {
	if len(encoded) == 0 || len(encoded) > 8 {
		return nil, ErrHostCredential
	}
	issuers := make([]*x509.Certificate, 0, len(encoded))
	subjects := make(map[string]struct{}, len(encoded))
	for _, certificate := range encoded {
		if certificate == nil || len(certificate.Raw) == 0 {
			return nil, ErrHostCredential
		}
		issuer, err := x509.ParseCertificate(append([]byte(nil), certificate.Raw...))
		if err != nil || !issuer.IsCA || issuer.KeyUsage&x509.KeyUsageCRLSign == 0 ||
			len(issuer.SubjectKeyId) == 0 || len(issuer.RawSubject) == 0 {
			return nil, ErrHostCredential
		}
		subject := string(issuer.RawSubject)
		if _, exists := subjects[subject]; exists {
			return nil, ErrHostCredential
		}
		subjects[subject] = struct{}{}
		issuers = append(issuers, issuer)
	}
	return issuers, nil
}

func hostCredentialRevocationListsCurrent(lists []*x509.RevocationList, now time.Time) bool {
	if len(lists) == 0 || now.IsZero() {
		return false
	}
	for _, list := range lists {
		if list == nil || list.Number == nil || list.Number.Sign() <= 0 ||
			list.ThisUpdate.IsZero() || list.NextUpdate.IsZero() ||
			now.Before(list.ThisUpdate) || !now.Before(list.NextUpdate) {
			return false
		}
	}
	return true
}

func hostCredentialRevocationListsValid(
	lists []*x509.RevocationList, issuers []*x509.Certificate, now time.Time,
) bool {
	if !hostCredentialRevocationListsCurrent(lists, now) || len(lists) != len(issuers) {
		return false
	}
	byIssuer := make(map[string]*x509.RevocationList, len(lists))
	for _, list := range lists {
		if list == nil || len(list.RawIssuer) == 0 {
			return false
		}
		byIssuer[string(list.RawIssuer)] = list
	}
	for _, issuer := range issuers {
		if issuer == nil || len(issuer.RawSubject) == 0 {
			return false
		}
		list, ok := byIssuer[string(issuer.RawSubject)]
		if !ok || !bytes.Equal(list.AuthorityKeyId, issuer.SubjectKeyId) ||
			issuer.KeyUsage&x509.KeyUsageCRLSign == 0 || list.CheckSignatureFrom(issuer) != nil {
			return false
		}
	}
	return true
}

func hostCredentialRevocationListsAdvance(previous, next []*x509.RevocationList) bool {
	nextByIssuer := make(map[string]*x509.RevocationList, len(next))
	for _, list := range next {
		if list == nil || list.Number == nil {
			return false
		}
		nextByIssuer[string(list.RawIssuer)] = list
	}
	for _, old := range previous {
		if old == nil || old.Number == nil {
			return false
		}
		current, ok := nextByIssuer[string(old.RawIssuer)]
		if !ok || current.Number.Cmp(old.Number) < 0 || current.ThisUpdate.Before(old.ThisUpdate) {
			return false
		}
		if current.Number.Cmp(old.Number) == 0 && !bytes.Equal(current.Raw, old.Raw) {
			return false
		}
	}
	return true
}
