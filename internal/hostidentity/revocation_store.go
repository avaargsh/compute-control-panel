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
// snapshots. Callers never receive the stored CRL pointers, so request-time
// verification can safely read a snapshot while another goroutine replaces it.
type HostCredentialRevocationListStore struct {
	mu      sync.Mutex
	current atomic.Pointer[hostCredentialRevocationSnapshot]
}

// NewHostCredentialRevocationListStore constructs a ready store from a
// bounded, structurally valid and currently valid issuer-CRL bundle.
func NewHostCredentialRevocationListStore(encoded [][]byte, now time.Time) (*HostCredentialRevocationListStore, error) {
	lists, err := ParseHostCredentialRevocationLists(encoded)
	if err != nil || !hostCredentialRevocationListsCurrent(lists, now) {
		return nil, ErrHostCredential
	}
	store := &HostCredentialRevocationListStore{}
	store.current.Store(&hostCredentialRevocationSnapshot{lists: lists})
	return store, nil
}

// ValidateCurrent verifies that the store is initialized and every CRL in its
// current snapshot is within its ThisUpdate/NextUpdate validity window.
func (s *HostCredentialRevocationListStore) ValidateCurrent(now time.Time) error {
	if s == nil || now.IsZero() {
		return ErrHostCredential
	}
	snapshot := s.current.Load()
	if snapshot == nil || !hostCredentialRevocationListsCurrent(snapshot.lists, now) {
		return ErrHostCredential
	}
	return nil
}

// Replace parses and validates a complete candidate bundle before publishing
// it. Existing issuers cannot disappear, CRL numbers cannot regress, and an
// equal number is accepted only for byte-identical DER. The last known-good
// snapshot remains active on every rejected update. New issuer CRLs may be
// added; removing a retired issuer requires a separately reviewed trust-bundle
// rotation and store reconstruction.
func (s *HostCredentialRevocationListStore) Replace(encoded [][]byte, now time.Time) error {
	if s == nil {
		return ErrHostCredential
	}
	lists, err := ParseHostCredentialRevocationLists(encoded)
	if err != nil || !hostCredentialRevocationListsCurrent(lists, now) {
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
// The existing verifier still checks the actual issuer signature, AKI,
// cRLSign usage, issuer coverage and revoked serial before returning a subject.
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
