// Package hostidentity contains pure enrollment/session fence predicates. It
// owns no storage, credentials, session issuance or GPU readiness authority.
package hostidentity

import (
	"strings"
	"time"
	"unicode"
)

// Binding is the immutable host identity tuple. Subject must come from an
// independently authenticated principal, not a caller-controlled payload.
type Binding struct {
	HostID       string
	ProviderID   string
	ClusterScope string
	NodeUID      string
	Subject      string
}

// SessionFence is a snapshot of a server-owned, durably persisted session.
// Epoch and SessionID are issued by the repository; the caller must read and
// lock the current row before evaluating and atomically recording a heartbeat.
type SessionFence struct {
	Binding      Binding
	Epoch        int64
	SessionID    string
	LastSequence int64
	LastDigest   string
	ExpiresAt    time.Time
}

// Heartbeat is a normalized observation after independent authentication and
// binding resolution. Digest must cover the canonical observation payload.
type Heartbeat struct {
	Binding   Binding
	Epoch     int64
	SessionID string
	Sequence  int64
	Digest    string
}

type Disposition string

const (
	Reject Disposition = "REJECT"
	Accept Disposition = "ACCEPT"
	Replay Disposition = "REPLAY"
)

func validIdentity(value string) bool {
	return value != "" && strings.TrimSpace(value) == value && strings.IndexFunc(value, unicode.IsControl) < 0
}

func validBinding(b Binding) bool {
	return validIdentity(b.HostID) && validIdentity(b.ProviderID) && validIdentity(b.ClusterScope) &&
		validIdentity(b.NodeUID) && validIdentity(b.Subject)
}

func validDigest(value string) bool {
	if len(value) != 71 || !strings.HasPrefix(value, "sha256:") {
		return false
	}
	for _, c := range value[7:] {
		if !(c >= '0' && c <= '9' || c >= 'a' && c <= 'f') {
			return false
		}
	}
	return true
}

// EvaluateHeartbeat is a fail-closed predicate, not a persistence operation.
// Use the database server clock and locked current fence, after checking the
// provider lifecycle and principal revocation. ACCEPT permits only the next
// transactional observation write; REPLAY must not extend session expiry.
// Neither outcome establishes hardware trust, capacity or GPU readiness.
func EvaluateHeartbeat(current SessionFence, incoming Heartbeat, now time.Time) Disposition {
	if now.IsZero() || current.ExpiresAt.IsZero() || !now.Before(current.ExpiresAt) ||
		!validBinding(current.Binding) || current.Epoch <= 0 || !validIdentity(current.SessionID) ||
		current.LastSequence < 0 ||
		(current.LastSequence == 0 && current.LastDigest != "") ||
		(current.LastSequence > 0 && !validDigest(current.LastDigest)) {
		return Reject
	}
	if incoming.Binding != current.Binding || incoming.Epoch != current.Epoch ||
		incoming.SessionID != current.SessionID || incoming.Sequence <= 0 || !validDigest(incoming.Digest) {
		return Reject
	}
	if incoming.Sequence < current.LastSequence {
		return Reject
	}
	if incoming.Sequence == current.LastSequence {
		if incoming.Digest == current.LastDigest {
			return Replay
		}
		return Reject
	}
	return Accept
}
