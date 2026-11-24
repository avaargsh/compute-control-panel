package hostidentity

import (
	"math"
	"time"
)

// PrepareSessionStart checks a proposed epoch against the locked latest durable
// fence, including expired predecessors. A nil predecessor is valid only after
// the repository proves there is no session history. The repository must
// authenticate the principal, check Provider lifecycle and revocation, create a
// unique random session ID, use database time, and persist using atomic CAS.
// This pure predicate neither issues a session nor provides concurrency safety.
func PrepareSessionStart(binding Binding, previous *SessionFence, expectedEpoch int64, sessionID string, now, expiresAt time.Time) (SessionFence, bool) {
	if !validBinding(binding) || !validIdentity(sessionID) || now.IsZero() || !now.Before(expiresAt) || expectedEpoch < 0 || expectedEpoch == math.MaxInt64 {
		return SessionFence{}, false
	}
	if previous == nil {
		if expectedEpoch != 0 {
			return SessionFence{}, false
		}
	} else if previous.Binding != binding || previous.Epoch <= 0 || previous.Epoch != expectedEpoch ||
		!validIdentity(previous.SessionID) || previous.SessionID == sessionID || previous.IssuedAt.IsZero() ||
		!previous.IssuedAt.Before(previous.ExpiresAt) || now.Before(previous.IssuedAt) || previous.LastSequence < 0 ||
		(previous.LastSequence == 0 && (previous.LastDigest != "" || !previous.LastRecordedAt.IsZero())) ||
		(previous.LastSequence > 0 && (!validDigest(previous.LastDigest) || previous.LastRecordedAt.IsZero() || previous.LastRecordedAt.Before(previous.IssuedAt) || !previous.LastRecordedAt.Before(previous.ExpiresAt) || now.Before(previous.LastRecordedAt))) {
		return SessionFence{}, false
	}
	return SessionFence{Binding: binding, Epoch: expectedEpoch + 1, SessionID: sessionID, IssuedAt: now, ExpiresAt: expiresAt}, true
}
