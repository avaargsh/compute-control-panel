package postgres

import (
	"context"
	"errors"
	"fmt"
	"math"
	"strings"
	"time"
	"unicode"

	"github.com/avaargsh/compute-control-panel/internal/hostidentity"
)

var (
	ErrHostSessionInvalid  = errors.New("invalid HostSession start or heartbeat")
	ErrHostSessionRejected = errors.New("HostSession persistence fence rejected the request")
)

const maxHostSessionTTL = 24 * time.Hour

func validHostSessionIdentity(value string, maxBytes int) bool {
	return value != "" && len(value) <= maxBytes &&
		strings.TrimSpace(value) == value &&
		strings.IndexFunc(value, unicode.IsControl) < 0
}

func validHostSessionBinding(binding hostidentity.Binding) bool {
	return validHostSessionIdentity(binding.HostID, 256) &&
		validHostSessionIdentity(binding.ProviderID, 256) &&
		validHostSessionIdentity(binding.ClusterScope, 1024) &&
		validHostSessionIdentity(binding.NodeUID, 256) &&
		validHostSessionIdentity(binding.Subject, 1024)
}

func validHostSessionDigest(value string) bool {
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

func hostSessionPersistenceError(operation string, err error) error {
	var state interface{ SQLState() string }
	if errors.As(err, &state) && state.SQLState() == "23514" {
		return fmt.Errorf("%w: %w", ErrHostSessionRejected, err)
	}
	return fmt.Errorf("%s: %w", operation, err)
}

// StartHostSession persists a new monotonic session epoch through the
// restricted SECURITY DEFINER entrypoint. PostgreSQL generates the opaque
// session ID and records issued_at/expires_at from its clock after acquiring
// Provider and immutable binding locks.
//
// This method is intentionally unmounted. Its caller MUST authenticate the
// machine principal and check revocation before passing the verified binding;
// the database compares the tuple but is not itself a host attestation system.
func (r *Repository) StartHostSession(
	ctx context.Context,
	binding hostidentity.Binding,
	expectedEpoch int64,
	ttl time.Duration,
) (hostidentity.SessionFence, error) {
	if r == nil || r.db == nil || !validHostSessionBinding(binding) ||
		expectedEpoch < 0 || expectedEpoch == math.MaxInt64 ||
		ttl < time.Second || ttl > maxHostSessionTTL || ttl%time.Second != 0 {
		return hostidentity.SessionFence{}, ErrHostSessionInvalid
	}

	var fence hostidentity.SessionFence
	fence.Binding = binding
	err := r.db.QueryRowContext(ctx, `
		SELECT host_id, epoch, session_id, provider_id, cluster_scope,
		       node_uid, subject, issued_at, expires_at
		FROM public.compute_host_session_start($1,$2,$3,$4,$5,$6,$7)`,
		binding.HostID, binding.ProviderID, binding.ClusterScope,
		binding.NodeUID, binding.Subject, expectedEpoch, int64(ttl/time.Second),
	).Scan(
		&fence.Binding.HostID, &fence.Epoch, &fence.SessionID,
		&fence.Binding.ProviderID, &fence.Binding.ClusterScope,
		&fence.Binding.NodeUID, &fence.Binding.Subject,
		&fence.IssuedAt, &fence.ExpiresAt,
	)
	if err != nil {
		return hostidentity.SessionFence{}, hostSessionPersistenceError("start HostSession", err)
	}
	if fence.Binding != binding || fence.Epoch <= 0 ||
		!validHostSessionIdentity(fence.SessionID, 256) ||
		!strings.HasPrefix(fence.SessionID, "hs_") ||
		!fence.IssuedAt.Before(fence.ExpiresAt) {
		return hostidentity.SessionFence{}, fmt.Errorf("%w: database returned an invalid session fence", ErrHostSessionRejected)
	}
	return fence, nil
}

// RecordHostHeartbeat appends a database-timestamped receipt for the current
// latest session epoch. Exact sequence/digest replay returns the original
// recorded_at and does not extend session expiry. The caller must authenticate
// the principal and resolve binding from the trusted identity authority first.
func (r *Repository) RecordHostHeartbeat(
	ctx context.Context,
	heartbeat hostidentity.Heartbeat,
) (hostidentity.Disposition, time.Time, error) {
	if r == nil || r.db == nil || !validHostSessionBinding(heartbeat.Binding) ||
		heartbeat.Epoch <= 0 || !validHostSessionIdentity(heartbeat.SessionID, 256) ||
		heartbeat.Sequence <= 0 || !validHostSessionDigest(heartbeat.Digest) {
		return hostidentity.Reject, time.Time{}, ErrHostSessionInvalid
	}

	var disposition string
	var recordedAt time.Time
	err := r.db.QueryRowContext(ctx, `
		SELECT disposition, recorded_at
		FROM public.compute_host_session_heartbeat($1,$2,$3,$4,$5,$6,$7,$8,$9)`,
		heartbeat.Binding.HostID, heartbeat.Binding.ProviderID,
		heartbeat.Binding.ClusterScope, heartbeat.Binding.NodeUID,
		heartbeat.Binding.Subject, heartbeat.Epoch, heartbeat.SessionID,
		heartbeat.Sequence, heartbeat.Digest,
	).Scan(&disposition, &recordedAt)
	if err != nil {
		return hostidentity.Reject, time.Time{}, hostSessionPersistenceError("record HostSession heartbeat", err)
	}
	switch hostidentity.Disposition(disposition) {
	case hostidentity.Accept, hostidentity.Replay:
		return hostidentity.Disposition(disposition), recordedAt, nil
	default:
		return hostidentity.Reject, time.Time{}, fmt.Errorf("%w: unknown database heartbeat disposition %q", ErrHostSessionRejected, disposition)
	}
}

var _ interface {
	StartHostSession(context.Context, hostidentity.Binding, int64, time.Duration) (hostidentity.SessionFence, error)
	RecordHostHeartbeat(context.Context, hostidentity.Heartbeat) (hostidentity.Disposition, time.Time, error)
} = (*Repository)(nil)
