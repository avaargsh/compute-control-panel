package postgres

import (
	"context"
	"errors"
	"fmt"
	"math"
	"strings"
	"time"

	"github.com/avaargsh/compute-control-panel/internal/hostidentity"
)

var ErrHostSessionUnauthorized = errors.New("host session authorization rejected")

const hostAgentService = "compute-host-agent"

// AuthenticatedHostSessionStartRequest contains only a server-verified identity
// and normalized session intent. The database resolves the authoritative
// binding; the remote host must never supply the tenant, service or subject.
type AuthenticatedHostSessionStartRequest struct {
	TenantID      string
	Service       string
	Subject       string
	ExpectedEpoch int64
	RequestID     string
	TTL           time.Duration
}

type AuthenticatedHostHeartbeatRequest struct {
	TenantID  string
	Service   string
	Subject   string
	Epoch     int64
	SessionID string
	Sequence  int64
	Digest    string
}

func validateAuthenticatedHostIdentity(tenant, service, subject string) error {
	if !validHostSessionIdentity(tenant, 256) || !validHostSessionIdentity(service, 256) ||
		!validHostSessionIdentity(subject, 1024) || !strings.HasPrefix(subject, "spiffe://") {
		return ErrHostSessionInvalid
	}
	if service != hostAgentService {
		return ErrHostSessionUnauthorized
	}
	return nil
}

func authenticatedHostSessionError(operation string, err error) error {
	var state interface{ SQLState() string }
	// The proposed authorization wrapper must return SQLSTATE 28000 for
	// rejected identity/grant authorization. Database ACL failures and
	// outages are operational errors, not a successful authorization check.
	if errors.As(err, &state) && state.SQLState() == "28000" {
		return fmt.Errorf("%w: %w", ErrHostSessionUnauthorized, err)
	}
	return hostSessionPersistenceError(operation, err)
}

// StartAuthenticatedHostSession is an unmounted adapter to the proposed
// transactional authorization wrapper. It never falls back to the raw tuple
// function. Migration and real database acceptance are required before use.
func (r *Repository) StartAuthenticatedHostSession(ctx context.Context, req AuthenticatedHostSessionStartRequest) (hostidentity.SessionFence, error) {
	if err := validateAuthenticatedHostIdentity(req.TenantID, req.Service, req.Subject); err != nil {
		return hostidentity.SessionFence{}, err
	}
	if r == nil || r.db == nil || !validHostSessionIdentity(req.RequestID, 256) ||
		req.ExpectedEpoch < 0 || req.ExpectedEpoch == math.MaxInt64 ||
		req.TTL < time.Second || req.TTL > maxHostSessionTTL || req.TTL%time.Second != 0 {
		return hostidentity.SessionFence{}, ErrHostSessionInvalid
	}

	var fence hostidentity.SessionFence
	err := r.db.QueryRowContext(ctx, `
		SELECT host_id, epoch, session_id, provider_id, cluster_scope,
		       node_uid, subject, issued_at, expires_at
		FROM public.compute_host_session_start_authenticated($1,$2,$3,$4,$5,$6)`,
		req.TenantID, req.Service, req.Subject, req.ExpectedEpoch,
		req.RequestID, int64(req.TTL/time.Second),
	).Scan(
		&fence.Binding.HostID, &fence.Epoch, &fence.SessionID,
		&fence.Binding.ProviderID, &fence.Binding.ClusterScope,
		&fence.Binding.NodeUID, &fence.Binding.Subject,
		&fence.IssuedAt, &fence.ExpiresAt,
	)
	if err != nil {
		return hostidentity.SessionFence{}, authenticatedHostSessionError("start authenticated HostSession", err)
	}
	if !validHostSessionBinding(fence.Binding) || fence.Binding.Subject != req.Subject ||
		fence.Epoch != req.ExpectedEpoch+1 || !validHostSessionIdentity(fence.SessionID, 256) ||
		!strings.HasPrefix(fence.SessionID, "hs_") || fence.IssuedAt.IsZero() ||
		!fence.IssuedAt.Before(fence.ExpiresAt) || fence.ExpiresAt.Sub(fence.IssuedAt) > req.TTL {
		return hostidentity.SessionFence{}, ErrHostSessionRejected
	}
	return fence, nil
}

// RecordAuthenticatedHostHeartbeat requires the database wrapper to resolve
// the binding and enforce active authorization and session fencing atomically.
func (r *Repository) RecordAuthenticatedHostHeartbeat(ctx context.Context, req AuthenticatedHostHeartbeatRequest) (hostidentity.Disposition, time.Time, error) {
	if err := validateAuthenticatedHostIdentity(req.TenantID, req.Service, req.Subject); err != nil {
		return hostidentity.Reject, time.Time{}, err
	}
	if r == nil || r.db == nil || req.Epoch <= 0 || req.Sequence <= 0 ||
		!validHostSessionIdentity(req.SessionID, 256) || !strings.HasPrefix(req.SessionID, "hs_") ||
		!validHostSessionDigest(req.Digest) {
		return hostidentity.Reject, time.Time{}, ErrHostSessionInvalid
	}

	var disposition string
	var recordedAt time.Time
	err := r.db.QueryRowContext(ctx, `
		SELECT disposition, recorded_at
		FROM public.compute_host_session_heartbeat_authenticated($1,$2,$3,$4,$5,$6,$7)`,
		req.TenantID, req.Service, req.Subject, req.Epoch, req.SessionID,
		req.Sequence, req.Digest,
	).Scan(&disposition, &recordedAt)
	if err != nil {
		return hostidentity.Reject, time.Time{}, authenticatedHostSessionError("record authenticated HostSession heartbeat", err)
	}
	if recordedAt.IsZero() {
		return hostidentity.Reject, time.Time{}, ErrHostSessionRejected
	}
	switch hostidentity.Disposition(disposition) {
	case hostidentity.Accept, hostidentity.Replay:
		return hostidentity.Disposition(disposition), recordedAt, nil
	default:
		return hostidentity.Reject, time.Time{}, ErrHostSessionRejected
	}
}
