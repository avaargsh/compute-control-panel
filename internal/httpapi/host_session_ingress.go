package httpapi

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"math"
	"mime"
	"net/http"
	"reflect"
	"strings"
	"time"
	"unicode"
	"unicode/utf8"

	"github.com/avaargsh/compute-control-panel/internal/hostidentity"
	"github.com/avaargsh/compute-control-panel/internal/store/postgres"
)

const (
	hostSessionStartPath = "/internal/v1/hosts/session/start"
	hostHeartbeatPath    = "/internal/v1/hosts/session/heartbeat"
	maxHostSessionBody   = 4096
)

type HostSessionIngressRepository interface {
	StartAuthenticatedHostSession(context.Context, postgres.AuthenticatedHostSessionStartRequest) (hostidentity.SessionFence, error)
	RecordAuthenticatedHostHeartbeat(context.Context, postgres.AuthenticatedHostHeartbeatRequest) (hostidentity.Disposition, time.Time, error)
}

type HostSessionIngressDependencies struct {
	Authenticator WorkloadAuthenticator
	Repository    HostSessionIngressRepository
}

type hostSessionIngressHandler struct {
	auth *SPIFFEWorkloadAuthenticator
	repo HostSessionIngressRepository
}

// NewHostSessionIngressHandler constructs an unmounted candidate only.
// Deployment requires a dedicated host trust bundle, direct mutual TLS,
// independently attested machine enrollment and transactional authorization.
func NewHostSessionIngressHandler(deps HostSessionIngressDependencies) (http.Handler, error) {
	auth, ok := deps.Authenticator.(*SPIFFEWorkloadAuthenticator)
	if !ok || auth == nil || auth.trustDomain == "" ||
		missingProviderManagementDependency(auth.resolver) ||
		missingProviderManagementDependency(deps.Repository) {
		return nil, errors.New("host session ingress requires verified SPIFFE authentication and restricted repository")
	}
	h := &hostSessionIngressHandler{auth: auth, repo: deps.Repository}
	mux := http.NewServeMux()
	mux.HandleFunc("POST "+hostSessionStartPath, h.start)
	mux.HandleFunc("POST "+hostHeartbeatPath, h.heartbeat)
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Cache-Control", "no-store")
		mux.ServeHTTP(w, r)
	}), nil
}

type hostSessionStartBody struct {
	ExpectedEpoch int64 `json:"expected_epoch"`
	TTLSeconds    int64 `json:"ttl_seconds"`
}

type hostHeartbeatBody struct {
	Epoch        int64  `json:"epoch"`
	SessionID    string `json:"session_id"`
	Sequence     int64  `json:"sequence"`
	AgentVersion string `json:"agent_version"`
}

func validHostIngressText(value string, limit int) bool {
	return value != "" && len(value) <= limit && utf8.ValidString(value) &&
		strings.TrimSpace(value) == value && strings.IndexFunc(value, unicode.IsControl) < 0
}

func readHostIngressBody(w http.ResponseWriter, r *http.Request, dst any) bool {
	values := r.Header.Values("Content-Type")
	if len(values) > 1 {
		writeWorkloadAPIError(w, http.StatusUnsupportedMediaType, "INVALID_CONTENT_TYPE", "Expected application/json")
		return false
	}
	if len(values) == 1 {
		media, _, err := mime.ParseMediaType(values[0])
		if err != nil || media != "application/json" {
			writeWorkloadAPIError(w, http.StatusUnsupportedMediaType, "INVALID_CONTENT_TYPE", "Expected application/json")
			return false
		}
	}
	if r.Body != nil {
		raw, err := io.ReadAll(http.MaxBytesReader(w, r.Body, maxHostSessionBody))
		if err == nil && utf8.Valid(raw) && validateWorkloadJSONTokens(raw) == nil &&
			exactDRAJSONFields(raw, reflect.TypeOf(dst).Elem()) &&
			json.Unmarshal(raw, dst) == nil {
			return true
		}
	}
	writeWorkloadAPIError(w, http.StatusBadRequest, "INVALID_HOST_REQUEST", "Invalid, ambiguous or oversized host request")
	return false
}

func (h *hostSessionIngressHandler) principal(w http.ResponseWriter, r *http.Request) (WorkloadPrincipal, bool) {
	if !verifiedDRAClientTLS(r) {
		writeWorkloadAPIError(w, http.StatusUnauthorized, "HOST_UNAUTHENTICATED", "Verified client TLS required")
		return WorkloadPrincipal{}, false
	}
	p, err := h.auth.Authenticate(r.Context(), r)
	if err != nil || !validWorkloadPrincipal(p) {
		writeWorkloadAPIError(w, http.StatusUnauthorized, "HOST_UNAUTHENTICATED", "Verified service identity required")
		return WorkloadPrincipal{}, false
	}
	if p.Service != "compute-host-agent" {
		writeWorkloadAPIError(w, http.StatusForbidden, "HOST_DENIED", "Host agent service required")
		return WorkloadPrincipal{}, false
	}
	return p, true
}

func hostIngressError(w http.ResponseWriter, err error) {
	switch {
	case errors.Is(err, postgres.ErrHostSessionUnauthorized):
		writeWorkloadAPIError(w, http.StatusForbidden, "HOST_DENIED", "Host authorization rejected")
	case errors.Is(err, postgres.ErrHostSessionInvalid):
		writeWorkloadAPIError(w, http.StatusBadRequest, "INVALID_HOST_REQUEST", "Invalid host session request")
	case errors.Is(err, postgres.ErrHostSessionRejected):
		writeWorkloadAPIError(w, http.StatusConflict, "HOST_SESSION_CONFLICT", "Session fence rejected request")
	default:
		writeWorkloadAPIError(w, http.StatusServiceUnavailable, "HOST_SESSION_UNAVAILABLE", "Host session repository unavailable")
	}
}

func (h *hostSessionIngressHandler) start(w http.ResponseWriter, r *http.Request) {
	p, ok := h.principal(w, r)
	if !ok {
		return
	}
	var body hostSessionStartBody
	if !readHostIngressBody(w, r, &body) {
		return
	}
	keys := r.Header.Values("Idempotency-Key")
	if len(keys) != 1 || !validHostIngressText(keys[0], 256) ||
		body.ExpectedEpoch < 0 || body.ExpectedEpoch == math.MaxInt64 ||
		body.TTLSeconds < 1 || body.TTLSeconds > 86400 {
		hostIngressError(w, postgres.ErrHostSessionInvalid)
		return
	}
	fence, err := h.repo.StartAuthenticatedHostSession(r.Context(), postgres.AuthenticatedHostSessionStartRequest{
		TenantID: p.TenantID, Service: p.Service, Subject: p.Subject,
		ExpectedEpoch: body.ExpectedEpoch, RequestID: keys[0], TTL: time.Duration(body.TTLSeconds) * time.Second,
	})
	if err != nil {
		hostIngressError(w, err)
		return
	}
	if fence.Binding.Subject != p.Subject || fence.Epoch != body.ExpectedEpoch+1 ||
		!strings.HasPrefix(fence.SessionID, "hs_") || !validHostIngressText(fence.SessionID, 256) ||
		fence.IssuedAt.IsZero() || !fence.IssuedAt.Before(fence.ExpiresAt) ||
		fence.ExpiresAt.Sub(fence.IssuedAt) > time.Duration(body.TTLSeconds)*time.Second {
		hostIngressError(w, errors.New("invalid session receipt"))
		return
	}
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusCreated)
	_ = json.NewEncoder(w).Encode(struct {
		Epoch     int64     `json:"epoch"`
		SessionID string    `json:"session_id"`
		IssuedAt  time.Time `json:"issued_at"`
		ExpiresAt time.Time `json:"expires_at"`
	}{fence.Epoch, fence.SessionID, fence.IssuedAt, fence.ExpiresAt})
}

func (h *hostSessionIngressHandler) heartbeat(w http.ResponseWriter, r *http.Request) {
	p, ok := h.principal(w, r)
	if !ok {
		return
	}
	var body hostHeartbeatBody
	if !readHostIngressBody(w, r, &body) {
		return
	}
	if body.Epoch <= 0 || body.Sequence <= 0 ||
		!strings.HasPrefix(body.SessionID, "hs_") || !validHostIngressText(body.SessionID, 256) ||
		!validHostIngressText(body.AgentVersion, 128) {
		hostIngressError(w, postgres.ErrHostSessionInvalid)
		return
	}
	canonical, err := json.Marshal(body)
	if err != nil {
		hostIngressError(w, postgres.ErrHostSessionInvalid)
		return
	}
	sum := sha256.Sum256(append([]byte("ccp:host-heartbeat:v1\x00"), canonical...))
	disposition, recordedAt, err := h.repo.RecordAuthenticatedHostHeartbeat(r.Context(), postgres.AuthenticatedHostHeartbeatRequest{
		TenantID: p.TenantID, Service: p.Service, Subject: p.Subject,
		Epoch: body.Epoch, SessionID: body.SessionID, Sequence: body.Sequence,
		Digest: "sha256:" + hex.EncodeToString(sum[:]),
	})
	if err != nil {
		hostIngressError(w, err)
		return
	}
	status := http.StatusAccepted
	if disposition == hostidentity.Replay {
		status = http.StatusOK
	} else if disposition != hostidentity.Accept {
		hostIngressError(w, errors.New("invalid heartbeat receipt"))
		return
	}
	if recordedAt.IsZero() {
		hostIngressError(w, errors.New("missing heartbeat receipt time"))
		return
	}
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(struct {
		Disposition hostidentity.Disposition `json:"disposition"`
		RecordedAt  time.Time                `json:"recorded_at"`
	}{disposition, recordedAt})
}

var _ HostSessionIngressRepository = (*postgres.Repository)(nil)
