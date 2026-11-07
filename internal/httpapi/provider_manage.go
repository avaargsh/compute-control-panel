package httpapi

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"regexp"
	"strings"
	"time"

	"github.com/avaargsh/compute-control-panel/internal/store/postgres"
)

const (
	providerManagementCollection = "/api/v1/manage/providers"
	maxProviderManagementBody    = 64 << 10
)

var (
	providerManageIDPattern     = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9._:-]{0,127}$`)
	providerManageReasonPattern = regexp.MustCompile(`^[A-Z][A-Z0-9_]{0,127}$`)
)

// ProviderManagementAuthorizer is a separate privileged management policy,
// not a Tenant/Project Workload grant. Implementations must verify explicit
// administrator authority for the supplied action and Provider identity.
type ProviderManagementAuthorizer interface {
	AuthorizeProviderManage(context.Context, WorkloadPrincipal, string, string) error
}

// ProviderManagementRepository writes through the durable Provider registry.
// Registered and Draining are intent/eligibility states, never GPU readiness.
type ProviderManagementRepository interface {
	RegisterProvider(context.Context, postgres.ProviderRegistration) (postgres.ProviderRecord, bool, error)
	GetProvider(context.Context, string) (postgres.ProviderRecord, error)
	BeginProviderDrain(context.Context, string, int64, string) (postgres.ProviderRecord, error)
}

type ProviderManagementDependencies struct {
	Authenticator WorkloadAuthenticator
	Authorizer    ProviderManagementAuthorizer
	Repository    ProviderManagementRepository
}

type providerManagementHandler struct {
	deps ProviderManagementDependencies
}

type providerRegistrationRequest struct {
	ProviderID       string `json:"provider_id"`
	ClusterScope     string `json:"cluster_scope"`
	AdapterSource    string `json:"adapter_source"`
	AdapterVersion   string `json:"adapter_version"`
	CapabilityDigest string `json:"capability_digest"`
}

type providerDrainRequest struct {
	ExpectedVersion int64  `json:"expected_version"`
	ReasonCode      string `json:"reason_code"`
}

type providerManagementResponse struct {
	ProviderID       string `json:"provider_id"`
	ClusterScope     string `json:"cluster_scope"`
	AdapterSource    string `json:"adapter_source"`
	AdapterVersion   string `json:"adapter_version"`
	CapabilityDigest string `json:"capability_digest"`
	State            string `json:"state"`
	Version          int64  `json:"version"`
	LastReasonCode   string `json:"last_reason_code"`
	RegisteredAt     string `json:"registered_at"`
	UpdatedAt        string `json:"updated_at"`
}

// NewProviderManagementHandler deliberately returns an isolated, unmounted
// handler. Neither cmd/compute-control-panel nor the normal Workload API mux
// exposes it until verified mTLS, explicit admin policy and least-privilege
// infrastructure are installed and independently tested.
func NewProviderManagementHandler(deps ProviderManagementDependencies) (http.Handler, error) {
	if deps.Authenticator == nil || deps.Authorizer == nil || deps.Repository == nil {
		return nil, errors.New("Provider management requires authentication, administrator authorization and repository")
	}
	h := &providerManagementHandler{deps: deps}
	mux := http.NewServeMux()
	mux.HandleFunc("POST "+providerManagementCollection, h.register)
	mux.HandleFunc("GET "+providerManagementCollection+"/{provider_id}", h.read)
	mux.HandleFunc("POST "+providerManagementCollection+"/{provider_id}/drain", h.drain)
	return mux, nil
}

func (h *providerManagementHandler) authenticate(w http.ResponseWriter, r *http.Request) (WorkloadPrincipal, bool) {
	principal, err := h.deps.Authenticator.Authenticate(r.Context(), r)
	if err != nil || !validWorkloadPrincipal(principal) {
		writeWorkloadAPIError(w, http.StatusUnauthorized, "UNAUTHENTICATED", "A verified management service identity is required")
		return WorkloadPrincipal{}, false
	}
	return principal, true
}

func (h *providerManagementHandler) authorize(w http.ResponseWriter, r *http.Request, principal WorkloadPrincipal, action, providerID string) bool {
	if err := h.deps.Authorizer.AuthorizeProviderManage(r.Context(), principal, action, providerID); err != nil {
		writeWorkloadAPIError(w, http.StatusForbidden, "PROVIDER_MANAGE_DENIED", "Provider management permission is required")
		return false
	}
	return true
}

func validProviderManageID(id string) bool {
	return providerManageIDPattern.MatchString(id)
}

func validProviderManageLabel(value string) bool {
	return value != "" && value == strings.TrimSpace(value) && len(value) <= 256 && !strings.ContainsRune(value, 0)
}

func readProviderManagementJSON(w http.ResponseWriter, r *http.Request, dst any) bool {
	body, err := io.ReadAll(http.MaxBytesReader(w, r.Body, maxProviderManagementBody))
	if err != nil || len(body) == 0 || validateWorkloadJSONTokens(body) != nil {
		writeWorkloadAPIError(w, http.StatusBadRequest, "INVALID_PROVIDER_REQUEST", "Invalid, ambiguous or oversized Provider JSON")
		return false
	}
	decoder := json.NewDecoder(bytes.NewReader(body))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(dst); err != nil {
		writeWorkloadAPIError(w, http.StatusBadRequest, "INVALID_PROVIDER_REQUEST", "The request does not match the Provider management contract")
		return false
	}
	return true
}

func (h *providerManagementHandler) register(w http.ResponseWriter, r *http.Request) {
	principal, ok := h.authenticate(w, r)
	if !ok {
		return
	}
	var request providerRegistrationRequest
	if !readProviderManagementJSON(w, r, &request) {
		return
	}
	if !validProviderManageID(request.ProviderID) || !validProviderManageLabel(request.ClusterScope) ||
		!validProviderManageLabel(request.AdapterSource) || !validProviderManageLabel(request.AdapterVersion) ||
		!validWorkloadDigest(request.CapabilityDigest) {
		writeWorkloadAPIError(w, http.StatusBadRequest, "INVALID_PROVIDER_REQUEST", "Invalid Provider registration identity or immutable fingerprint")
		return
	}
	if !h.authorize(w, r, principal, "register", request.ProviderID) {
		return
	}
	p, created, err := h.deps.Repository.RegisterProvider(r.Context(), postgres.ProviderRegistration{
		ProviderID: request.ProviderID, ClusterScope: request.ClusterScope,
		AdapterSource: request.AdapterSource, AdapterVersion: request.AdapterVersion,
		CapabilityDigest: request.CapabilityDigest,
	})
	switch {
	case errors.Is(err, postgres.ErrProviderInvalid):
		writeWorkloadAPIError(w, http.StatusBadRequest, "INVALID_PROVIDER_REQUEST", "Invalid Provider registration")
	case errors.Is(err, postgres.ErrProviderConflict):
		writeWorkloadAPIError(w, http.StatusConflict, "PROVIDER_FINGERPRINT_CONFLICT", "Provider identity is bound to another immutable registration")
	case err != nil:
		writeWorkloadAPIError(w, http.StatusInternalServerError, "PROVIDER_MANAGE_FAILED", "Provider registration could not be persisted")
	default:
		w.Header().Set("Location", providerManagementCollection+"/"+p.ProviderID)
		if created {
			writeProviderManagementRecord(w, http.StatusCreated, p)
		} else {
			writeProviderManagementRecord(w, http.StatusOK, p)
		}
	}
}

func (h *providerManagementHandler) read(w http.ResponseWriter, r *http.Request) {
	principal, ok := h.authenticate(w, r)
	if !ok {
		return
	}
	id := r.PathValue("provider_id")
	if !validProviderManageID(id) {
		writeWorkloadAPIError(w, http.StatusNotFound, "PROVIDER_NOT_FOUND", "Provider not found")
		return
	}
	if !h.authorize(w, r, principal, "read", id) {
		return
	}
	p, err := h.deps.Repository.GetProvider(r.Context(), id)
	switch {
	case errors.Is(err, sql.ErrNoRows):
		writeWorkloadAPIError(w, http.StatusNotFound, "PROVIDER_NOT_FOUND", "Provider not found")
	case err != nil:
		writeWorkloadAPIError(w, http.StatusInternalServerError, "PROVIDER_MANAGE_FAILED", "Provider state could not be read")
	default:
		writeProviderManagementRecord(w, http.StatusOK, p)
	}
}

func (h *providerManagementHandler) drain(w http.ResponseWriter, r *http.Request) {
	principal, ok := h.authenticate(w, r)
	if !ok {
		return
	}
	id := r.PathValue("provider_id")
	if !validProviderManageID(id) {
		writeWorkloadAPIError(w, http.StatusNotFound, "PROVIDER_NOT_FOUND", "Provider not found")
		return
	}
	var request providerDrainRequest
	if !readProviderManagementJSON(w, r, &request) {
		return
	}
	if request.ExpectedVersion <= 0 || !providerManageReasonPattern.MatchString(request.ReasonCode) ||
		request.ReasonCode == "REGISTERED" {
		writeWorkloadAPIError(w, http.StatusBadRequest, "INVALID_PROVIDER_REQUEST", "Drain requires a positive expected version and valid reason")
		return
	}
	if !h.authorize(w, r, principal, "drain", id) {
		return
	}
	p, err := h.deps.Repository.BeginProviderDrain(r.Context(), id, request.ExpectedVersion, request.ReasonCode)
	switch {
	case errors.Is(err, sql.ErrNoRows):
		writeWorkloadAPIError(w, http.StatusNotFound, "PROVIDER_NOT_FOUND", "Provider not found")
	case errors.Is(err, postgres.ErrProviderInvalid):
		writeWorkloadAPIError(w, http.StatusBadRequest, "INVALID_PROVIDER_REQUEST", "Invalid Provider drain request")
	case errors.Is(err, postgres.ErrProviderStale), errors.Is(err, postgres.ErrProviderTransition):
		writeWorkloadAPIError(w, http.StatusConflict, "PROVIDER_VERSION_OR_STATE_CONFLICT", "Provider version or lifecycle state no longer allows this transition")
	case err != nil:
		writeWorkloadAPIError(w, http.StatusInternalServerError, "PROVIDER_MANAGE_FAILED", "Provider drain could not be persisted")
	default:
		writeProviderManagementRecord(w, http.StatusOK, p)
	}
}

func writeProviderManagementRecord(w http.ResponseWriter, status int, p postgres.ProviderRecord) {
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Cache-Control", "no-store")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(providerManagementResponse{
		ProviderID: p.ProviderID, ClusterScope: p.ClusterScope, AdapterSource: p.AdapterSource,
		AdapterVersion: p.AdapterVersion, CapabilityDigest: p.CapabilityDigest,
		State: string(p.State), Version: p.Version, LastReasonCode: p.LastReasonCode,
		RegisteredAt: p.RegisteredAt.UTC().Format(time.RFC3339Nano),
		UpdatedAt: p.UpdatedAt.UTC().Format(time.RFC3339Nano),
	})
}

var _ ProviderManagementRepository = (*postgres.Repository)(nil)
