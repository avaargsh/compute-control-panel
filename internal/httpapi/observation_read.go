package httpapi

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"sort"
	"time"

	"github.com/avaargsh/compute-control-panel/internal/observation"
)

const observationReadPath = "/api/v1/observations"
const maxObservationItems = 1000

// ObservationSnapshot contains only observations for the authenticated caller.
// The reader must assign a single trusted data mode for the whole result.
type ObservationSnapshot struct {
	DataMode observation.Mode     `json:"data_mode"`
	Items    []observation.Record `json:"items"`
}

type ObservationReader interface {
	ListObservations(ctx context.Context, tenantID string, projectIDs []string) (ObservationSnapshot, error)
}

type ObservationReadAuthorizer interface {
	AuthorizeObservationRead(ctx context.Context, principal WorkloadPrincipal) error
}

type ObservationReadDependencies struct {
	Authenticator WorkloadAuthenticator
	Authorizer    ObservationReadAuthorizer
	Reader        ObservationReader
	Now           func() time.Time
}

type observationReadHandler struct{ deps ObservationReadDependencies }

// NewObservationReadHandler is deliberately NOT wired to the default service.
// An authenticated identity directory and project-granted reader must be
// provisioned before a production HTTP server may mount this endpoint.
func NewObservationReadHandler(deps ObservationReadDependencies) (http.Handler, error) {
	if deps.Authenticator == nil || deps.Authorizer == nil || deps.Reader == nil || deps.Now == nil {
		return nil, errors.New("observation read handler requires authentication, authorization, reader and clock")
	}
	return &observationReadHandler{deps: deps}, nil
}

func (h *observationReadHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Cache-Control", "no-store")
	w.Header().Set("X-Content-Type-Options", "nosniff")
	if r.URL.Path != observationReadPath {
		writeWorkloadAPIError(w, http.StatusNotFound, "NOT_FOUND", "The endpoint was not found")
		return
	}
	if r.Method != http.MethodGet {
		w.Header().Set("Allow", http.MethodGet)
		writeWorkloadAPIError(w, http.StatusMethodNotAllowed, "METHOD_NOT_ALLOWED", "Only GET is supported")
		return
	}
	if r.URL.RawQuery != "" || r.URL.ForceQuery {
		writeWorkloadAPIError(w, http.StatusBadRequest, "INVALID_QUERY", "Query parameters are not supported")
		return
	}
	principal, err := h.deps.Authenticator.Authenticate(r.Context(), r)
	if err != nil || !validWorkloadPrincipal(principal) {
		writeWorkloadAPIError(w, http.StatusUnauthorized, "UNAUTHENTICATED", "A valid service identity is required")
		return
	}
	if len(principal.ProjectIDs) == 0 {
		writeWorkloadAPIError(w, http.StatusForbidden, "OBSERVATION_READ_DENIED", "Observation access requires an authorized project")
		return
	}
	if err := h.deps.Authorizer.AuthorizeObservationRead(r.Context(), principal); err != nil {
		writeWorkloadAPIError(w, http.StatusForbidden, "OBSERVATION_READ_DENIED", "Observation access is denied")
		return
	}
	// A cloned project list protects against readers mutating caller grants.
	snapshot, err := h.deps.Reader.ListObservations(r.Context(), principal.TenantID, append([]string(nil), principal.ProjectIDs...))
	if err != nil || observation.ValidateMode(snapshot.DataMode) != nil || len(snapshot.Items) > maxObservationItems {
		writeWorkloadAPIError(w, http.StatusBadGateway, "OBSERVATION_SOURCE_FAILED", "The observation source cannot be verified")
		return
	}
	now := h.deps.Now().UTC()
	items := make([]observation.View, 0, len(snapshot.Items))
	for _, record := range snapshot.Items {
		// Always re-check the returned scope even if a Reader promises isolation.
		// Reject the whole batch instead of selectively leaking partial records.
		if record.TenantID != principal.TenantID || !projectAllowed(principal, record.ProjectID) {
			writeWorkloadAPIError(w, http.StatusBadGateway, "OBSERVATION_SCOPE_MISMATCH", "The observation source cannot be verified")
			return
		}
		view, err := observation.Project(record, now)
		if err != nil {
			writeWorkloadAPIError(w, http.StatusBadGateway, "OBSERVATION_INVALID", "The observation source cannot be verified")
			return
		}
		items = append(items, view)
	}
	sort.Slice(items, func(i, j int) bool {
		a, b := items[i], items[j]
		if a.ProjectID != b.ProjectID {
			return a.ProjectID < b.ProjectID
		}
		if a.Resource.ClusterID != b.Resource.ClusterID {
			return a.Resource.ClusterID < b.Resource.ClusterID
		}
		if a.Resource.Kind != b.Resource.Kind {
			return a.Resource.Kind < b.Resource.Kind
		}
		return a.Resource.Name < b.Resource.Name
	})
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(struct {
		DataMode    observation.Mode   `json:"data_mode"`
		GeneratedAt time.Time          `json:"generated_at"`
		Items       []observation.View `json:"items"`
	}{DataMode: snapshot.DataMode, GeneratedAt: now, Items: items})
}
