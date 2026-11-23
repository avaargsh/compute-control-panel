package httpapi

import (
	"bytes"
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
const maxObservationResponseBytes = 1 << 20

var errObservationResponseTooLarge = errors.New("observation response size exceeded")

// Buffer the whole response before writing headers or bytes to the client.
// Reject oversized observations without leaking truncated JSON.
type boundedObservationResponse struct{ bytes.Buffer }

func (b *boundedObservationResponse) Write(p []byte) (int, error) {
	if len(p) > maxObservationResponseBytes-b.Len() {
		return 0, errObservationResponseTooLarge
	}
	return b.Buffer.Write(p)
}

// UID is intentionally not part of this logical key: conflicting versions
// of the same resource in one snapshot must be rejected, never overwritten.
type observationLogicalIdentity struct {
	projectID string
	clusterID string
	namespace string
	kind      string
	name      string
}

// ObservationSnapshot contains only observations for the authenticated caller.
// The reader must assign a single trusted data mode for the whole result.
type ObservationSnapshot = observation.Snapshot

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
	if now.IsZero() {
		writeWorkloadAPIError(w, http.StatusBadGateway, "OBSERVATION_INVALID", "The observation source cannot be verified")
		return
	}
	items := make([]observation.View, 0, len(snapshot.Items))
	seen := make(map[observationLogicalIdentity]struct{}, len(snapshot.Items))
	for _, record := range snapshot.Items {
		// Always re-check the returned scope even if a Reader promises isolation.
		// Reject the whole batch instead of selectively leaking partial records.
		if record.TenantID != principal.TenantID || !projectAllowed(principal, record.ProjectID) {
			writeWorkloadAPIError(w, http.StatusBadGateway, "OBSERVATION_SCOPE_MISMATCH", "The observation source cannot be verified")
			return
		}
		key := observationLogicalIdentity{
			projectID: record.ProjectID, clusterID: record.Resource.ClusterID,
			namespace: record.Resource.Namespace, kind: record.Resource.Kind, name: record.Resource.Name,
		}
		if _, duplicate := seen[key]; duplicate {
			writeWorkloadAPIError(w, http.StatusBadGateway, "OBSERVATION_DUPLICATE", "The observation source cannot be verified")
			return
		}
		seen[key] = struct{}{}
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
	// Do not send a success header or partial JSON until the encoded body has
	// passed the response byte cap.
	body := &boundedObservationResponse{}
	if err := json.NewEncoder(body).Encode(struct {
		DataMode    observation.Mode   `json:"data_mode"`
		GeneratedAt time.Time          `json:"generated_at"`
		Items       []observation.View `json:"items"`
	}{DataMode: snapshot.DataMode, GeneratedAt: now, Items: items}); err != nil {
		writeWorkloadAPIError(w, http.StatusBadGateway, "OBSERVATION_TOO_LARGE", "The observation source cannot be verified")
		return
	}
	w.Header().Set("Content-Type", "application/json")
	_, _ = w.Write(body.Bytes())
}
