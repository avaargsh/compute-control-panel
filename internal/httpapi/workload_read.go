package httpapi

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"net/http"
	"strings"

	"github.com/avaargsh/compute-control-panel/internal/operation"
	"github.com/avaargsh/compute-control-panel/internal/store/postgres"
)

const workloadReadRoutePrefix = "/api/v1/workloads/"

type WorkloadReadAuthorizer interface {
	AuthorizeRead(context.Context, WorkloadPrincipal, postgres.Workload) error
}

type WorkloadReadRepository interface {
	GetWorkloadDetails(context.Context, string, string) (postgres.WorkloadDetails, error)
}

type WorkloadReadDependencies struct {
	Authenticator WorkloadAuthenticator
	Authorizer    WorkloadReadAuthorizer
	Repository    WorkloadReadRepository
}

type WorkloadReadHandler struct {
	deps WorkloadReadDependencies
}

type workloadReadResponse struct {
	Workload    workloadReadWorkloadResponse     `json:"workload"`
	Allocations []workloadReadAllocationResponse `json:"allocations"`
	Operations  []workloadReadOperationResponse  `json:"operations"`
}

type workloadReadWorkloadResponse struct {
	WorkloadID         string  `json:"workload_id"`
	ProjectID          string  `json:"project_id"`
	DesiredGeneration  int64   `json:"desired_generation"`
	Version            int64   `json:"version"`
	DesiredState       string  `json:"desired_state"`
	ObservedPhase      string  `json:"observed_phase"`
	LatestAllocationID *string `json:"latest_allocation_id,omitempty"`
	CreatedAt          string  `json:"created_at"`
	UpdatedAt          string  `json:"updated_at"`
}

type workloadReadAllocationResponse struct {
	AllocationID      string `json:"allocation_id"`
	ProviderID        string `json:"provider_id"`
	PoolID            string `json:"pool_id"`
	DesiredGeneration int64  `json:"desired_generation"`
	Phase             string `json:"phase"`
	Version           int64  `json:"version"`
}

type workloadReadOperationResponse struct {
	OperationID       string           `json:"compute_operation_id"`
	AllocationID      string           `json:"allocation_id,omitempty"`
	ProviderID        string           `json:"provider_id"`
	Kind              operation.Kind   `json:"operation_kind"`
	DesiredGeneration uint64           `json:"desired_generation"`
	Version           uint64           `json:"version"`
	Status            operation.Status `json:"status"`
	CleanupRequired   bool             `json:"cleanup_required,omitempty"`
	ReconcileRequired bool             `json:"reconcile_required,omitempty"`
}

func NewWorkloadReadHandler(deps WorkloadReadDependencies) (http.Handler, error) {
	if deps.Authenticator == nil || deps.Authorizer == nil || deps.Repository == nil {
		return nil, errors.New("workload read handler requires authentication, authorization and repository dependencies")
	}
	return &WorkloadReadHandler{deps: deps}, nil
}

// NewWorkloadAPIHandler composes the independently testable create/read
// handlers. The returned mux is still not mounted by the default service main:
// deployments must first configure verified mTLS, identity resolution,
// authorization and a production Provider selector.
func NewWorkloadAPIHandler(create WorkloadCreateDependencies, read WorkloadReadDependencies) (http.Handler, error) {
	createHandler, err := NewWorkloadCreateHandler(create)
	if err != nil {
		return nil, err
	}
	readHandler, err := NewWorkloadReadHandler(read)
	if err != nil {
		return nil, err
	}
	mux := http.NewServeMux()
	mux.Handle("/api/v1/workloads", createHandler)
	mux.Handle(workloadReadRoutePrefix, readHandler)
	return mux, nil
}

func (h *WorkloadReadHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		w.Header().Set("Allow", http.MethodGet)
		writeWorkloadAPIError(w, http.StatusMethodNotAllowed, "METHOD_NOT_ALLOWED", "Only GET is supported")
		return
	}
	workloadID, ok := workloadIDFromReadPath(r.URL.Path)
	if !ok {
		writeWorkloadAPIError(w, http.StatusNotFound, "WORKLOAD_NOT_FOUND", "The requested Workload was not found")
		return
	}

	principal, err := h.deps.Authenticator.Authenticate(r.Context(), r)
	if err != nil || !validWorkloadPrincipal(principal) {
		writeWorkloadAPIError(w, http.StatusUnauthorized, "UNAUTHENTICATED", "A valid service identity is required")
		return
	}

	details, err := h.deps.Repository.GetWorkloadDetails(r.Context(), principal.TenantID, workloadID)
	if errors.Is(err, sql.ErrNoRows) {
		writeWorkloadAPIError(w, http.StatusNotFound, "WORKLOAD_NOT_FOUND", "The requested Workload was not found")
		return
	}
	if err != nil {
		writeWorkloadAPIError(w, http.StatusInternalServerError, "WORKLOAD_READ_FAILED", "The Workload state could not be read")
		return
	}

	// Use the same not-found result for another Project in the same Tenant so
	// callers cannot enumerate Workload IDs outside their granted Project set.
	if !projectAllowed(principal, details.Workload.ProjectID) {
		writeWorkloadAPIError(w, http.StatusNotFound, "WORKLOAD_NOT_FOUND", "The requested Workload was not found")
		return
	}
	if err := h.deps.Authorizer.AuthorizeRead(r.Context(), principal, details.Workload); err != nil {
		writeWorkloadAPIError(w, http.StatusForbidden, "WORKLOAD_READ_DENIED", "The requested Workload is not authorized for this service")
		return
	}

	allocations := make([]workloadReadAllocationResponse, 0, len(details.Allocations))
	for _, allocation := range details.Allocations {
		allocations = append(allocations, workloadReadAllocationResponse{
			AllocationID:      allocation.AllocationID,
			ProviderID:        allocation.ProviderID,
			PoolID:            allocation.PoolID,
			DesiredGeneration: allocation.DesiredGeneration,
			Phase:             allocation.Phase,
			Version:           allocation.Version,
		})
	}
	operations := make([]workloadReadOperationResponse, 0, len(details.Operations))
	for _, item := range details.Operations {
		operations = append(operations, workloadReadOperationResponse{
			OperationID:       item.ID,
			AllocationID:      item.AllocationID,
			ProviderID:        item.ProviderID,
			Kind:              item.Kind,
			DesiredGeneration: item.Generation,
			Version:           item.Version,
			Status:            item.Status,
			CleanupRequired:   item.CleanupRequired,
			ReconcileRequired: item.ReconcileRequired,
		})
	}
	var latestAllocationID *string
	if details.Workload.LatestAllocationID.Valid {
		value := details.Workload.LatestAllocationID.String
		latestAllocationID = &value
	}
	response := workloadReadResponse{
		Workload: workloadReadWorkloadResponse{
			WorkloadID:         details.Workload.WorkloadID,
			ProjectID:          details.Workload.ProjectID,
			DesiredGeneration:  details.Workload.DesiredGeneration,
			Version:            details.Workload.Version,
			DesiredState:       details.Workload.DesiredState,
			ObservedPhase:      details.Workload.ObservedPhase,
			LatestAllocationID: latestAllocationID,
			CreatedAt:          details.Workload.CreatedAt.UTC().Format("2006-01-02T15:04:05.999999999Z07:00"),
			UpdatedAt:          details.Workload.UpdatedAt.UTC().Format("2006-01-02T15:04:05.999999999Z07:00"),
		},
		Allocations: allocations,
		Operations:  operations,
	}
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Cache-Control", "no-store")
	_ = json.NewEncoder(w).Encode(response)
}

func workloadIDFromReadPath(path string) (string, bool) {
	if !strings.HasPrefix(path, workloadReadRoutePrefix) {
		return "", false
	}
	id := strings.TrimPrefix(path, workloadReadRoutePrefix)
	if id == "" || strings.Contains(id, "/") || strings.TrimSpace(id) != id || len(id) > 256 || id == "." || id == ".." {
		return "", false
	}
	return id, true
}
