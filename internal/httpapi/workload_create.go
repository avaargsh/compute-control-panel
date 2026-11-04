package httpapi

import (
	"bytes"
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"regexp"
	"strings"
	"unicode/utf8"

	"github.com/avaargsh/compute-control-panel/internal/identity"
	"github.com/avaargsh/compute-control-panel/internal/operation"
	"github.com/avaargsh/compute-control-panel/internal/store/postgres"
)

const (
	workloadCreateAPIVersion = "compute.platform/v1"
	maxWorkloadCreateBody    = 1 << 20
)

var workloadCPUDecimal = regexp.MustCompile(`^[0-9]+(\.[0-9]+)?package httpapi

import (
	"bytes"
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"regexp"
	"strings"
	"unicode/utf8"

	"github.com/avaargsh/compute-control-panel/internal/identity"
	"github.com/avaargsh/compute-control-panel/internal/operation"
	"github.com/avaargsh/compute-control-panel/internal/store/postgres"
)

const (
	workloadCreateAPIVersion = "compute.platform/v1"
	)

type WorkloadPrincipal = identity.WorkloadPrincipal

type WorkloadAuthenticator interface {
	Authenticate(context.Context, *http.Request) (WorkloadPrincipal, error)
}

type WorkloadCreateAuthorizer interface {
	// AuthorizeCreate must enforce service policy and, for protected Agent-originated
	// actions, verify the Guard Permit against audience and the normalized request
	// digest. It is called before provider selection or repository mutation.
	AuthorizeCreate(context.Context, WorkloadPrincipal, WorkloadCreateRequest, string) error
}

type WorkloadProviderSelector interface {
	// SelectProvider resolves Provider and Pool server-side. The request body never
	// supplies provider identities or provider-native resource references.
	SelectProvider(context.Context, WorkloadPrincipal, WorkloadCreateRequest) (WorkloadProviderSelection, error)
}

type WorkloadAcceptanceRepository interface {
	GetAcceptanceByIdempotency(context.Context, postgres.AcceptanceLookup) (postgres.WorkloadAcceptance, bool, error)
	AcceptWorkload(context.Context, postgres.AcceptWorkloadParams) (postgres.WorkloadAcceptance, bool, error)
}

type WorkloadIDGenerator interface {
	NewID(prefix string) (string, error)
}

type WorkloadProviderSelection struct {
	ProviderID string
	PoolID     string
}

type WorkloadCreateDependencies struct {
	Authenticator    WorkloadAuthenticator
	Authorizer       WorkloadCreateAuthorizer
	ProviderSelector WorkloadProviderSelector
	Repository       WorkloadAcceptanceRepository
	IDs              WorkloadIDGenerator
}

type WorkloadCreateHandler struct {
	deps WorkloadCreateDependencies
}

type WorkloadOwnerRef struct {
	Service      string `json:"service"`
	ResourceType string `json:"resource_type"`
	ResourceID   string `json:"resource_id"`
}

type WorkloadComputeProfileRef struct {
	ID       string `json:"id"`
	Revision string `json:"revision"`
}

type WorkloadAcceleratorRequest struct {
	Vendor string `json:"vendor"`
	Model  string `json:"model"`
	Count  int    `json:"count"`
}

type WorkloadResourcesRequest struct {
	CPU          string                       `json:"cpu"`
	Memory       string                       `json:"memory"`
	Accelerators []WorkloadAcceleratorRequest `json:"accelerators,omitempty"`
	Storage      *string                      `json:"storage,omitempty"`
}

type WorkloadConstraintsRequest struct {
	PoolSelector       map[string]string `json:"pool_selector,omitempty"`
	MaxDurationSeconds *int              `json:"max_duration_seconds,omitempty"`
}

type WorkloadAuthorizationRequest struct {
	GuardOperationID string `json:"guard_operation_id"`
	PermitRef        string `json:"permit_ref"`
	ActionHash       string `json:"action_hash"`
	Audience         string `json:"audience"`
}

type WorkloadCreateRequest struct {
	APIVersion        string                        `json:"api_version"`
	ProjectID         string                        `json:"project_id"`
	OwnerRef          WorkloadOwnerRef              `json:"owner_ref"`
	ComputeProfileRef *WorkloadComputeProfileRef    `json:"compute_profile_ref,omitempty"`
	Resources         *WorkloadResourcesRequest     `json:"resources,omitempty"`
	Constraints       *WorkloadConstraintsRequest   `json:"constraints,omitempty"`
	Authorization     *WorkloadAuthorizationRequest `json:"authorization,omitempty"`
}

type workloadAcceptedResponse struct {
	WorkloadID         string `json:"workload_id"`
	DesiredGeneration  int64  `json:"desired_generation"`
	DesiredState       string `json:"desired_state"`
	ObservedPhase      string `json:"observed_phase"`
	AllocationID       string `json:"allocation_id"`
	AllocationPhase    string `json:"allocation_phase"`
	ComputeOperationID string `json:"compute_operation_id"`
	StatusURL          string `json:"status_url"`
}

type workloadAPIError struct {
	Code    string `json:"code"`
	Message string `json:"message"`
}

type cryptoWorkloadIDGenerator struct{}

func (cryptoWorkloadIDGenerator) NewID(prefix string) (string, error) {
	if strings.TrimSpace(prefix) == "" {
		return "", errors.New("ID prefix is required")
	}
	var entropy [16]byte
	if _, err := rand.Read(entropy[:]); err != nil {
		return "", fmt.Errorf("generate workload identity: %w", err)
	}
	return prefix + "_" + hex.EncodeToString(entropy[:]), nil
}

func NewWorkloadCreateHandler(deps WorkloadCreateDependencies) (http.Handler, error) {
	if deps.Authenticator == nil || deps.Authorizer == nil || deps.ProviderSelector == nil || deps.Repository == nil {
		return nil, errors.New("workload create handler requires authentication, authorization, provider selection and repository dependencies")
	}
	if deps.IDs == nil {
		deps.IDs = cryptoWorkloadIDGenerator{}
	}
	return &WorkloadCreateHandler{deps: deps}, nil
}

func (h *WorkloadCreateHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		w.Header().Set("Allow", http.MethodPost)
		writeWorkloadAPIError(w, http.StatusMethodNotAllowed, "METHOD_NOT_ALLOWED", "Only POST is supported")
		return
	}

	principal, err := h.deps.Authenticator.Authenticate(r.Context(), r)
	if err != nil || !validWorkloadPrincipal(principal) {
		writeWorkloadAPIError(w, http.StatusUnauthorized, "UNAUTHENTICATED", "A valid service identity is required")
		return
	}

	idempotencyKey, err := readIdempotencyKey(r)
	if err != nil {
		writeWorkloadAPIError(w, http.StatusBadRequest, "INVALID_IDEMPOTENCY_KEY", "A single non-empty Idempotency-Key header is required")
		return
	}

	body, err := io.ReadAll(http.MaxBytesReader(w, r.Body, maxWorkloadCreateBody))
	if err != nil {
		writeWorkloadAPIError(w, http.StatusBadRequest, "INVALID_BODY", "Request body is invalid or exceeds the size limit")
		return
	}
	if err := rejectDuplicateJSONKeys(body); err != nil {
		writeWorkloadAPIError(w, http.StatusBadRequest, "INVALID_JSON", "Request JSON is malformed or contains duplicate object keys")
		return
	}

	request, normalized, requestDigest, err := decodeWorkloadCreate(body)
	if err != nil {
		writeWorkloadAPIError(w, http.StatusBadRequest, "INVALID_WORKLOAD_REQUEST", "Request does not match the Workload v1 contract")
		return
	}
	if !projectAllowed(principal, request.ProjectID) || !ownerMatchesPrincipal(principal, request.OwnerRef) {
		writeWorkloadAPIError(w, http.StatusForbidden, "WORKLOAD_SCOPE_DENIED", "The authenticated service identity is not authorized for this project or owner reference")
		return
	}

	// Authorization (including Guard Permit/action digest binding) precedes
	// either replay lookup or provider resolution.
	if err := h.deps.Authorizer.AuthorizeCreate(r.Context(), principal, request, requestDigest); err != nil {
		writeWorkloadAPIError(w, http.StatusForbidden, "WORKLOAD_CREATE_DENIED", "The requested Workload is not authorized")
		return
	}

	lookup, err := buildAcceptanceLookup(principal, request, requestDigest, idempotencyKey)
	if err != nil {
		writeWorkloadAPIError(w, http.StatusInternalServerError, "WORKLOAD_ACCEPTANCE_FAILED", "The Workload acceptance lookup could not be prepared")
		return
	}
	existing, found, err := h.deps.Repository.GetAcceptanceByIdempotency(r.Context(), lookup)
	if err != nil {
		if errors.Is(err, postgres.ErrIdempotencyConflict) {
			writeWorkloadAPIError(w, http.StatusConflict, "IDEMPOTENCY_KEY_REUSED", "The Idempotency-Key is already bound to a different Workload request")
		} else {
			writeWorkloadAPIError(w, http.StatusInternalServerError, "WORKLOAD_ACCEPTANCE_FAILED", "The Workload acceptance lookup could not be completed")
		}
		return
	}
	if found {
		writeWorkloadAccepted(w, existing)
		return
	}

	// Authorization references are consumed by the Authorizer only. Do not
	// pass PermitRef/Guard metadata into Provider capacity-selection code.
	providerRequest := request
	providerRequest.Authorization = nil
	selection, err := h.deps.ProviderSelector.SelectProvider(r.Context(), principal, providerRequest)
	if err != nil || strings.TrimSpace(selection.ProviderID) == "" || strings.TrimSpace(selection.PoolID) == "" {
		writeWorkloadAPIError(w, http.StatusServiceUnavailable, "NO_ELIGIBLE_PROVIDER", "No eligible Provider and Pool are currently available")
		return
	}

	acceptance, created, err := h.accept(r.Context(), principal, request, normalized, requestDigest, idempotencyKey, selection)
	if err != nil {
		switch {
		case errors.Is(err, postgres.ErrIdempotencyConflict):
			writeWorkloadAPIError(w, http.StatusConflict, "IDEMPOTENCY_KEY_REUSED", "The Idempotency-Key is already bound to a different Workload request")
		case errors.Is(err, postgres.ErrInvalidRequest), errors.Is(err, operation.ErrInvalid):
			writeWorkloadAPIError(w, http.StatusUnprocessableEntity, "INVALID_WORKLOAD_REQUEST", "The Workload request failed domain validation")
		default:
			writeWorkloadAPIError(w, http.StatusInternalServerError, "WORKLOAD_ACCEPTANCE_FAILED", "The Workload acceptance transaction could not be completed")
		}
		return
	}

	_ = created // The response shape is intentionally identical for a first accept and an exact idempotent replay.
	writeWorkloadAccepted(w, acceptance)
}

func buildAcceptanceLookup(principal WorkloadPrincipal, request WorkloadCreateRequest, requestDigest, idempotencyKey string) (postgres.AcceptanceLookup, error) {
	operationDigest, err := workloadOperationIntentDigest(requestDigest)
	if err != nil {
		return postgres.AcceptanceLookup{}, err
	}
	profileID, profileRevision := "", ""
	if request.ComputeProfileRef != nil {
		profileID = request.ComputeProfileRef.ID
		profileRevision = request.ComputeProfileRef.Revision
	}
	return postgres.AcceptanceLookup{
		TenantID:                principal.TenantID,
		IdempotencyKey:          idempotencyKey,
		RequestDigest:           requestDigest,
		ProjectID:               request.ProjectID,
		OwnerService:            request.OwnerRef.Service,
		OwnerResourceType:       request.OwnerRef.ResourceType,
		OwnerResourceID:         request.OwnerRef.ResourceID,
		ComputeProfileID:        profileID,
		ComputeProfileRevision:  profileRevision,
		OperationIdempotencyKey: stableOperationIdempotencyKey(principal.TenantID, idempotencyKey),
		OperationRequestDigest:  operationDigest,
	}, nil
}

func workloadOperationIntentDigest(requestDigest string) (string, error) {
	intent, err := json.Marshal(struct {
		RequestDigest string `json:"request_digest"`
		Generation    uint64 `json:"generation"`
		Kind          string `json:"operation_kind"`
	}{
		RequestDigest: requestDigest,
		Generation:    1,
		Kind:          string(operation.Create),
	})
	if err != nil {
		return "", err
	}
	return postgres.DigestRequest(intent)
}

func writeWorkloadAccepted(w http.ResponseWriter, acceptance postgres.WorkloadAcceptance) {
	statusURL := workloadReadRoutePrefix + acceptance.Workload.WorkloadID
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Location", statusURL)
	w.Header().Set("Cache-Control", "no-store")
	w.WriteHeader(http.StatusAccepted)
	_ = json.NewEncoder(w).Encode(workloadAcceptedResponse{
		WorkloadID:         acceptance.Workload.WorkloadID,
		DesiredGeneration:  acceptance.Workload.DesiredGeneration,
		DesiredState:       acceptance.Workload.DesiredState,
		ObservedPhase:      acceptance.Workload.ObservedPhase,
		AllocationID:       acceptance.Allocation.AllocationID,
		AllocationPhase:    acceptance.Allocation.Phase,
		ComputeOperationID: acceptance.Operation.ID,
		StatusURL:          statusURL,
	})
}

func (h *WorkloadCreateHandler) accept(
	ctx context.Context,
	principal WorkloadPrincipal,
	request WorkloadCreateRequest,
	normalized []byte,
	requestDigest string,
	idempotencyKey string,
	selection WorkloadProviderSelection,
) (postgres.WorkloadAcceptance, bool, error) {
	workloadID, err := h.deps.IDs.NewID("workload")
	if err != nil {
		return postgres.WorkloadAcceptance{}, false, err
	}
	allocationID, err := h.deps.IDs.NewID("allocation")
	if err != nil {
		return postgres.WorkloadAcceptance{}, false, err
	}
	operationID, err := h.deps.IDs.NewID("operation")
	if err != nil {
		return postgres.WorkloadAcceptance{}, false, err
	}
	workloadEventID, err := h.deps.IDs.NewID("event")
	if err != nil {
		return postgres.WorkloadAcceptance{}, false, err
	}
	operationEventID, err := h.deps.IDs.NewID("event")
	if err != nil {
		return postgres.WorkloadAcceptance{}, false, err
	}
	correlationID, err := h.deps.IDs.NewID("correlation")
	if err != nil {
		return postgres.WorkloadAcceptance{}, false, err
	}

	operationKey := stableOperationIdempotencyKey(principal.TenantID, idempotencyKey)
	providerDigest, err := workloadOperationIntentDigest(requestDigest)
	if err != nil {
		return postgres.WorkloadAcceptance{}, false, err
	}

	profileID, profileRevision := "", ""
	if request.ComputeProfileRef != nil {
		profileID = request.ComputeProfileRef.ID
		profileRevision = request.ComputeProfileRef.Revision
	}
	op := operation.Operation{
		ID:             operationID,
		TenantID:       principal.TenantID,
		WorkloadID:     workloadID,
		AllocationID:   allocationID,
		ProviderID:     selection.ProviderID,
		IdempotencyKey: operationKey,
		RequestDigest:  providerDigest,
		Generation:     1,
		Version:        1,
		Kind:           operation.Create,
		Status:         operation.Pending,
	}
	params := postgres.AcceptWorkloadParams{
		Workload: postgres.CreateWorkloadParams{
			WorkloadID:             workloadID,
			TenantID:               principal.TenantID,
			ProjectID:              request.ProjectID,
			OwnerService:           request.OwnerRef.Service,
			OwnerResourceType:      request.OwnerRef.ResourceType,
			OwnerResourceID:        request.OwnerRef.ResourceID,
			ComputeProfileID:       profileID,
			ComputeProfileRevision: profileRevision,
			NormalizedRequest:      json.RawMessage(normalized),
			RequestDigest:          requestDigest,
			IdempotencyKey:         idempotencyKey,
			EventID:                workloadEventID,
			CorrelationID:          correlationID,
		},
		AllocationID:     allocationID,
		ProviderID:       selection.ProviderID,
		PoolID:           selection.PoolID,
		Operation:        op,
		OperationEventID: operationEventID,
	}
	return h.deps.Repository.AcceptWorkload(ctx, params)
}

func decodeWorkloadCreate(body []byte) (WorkloadCreateRequest, []byte, string, error) {
	var request WorkloadCreateRequest
	decoder := json.NewDecoder(bytes.NewReader(body))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&request); err != nil {
		return WorkloadCreateRequest{}, nil, "", err
	}
	var trailing any
	if err := decoder.Decode(&trailing); !errors.Is(err, io.EOF) {
		if err == nil {
			return WorkloadCreateRequest{}, nil, "", errors.New("multiple JSON values")
		}
		return WorkloadCreateRequest{}, nil, "", err
	}
	if err := validateWorkloadCreateRequest(request); err != nil {
		return WorkloadCreateRequest{}, nil, "", err
	}
	// Authorization is a transient proof over the intent, not part of the
	// desired Workload state or idempotency identity. It is neither persisted
	// nor included in the request digest, allowing a renewed Permit to replay
	// the same idempotent Workload safely.
	intent := request
	intent.Authorization = nil
	normalized, err := json.Marshal(intent)
	if err != nil {
		return WorkloadCreateRequest{}, nil, "", err
	}
	digest, err := postgres.DigestRequest(normalized)
	if err != nil {
		return WorkloadCreateRequest{}, nil, "", err
	}
	return request, normalized, digest, nil
}

func validateWorkloadCreateRequest(request WorkloadCreateRequest) error {
	if request.APIVersion != workloadCreateAPIVersion ||
		strings.TrimSpace(request.ProjectID) == "" ||
		strings.TrimSpace(request.OwnerRef.Service) == "" ||
		strings.TrimSpace(request.OwnerRef.ResourceType) == "" ||
		strings.TrimSpace(request.OwnerRef.ResourceID) == "" {
		return errors.New("missing or unsupported Workload fields")
	}
	if (request.ComputeProfileRef == nil) == (request.Resources == nil) {
		return errors.New("exactly one compute_profile_ref or resources object is required")
	}
	if request.ComputeProfileRef != nil &&
		(strings.TrimSpace(request.ComputeProfileRef.ID) == "" || strings.TrimSpace(request.ComputeProfileRef.Revision) == "") {
		return errors.New("incomplete compute profile reference")
	}
	if request.Resources != nil {
		if strings.TrimSpace(request.Resources.CPU) == "" || strings.TrimSpace(request.Resources.Memory) == "" {
			return errors.New("resources require CPU and memory")
		}
		for _, accelerator := range request.Resources.Accelerators {
			if strings.TrimSpace(accelerator.Vendor) == "" || strings.TrimSpace(accelerator.Model) == "" || accelerator.Count <= 0 {
				return errors.New("invalid accelerator requirement")
			}
		}
	}
	if request.Constraints != nil {
		if request.Constraints.MaxDurationSeconds < 0 {
			return errors.New("invalid max duration")
		}
		for k, v := range request.Constraints.PoolSelector {
			if strings.TrimSpace(k) == "" || strings.TrimSpace(v) == "" {
				return errors.New("invalid pool selector")
			}
		}
	}
	if auth := request.Authorization; auth != nil {
		if strings.TrimSpace(auth.GuardOperationID) == "" ||
			strings.TrimSpace(auth.PermitRef) == "" ||
			!validWorkloadDigest(auth.ActionHash) ||
			auth.Audience != "compute-control-panel" {
			return errors.New("invalid delegated authorization reference")
		}
	}
	return nil
}

func rejectDuplicateJSONKeys(body []byte) error {
	decoder := json.NewDecoder(bytes.NewReader(body))
	decoder.UseNumber()
	if err := consumeJSONValue(decoder); err != nil {
		return err
	}
	if _, err := decoder.Token(); !errors.Is(err, io.EOF) {
		if err == nil {
			return errors.New("trailing JSON value")
		}
		return err
	}
	return nil
}

func consumeJSONValue(decoder *json.Decoder) error {
	token, err := decoder.Token()
	if err != nil {
		return err
	}
	delim, isDelim := token.(json.Delim)
	if !isDelim {
		return nil
	}
	switch delim {
	case '{':
		seen := make(map[string]struct{})
		for decoder.More() {
			keyToken, err := decoder.Token()
			if err != nil {
				return err
			}
			key, ok := keyToken.(string)
			if !ok {
				return errors.New("object key is not a string")
			}
			if _, exists := seen[key]; exists {
				return errors.New("duplicate JSON object key")
			}
			seen[key] = struct{}{}
			if err := consumeJSONValue(decoder); err != nil {
				return err
			}
		}
		end, err := decoder.Token()
		if err != nil || end != json.Delim('}') {
			return errors.New("unterminated JSON object")
		}
	case '[':
		for decoder.More() {
			if err := consumeJSONValue(decoder); err != nil {
				return err
			}
		}
		end, err := decoder.Token()
		if err != nil || end != json.Delim(']') {
			return errors.New("unterminated JSON array")
		}
	default:
		return errors.New("unexpected JSON delimiter")
	}
	return nil
}

func validWorkloadPrincipal(principal WorkloadPrincipal) bool {
	return strings.TrimSpace(principal.TenantID) != "" &&
		strings.TrimSpace(principal.Subject) != "" &&
		strings.TrimSpace(principal.Service) != ""
}

func projectAllowed(principal WorkloadPrincipal, projectID string) bool {
	for _, allowed := range principal.ProjectIDs {
		if allowed == projectID {
			return true
		}
	}
	return false
}

func ownerMatchesPrincipal(principal WorkloadPrincipal, owner WorkloadOwnerRef) bool {
	if principal.Service != owner.Service {
		return false
	}
	switch owner.Service {
	case "agent-runtime":
		return owner.ResourceType == "run"
	case "sandbox-provider":
		return owner.ResourceType == "sandbox-claim"
	default:
		return false
	}
}

func readIdempotencyKey(r *http.Request) (string, error) {
	values := r.Header.Values("Idempotency-Key")
	if len(values) != 1 {
		return "", errors.New("exactly one Idempotency-Key header is required")
	}
	value := strings.TrimSpace(values[0])
	if value == "" || len(value) > 256 || strings.ContainsAny(value, "\r\n") {
		return "", errors.New("invalid Idempotency-Key")
	}
	return value, nil
}

func stableOperationIdempotencyKey(tenantID, requestKey string) string {
	sum := sha256.Sum256([]byte(tenantID + "\x00" + requestKey + "\x00CREATE"))
	return "workload-create:" + hex.EncodeToString(sum[:])
}

func validWorkloadDigest(value string) bool {
	if !strings.HasPrefix(value, "sha256:") || len(value) != len("sha256:")+64 {
		return false
	}
	digest := strings.TrimPrefix(value, "sha256:")
	if strings.ToLower(digest) != digest {
		return false
	}
	_, err := hex.DecodeString(digest)
	return err == nil
}

func writeWorkloadAPIError(w http.ResponseWriter, status int, code, message string) {
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Cache-Control", "no-store")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(workloadAPIError{Code: code, Message: message})
}

// Ensure compile-time interface assignments remain visible and intentional.
var _ WorkloadAcceptanceRepository = (*postgres.Repository)(nil)
