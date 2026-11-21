package pdactivation

import (
	"context"
	"encoding/json"
	"io"
	"mime"
	"net/http"
	"strings"
	"time"
)

const maxAdmissionBytes = 1 << 20

type admissionGroupVersionKind struct {
	Group string `json:"group"`
	Version string `json:"version"`
	Kind string `json:"kind"`
}
type admissionGroupVersionResource struct {
	Group string `json:"group"`
	Version string `json:"version"`
	Resource string `json:"resource"`
}
type admissionUserInfo struct {
	Username string `json:"username"`
}
type admissionRequest struct {
	UID string `json:"uid"`
	Kind admissionGroupVersionKind `json:"kind"`
	Resource admissionGroupVersionResource `json:"resource"`
	SubResource string `json:"subResource,omitempty"`
	Namespace string `json:"namespace"`
	Name string `json:"name"`
	Operation string `json:"operation"`
	UserInfo admissionUserInfo `json:"userInfo"`
	DryRun *bool `json:"dryRun,omitempty"`
	OldObject json.RawMessage `json:"oldObject"`
	Object json.RawMessage `json:"object"`
}
type admissionStatus struct {
	Code int `json:"code"`
	Reason string `json:"reason"`
	Message string `json:"message"`
}
type admissionResponse struct {
	UID string `json:"uid"`
	Allowed bool `json:"allowed"`
	Status *admissionStatus `json:"status,omitempty"`
	// Validating webhooks NEVER return patches; these fields exist only to
	// make an accidental mutation detectable in the integration tests.
	Patch []byte `json:"patch,omitempty"`
	PatchType string `json:"patchType,omitempty"`
}
type admissionReview struct {
	APIVersion string `json:"apiVersion"`
	Kind string `json:"kind"`
	Request *admissionRequest `json:"request,omitempty"`
	Response *admissionResponse `json:"response,omitempty"`
}

type admissionHandler struct {
	trust Trust
	load AuthorityLoader
	now func() time.Time
}

// NewAdmissionHandler is the HTTP protocol adapter for a future cluster
// ValidatingAdmissionWebhook. The TLS listener, independent approval issuer,
// independently authenticated current AuthorityLoader, admission-registration
// failurePolicy=Fail and trust-root rotation MUST be deployed separately.
// A nil/failed loader is deny-only. This does not activate JobSets.
func NewAdmissionHandler(trust Trust, load AuthorityLoader) http.Handler {
	return &admissionHandler{trust: trust, load: load, now: time.Now}
}

func (h *admissionHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	kind, _, err := mime.ParseMediaType(r.Header.Get("Content-Type"))
	if err != nil || kind != "application/json" {
		http.Error(w, "unsupported media type", http.StatusUnsupportedMediaType)
		return
	}
	raw, err := io.ReadAll(io.LimitReader(r.Body, maxAdmissionBytes+1))
	if err != nil {
		http.Error(w, "invalid request", http.StatusBadRequest)
		return
	}
	if len(raw) > maxAdmissionBytes {
		http.Error(w, "request too large", http.StatusRequestEntityTooLarge)
		return
	}
	decoder := json.NewDecoder(strings.NewReader(string(raw)))
	var review admissionReview
	if err := decoder.Decode(&review); err != nil {
		http.Error(w, "invalid AdmissionReview", http.StatusBadRequest)
		return
	}
	var trailing any
	if err := decoder.Decode(&trailing); err != io.EOF || review.Request == nil {
		http.Error(w, "invalid AdmissionReview", http.StatusBadRequest)
		return
	}
	req := review.Request
	response := &admissionResponse{
		UID: req.UID,
		Allowed: false,
		Status: &admissionStatus{Code: http.StatusForbidden, Reason: "Forbidden", Message: "PD scoped activation denied"},
	}
	// Only this one authenticated ServiceAccount may attempt this exact
	// JobSet gate transition. Server-side validation MUST still run for
	// all of its UPDATEs; other controllers are handled by admission
	// matching rules and never implicitly granted authority by this handler.
	valid := review.APIVersion == "admission.k8s.io/v1" && review.Kind == "AdmissionReview" &&
		review.Response == nil && req.UID != "" && len(req.UID) <= 256 &&
		req.Operation == "UPDATE" && req.SubResource == "" &&
		req.DryRun != nil && !*req.DryRun &&
		req.Resource.Group == "jobset.x-k8s.io" && req.Resource.Version == "v1alpha2" &&
		req.Resource.Resource == "jobsets" &&
		req.Kind.Group == "jobset.x-k8s.io" && req.Kind.Version == "v1alpha2" &&
		req.Kind.Kind == "JobSet" &&
		validDNSName(req.Namespace) && validDNSName(req.Name) &&
		req.UserInfo.Username == "system:serviceaccount:"+req.Namespace+":ccp-pd-activator"
	if valid && h != nil && h.now != nil && h.load != nil {
		// Time-bounded independent authority lookup; errors, canceled
		// contexts, stale receipts and malformed object diffs deny.
		ctx, cancel := context.WithTimeout(r.Context(), 3*time.Second)
		verified, verifyErr := VerifyGateRelease(ctx,
			OldNew{Old: req.OldObject, New: req.Object}, h.trust, h.load, h.now())
		valid = verifyErr == nil && ctx.Err() == nil &&
			verified.Namespace == req.Namespace && verified.JobSetName == req.Name
		cancel()
	}
	if valid && h != nil && h.now != nil && h.load != nil {
		response.Allowed = true
		response.Status = nil
	}
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(admissionReview{
		APIVersion: "admission.k8s.io/v1", Kind: "AdmissionReview",
		Response: response,
	})
}

var _ context.Context
