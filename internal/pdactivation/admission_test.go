package pdactivation

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

func admissionFixture(t *testing.T) (admissionReview, Trust, AuthorityLoader, time.Time) {
	t.Helper()
	pair, _, authority, keys, now := permitFixture(t)
	review := admissionReview{
		APIVersion: "admission.k8s.io/v1",
		Kind: "AdmissionReview",
		Request: &admissionRequest{
			UID: "test-admission-uid-001",
			Operation: "UPDATE",
			Kind: admissionGroupVersionKind{
				Group: "jobset.x-k8s.io", Version: "v1alpha2", Kind: "JobSet",
			},
			Resource: admissionGroupVersionResource{
				Group: "jobset.x-k8s.io", Version: "v1alpha2", Resource: "jobsets",
			},
			Name: "pd-gated", Namespace: "ccp-pd-paired-smoke",
			UserInfo: admissionUserInfo{
				Username: "system:serviceaccount:ccp-pd-paired-smoke:ccp-pd-activator",
			},
			OldObject: pair.Old, Object: pair.New,
		},
	}
	return review, trustFixture(keys), func(context.Context, Claims) (Authority, error) {
		return authority, nil
	}, now
}

func callAdmission(t *testing.T, handler http.Handler, review admissionReview) (int, admissionReview) {
	t.Helper()
	raw, err := json.Marshal(review)
	if err != nil {
		t.Fatal(err)
	}
	req := httptest.NewRequest(http.MethodPost, "/validate", bytes.NewReader(raw))
	req.Header.Set("Content-Type", "application/json")
	res := httptest.NewRecorder()
	handler.ServeHTTP(res, req)
	var body admissionReview
	if res.Code == http.StatusOK {
		if err := json.Unmarshal(res.Body.Bytes(), &body); err != nil {
			t.Fatalf("invalid v1 AdmissionReview: %v body=%s", err, res.Body.String())
		}
	}
	return res.Code, body
}

func TestAdmissionReviewAllowsOnlySignedScopedActivator(t *testing.T) {
	req, trust, load, now := admissionFixture(t)
	handler := &admissionHandler{trust: trust, load: load, now: func() time.Time { return now }}
	status, got := callAdmission(t, handler, req)
	if status != http.StatusOK || got.Response == nil || !got.Response.Allowed ||
		got.APIVersion != "admission.k8s.io/v1" || got.Kind != "AdmissionReview" ||
		got.Response.UID != req.Request.UID || len(got.Response.Patch) > 0 ||
		got.Response.PatchType != "" {
		t.Fatalf("independent signed admission refused or returned mutating response: http=%d %+v", status, got)
	}
}

func TestAdmissionReviewRejectsImpersonationScopeAndUnauthorizedMutation(t *testing.T) {
	original, trust, load, now := admissionFixture(t)
	for _, tc := range []struct {
		name string
		edit func(*admissionReview)
	}{
		{"staging-identity", func(r *admissionReview) {
			r.Request.UserInfo.Username = "system:serviceaccount:ccp-pd-paired-smoke:ccp-pd-staging"
		}},
		{"unrelated-account", func(r *admissionReview) {
			r.Request.UserInfo.Username = "system:serviceaccount:ccp-pd-paired-smoke:other"
		}},
		{"user-account", func(r *admissionReview) {
			r.Request.UserInfo.Username = "tenant-user"
		}},
		{"wrong-namespace", func(r *admissionReview) { r.Request.Namespace = "other" }},
		{"wrong-name", func(r *admissionReview) { r.Request.Name = "other" }},
		{"create-instead-of-patch", func(r *admissionReview) { r.Request.Operation = "CREATE" }},
		{"delete-instead-of-patch", func(r *admissionReview) { r.Request.Operation = "DELETE" }},
		{"status-subresource", func(r *admissionReview) { r.Request.SubResource = "status" }},
		{"wrong-gvk", func(r *admissionReview) { r.Request.Kind.Kind = "Pod" }},
		{"wrong-group", func(r *admissionReview) { r.Request.Resource.Group = "batch" }},
		{"wrong-api-version", func(r *admissionReview) { r.APIVersion = "admission.k8s.io/v1beta1" }},
		{"wrong-review-kind", func(r *admissionReview) { r.Kind = "Status" }},
		{"empty-admission-uid", func(r *admissionReview) { r.Request.UID = "" }},
		{"dry-run", func(r *admissionReview) { yes := true; r.Request.DryRun = &yes }},
		{"spec-mutation", func(r *admissionReview) {
			var obj map[string]any
			if err := json.Unmarshal(r.Request.Object, &obj); err != nil { t.Fatal(err) }
			obj["spec"].(map[string]any)["suspend"] = false
			r.Request.Object, _ = json.Marshal(obj)
		}},
		{"missing-permit", func(r *admissionReview) {
			var obj map[string]any
			if err := json.Unmarshal(r.Request.Object, &obj); err != nil { t.Fatal(err) }
			delete(obj["metadata"].(map[string]any)["annotations"].(map[string]any), PermitAnnotation)
			r.Request.Object, _ = json.Marshal(obj)
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			raw, err := json.Marshal(original)
			if err != nil { t.Fatal(err) }
			var req admissionReview
			if err := json.Unmarshal(raw, &req); err != nil { t.Fatal(err) }
			tc.edit(&req)
			h := &admissionHandler{trust: trust, load: load, now: func() time.Time { return now }}
			code, res := callAdmission(t, h, req)
			if code != http.StatusOK || res.Response == nil || res.Response.Allowed ||
				res.Response.Status == nil || res.Response.Status.Code != http.StatusForbidden ||
				res.Response.UID != req.Request.UID {
				t.Fatalf("improper activation allowed/invalid v1 denial: code=%d %+v", code, res)
			}
		})
	}
}

func TestAdmissionReviewDeniesMissingOrRevokedIndependentAuthority(t *testing.T) {
	req, trust, _, now := admissionFixture(t)
	for _, tc := range []struct {
		name string
		loader AuthorityLoader
	}{
		{"absent", nil},
		{"error", func(context.Context, Claims) (Authority, error) {
			return Authority{}, errors.New("authoritative approval store offline")
		}},
		{"revoked", func(context.Context, Claims) (Authority, error) {
			return Authority{Approved: true, Revoked: true}, nil
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			h := &admissionHandler{trust: trust, load: tc.loader, now: func() time.Time { return now }}
			code, res := callAdmission(t, h, req)
			if code != http.StatusOK || res.Response == nil || res.Response.Allowed ||
				res.Response.Status == nil || res.Response.Status.Code != http.StatusForbidden ||
				strings.Contains(strings.ToLower(res.Response.Status.Message), "offline") {
				t.Fatalf("missing/revoked source leaked or was allowed: code=%d %+v", code, res)
			}
		})
	}
}

func TestAdmissionReviewRejectsMalformedAndOversizedRequests(t *testing.T) {
	_, trust, load, now := admissionFixture(t)
	handler := &admissionHandler{trust: trust, load: load, now: func() time.Time { return now }}
	for _, tc := range []struct {
		name, method, contentType, body string
		expected int
	}{
		{"wrong-method", "GET", "application/json", "{}", http.StatusMethodNotAllowed},
		{"wrong-media-type", "POST", "text/plain", "{}", http.StatusUnsupportedMediaType},
		{"bad-json", "POST", "application/json", "{", http.StatusBadRequest},
		{"trailing-json", "POST", "application/json", "{}{}", http.StatusBadRequest},
		{"oversized", "POST", "application/json", strings.Repeat("a", 2<<20), http.StatusRequestEntityTooLarge},
		{"missing-request", "POST", "application/json", `{"apiVersion":"admission.k8s.io/v1","kind":"AdmissionReview"}`, http.StatusBadRequest},
	} {
		t.Run(tc.name, func(t *testing.T) {
			r := httptest.NewRequest(tc.method, "/validate", strings.NewReader(tc.body))
			r.Header.Set("Content-Type", tc.contentType)
			w := httptest.NewRecorder()
			handler.ServeHTTP(w, r)
			if w.Code != tc.expected {
				t.Fatalf("malformed AdmissionReview allowed: want %d got %d", tc.expected, w.Code)
			}
		})
	}
}
