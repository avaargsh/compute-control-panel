package provider

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/avaargsh/compute-control-panel/internal/operation"
)

type pdJobSetAPI struct {
	mu         sync.Mutex
	obj        map[string]any
	posts      int
	loseReply  bool
	replaceUID bool
	deleting   bool
}

func (s *pdJobSetAPI) serve(w http.ResponseWriter, r *http.Request) {
	s.mu.Lock()
	defer s.mu.Unlock()
	switch r.Method {
	case http.MethodGet:
		if !strings.HasPrefix(r.URL.Path, "/apis/jobset.x-k8s.io/v1alpha2/namespaces/tenant-a/jobsets/") {
			http.NotFound(w, r)
			return
		}
		if s.obj == nil {
			http.NotFound(w, r)
			return
		}
		obj := map[string]any{}
		for k, v := range s.obj {
			obj[k] = v
		}
		if s.replaceUID || s.deleting {
			old := obj["metadata"].(map[string]any)
			fresh := map[string]any{}
			for k, v := range old {
				fresh[k] = v
			}
			if s.replaceUID {
				fresh["uid"] = "successor-uid"
			}
			if s.deleting {
				fresh["deletionTimestamp"] = "2026-11-18T07:00:00Z"
			}
			obj["metadata"] = fresh
		}
		_ = json.NewEncoder(w).Encode(obj)
	case http.MethodPost:
		if r.URL.Path != "/apis/jobset.x-k8s.io/v1alpha2/namespaces/tenant-a/jobsets" {
			http.NotFound(w, r)
			return
		}
		s.posts++
		if s.obj != nil {
			w.WriteHeader(http.StatusConflict)
			return
		}
		var obj map[string]any
		if json.NewDecoder(r.Body).Decode(&obj) != nil {
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		meta := obj["metadata"].(map[string]any)
		meta["uid"] = "real-uid"
		meta["resourceVersion"] = "57"
		meta["generation"] = float64(1)
		s.obj = obj
		if s.loseReply {
			w.WriteHeader(http.StatusBadGateway)
			return
		}
		w.WriteHeader(http.StatusCreated)
		_ = json.NewEncoder(w).Encode(obj)
	default:
		w.WriteHeader(http.StatusMethodNotAllowed)
	}
}

func (s *pdJobSetAPI) count() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.posts
}

func pdJobSetTestConfig() PDJobSetPlan {
	return PDJobSetPlan{
		IntentID:                  strings.Repeat("a", 64),
		TenantID:                  "tenant-a",
		WorkloadID:                "w-a",
		ProviderID:                "pd-provider",
		Generation:                2,
		PrefillReplicas:           2,
		DecodeReplicas:            3,
		PrefillGPUsPerReplica:     1,
		DecodeGPUsPerReplica:      1,
		ResourceClaimTemplateName: "gpu-claim-template",
	}
}

func pdJobSetTestRequest(plan PDJobSetPlan) Request {
	return Request{
		OperationID: "pd-operation-" + plan.IntentID,
		TenantID:    "tenant-a", WorkloadID: "w-a", ProviderID: "pd-provider",
		DesiredGeneration: 2,
		IdempotencyKey:    "pd-create-" + plan.IntentID,
		RequestDigest:     "sha256:" + plan.IntentID,
		Kind:              operation.Create,
	}
}

func pdJobSetTestAdapter(t *testing.T, api *pdJobSetAPI, bindings KueueJobBindingStore, plan PDJobSetPlan) (*PDJobSetAdapter, func()) {
	t.Helper()
	s := httptest.NewTLSServer(http.HandlerFunc(api.serve))
	client := s.Client()
	client.Timeout = 3 * time.Second
	a, err := NewPDJobSetAdapterWithBindings(s.URL, "tenant-a", "local-a", client, bindings, plan)
	if err != nil {
		s.Close()
		t.Fatal(err)
	}
	return a, s.Close
}

func TestPDJobSetCreatePinnedReplayAndRestart(t *testing.T) {
	plan := pdJobSetTestConfig()
	req := pdJobSetTestRequest(plan)
	api := &pdJobSetAPI{}
	bindings := NewMemoryKueueJobBindingStore()
	a, closeServer := pdJobSetTestAdapter(t, api, bindings, plan)
	defer closeServer()
	ctx := context.Background()
	for i := 0; i < 3; i++ {
		if _, err := a.Apply(ctx, req); err != nil {
			t.Fatalf("idempotent apply #%d: %v", i, err)
		}
	}
	if api.count() != 1 {
		t.Fatalf("retries created %d JobSets", api.count())
	}
	api.mu.Lock()
	spec := api.obj["spec"].(map[string]any)
	if spec["suspend"] != true {
		t.Fatal("writer must initially create inert suspended JobSet")
	}
	// Kueue admission can otherwise unsuspend even a suspended JobSet.
	// The static gate is defense in depth; production still needs proof that
	// AdmissionGatedBy is enabled for this specific JobSet controller version.
	annotations := api.obj["metadata"].(map[string]any)["annotations"].(map[string]any)
	if annotations["kueue.x-k8s.io/admission-gated-by"] != "compute-control-panel.io/pd-activation" {
		t.Fatal("PD staging lacks the explicit Kueue admission gate")
	}
	phases := spec["replicatedJobs"].([]any)
	if len(phases) != 2 || phases[0].(map[string]any)["replicas"] != float64(2) ||
		phases[1].(map[string]any)["replicas"] != float64(3) {
		t.Fatalf("phase shape lost: %+v", phases)
	}
	api.mu.Unlock()
	observed, err := a.Inspect(ctx, req)
	if err != nil || observed.Result != operation.ResourcePresent ||
		observed.ResourceUID != "real-uid" || observed.ResourceGeneration != 2 {
		t.Fatalf("pinned inspection: %+v %v", observed, err)
	}
	if _, err := a.Apply(ctx, req); err != nil || api.count() != 1 {
		t.Fatalf("restart-compatible replay: %v, posts %d", err, api.count())
	}
	api.mu.Lock()
	api.replaceUID = true
	api.mu.Unlock()
	if _, err := a.Apply(ctx, req); !errors.Is(err, ErrOperationConflict) {
		t.Fatalf("successor UID incorrectly adopted: %v", err)
	}
	observed, err = a.Inspect(ctx, req)
	if err != nil || observed.Result != operation.ObservationConflict {
		t.Fatalf("successor UID incorrectly reconciled: %+v %v", observed, err)
	}
	api.mu.Lock()
	api.replaceUID, api.deleting = false, true
	api.mu.Unlock()
	if _, err := a.Apply(ctx, req); !errors.Is(err, ErrOperationConflict) {
		t.Fatalf("terminating JobSet was accepted as live replay: %v", err)
	}
	observed, err = a.Inspect(ctx, req)
	if err != nil || observed.Result != operation.ObservationConflict {
		t.Fatalf("terminating JobSet incorrectly reported present: %+v %v", observed, err)
	}
}

func TestPDJobSetLostAckRetainsUnknownAndNoAutomaticAdoption(t *testing.T) {
	plan := pdJobSetTestConfig()
	req := pdJobSetTestRequest(plan)
	api := &pdJobSetAPI{loseReply: true}
	bindings := NewMemoryKueueJobBindingStore()
	a, closeServer := pdJobSetTestAdapter(t, api, bindings, plan)
	defer closeServer()
	ctx := context.Background()
	if _, err := a.Apply(ctx, req); !errors.Is(err, ErrAcknowledgementLost) {
		t.Fatalf("lost response misreported: %v", err)
	}
	if _, err := a.Apply(ctx, req); !errors.Is(err, ErrAcknowledgementLost) || api.count() != 1 {
		t.Fatalf("unbound live UID adopted or duplicate POST: %v", err)
	}
	obs, err := a.Inspect(ctx, req)
	if err != nil || obs.Result != operation.OperationInProgress ||
		obs.ResourceUID != "" {
		t.Fatalf("unbound JobSet fabricated success: %+v %v", obs, err)
	}
}

func TestPDJobSetRejectsCrossIntentAndMalformedScopeBeforePOST(t *testing.T) {
	plan := pdJobSetTestConfig()
	req := pdJobSetTestRequest(plan)
	api := &pdJobSetAPI{}
	a, closeServer := pdJobSetTestAdapter(t, api, NewMemoryKueueJobBindingStore(), plan)
	defer closeServer()
	tests := []Request{
		func() Request { v := req; v.RequestDigest = "sha256:" + strings.Repeat("b", 64); return v }(),
		func() Request { v := req; v.OperationID += "-other"; return v }(),
		func() Request { v := req; v.IdempotencyKey += "-other"; return v }(),
		func() Request { v := req; v.Kind = operation.Update; return v }(),
		func() Request { v := req; v.TenantID = "other-tenant"; return v }(),
		func() Request { v := req; v.WorkloadID = "other-workload"; return v }(),
		func() Request { v := req; v.ProviderID = "other-provider"; return v }(),
		func() Request { v := req; v.DesiredGeneration++; return v }(),
	}
	for _, input := range tests {
		if _, err := a.Apply(context.Background(), input); err == nil {
			t.Fatalf("accepted different identity %+v", input)
		}
	}
	if api.count() != 0 {
		t.Fatalf("unsafe input reached POST: %d", api.count())
	}
}

func TestPDJobSetRejectsUnprovenMultiGPUClaimTemplate(t *testing.T) {
	plan := pdJobSetTestConfig()
	plan.PrefillGPUsPerReplica = 2
	if validPDJobSetPlan(plan) {
		t.Fatal("accepted unverified multi-GPU ClaimTemplate cardinality")
	}
	plan = pdJobSetTestConfig()
	plan.DecodeGPUsPerReplica = 2
	if validPDJobSetPlan(plan) {
		t.Fatal("accepted unverified Decode multi-GPU ClaimTemplate cardinality")
	}
}

func TestPDJobSetPinnedControllerDefaultsAreStrictlyNonExecutable(t *testing.T) {
	plan := pdJobSetTestConfig()
	req := pdJobSetTestRequest(plan)
	a, closeServer := pdJobSetTestAdapter(t, &pdJobSetAPI{}, NewMemoryKueueJobBindingStore(), plan)
	defer closeServer()
	want := a.expected(req)
	payload, err := json.Marshal(want)
	if err != nil {
		t.Fatal(err)
	}
	var server map[string]any
	if err := json.Unmarshal(payload, &server); err != nil {
		t.Fatal(err)
	}
	meta := server["metadata"].(map[string]any)
	meta["uid"], meta["resourceVersion"] = "pinned-uid", "57"
	spec := server["spec"].(map[string]any)
	spec["network"] = map[string]any{"enableDNSHostnames": true, "publishNotReadyAddresses": true}
	spec["startupPolicy"] = map[string]any{"startupPolicyOrder": "AnyOrder"}
	spec["successPolicy"] = map[string]any{"operator": "All"}
	for _, phase := range spec["replicatedJobs"].([]any) {
		job := phase.(map[string]any)
		job["groupName"] = "default"
		tmpl := job["template"].(map[string]any)
		tmpl["metadata"] = map[string]any{}
		jobSpec := tmpl["spec"].(map[string]any)
		jobSpec["completionMode"] = "Indexed"
		pod := jobSpec["template"].(map[string]any)
		pod["metadata"] = map[string]any{}
	}
	serverDefaulted, err := json.Marshal(server)
	if err != nil {
		t.Fatal(err)
	}
	parsed, err := parsePDJobSet(serverDefaulted)
	if err != nil || !reflect.DeepEqual(parsed.Spec, want.Spec) || !a.exact(parsed, req) {
		t.Fatalf("exact JobSet v0.13 safe defaults rejected: %+v %v", parsed.Spec, err)
	}
	// This list is deliberately limited to actual observed defaults, not an
	// open-ended JSON unknown-field bypass.
	for _, tc := range []struct {
		name   string
		mutate func(map[string]any)
	}{
		{"network-disabled", func(v map[string]any) {
			v["spec"].(map[string]any)["network"].(map[string]any)["enableDNSHostnames"] = false
		}},
		{"extra-network-field", func(v map[string]any) {
			v["spec"].(map[string]any)["network"].(map[string]any)["hostNetwork"] = true
		}},
		{"unsanctioned-startup-order", func(v map[string]any) {
			v["spec"].(map[string]any)["startupPolicy"].(map[string]any)["startupPolicyOrder"] = "InOrder"
		}},
		{"unsanctioned-success-policy", func(v map[string]any) {
			v["spec"].(map[string]any)["successPolicy"].(map[string]any)["operator"] = "Any"
		}},
		{"custom-job-group", func(v map[string]any) {
			v["spec"].(map[string]any)["replicatedJobs"].([]any)[0].(map[string]any)["groupName"] = "other"
		}},
		{"executable-job-template-metadata", func(v map[string]any) {
			j := v["spec"].(map[string]any)["replicatedJobs"].([]any)[0].(map[string]any)
			j["template"].(map[string]any)["metadata"] = map[string]any{"annotations": map[string]any{"evil": "true"}}
		}},
		{"nonindexed-job-completion", func(v map[string]any) {
			j := v["spec"].(map[string]any)["replicatedJobs"].([]any)[0].(map[string]any)
			j["template"].(map[string]any)["spec"].(map[string]any)["completionMode"] = "NonIndexed"
		}},
		{"pod-template-label-injection", func(v map[string]any) {
			j := v["spec"].(map[string]any)["replicatedJobs"].([]any)[0].(map[string]any)
			j["template"].(map[string]any)["spec"].(map[string]any)["template"].(map[string]any)["metadata"] =
				map[string]any{"labels": map[string]any{"privileged": "true"}}
		}},
		{"hostnetwork-pod", func(v map[string]any) {
			j := v["spec"].(map[string]any)["replicatedJobs"].([]any)[0].(map[string]any)
			j["template"].(map[string]any)["spec"].(map[string]any)["template"].(map[string]any)["spec"].(map[string]any)["hostNetwork"] = true
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var cloned map[string]any
			if err := json.Unmarshal(serverDefaulted, &cloned); err != nil {
				t.Fatal(err)
			}
			tc.mutate(cloned)
			raw, err := json.Marshal(cloned)
			if err != nil {
				t.Fatal(err)
			}
			if got, err := parsePDJobSet(raw); err == nil {
				t.Fatalf("accepted unsafe webhook spec drift %+v", got.Spec)
			}
		})
	}
}
