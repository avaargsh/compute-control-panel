package provider

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/avaargsh/compute-control-panel/internal/operation"
)

type pdJobSetAPI struct {
	mu          sync.Mutex
	obj         map[string]any
	posts       int
	loseReply   bool
	replaceUID  bool
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
		if s.replaceUID {
			old := obj["metadata"].(map[string]any)
			fresh := map[string]any{}
			for k, v := range old {
				fresh[k] = v
			}
			fresh["uid"] = "successor-uid"
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
		PrefillReplicas:           2,
		DecodeReplicas:            3,
		PrefillGPUsPerReplica:     4,
		DecodeGPUsPerReplica:      2,
		ResourceClaimTemplateName: "gpu-claim-template",
	}
}

func pdJobSetTestRequest(plan PDJobSetPlan) Request {
	return Request{
		OperationID: "pd-operation-" + plan.IntentID,
		TenantID: "tenant-a", WorkloadID: "w-a", ProviderID: "pd-provider",
		DesiredGeneration: 2,
		IdempotencyKey: "pd-create-" + plan.IntentID,
		RequestDigest: "sha256:" + plan.IntentID,
		Kind: operation.Create,
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
