package provider

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/avaargsh/compute-control-panel/internal/operation"
)

func kueueAdapterRequest() Request {
	return Request{
		OperationID: "op-create-1", TenantID: "tenant-a", WorkloadID: "workload-a",
		ProviderID: "kueue", IdempotencyKey: "idem-create-1",
		RequestDigest:     "sha256:" + strings.Repeat("ab", 32),
		DesiredGeneration: 7, Kind: operation.Create,
	}
}

type kueueAPICall struct {
	method string
	path   string
	body   []byte
	auth   string
}

type kueueAPI struct {
	t              *testing.T
	mu             sync.Mutex
	jobs           map[string]kubeObject
	calls          []kueueAPICall
	loseCreateBody bool
	createStatus   int
	loseDeleteBody bool
	deleteStatus   int
	replaceUID     string
	replaceGen     string
	placed         bool
	token          string
}

func (a *kueueAPI) snapshot() []kueueAPICall {
	a.mu.Lock()
	defer a.mu.Unlock()
	out := make([]kueueAPICall, len(a.calls))
	copy(out, a.calls)
	return out
}

func (a *kueueAPI) postCount() int {
	return a.methodCount(http.MethodPost)
}

func (a *kueueAPI) deleteCount() int {
	return a.methodCount(http.MethodDelete)
}

func (a *kueueAPI) methodCount(method string) int {
	n := 0
	for _, call := range a.snapshot() {
		if call.method == method {
			n++
		}
	}
	return n
}

func (a *kueueAPI) storedJobs() int {
	a.mu.Lock()
	defer a.mu.Unlock()
	return len(a.jobs)
}

func (a *kueueAPI) setPlaced(placed bool) {
	a.mu.Lock()
	a.placed = placed
	a.mu.Unlock()
}

func (a *kueueAPI) setReplacement(uid, generation string) {
	a.mu.Lock()
	a.replaceUID = uid
	a.replaceGen = generation
	a.mu.Unlock()
}

func (a *kueueAPI) setDeleteStatus(status int) {
	a.mu.Lock()
	a.deleteStatus = status
	a.mu.Unlock()
}

func (a *kueueAPI) plantJob(name, uid string) {
	a.mu.Lock()
	defer a.mu.Unlock()
	obj := kubeObject{}
	obj.Metadata.Name = name
	obj.Metadata.Namespace = "tenant-a"
	obj.Metadata.UID = uid
	obj.Metadata.ResourceVersion = "99"
	suspend := true
	parallelism := 1
	completions := 1
	obj.Spec.Suspend = &suspend
	obj.Spec.Parallelism = &parallelism
	obj.Spec.Completions = &completions
	obj.Spec.Template.Spec.RestartPolicy = "Never"
	obj.Spec.Template.Spec.Containers = []kueueBatchContainer{{
		Name:  "ccp-suspended",
		Image: kueueSuspendedJobImage,
	}}
	a.jobs[name] = obj
}

func (a *kueueAPI) view(obj kubeObject) kubeObject {
	ann := make(map[string]string, len(obj.Metadata.Annotations))
	for key, value := range obj.Metadata.Annotations {
		ann[key] = value
	}
	obj.Metadata.Annotations = ann
	if a.replaceUID != "" {
		obj.Metadata.UID = a.replaceUID
	}
	if a.replaceGen != "" {
		obj.Metadata.Annotations[kueueAnnotationDesiredGeneration] = a.replaceGen
	}
	return obj
}

func (a *kueueAPI) serve(w http.ResponseWriter, r *http.Request) {
	body, err := io.ReadAll(io.LimitReader(r.Body, 1<<20))
	if err != nil {
		a.t.Errorf("read request: %v", err)
		http.Error(w, "bad request", http.StatusBadRequest)
		return
	}
	a.mu.Lock()
	a.calls = append(a.calls, kueueAPICall{method: r.Method, path: r.URL.Path, body: body, auth: r.Header.Get("Authorization")})
	token := a.token
	a.mu.Unlock()
	if r.Method != http.MethodGet && r.Method != http.MethodPost && r.Method != http.MethodDelete {
		a.t.Errorf("unexpected mutating method %s %s", r.Method, r.URL.Path)
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	if token != "" && r.Header.Get("Authorization") != "Bearer "+token {
		http.Error(w, "forbidden", http.StatusForbidden)
		return
	}
	switch {
	case r.Method == http.MethodPost && r.URL.Path == "/apis/batch/v1/namespaces/tenant-a/jobs":
		a.create(w, body)
	case r.Method == http.MethodGet && strings.HasPrefix(r.URL.Path, "/apis/batch/v1/namespaces/tenant-a/jobs/"):
		a.get(w, strings.TrimPrefix(r.URL.Path, "/apis/batch/v1/namespaces/tenant-a/jobs/"))
	case r.Method == http.MethodDelete && strings.HasPrefix(r.URL.Path, "/apis/batch/v1/namespaces/tenant-a/jobs/"):
		a.remove(w, strings.TrimPrefix(r.URL.Path, "/apis/batch/v1/namespaces/tenant-a/jobs/"), body)
	case r.Method == http.MethodGet && r.URL.Path == "/apis/kueue.x-k8s.io/v1beta1/namespaces/tenant-a/workloads":
		a.workloads(w)
	case r.Method == http.MethodGet && r.URL.Path == "/api/v1/namespaces/tenant-a/pods":
		a.pods(w, r.URL.Query().Get("labelSelector"))
	default:
		a.t.Errorf("unexpected Kubernetes call %s %s", r.Method, r.URL.RequestURI())
		http.NotFound(w, r)
	}
}

func (a *kueueAPI) create(w http.ResponseWriter, body []byte) {
	if a.createStatus != 0 {
		http.Error(w, "create rejected", a.createStatus)
		return
	}
	var posted kueueBatchJob
	if json.Unmarshal(body, &posted) != nil || posted.Metadata.Name == "" {
		http.Error(w, "bad job", http.StatusBadRequest)
		return
	}
	a.mu.Lock()
	defer a.mu.Unlock()
	if _, exists := a.jobs[posted.Metadata.Name]; exists {
		http.Error(w, "exists", http.StatusConflict)
		return
	}
	obj := kubeObject{}
	obj.Metadata.Name = posted.Metadata.Name
	obj.Metadata.Namespace = posted.Metadata.Namespace
	obj.Metadata.UID = "job-uid-created"
	obj.Metadata.ResourceVersion = "11"
	obj.Metadata.Annotations = posted.Metadata.Annotations
	obj.Metadata.Labels = posted.Metadata.Labels
	suspend := posted.Spec.Suspend
	parallelism := posted.Spec.Parallelism
	completions := posted.Spec.Completions
	obj.Spec.Suspend = &suspend
	obj.Spec.Parallelism = &parallelism
	obj.Spec.Completions = &completions
	obj.Spec.Template = posted.Spec.Template
	a.jobs[posted.Metadata.Name] = obj
	if a.loseCreateBody {
		w.WriteHeader(http.StatusCreated)
		_, _ = w.Write([]byte("{"))
		return
	}
	raw, err := json.Marshal(obj)
	if err != nil {
		a.t.Errorf("marshal created job: %v", err)
		http.Error(w, "marshal", http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusCreated)
	_, _ = w.Write(raw)
}

func (a *kueueAPI) remove(w http.ResponseWriter, name string, body []byte) {
	var opts struct {
		Preconditions struct {
			UID string `json:"uid"`
		} `json:"preconditions"`
	}
	if json.Unmarshal(body, &opts) != nil || opts.Preconditions.UID == "" {
		http.Error(w, "uid precondition required", http.StatusBadRequest)
		return
	}
	a.mu.Lock()
	status := a.deleteStatus
	lose := a.loseDeleteBody
	stored, ok := a.jobs[name]
	liveUID := stored.Metadata.UID
	if ok && a.replaceUID != "" {
		liveUID = a.replaceUID
	}
	if status == 0 && ok && opts.Preconditions.UID == stored.Metadata.UID && opts.Preconditions.UID == liveUID {
		delete(a.jobs, name)
	}
	a.mu.Unlock()
	if status != 0 {
		http.Error(w, "delete rejected", status)
		return
	}
	if !ok {
		http.NotFound(w, nil)
		return
	}
	if opts.Preconditions.UID != liveUID || opts.Preconditions.UID != stored.Metadata.UID {
		http.Error(w, "uid precondition failed", http.StatusConflict)
		return
	}
	if lose {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("{"))
		return
	}
	raw, err := json.Marshal(stored)
	if err != nil {
		a.t.Errorf("marshal deleted job: %v", err)
		http.Error(w, "marshal", http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write(raw)
}

func (a *kueueAPI) get(w http.ResponseWriter, name string) {
	a.mu.Lock()
	obj, ok := a.jobs[name]
	if ok {
		obj = a.view(obj)
	}
	a.mu.Unlock()
	if !ok {
		http.NotFound(w, nil)
		return
	}
	raw, err := json.Marshal(obj)
	if err != nil {
		a.t.Errorf("marshal job: %v", err)
		http.Error(w, "marshal", http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	_, _ = w.Write(raw)
}

func (a *kueueAPI) workloads(w http.ResponseWriter) {
	a.mu.Lock()
	placed := a.placed
	a.mu.Unlock()
	body := `{"items":[]}`
	if placed {
		body = strings.ReplaceAll(testKueueWorkloads, "job-uid-a", "job-uid-created")
	}
	w.Header().Set("Content-Type", "application/json")
	_, _ = w.Write([]byte(body))
}

func (a *kueueAPI) pods(w http.ResponseWriter, selector string) {
	a.mu.Lock()
	placed := a.placed
	a.mu.Unlock()
	if !placed {
		a.t.Errorf("pod list requested before a Kueue admission fixture")
	}
	if selector != "batch.kubernetes.io/controller-uid=job-uid-created" {
		a.t.Errorf("unexpected pod selector %q", selector)
	}
	w.Header().Set("Content-Type", "application/json")
	_, _ = w.Write([]byte(strings.ReplaceAll(testKubePods, "job-uid-a", "job-uid-created")))
}

func newKueueAdapterFixture(t *testing.T, api *kueueAPI) (*KueueJobAdapter, *kueueAPI, *httptest.Server) {
	t.Helper()
	return newKueueAdapterFixtureWithStore(t, api, NewMemoryKueueJobBindingStore())
}

func newKueueAdapterFixtureWithStore(t *testing.T, api *kueueAPI, store KueueJobBindingStore) (*KueueJobAdapter, *kueueAPI, *httptest.Server) {
	t.Helper()
	api.t = t
	if api.jobs == nil {
		api.jobs = make(map[string]kubeObject)
	}
	server := httptest.NewTLSServer(http.HandlerFunc(api.serve))
	client := server.Client()
	client.Timeout = 3 * time.Second
	adapter, err := NewKueueJobAdapterWithBindings(server.URL, "tenant-a", client, store)
	if err != nil {
		server.Close()
		t.Fatalf("adapter: %v", err)
	}
	return adapter, api, server
}

func reopenKueueAdapter(t *testing.T, server *httptest.Server, store KueueJobBindingStore) *KueueJobAdapter {
	t.Helper()
	client := server.Client()
	client.Timeout = 3 * time.Second
	adapter, err := NewKueueJobAdapterWithBindings(server.URL, "tenant-a", client, store)
	if err != nil {
		t.Fatal(err)
	}
	return adapter
}

func assertSuspendedCreate(t *testing.T, api *kueueAPI, req Request) {
	t.Helper()
	var posted kueueBatchJob
	found := false
	for _, call := range api.snapshot() {
		if call.method != http.MethodPost {
			continue
		}
		found = true
		if call.path != "/apis/batch/v1/namespaces/tenant-a/jobs" {
			t.Fatalf("create path %s", call.path)
		}
		if json.Unmarshal(call.body, &posted) != nil {
			t.Fatalf("create body is not a Job: %s", call.body)
		}
		raw := string(call.body)
		for _, forbidden := range []string{"finalizers", "queue-name", "nvidia.com/gpu", "resourceClaims", "resourceclaims"} {
			if strings.Contains(raw, forbidden) {
				t.Fatalf("create body contains %s: %s", forbidden, raw)
			}
		}
	}
	if !found {
		t.Fatal("expected one Job POST")
	}
	name := kueueCreateJobName(req.TenantID, req.OperationID)
	if posted.APIVersion != "batch/v1" || posted.Kind != "Job" || !posted.Spec.Suspend ||
		posted.Metadata.Name != name || posted.Metadata.Namespace != "tenant-a" ||
		posted.Metadata.Annotations[kueueAnnotationTenantID] != req.TenantID ||
		posted.Metadata.Annotations[kueueAnnotationWorkloadID] != req.WorkloadID ||
		posted.Metadata.Annotations[kueueAnnotationDesiredGeneration] != "7" ||
		posted.Metadata.Annotations[kueueAnnotationOperationID] != req.OperationID ||
		posted.Metadata.Annotations[kueueAnnotationRequestDigest] != req.RequestDigest {
		t.Fatalf("suspended Job identity: %+v", posted)
	}
}

func TestKueueJobAdapterCreateIsIdempotentAndInspectsThroughReader(t *testing.T) {
	adapter, api, server := newKueueAdapterFixture(t, &kueueAPI{})
	defer server.Close()
	ctx := context.Background()
	req := kueueAdapterRequest()

	ack, err := adapter.Apply(ctx, req)
	if err != nil || ack.OperationID != req.OperationID || ack.RequestDigest != req.RequestDigest {
		t.Fatalf("create: %+v %v", ack, err)
	}
	assertSuspendedCreate(t, api, req)
	again, err := adapter.Apply(ctx, req)
	if err != nil || again != ack || api.postCount() != 1 {
		t.Fatalf("replay posted again: %+v posts=%d err=%v", again, api.postCount(), err)
	}

	observed, err := adapter.Inspect(ctx, req)
	if err != nil || observed.Result != operation.ResourcePresent ||
		observed.ResourceUID != "job-uid-created" || observed.ResourceOperationID != req.OperationID ||
		observed.ResourceGeneration != req.DesiredGeneration || observed.ReasonCode != "" ||
		observed.ErrorCode != "" || observed.ResourceVersion == "" || observed.ObservedAt.IsZero() {
		t.Fatalf("inspect: %+v %v", observed, err)
	}
	if observed.ResourceID != "tenant-a/"+kueueCreateJobName(req.TenantID, req.OperationID) {
		t.Fatalf("resource id %s", observed.ResourceID)
	}
	sawWorkloads := false
	for _, call := range api.snapshot() {
		if call.method == http.MethodGet && call.path == "/apis/kueue.x-k8s.io/v1beta1/namespaces/tenant-a/workloads" {
			sawWorkloads = true
		}
	}
	if !sawWorkloads {
		t.Fatal("inspect did not reuse the Kueue workload read")
	}

	restarted, err := NewKueueJobAdapter(server.URL, "tenant-a", server.Client())
	if err != nil {
		t.Fatal(err)
	}
	replay, err := restarted.Apply(ctx, req)
	if !errors.Is(err, ErrAcknowledgementLost) || api.postCount() != 1 {
		t.Fatalf("restarted adapter trusted unbound Job: %+v posts=%d err=%v", replay, api.postCount(), err)
	}
	reread, err := restarted.Inspect(ctx, req)
	if err != nil || reread.Result != operation.OperationInProgress || reread.ResourceUID != "" {
		t.Fatalf("restarted Inspect claimed Job without durable UID: %+v %v", reread, err)
	}

	conflict := req
	conflict.RequestDigest = "sha256:" + strings.Repeat("b", 64)
	if _, err := adapter.Apply(ctx, conflict); !errors.Is(err, ErrOperationConflict) || api.postCount() != 1 {
		t.Fatalf("digest conflict: %v posts=%d", err, api.postCount())
	}
}

func TestKueueJobAdapterLostCreateACKIsInspectableWithoutASecondPost(t *testing.T) {
	adapter, api, server := newKueueAdapterFixture(t, &kueueAPI{loseCreateBody: true})
	defer server.Close()
	ctx := context.Background()
	req := kueueAdapterRequest()
	if _, err := adapter.Apply(ctx, req); !errors.Is(err, ErrAcknowledgementLost) {
		t.Fatalf("lost create acknowledgement: %v", err)
	}
	if api.postCount() != 1 {
		t.Fatalf("posts=%d", api.postCount())
	}
	observed, err := adapter.Inspect(ctx, req)
	if err != nil || observed.Result != operation.OperationInProgress ||
		observed.ResourceUID != "" || observed.ResourceID != "" {
		t.Fatalf("inspect after lost ACK decided unbound Job: %+v %v", observed, err)
	}
	ack, err := adapter.Apply(ctx, req)
	if !errors.Is(err, ErrAcknowledgementLost) || api.postCount() != 1 {
		t.Fatalf("replay after lost ACK acknowledged unbound Job: %+v posts=%d err=%v", ack, api.postCount(), err)
	}

	now := time.Now()
	op := operation.Operation{
		ID: req.OperationID, TenantID: req.TenantID, WorkloadID: req.WorkloadID,
		ProviderID: req.ProviderID, IdempotencyKey: req.IdempotencyKey,
		RequestDigest: req.RequestDigest, Generation: req.DesiredGeneration,
		Version: 1, Kind: operation.Create, Status: operation.Pending,
	}
	claimed, err := op.Claim(1, "worker-1", now, time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	inFlight, err := claimed.Start(claimed.Version, "worker-1", claimed.FenceToken, now.Add(time.Second))
	if err != nil {
		t.Fatal(err)
	}
	unknown, err := inFlight.MarkUnknown(inFlight.Version, "worker-1", inFlight.FenceToken, now.Add(2*time.Second))
	if err != nil {
		t.Fatal(err)
	}
	reconciling, err := unknown.BeginReconciliation(unknown.Version, "", 0, "worker-2", now.Add(3*time.Second), time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	verified, err := reconciling.ApplyObservation(reconciling.Version, "worker-2", reconciling.FenceToken,
		now.Add(time.Minute), req.DesiredGeneration, observed)
	if err != nil || verified.Status != operation.Unknown ||
		verified.ProviderResourceUID != "" || verified.ProviderResourceID != "" {
		t.Fatalf("lost-ACK without trusted UID incorrectly completed operation: %+v %v", verified, err)
	}
}

func TestKueueJobAdapterDoesNotTreatPodPlacedAsReady(t *testing.T) {
	adapter, api, server := newKueueAdapterFixture(t, &kueueAPI{})
	defer server.Close()
	ctx := context.Background()
	req := kueueAdapterRequest()
	if _, err := adapter.Apply(ctx, req); err != nil {
		t.Fatal(err)
	}
	queued, err := adapter.Inspect(ctx, req)
	if err != nil || queued.Result != operation.ResourcePresent || queued.ReasonCode != "" {
		t.Fatalf("unadmitted Job inspect: %+v %v", queued, err)
	}
	api.setPlaced(true)
	reader, err := NewKubernetesObservationReader(server.URL, server.Client())
	if err != nil {
		t.Fatal(err)
	}
	collected, err := reader.Collect(ctx, KubernetesJobQuery{
		TenantID: req.TenantID, WorkloadID: req.WorkloadID, DesiredGeneration: req.DesiredGeneration,
		Namespace: "tenant-a", JobName: kueueCreateJobName(req.TenantID, req.OperationID),
		ExpectedJobUID: queued.ResourceUID,
	})
	if err != nil || collected.Stage != KueuePodPlaced {
		t.Fatalf("reader stage=%s err=%v", collected.Stage, err)
	}
	placed, err := adapter.Inspect(ctx, req)
	if err != nil || placed.Result != queued.Result || placed.Result != operation.ResourcePresent ||
		placed.ResourceUID != queued.ResourceUID || placed.ReasonCode != "" || placed.ErrorCode != "" ||
		api.postCount() != 1 {
		t.Fatalf("POD_PLACED changed Job presence into another result: %+v err=%v", placed, err)
	}
}

func TestKueueJobAdapterRejectsReplacedUIDAndGeneration(t *testing.T) {
	tests := []struct {
		name   string
		uid    string
		gen    string
		reason string
	}{
		{name: "successor uid", uid: "job-uid-successor", reason: "JOB_UID_REPLACED"},
		{name: "replaced generation", gen: "6", reason: "RESOURCE_GENERATION_SUPERSEDED"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			adapter, api, server := newKueueAdapterFixture(t, &kueueAPI{})
			defer server.Close()
			ctx := context.Background()
			req := kueueAdapterRequest()
			if _, err := adapter.Apply(ctx, req); err != nil {
				t.Fatal(err)
			}
			api.setReplacement(tc.uid, tc.gen)
			observed, err := adapter.Inspect(ctx, req)
			if err != nil || observed.Result != operation.ObservationConflict || observed.ReasonCode != tc.reason ||
				observed.Result == operation.ResourcePresent || observed.ResourceUID != "" {
				t.Fatalf("inspect: %+v %v", observed, err)
			}
			if _, err := adapter.Apply(ctx, req); !errors.Is(err, ErrOperationConflict) || api.postCount() != 1 {
				t.Fatalf("apply adopted replacement: %v posts=%d", err, api.postCount())
			}
			for _, call := range api.snapshot() {
				if call.method != http.MethodGet && call.method != http.MethodPost {
					t.Fatalf("mutating call %s %s", call.method, call.path)
				}
			}
		})
	}
}

func TestKueueJobAdapterRejectsDefiniteCreateFailureAndUnimplementedKinds(t *testing.T) {
	ctx := context.Background()
	req := kueueAdapterRequest()

	forbidden, api, server := newKueueAdapterFixture(t, &kueueAPI{createStatus: http.StatusForbidden})
	defer server.Close()
	if _, err := forbidden.Apply(ctx, req); !errors.Is(err, ErrInvalidRequest) || errors.Is(err, ErrAcknowledgementLost) {
		t.Fatalf("forbidden create: %v", err)
	}
	missing, err := forbidden.Inspect(ctx, req)
	if err != nil || missing.Result != operation.ObservationConflict ||
		missing.ReasonCode != "MISSING_AUTHORITATIVE_OPERATION" ||
		missing.Result == operation.OperationNotAppliedFinal {
		t.Fatalf("rejected create inspect: %+v %v", missing, err)
	}

	ambiguous, ambiguousAPI, ambiguousServer := newKueueAdapterFixture(t, &kueueAPI{createStatus: http.StatusInternalServerError})
	defer ambiguousServer.Close()
	if _, err := ambiguous.Apply(ctx, req); !errors.Is(err, ErrAcknowledgementLost) {
		t.Fatalf("ambiguous create: %v", err)
	}
	unknown, err := ambiguous.Inspect(ctx, req)
	if err != nil || unknown.Result == operation.ResourcePresent || unknown.Result == operation.OperationNotAppliedFinal ||
		ambiguousAPI.storedJobs() != 0 {
		t.Fatalf("ambiguous create looked decided: %+v stored=%d err=%v", unknown, ambiguousAPI.storedJobs(), err)
	}

	kinds, kindAPI, kindServer := newKueueAdapterFixture(t, &kueueAPI{})
	defer kindServer.Close()
	before := len(kindAPI.snapshot())
	for _, kind := range []operation.Kind{operation.Update} {
		change := req
		change.Kind = kind
		change.OperationID = "op-" + string(kind)
		change.IdempotencyKey = "idem-" + string(kind)
		change.TargetResourceID = "tenant-a/job-a"
		change.TargetResourceUID = "job-uid-created"
		if _, err := kinds.Apply(ctx, change); !errors.Is(err, ErrInvalidRequest) {
			t.Fatalf("%s: %v", kind, err)
		}
	}
	if len(kindAPI.snapshot()) != before || kindAPI.postCount() != 0 {
		t.Fatal("unimplemented kinds called Kubernetes")
	}
	canceled, cancel := context.WithCancel(ctx)
	cancel()
	if _, err := kinds.Apply(canceled, req); !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled apply: %v", err)
	}
	absent, err := kinds.Inspect(ctx, req)
	if err != nil || absent.Result != operation.ObservationConflict || absent.Result == operation.OperationNotAppliedFinal {
		t.Fatalf("inspect before create: %+v %v", absent, err)
	}

	client := server.Client()
	if _, err := NewKueueJobAdapter(server.URL, "Not_A_Namespace", client); !errors.Is(err, ErrInvalidKueueObservation) {
		t.Fatalf("invalid namespace accepted: %v", err)
	}
	plain := strings.Replace(server.URL, "https://", "http://", 1)
	if _, err := NewKueueJobAdapter(plain, "tenant-a", client); !errors.Is(err, ErrInvalidKueueObservation) {
		t.Fatalf("plaintext API accepted: %v", err)
	}
	if api.postCount() != 1 || api.storedJobs() != 0 {
		t.Fatalf("forbidden create persisted a Job: posts=%d stored=%d", api.postCount(), api.storedJobs())
	}
}

func TestKueueJobAdapterServiceAccountClientCreatesThenObserves(t *testing.T) {
	api := &kueueAPI{t: t, jobs: make(map[string]kubeObject), token: "test-token-a"}
	server := httptest.NewTLSServer(http.HandlerFunc(api.serve))
	defer server.Close()
	ca, tokenFile := serviceAccountTestFiles(t, server)
	if _, err := NewServiceAccountKueueJobAdapter(server.URL, ca, tokenFile, "Not_A_Namespace"); !errors.Is(err, ErrInvalidKueueObservation) {
		t.Fatalf("invalid namespace accepted: %v", err)
	}
	plain := strings.Replace(server.URL, "https://", "http://", 1)
	if _, err := NewServiceAccountKueueJobAdapter(plain, ca, tokenFile, "tenant-a"); !errors.Is(err, ErrInvalidKueueObservation) {
		t.Fatalf("plaintext API accepted: %v", err)
	}
	adapter, err := NewServiceAccountKueueJobAdapter(server.URL, ca, tokenFile, "tenant-a")
	if err != nil {
		t.Fatal(err)
	}
	req := kueueAdapterRequest()
	if _, err := adapter.Apply(context.Background(), req); err != nil {
		t.Fatal(err)
	}
	observed, err := adapter.Inspect(context.Background(), req)
	if err != nil || observed.Result != operation.ResourcePresent || observed.ResourceUID != "job-uid-created" {
		t.Fatalf("service account inspect: %+v %v", observed, err)
	}
	authed := false
	for _, call := range api.snapshot() {
		if call.method == http.MethodPost {
			authed = call.auth == "Bearer test-token-a"
		}
	}
	if !authed {
		t.Fatal("create did not present the ServiceAccount token")
	}
}

func TestKueueCreateJobNameIsStableDNSLabel(t *testing.T) {
	req := kueueAdapterRequest()
	name := kueueCreateJobName(req.TenantID, req.OperationID)
	if name != kueueCreateJobName(req.TenantID, req.OperationID) || !validKubeName(name) ||
		name == kueueCreateJobName(req.TenantID, req.OperationID+"-other") ||
		name == kueueCreateJobName("other-tenant", req.OperationID) {
		t.Fatalf("job name %s", name)
	}
}

func TestKueueJobAdapterRejectsReplayedJobWithUnsafeChangedSpec(t *testing.T) {
	cases := []struct {
		name   string
		mutate func(*kubeObject)
	}{
		{
			name: "job was unsuspended",
			mutate: func(job *kubeObject) {
				suspended := false
				job.Spec.Suspend = &suspended
			},
		},
		{
			name: "job image changed",
			mutate: func(job *kubeObject) {
				job.Spec.Template.Spec.Containers[0].Image = "busybox:1.36"
			},
		},
		{
			name: "job parallelism changed",
			mutate: func(job *kubeObject) {
				parallelism := 2
				job.Spec.Parallelism = &parallelism
			},
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			adapter, api, server := newKueueAdapterFixture(t, &kueueAPI{})
			defer server.Close()
			req := kueueAdapterRequest()
			if _, err := adapter.Apply(context.Background(), req); err != nil {
				t.Fatal(err)
			}
			name := kueueCreateJobName(req.TenantID, req.OperationID)
			api.mu.Lock()
			obj := api.jobs[name]
			tc.mutate(&obj)
			api.jobs[name] = obj
			api.mu.Unlock()

			if _, err := adapter.Apply(context.Background(), req); !errors.Is(err, ErrOperationConflict) {
				t.Fatalf("replay accepted changed job spec: %v", err)
			}
			got, err := adapter.Inspect(context.Background(), req)
			if err != nil || got.Result != operation.ObservationConflict ||
				got.ReasonCode != "JOB_SPEC_MISMATCH" || got.ResourceUID != "" {
				t.Fatalf("unsafe Job treated as authorized: %+v err=%v", got, err)
			}
			if api.postCount() != 1 {
				t.Fatalf("replay created a second job: %d posts", api.postCount())
			}
		})
	}
}

func kueueDeleteRequest(create Request, observed operation.Observation) Request {
	req := create
	req.OperationID = "op-delete-1"
	req.IdempotencyKey = "idem-delete-1"
	req.Kind = operation.Delete
	req.TargetResourceID = observed.ResourceID
	req.TargetResourceUID = observed.ResourceUID
	return req
}

func createSuspendedJob(t *testing.T, adapter *KueueJobAdapter, api *kueueAPI) (Request, operation.Observation) {
	t.Helper()
	req := kueueAdapterRequest()
	if _, err := adapter.Apply(context.Background(), req); err != nil {
		t.Fatal(err)
	}
	assertSuspendedCreate(t, api, req)
	observed, err := adapter.Inspect(context.Background(), req)
	if err != nil || observed.Result != operation.ResourcePresent || observed.ResourceUID != "job-uid-created" {
		t.Fatalf("create inspect: %+v %v", observed, err)
	}
	return req, observed
}

func assertUIDPreconditions(t *testing.T, api *kueueAPI, uid string) {
	t.Helper()
	n := 0
	for _, call := range api.snapshot() {
		if call.method != http.MethodDelete {
			continue
		}
		n++
		if !strings.HasPrefix(call.path, "/apis/batch/v1/namespaces/tenant-a/jobs/") {
			t.Fatalf("delete path %s", call.path)
		}
		var opts kueueDeleteOptions
		if json.Unmarshal(call.body, &opts) != nil || opts.Kind != "DeleteOptions" ||
			opts.APIVersion != "v1" || opts.Preconditions.UID != uid {
			t.Fatalf("delete body %s", call.body)
		}
		raw := string(call.body)
		for _, forbidden := range []string{"finalizers", "queue-name", "nvidia.com/gpu", "resourceClaims", "resourceclaims"} {
			if strings.Contains(raw, forbidden) {
				t.Fatalf("delete body contains %s: %s", forbidden, raw)
			}
		}
	}
	if n == 0 {
		t.Fatal("expected a UID-preconditioned DELETE")
	}
}

func consumeDeleteObservation(t *testing.T, req Request, obs operation.Observation) {
	t.Helper()
	now := time.Now()
	op := operation.Operation{
		ID: req.OperationID, TenantID: req.TenantID, WorkloadID: req.WorkloadID,
		ProviderID: req.ProviderID, IdempotencyKey: req.IdempotencyKey,
		RequestDigest: req.RequestDigest, Generation: req.DesiredGeneration,
		Version: 1, Kind: operation.Delete, Status: operation.Pending,
		TargetResourceID: req.TargetResourceID, TargetResourceUID: req.TargetResourceUID,
	}
	claimed, err := op.Claim(1, "worker-1", now, time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	inFlight, err := claimed.Start(claimed.Version, "worker-1", claimed.FenceToken, now.Add(time.Second))
	if err != nil {
		t.Fatal(err)
	}
	unknown, err := inFlight.MarkUnknown(inFlight.Version, "worker-1", inFlight.FenceToken, now.Add(2*time.Second))
	if err != nil {
		t.Fatal(err)
	}
	reconciling, err := unknown.BeginReconciliation(unknown.Version, "", 0, "worker-2", now.Add(3*time.Second), time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	verified, err := reconciling.ApplyObservation(reconciling.Version, "worker-2", reconciling.FenceToken,
		now.Add(time.Minute), req.DesiredGeneration, obs)
	if err != nil || verified.Status != operation.Succeeded ||
		verified.ProviderResourceID != "" || verified.ProviderResourceUID != "" ||
		verified.TargetResourceID != req.TargetResourceID ||
		verified.TargetResourceUID != req.TargetResourceUID {
		t.Fatalf("delete absence was not consumable: %+v %v", verified, err)
	}
}

func TestKueueJobAdapterDeletesExactUIDAndInspectsAbsence(t *testing.T) {
	adapter, api, server := newKueueAdapterFixture(t, &kueueAPI{})
	defer server.Close()
	ctx := context.Background()
	create, observed := createSuspendedJob(t, adapter, api)
	remove := kueueDeleteRequest(create, observed)

	ack, err := adapter.Apply(ctx, remove)
	if err != nil || ack.OperationID != remove.OperationID || ack.RequestDigest != remove.RequestDigest {
		t.Fatalf("delete: %+v %v", ack, err)
	}
	if api.deleteCount() != 1 || api.postCount() != 1 || api.storedJobs() != 0 {
		t.Fatalf("posts=%d deletes=%d stored=%d", api.postCount(), api.deleteCount(), api.storedJobs())
	}
	assertUIDPreconditions(t, api, observed.ResourceUID)

	absent, err := adapter.Inspect(ctx, remove)
	if err != nil || absent.Result != operation.TargetAbsent ||
		absent.TargetResourceID != remove.TargetResourceID ||
		absent.TargetResourceUID != remove.TargetResourceUID ||
		absent.ResourceID != "" || absent.ResourceUID != "" ||
		absent.ResourceOperationID != "" || absent.ResourceGeneration != 0 ||
		absent.ReasonCode != "" || absent.ErrorCode != "" ||
		absent.ResourceVersion == "" || absent.ObservedAt.IsZero() ||
		absent.Result == operation.OperationNotAppliedFinal ||
		absent.ReasonCode == "MISSING_AUTHORITATIVE_OPERATION" {
		t.Fatalf("intended absence: %+v %v", absent, err)
	}
	consumeDeleteObservation(t, remove, absent)

	replay, err := adapter.Apply(ctx, remove)
	if err != nil || replay != ack || api.deleteCount() != 1 || api.postCount() != 1 {
		t.Fatalf("delete replay: %+v deletes=%d posts=%d err=%v", replay, api.deleteCount(), api.postCount(), err)
	}
	again, err := adapter.Inspect(ctx, remove)
	if err != nil || again.Result != operation.TargetAbsent || again.TargetResourceUID != observed.ResourceUID {
		t.Fatalf("replay inspect: %+v %v", again, err)
	}

	staleCreate, err := adapter.Inspect(ctx, create)
	if err != nil || staleCreate.Result != operation.ObservationConflict ||
		staleCreate.ReasonCode != "MISSING_AUTHORITATIVE_OPERATION" ||
		staleCreate.Result == operation.OperationNotAppliedFinal ||
		staleCreate.Result == operation.ResourcePresent {
		t.Fatalf("create inspect after delete: %+v %v", staleCreate, err)
	}

	other := remove
	other.OperationID = "op-delete-2"
	other.IdempotencyKey = "idem-delete-2"
	unintended, err := adapter.Inspect(ctx, other)
	if err != nil || unintended.Result != operation.ObservationConflict ||
		unintended.ReasonCode != "MISSING_AUTHORITATIVE_OPERATION" ||
		unintended.Result == operation.TargetAbsent ||
		unintended.Result == operation.OperationNotAppliedFinal {
		t.Fatalf("second delete inferred absence: %+v %v", unintended, err)
	}
	if _, err := adapter.Apply(ctx, other); !errors.Is(err, ErrOperationConflict) || api.deleteCount() != 1 || api.postCount() != 1 {
		t.Fatalf("second delete treated 404 as success: %v deletes=%d", err, api.deleteCount())
	}

	name := kueueCreateJobName(create.TenantID, create.OperationID)
	api.plantJob(name, "job-uid-successor")
	if _, err := adapter.Apply(ctx, remove); !errors.Is(err, ErrOperationConflict) || api.deleteCount() != 1 || api.storedJobs() != 1 {
		t.Fatalf("acknowledged replay deleted successor: %v deletes=%d stored=%d", err, api.deleteCount(), api.storedJobs())
	}
	blocked, err := adapter.Inspect(ctx, remove)
	if err != nil || blocked.Result != operation.ObservationConflict || blocked.ReasonCode != "JOB_UID_REPLACED" ||
		blocked.Result == operation.TargetAbsent {
		t.Fatalf("successor inspect: %+v %v", blocked, err)
	}
}

func TestKueueJobAdapterDeleteDoesNotRemoveSuccessor(t *testing.T) {
	tests := []struct {
		name     string
		uid      string
		gen      string
		retarget bool
		reason   string
	}{
		{name: "successor uid", uid: "job-uid-successor", reason: "JOB_UID_REPLACED"},
		{name: "request adopts successor uid", uid: "job-uid-successor", retarget: true, reason: "JOB_UID_REPLACED"},
		{name: "replaced generation", gen: "6", reason: "RESOURCE_GENERATION_SUPERSEDED"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			adapter, api, server := newKueueAdapterFixture(t, &kueueAPI{})
			defer server.Close()
			ctx := context.Background()
			create, observed := createSuspendedJob(t, adapter, api)
			api.setReplacement(tc.uid, tc.gen)
			remove := kueueDeleteRequest(create, observed)
			if tc.retarget {
				remove.TargetResourceUID = tc.uid
			}
			if _, err := adapter.Apply(ctx, remove); !errors.Is(err, ErrOperationConflict) || !strings.Contains(err.Error(), tc.reason) ||
				api.deleteCount() != 0 || api.storedJobs() != 1 || api.postCount() != 1 {
				t.Fatalf("delete adopted replacement: %v deletes=%d stored=%d", err, api.deleteCount(), api.storedJobs())
			}
			seen, err := adapter.Inspect(ctx, remove)
			if err != nil || seen.Result != operation.ObservationConflict || seen.ReasonCode != tc.reason ||
				seen.Result == operation.TargetAbsent || seen.Result == operation.OperationNotAppliedFinal {
				t.Fatalf("inspect: %+v %v", seen, err)
			}
			if !tc.retarget {
				restarted, err := NewKueueJobAdapter(server.URL, "tenant-a", server.Client())
				if err != nil {
					t.Fatal(err)
				}
				if _, err := restarted.Apply(ctx, remove); !errors.Is(err, ErrOperationConflict) || api.deleteCount() != 0 || api.storedJobs() != 1 {
					t.Fatalf("restarted adapter deleted successor: %v deletes=%d stored=%d", err, api.deleteCount(), api.storedJobs())
				}
			}
		})
	}
}

type roundTripFunc func(*http.Request) (*http.Response, error)

func (f roundTripFunc) RoundTrip(r *http.Request) (*http.Response, error) { return f(r) }

func TestKueueJobAdapterLostDeleteACKRecoversWithoutDeletingSuccessor(t *testing.T) {
	t.Run("unreadable body", func(t *testing.T) {
		adapter, api, server := newKueueAdapterFixture(t, &kueueAPI{loseDeleteBody: true})
		defer server.Close()
		ctx := context.Background()
		create, observed := createSuspendedJob(t, adapter, api)
		remove := kueueDeleteRequest(create, observed)
		if _, err := adapter.Apply(ctx, remove); !errors.Is(err, ErrAcknowledgementLost) || api.deleteCount() != 1 || api.storedJobs() != 0 {
			t.Fatalf("lost delete acknowledgement: %v deletes=%d stored=%d", err, api.deleteCount(), api.storedJobs())
		}
		assertUIDPreconditions(t, api, observed.ResourceUID)
		absent, err := adapter.Inspect(ctx, remove)
		if err != nil || absent.Result != operation.TargetAbsent || absent.TargetResourceUID != observed.ResourceUID ||
			absent.Result == operation.OperationNotAppliedFinal {
			t.Fatalf("inspect after lost delete: %+v %v", absent, err)
		}
		consumeDeleteObservation(t, remove, absent)
		if _, err := adapter.Apply(ctx, remove); err != nil || api.deleteCount() != 1 || api.postCount() != 1 {
			t.Fatalf("recover replay deleted again: %v deletes=%d", err, api.deleteCount())
		}
		api.plantJob(kueueCreateJobName(create.TenantID, create.OperationID), "job-uid-successor")
		if _, err := adapter.Apply(ctx, remove); !errors.Is(err, ErrOperationConflict) || api.deleteCount() != 1 || api.storedJobs() != 1 {
			t.Fatalf("recovery deleted successor: %v deletes=%d stored=%d", err, api.deleteCount(), api.storedJobs())
		}
	})

	t.Run("server error keeps object", func(t *testing.T) {
		adapter, api, server := newKueueAdapterFixture(t, &kueueAPI{deleteStatus: http.StatusInternalServerError})
		defer server.Close()
		ctx := context.Background()
		_, observed := createSuspendedJob(t, adapter, api)
		remove := kueueDeleteRequest(kueueAdapterRequest(), observed)
		if _, err := adapter.Apply(ctx, remove); !errors.Is(err, ErrAcknowledgementLost) || api.storedJobs() != 1 {
			t.Fatalf("ambiguous delete: %v stored=%d", err, api.storedJobs())
		}
		stuck, err := adapter.Inspect(ctx, remove)
		if err != nil || stuck.Result == operation.TargetAbsent || stuck.Result == operation.OperationNotAppliedFinal ||
			stuck.ReasonCode != "DELETE_TARGET_STILL_PRESENT" {
			t.Fatalf("lost delete looked finished: %+v %v", stuck, err)
		}
		api.setDeleteStatus(0)
		ack, err := adapter.Apply(ctx, remove)
		if err != nil || ack.OperationID != remove.OperationID || api.deleteCount() != 2 || api.storedJobs() != 0 {
			t.Fatalf("retry after lost delete: %+v deletes=%d stored=%d err=%v", ack, api.deleteCount(), api.storedJobs(), err)
		}
		assertUIDPreconditions(t, api, observed.ResourceUID)
		absent, err := adapter.Inspect(ctx, remove)
		if err != nil || absent.Result != operation.TargetAbsent || absent.TargetResourceUID != observed.ResourceUID {
			t.Fatalf("inspect after retry: %+v %v", absent, err)
		}
	})

	t.Run("server error then successor", func(t *testing.T) {
		adapter, api, server := newKueueAdapterFixture(t, &kueueAPI{deleteStatus: http.StatusInternalServerError})
		defer server.Close()
		ctx := context.Background()
		create, observed := createSuspendedJob(t, adapter, api)
		remove := kueueDeleteRequest(create, observed)
		if _, err := adapter.Apply(ctx, remove); !errors.Is(err, ErrAcknowledgementLost) || api.deleteCount() != 1 {
			t.Fatalf("ambiguous delete: %v", err)
		}
		api.setDeleteStatus(0)
		api.setReplacement("job-uid-successor", "")
		if _, err := adapter.Apply(ctx, remove); !errors.Is(err, ErrOperationConflict) || api.deleteCount() != 1 || api.storedJobs() != 1 {
			t.Fatalf("retry deleted successor: %v deletes=%d stored=%d", err, api.deleteCount(), api.storedJobs())
		}
		seen, err := adapter.Inspect(ctx, remove)
		if err != nil || seen.Result != operation.ObservationConflict || seen.ReasonCode != "JOB_UID_REPLACED" ||
			seen.Result == operation.TargetAbsent || seen.Result == operation.OperationNotAppliedFinal {
			t.Fatalf("successor after lost delete: %+v %v", seen, err)
		}
	})

	t.Run("transport error", func(t *testing.T) {
		adapter, api, server := newKueueAdapterFixture(t, &kueueAPI{})
		defer server.Close()
		ctx := context.Background()
		_, observed := createSuspendedJob(t, adapter, api)
		base := adapter.client.Transport
		failDelete := true
		adapter.client.Transport = roundTripFunc(func(r *http.Request) (*http.Response, error) {
			if failDelete && r.Method == http.MethodDelete {
				failDelete = false
				return nil, errors.New("connection reset by peer")
			}
			return base.RoundTrip(r)
		})
		remove := kueueDeleteRequest(kueueAdapterRequest(), observed)
		if _, err := adapter.Apply(ctx, remove); !errors.Is(err, ErrAcknowledgementLost) || api.storedJobs() != 1 {
			t.Fatalf("transport loss: %v stored=%d", err, api.storedJobs())
		}
		api.setReplacement("job-uid-successor", "")
		if _, err := adapter.Apply(ctx, remove); !errors.Is(err, ErrOperationConflict) || api.deleteCount() != 0 || api.storedJobs() != 1 {
			t.Fatalf("transport retry deleted successor: %v deletes=%d stored=%d", err, api.deleteCount(), api.storedJobs())
		}
		seen, err := adapter.Inspect(ctx, remove)
		if err != nil || seen.Result == operation.TargetAbsent || seen.Result == operation.OperationNotAppliedFinal ||
			seen.ReasonCode != "JOB_UID_REPLACED" {
			t.Fatalf("inspect after transport loss: %+v %v", seen, err)
		}
	})
}

func TestKueueJobAdapterRejectsDefiniteDeleteAndUnintendedAbsence(t *testing.T) {
	ctx := context.Background()
	for _, status := range []int{
		http.StatusBadRequest, http.StatusUnauthorized, http.StatusForbidden, http.StatusUnprocessableEntity,
	} {
		t.Run(http.StatusText(status), func(t *testing.T) {
			adapter, api, server := newKueueAdapterFixture(t, &kueueAPI{deleteStatus: status})
			defer server.Close()
			_, observed := createSuspendedJob(t, adapter, api)
			remove := kueueDeleteRequest(kueueAdapterRequest(), observed)
			if _, err := adapter.Apply(ctx, remove); !errors.Is(err, ErrInvalidRequest) || errors.Is(err, ErrAcknowledgementLost) {
				t.Fatalf("definite delete failure: %v", err)
			}
			if api.storedJobs() != 1 || api.postCount() != 1 || api.deleteCount() != 1 {
				t.Fatalf("rejected delete changed the Job: posts=%d deletes=%d stored=%d", api.postCount(), api.deleteCount(), api.storedJobs())
			}
			obs, err := adapter.Inspect(ctx, remove)
			if err != nil || obs.Result == operation.TargetAbsent || obs.Result == operation.OperationNotAppliedFinal ||
				obs.Result == operation.ResourcePresent {
				t.Fatalf("rejected delete inspect: %+v %v", obs, err)
			}
		})
	}

	t.Run("missing without intent", func(t *testing.T) {
		adapter, api, server := newKueueAdapterFixture(t, &kueueAPI{})
		defer server.Close()
		create := kueueAdapterRequest()
		remove := create
		remove.Kind = operation.Delete
		remove.OperationID = "op-delete-missing"
		remove.IdempotencyKey = "idem-delete-missing"
		remove.TargetResourceID = "tenant-a/" + kueueCreateJobName(create.TenantID, create.OperationID)
		remove.TargetResourceUID = "job-uid-never"
		missing, err := adapter.Inspect(ctx, remove)
		if err != nil || missing.Result != operation.ObservationConflict ||
			missing.ReasonCode != "MISSING_AUTHORITATIVE_OPERATION" ||
			missing.Result == operation.TargetAbsent || missing.Result == operation.OperationNotAppliedFinal {
			t.Fatalf("unintended absence: %+v %v", missing, err)
		}
		if _, err := adapter.Apply(ctx, remove); !errors.Is(err, ErrOperationConflict) ||
			api.deleteCount() != 0 || api.postCount() != 0 || api.storedJobs() != 0 {
			t.Fatalf("missing target was deleted or created: %v deletes=%d posts=%d", err, api.deleteCount(), api.postCount())
		}
	})

	t.Run("update rejected", func(t *testing.T) {
		adapter, api, server := newKueueAdapterFixture(t, &kueueAPI{})
		defer server.Close()
		before := len(api.snapshot())
		change := kueueAdapterRequest()
		change.Kind = operation.Update
		change.OperationID = "op-UPDATE"
		change.IdempotencyKey = "idem-UPDATE"
		change.TargetResourceID = "tenant-a/job-a"
		change.TargetResourceUID = "job-uid-created"
		if _, err := adapter.Apply(ctx, change); !errors.Is(err, ErrInvalidRequest) {
			t.Fatalf("update apply: %v", err)
		}
		if _, err := adapter.Inspect(ctx, change); !errors.Is(err, ErrInvalidRequest) {
			t.Fatalf("update inspect: %v", err)
		}
		if len(api.snapshot()) != before || api.postCount() != 0 || api.deleteCount() != 0 {
			t.Fatal("update called Kubernetes")
		}
	})
}
