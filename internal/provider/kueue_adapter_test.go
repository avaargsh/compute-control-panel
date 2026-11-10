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
	n := 0
	for _, call := range a.snapshot() {
		if call.method == http.MethodPost {
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
	if r.Method != http.MethodGet && r.Method != http.MethodPost {
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
	api.t = t
	if api.jobs == nil {
		api.jobs = make(map[string]kubeObject)
	}
	server := httptest.NewTLSServer(http.HandlerFunc(api.serve))
	client := server.Client()
	client.Timeout = 3 * time.Second
	adapter, err := NewKueueJobAdapter(server.URL, "tenant-a", client)
	if err != nil {
		server.Close()
		t.Fatalf("adapter: %v", err)
	}
	return adapter, api, server
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
	if err != nil || replay != ack || api.postCount() != 1 {
		t.Fatalf("restarted adapter created a second Job: %+v posts=%d err=%v", replay, api.postCount(), err)
	}
	reread, err := restarted.Inspect(ctx, req)
	if err != nil || reread.ResourceUID != observed.ResourceUID || reread.Result != operation.ResourcePresent {
		t.Fatalf("restarted inspect: %+v %v", reread, err)
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
	if err != nil || observed.Result != operation.ResourcePresent || observed.ResourceUID != "job-uid-created" {
		t.Fatalf("inspect after lost ACK: %+v %v", observed, err)
	}
	ack, err := adapter.Apply(ctx, req)
	if err != nil || ack.OperationID != req.OperationID || api.postCount() != 1 {
		t.Fatalf("replay after lost ACK: %+v posts=%d err=%v", ack, api.postCount(), err)
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
	if err != nil || verified.Status != operation.Succeeded || verified.ProviderResourceUID != observed.ResourceUID {
		t.Fatalf("lost-ACK observation did not record Job presence: %+v %v", verified, err)
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
	for _, kind := range []operation.Kind{operation.Update, operation.Delete} {
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
