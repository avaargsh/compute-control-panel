package provider

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/avaargsh/compute-control-panel/internal/operation"
)

// Extra annotations the Job adapter writes so a later GET can fence the
// object to one operation. The read-only reader does not require them; Inspect
// refuses a Job that lacks them or carries a different value.
const (
	kueueAnnotationOperationID    = "compute-control-panel.io/operation-id"
	kueueAnnotationRequestDigest  = "compute-control-panel.io/request-digest"
	kueueAnnotationIdempotencyKey = "compute-control-panel.io/idempotency-key"
	kueueAnnotationProviderID     = "compute-control-panel.io/provider-id"

	// kueueSuspendedJobImage is used for inert default and CPU-only queued
	// smoke Jobs. Neither path demonstrates any physical GPU assignment.
	kueueSuspendedJobImage = "registry.k8s.io/pause:3.10"
)

var errKueueJobAlreadyExists = errors.New("kubernetes job already exists")

// KueueJobAdapter is a batch/v1 Job writer. Default CREATE posts an inert,
// unqueued suspended Job. An explicit queued constructor submits a labeled
// Job with bounded requests, allowing Kueue to own its suspension and resume.
// Both modes pin the returned UID in the binding store. DELETE targets only
// target UID. CREATE Inspect reads that UID back through
// KubernetesObservationReader.Collect.
//
// Structural stages UNPROVEN, KUEUE_ADMITTED and POD_PLACED all mean only that
// the pinned Job object was observed. POD_PLACED is not Allocation READY.
// DELETE TARGET_ABSENT is a stored tombstone for that UID plus a UID-fenced
// 404. It is not Allocation cleanup, Kueue quota release, or device or claim
// deallocation. A 404 with no stored CREATE pin and no stored DELETE tombstone
// stays ambiguous. The default in-memory store is test-only; in-cluster
// ServiceAccount construction requires a durable PostgreSQL binding store.
// A later adapter must not reconstruct a lost pin from a name or annotations.
// The adapter itself does not admit, schedule, allocate devices, or mount an
// HTTP handler. A Kueue-managed queued Job can later be admitted and placed.
type KueueJobAdapter struct {
	namespace string
	apiURL    string
	client    http.Client
	reader    *KubernetesObservationReader
	bindings  KueueJobBindingStore
	// Empty means the original inert suspended Job mode. A non-empty LocalQueue
	// explicitly opts into Kueue-controlled suspend/unsuspend and Pod creation.
	queueName string
}

// NewKueueJobAdapter binds CREATE and Inspect to one namespace and an HTTPS
// client the caller has already authenticated. The client is shared with the
// read-only reader. No cluster credential is built here. This is an
// explicit test-only in-memory constructor; in-cluster use requires the
// durable ServiceAccount constructor and a restricted database login.
func NewKueueJobAdapter(apiURL, namespace string, client *http.Client) (*KueueJobAdapter, error) {
	return NewKueueJobAdapterWithBindings(apiURL, namespace, client, NewMemoryKueueJobBindingStore())
}

// NewKueueJobAdapterWithBindings is NewKueueJobAdapter with a caller-supplied
// binding store. Pass the same store to a later adapter to resume CREATE pins
// and DELETE tombstones. A nil store is rejected.
func NewKueueJobAdapterWithBindings(apiURL, namespace string, client *http.Client, bindings KueueJobBindingStore) (*KueueJobAdapter, error) {
	if !validKubeName(namespace) || bindings == nil {
		return nil, ErrInvalidKueueObservation
	}
	reader, err := NewKubernetesObservationReader(apiURL, client)
	if err != nil {
		return nil, err
	}
	return &KueueJobAdapter{
		namespace: namespace,
		apiURL:    reader.apiURL,
		client:    reader.client,
		reader:    reader,
		bindings:  bindings,
	}, nil
}

// NewQueuedKueueJobAdapterWithBindings opts into submitting a Job to the
// specified LocalQueue. Kueue, not CCP, owns its suspend/unsuspend transitions.
// A persistent binding store and authenticated client remain caller requirements.
func NewQueuedKueueJobAdapterWithBindings(
	apiURL, namespace, localQueue string, client *http.Client, bindings KueueJobBindingStore,
) (*KueueJobAdapter, error) {
	if !validKubeName(localQueue) {
		return nil, ErrInvalidKueueObservation
	}
	adapter, err := NewKueueJobAdapterWithBindings(apiURL, namespace, client, bindings)
	if err != nil {
		return nil, err
	}
	adapter.queueName = localQueue
	return adapter, nil
}

// NewQueuedServiceAccountKueueJobAdapterWithBindings is the authenticated
// scoped writer for the opt-in queued mode; no patch/update rights are needed.
func NewQueuedServiceAccountKueueJobAdapterWithBindings(
	apiURL, caPath, tokenPath, namespace, localQueue string, bindings KueueJobBindingStore,
) (*KueueJobAdapter, error) {
	if !validKubeName(localQueue) || !validKubeName(namespace) || bindings == nil {
		return nil, ErrInvalidKueueObservation
	}
	client, closeIdle, err := newServiceAccountHTTPClient(caPath, tokenPath)
	if err != nil {
		return nil, err
	}
	adapter, err := NewQueuedKueueJobAdapterWithBindings(apiURL, namespace, localQueue, client, bindings)
	if err != nil {
		closeIdle()
		return nil, err
	}
	return adapter, nil
}

// NewServiceAccountKueueJobAdapter is the in-cluster constructor. It uses the
// same pinned ServiceAccount HTTPS client as the read-only observer and opens
// a PostgresKueueJobBindingStore from bindingDSN. Bind a separate Kubernetes
// Role: create, delete, and get batch/v1 Jobs, plus list Pods and Kueue
// Workloads for Inspect. Do not add update, patch, or cluster-admin, and do
// not add create or delete to the observer Role. Provenance from Inspect
// stays unattested.
//
// An empty DSN, an unreachable database, a missing binding schema, or a
// connected role with broader privileges than the binding DML returns an
// error before an adapter exists. This constructor never uses an in-memory
// store. Tests that need a private memory store call
// NewServiceAccountKueueJobAdapterWithBindings with
// NewMemoryKueueJobBindingStore. Neither constructor mounts an HTTP handler.
func NewServiceAccountKueueJobAdapter(apiURL, caPath, tokenPath, namespace, bindingDSN string) (*KueueJobAdapter, error) {
	if !validKubeName(namespace) {
		return nil, ErrInvalidKueueObservation
	}
	// Reject a bad API URL before opening the database so a plaintext or
	// malformed endpoint cannot be paired with a live binding login.
	if _, err := NewKubernetesObservationReader(apiURL, &http.Client{Timeout: 15 * time.Second}); err != nil {
		return nil, err
	}
	if strings.TrimSpace(caPath) == "" || strings.TrimSpace(tokenPath) == "" {
		return nil, ErrInvalidKueueObservation
	}
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	store, db, err := OpenDurablePostgresKueueJobBindingStore(ctx, bindingDSN)
	if err != nil {
		return nil, err
	}
	adapter, err := NewServiceAccountKueueJobAdapterWithBindings(apiURL, caPath, tokenPath, namespace, store)
	if err != nil {
		_ = db.Close()
		return nil, err
	}
	return adapter, nil
}

// NewServiceAccountKueueJobAdapterWithBindings reuses the authenticated,
// CA-pinned ServiceAccount transport and an explicit binding store. Tests
// and the disposable kind smoke pass either a memory store or a database
// they already opened. In-cluster deployment uses NewServiceAccountKueueJobAdapter,
// which opens the durable store itself and fail-closes. A
// PostgresKueueJobBindingStore keeps the pin and tombstone across a new
// database connection. This constructor does not mount an HTTP handler.
func NewServiceAccountKueueJobAdapterWithBindings(
	apiURL, caPath, tokenPath, namespace string, bindings KueueJobBindingStore,
) (*KueueJobAdapter, error) {
	if !validKubeName(namespace) || bindings == nil {
		return nil, ErrInvalidKueueObservation
	}
	client, closeIdle, err := newServiceAccountHTTPClient(caPath, tokenPath)
	if err != nil {
		return nil, err
	}
	adapter, err := NewKueueJobAdapterWithBindings(apiURL, namespace, client, bindings)
	if err != nil {
		closeIdle()
		return nil, err
	}
	return adapter, nil
}

func kueueCreateJobName(tenantID, operationID string) string {
	sum := sha256.Sum256([]byte(tenantID + "\n" + operationID))
	return "ccp-" + hex.EncodeToString(sum[:16])
}

func acceptableJobUID(uid string) bool {
	return uid != "" && uid == strings.TrimSpace(uid) &&
		!strings.ContainsRune(uid, 0) && !strings.ContainsAny(uid, "/\\ \r\n\t")
}

func kueueJobAnnotations(req Request) map[string]string {
	return map[string]string{
		kueueAnnotationTenantID:          req.TenantID,
		kueueAnnotationWorkloadID:        req.WorkloadID,
		kueueAnnotationDesiredGeneration: strconv.FormatUint(req.DesiredGeneration, 10),
		kueueAnnotationOperationID:       req.OperationID,
		kueueAnnotationRequestDigest:     req.RequestDigest,
		kueueAnnotationIdempotencyKey:    req.IdempotencyKey,
		kueueAnnotationProviderID:        req.ProviderID,
	}
}

func (a *KueueJobAdapter) lookupPin(req Request) (KueueCreatePin, bool, error) {
	if a == nil || a.bindings == nil {
		return KueueCreatePin{}, false, ErrInvalidKueueObservation
	}
	return a.bindings.GetCreatePin(a.namespace, req)
}

func (a *KueueJobAdapter) pin(req Request, name, uid string) error {
	if a == nil || a.bindings == nil {
		return ErrInvalidKueueObservation
	}
	return a.bindings.PutCreatePin(a.namespace, req, name, uid)
}

func (a *KueueJobAdapter) lookupDelete(req Request) (KueueDeleteTombstone, bool, error) {
	if a == nil || a.bindings == nil {
		return KueueDeleteTombstone{}, false, ErrInvalidKueueObservation
	}
	return a.bindings.GetDeleteTombstone(a.namespace, req)
}

func (a *KueueJobAdapter) recordDelete(req Request, name, uid string, acknowledged bool) error {
	if a == nil || a.bindings == nil {
		return ErrInvalidKueueObservation
	}
	return a.bindings.PutDeleteTombstone(a.namespace, req, name, uid, acknowledged)
}

func (a *KueueJobAdapter) pinnedUIDForName(name string) (string, bool, error) {
	if a == nil || a.bindings == nil {
		return "", false, ErrInvalidKueueObservation
	}
	return a.bindings.CreatePinUID(a.namespace, name)
}

// jobConflictReason reports why this Job must not be adopted. A pinned UID is
// never moved onto a successor object that reused the name.
func (a *KueueJobAdapter) jobConflictReason(req Request, name string, meta kubeMeta) (string, bool, error) {
	if meta.Name != name || meta.Namespace != a.namespace || !validKubernetesRef(refFromKube(meta)) ||
		!acceptableJobUID(meta.UID) {
		return "JOB_IDENTITY_MISMATCH", true, nil
	}
	got := meta.Annotations
	switch {
	case got[kueueAnnotationDesiredGeneration] != strconv.FormatUint(req.DesiredGeneration, 10):
		return "RESOURCE_GENERATION_SUPERSEDED", true, nil
	case got[kueueAnnotationTenantID] != req.TenantID ||
		got[kueueAnnotationWorkloadID] != req.WorkloadID ||
		got[kueueAnnotationProviderID] != req.ProviderID ||
		got[kueueAnnotationOperationID] != req.OperationID ||
		got[kueueAnnotationRequestDigest] != req.RequestDigest ||
		got[kueueAnnotationIdempotencyKey] != req.IdempotencyKey:
		return "JOB_IDENTITY_MISMATCH", true, nil
	}
	pin, ok, err := a.lookupPin(req)
	if err != nil {
		return "", false, err
	}
	if ok && (pin.UID != meta.UID || pin.Name != name || !sameRequest(pin.Request, req)) {
		return "JOB_UID_REPLACED", true, nil
	}
	return "", false, nil
}

func (a *KueueJobAdapter) jobsPath() string {
	return "/apis/batch/v1/namespaces/" + url.PathEscape(a.namespace) + "/jobs"
}

// Binding failures after an external side effect must preserve the UNKNOWN
// outcome: Kubernetes may already have applied the operation while the
// authoritative UID receipt or DELETE intent could not be committed.
func uncommittedKueueBinding(kind string, cause error) error {
	return fmt.Errorf("%w: %s binding not committed after Kubernetes side effect: %w", ErrAcknowledgementLost, kind, cause)
}

func (a *KueueJobAdapter) jobPath(name string) string {
	return a.jobsPath() + "/" + url.PathEscape(name)
}

func (a *KueueJobAdapter) exchange(ctx context.Context, method, path string, payload []byte) (int, []byte, error) {
	var body io.Reader
	if payload != nil {
		body = bytes.NewReader(payload)
	}
	req, err := http.NewRequestWithContext(ctx, method, a.apiURL+path, body)
	if err != nil {
		return 0, nil, err
	}
	req.Header.Set("Accept", "application/json")
	if payload != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	resp, err := a.client.Do(req)
	if err != nil {
		switch method {
		case http.MethodPost:
			return 0, nil, fmt.Errorf("%w: job create response lost: %v", ErrAcknowledgementLost, err)
		case http.MethodDelete:
			// The DELETE may already have been accepted. Do not report a clean failure.
			return 0, nil, fmt.Errorf("%w: job delete response lost: %v", ErrAcknowledgementLost, err)
		default:
			return 0, nil, err
		}
	}
	defer resp.Body.Close()
	read, err := io.ReadAll(io.LimitReader(resp.Body, 1<<20+1))
	if err != nil || len(read) > 1<<20 {
		switch method {
		case http.MethodPost:
			return 0, nil, fmt.Errorf("%w: job create response unreadable", ErrAcknowledgementLost)
		case http.MethodDelete:
			return 0, nil, fmt.Errorf("%w: job delete response unreadable", ErrAcknowledgementLost)
		default:
			if err != nil {
				return 0, nil, err
			}
			return 0, nil, ErrInvalidKueueObservation
		}
	}
	return resp.StatusCode, read, nil
}

// exactSuspendedJobSpec requires the inert, single-container Job this adapter
// actually creates. A name and matching annotations alone do not authorize
// adoption of a changed executable or a separately queued Job.
func exactSuspendedJobSpec(job kubeObject) bool {
	if job.Spec.Suspend == nil || !*job.Spec.Suspend ||
		job.Spec.Parallelism == nil || *job.Spec.Parallelism != 1 ||
		job.Spec.Completions == nil || *job.Spec.Completions != 1 ||
		job.Metadata.Labels["kueue.x-k8s.io/queue-name"] != "" ||
		job.Spec.Template.Spec.RestartPolicy != "Never" ||
		len(job.Spec.Template.Spec.Containers) != 1 {
		return false
	}
	container := job.Spec.Template.Spec.Containers[0]
	return container.Name == "ccp-suspended" && container.Image == kueueSuspendedJobImage
}

// exactJobSpec preserves the immutable submitted shape while allowing the
// Kueue controller to change only suspend. Admission is never assumed here.
func (a *KueueJobAdapter) exactJobSpec(job kubeObject) bool {
	if a.queueName == "" {
		return exactSuspendedJobSpec(job)
	}
	if job.Metadata.Labels["kueue.x-k8s.io/queue-name"] != a.queueName ||
		job.Spec.Parallelism == nil || *job.Spec.Parallelism != 1 ||
		job.Spec.Completions == nil || *job.Spec.Completions != 1 ||
		job.Spec.Template.Spec.RestartPolicy != "Never" ||
		len(job.Spec.Template.Spec.Containers) != 1 {
		return false
	}
	container := job.Spec.Template.Spec.Containers[0]
	return container.Name == "ccp-queued" && container.Image == kueueSuspendedJobImage &&
		len(container.Command) == 0 && len(container.Args) == 0 &&
		container.Resources != nil && len(container.Resources.Requests) == 2 &&
		len(container.Resources.Limits) == 0 &&
		container.Resources.Requests["cpu"] == "100m" && container.Resources.Requests["memory"] == "32Mi"
}

func (a *KueueJobAdapter) getJob(ctx context.Context, name string) (kubeMeta, int, error) {
	status, body, err := a.exchange(ctx, http.MethodGet, a.jobPath(name), nil)
	if err != nil {
		return kubeMeta{}, 0, fmt.Errorf("read kubernetes job: %w", err)
	}
	if status != http.StatusOK {
		return kubeMeta{}, status, nil
	}
	var obj kubeObject
	if json.Unmarshal(body, &obj) != nil {
		return kubeMeta{}, 0, ErrInvalidKueueObservation
	}
	if !a.exactJobSpec(obj) {
		return kubeMeta{}, 0, fmt.Errorf("%w: JOB_SPEC_MISMATCH", ErrOperationConflict)
	}
	return obj.Metadata, status, nil
}

type kueueBatchJob struct {
	APIVersion string            `json:"apiVersion"`
	Kind       string            `json:"kind"`
	Metadata   kueueBatchJobMeta `json:"metadata"`
	Spec       kueueBatchJobSpec `json:"spec"`
}

type kueueBatchJobMeta struct {
	Name        string            `json:"name"`
	Namespace   string            `json:"namespace"`
	Annotations map[string]string `json:"annotations"`
	Labels      map[string]string `json:"labels,omitempty"`
}

type kueueBatchJobSpec struct {
	Suspend     bool                  `json:"suspend"`
	Parallelism int                   `json:"parallelism"`
	Completions int                   `json:"completions"`
	Template    kueueBatchPodTemplate `json:"template"`
}

type kueueBatchPodTemplate struct {
	Spec kueueBatchPodSpec `json:"spec"`
}

type kueueBatchPodSpec struct {
	RestartPolicy string                `json:"restartPolicy"`
	Containers    []kueueBatchContainer `json:"containers"`
}

type kueueBatchContainer struct {
	Name      string               `json:"name"`
	Image     string               `json:"image"`
	Command   []string             `json:"command,omitempty"`
	Args      []string             `json:"args,omitempty"`
	Resources *kueueBatchResources `json:"resources,omitempty"`
}

type kueueBatchResources struct {
	Requests map[string]string `json:"requests,omitempty"`
	Limits   map[string]string `json:"limits,omitempty"`
}

func (a *KueueJobAdapter) jobBody(req Request, name string) ([]byte, error) {
	payload := kueueBatchJob{
		APIVersion: "batch/v1",
		Kind:       "Job",
		Metadata: kueueBatchJobMeta{
			Name:        name,
			Namespace:   a.namespace,
			Annotations: kueueJobAnnotations(req),
		},
		Spec: kueueBatchJobSpec{
			Suspend:     true,
			Parallelism: 1,
			Completions: 1,
			Template: kueueBatchPodTemplate{Spec: kueueBatchPodSpec{
				RestartPolicy: "Never",
				Containers: []kueueBatchContainer{{
					Name:  "ccp-suspended",
					Image: kueueSuspendedJobImage,
				}},
			}},
		},
	}
	if a.queueName != "" {
		payload.Metadata.Labels = map[string]string{"kueue.x-k8s.io/queue-name": a.queueName}
		payload.Spec.Suspend = false // Kueue's mutating webhook may immediately suspend it.
		payload.Spec.Template.Spec.Containers[0] = kueueBatchContainer{
			Name: "ccp-queued", Image: kueueSuspendedJobImage,
			Resources: &kueueBatchResources{Requests: map[string]string{"cpu": "100m", "memory": "32Mi"}},
		}
	}
	body, err := json.Marshal(payload)
	if err != nil {
		return nil, ErrInvalidRequest
	}
	return body, nil
}

func (a *KueueJobAdapter) postJob(ctx context.Context, req Request, name string) (kubeMeta, error) {
	body, err := a.jobBody(req, name)
	if err != nil {
		return kubeMeta{}, err
	}
	status, raw, err := a.exchange(ctx, http.MethodPost, a.jobsPath(), body)
	if err != nil {
		return kubeMeta{}, err
	}
	switch status {
	case http.StatusCreated, http.StatusOK:
		var obj kubeObject
		if json.Unmarshal(raw, &obj) != nil || !acceptableJobUID(obj.Metadata.UID) ||
			obj.Metadata.Name != name || obj.Metadata.Namespace != a.namespace ||
			!validKubernetesRef(refFromKube(obj.Metadata)) ||
			!a.exactJobSpec(obj) {
			return kubeMeta{}, ErrAcknowledgementLost
		}
		reason, bad, err := a.jobConflictReason(req, name, obj.Metadata)
		if err != nil {
			// The API has already accepted CREATE. A failed binding lookup
			// is UNKNOWN, not evidence that no Job was created.
			return kubeMeta{}, uncommittedKueueBinding("CREATE", err)
		}
		if bad {
			return kubeMeta{}, fmt.Errorf("%w: %s", ErrOperationConflict, reason)
		}
		return obj.Metadata, nil
	case http.StatusConflict:
		return kubeMeta{}, errKueueJobAlreadyExists
	case http.StatusBadRequest, http.StatusUnauthorized, http.StatusForbidden, http.StatusUnprocessableEntity:
		return kubeMeta{}, fmt.Errorf("%w: job create rejected with HTTP %d", ErrInvalidRequest, status)
	default:
		return kubeMeta{}, fmt.Errorf("%w: job create returned HTTP %d", ErrAcknowledgementLost, status)
	}
}

func (a *KueueJobAdapter) acknowledgeExisting(ctx context.Context, req Request, name string) (Acknowledgement, error) {
	meta, status, err := a.getJob(ctx, name)
	if errors.Is(err, ErrOperationConflict) {
		return Acknowledgement{}, err
	}
	if err != nil || status != http.StatusOK {
		if err != nil && ctx.Err() != nil {
			return Acknowledgement{}, fmt.Errorf("%w: %w", ErrAcknowledgementLost, ctx.Err())
		}
		return Acknowledgement{}, ErrAcknowledgementLost
	}
	reason, bad, err := a.jobConflictReason(req, name, meta)
	if err != nil {
		return Acknowledgement{}, err
	}
	if bad {
		return Acknowledgement{}, fmt.Errorf("%w: %s", ErrOperationConflict, reason)
	}
	if _, pinned, err := a.lookupPin(req); err != nil {
		return Acknowledgement{}, err
	} else if !pinned {
		// A 409 after a lost response is NOT proof this is the original Job.
		return Acknowledgement{}, ErrAcknowledgementLost
	}
	if err := a.pin(req, name, meta.UID); err != nil {
		return Acknowledgement{}, err
	}
	return Acknowledgement{OperationID: req.OperationID, RequestDigest: req.RequestDigest}, nil
}

// Apply creates one default suspended Job (or explicitly configured queued
// Job) for CREATE and pins its UID, or deletes that exact UID for DELETE.
// An identical replay
// does not post again and, once DELETE is acknowledged, does not delete again.
// A later adapter with the same binding store can make that replay. UPDATE is
// rejected. A lost or unreadable create or delete response returns
// ErrAcknowledgementLost after the side effect may already have happened.
func (a *KueueJobAdapter) Apply(ctx context.Context, req Request) (Acknowledgement, error) {
	if err := ctx.Err(); err != nil {
		return Acknowledgement{}, err
	}
	if a == nil || a.reader == nil {
		return Acknowledgement{}, ErrInvalidKueueObservation
	}
	if err := req.Validate(); err != nil {
		return Acknowledgement{}, err
	}
	if req.Kind == operation.Delete {
		return a.applyDelete(ctx, req)
	}
	if req.Kind != operation.Create {
		return Acknowledgement{}, ErrInvalidRequest
	}
	name := kueueCreateJobName(req.TenantID, req.OperationID)
	pin, pinned, err := a.lookupPin(req)
	if err != nil {
		return Acknowledgement{}, err
	}
	if pinned && !sameRequest(pin.Request, req) {
		return Acknowledgement{}, ErrOperationConflict
	}
	meta, status, err := a.getJob(ctx, name)
	if err != nil {
		return Acknowledgement{}, err
	}
	switch status {
	case http.StatusOK:
		reason, bad, err := a.jobConflictReason(req, name, meta)
		if err != nil {
			return Acknowledgement{}, err
		}
		if bad {
			return Acknowledgement{}, fmt.Errorf("%w: %s", ErrOperationConflict, reason)
		}
		if _, pinned, err := a.lookupPin(req); err != nil {
			return Acknowledgement{}, err
		} else if !pinned {
			// Matching annotations alone cannot prove the original UID.
			return Acknowledgement{}, ErrAcknowledgementLost
		}
		if err := a.pin(req, name, meta.UID); err != nil {
			return Acknowledgement{}, err
		}
		return Acknowledgement{OperationID: req.OperationID, RequestDigest: req.RequestDigest}, nil
	case http.StatusNotFound:
		if _, pinned, err := a.lookupPin(req); err != nil {
			return Acknowledgement{}, err
		} else if pinned {
			return Acknowledgement{}, fmt.Errorf("%w: pinned job is absent", ErrOperationConflict)
		}
	default:
		return Acknowledgement{}, fmt.Errorf("%w: job lookup returned HTTP %d", ErrInvalidKueueObservation, status)
	}
	created, err := a.postJob(ctx, req, name)
	if errors.Is(err, errKueueJobAlreadyExists) {
		return a.acknowledgeExisting(ctx, req, name)
	}
	if err != nil {
		return Acknowledgement{}, err
	}
	if err := a.pin(req, name, created.UID); err != nil {
		return Acknowledgement{}, uncommittedKueueBinding("CREATE", err)
	}
	return Acknowledgement{OperationID: req.OperationID, RequestDigest: req.RequestDigest}, nil
}

func (a *KueueJobAdapter) deleteJobName(req Request) (string, error) {
	namespace, name, ok := strings.Cut(req.TargetResourceID, "/")
	if !ok || namespace != a.namespace || req.TargetResourceID != namespace+"/"+name ||
		!validKubeName(name) || !acceptableJobUID(req.TargetResourceUID) {
		return "", ErrInvalidRequest
	}
	return name, nil
}

// deleteConflictReason refuses a same-name object that is not the exact UID
// and desired generation this DELETE named. The successor is left in place.
func (a *KueueJobAdapter) deleteConflictReason(req Request, name string, meta kubeMeta) (string, bool, error) {
	if meta.Name != name || meta.Namespace != a.namespace || !acceptableJobUID(meta.UID) {
		return "JOB_IDENTITY_MISMATCH", true, nil
	}
	if meta.UID != req.TargetResourceUID {
		return "JOB_UID_REPLACED", true, nil
	}
	got := meta.Annotations
	if got[kueueAnnotationDesiredGeneration] != strconv.FormatUint(req.DesiredGeneration, 10) {
		return "RESOURCE_GENERATION_SUPERSEDED", true, nil
	}
	if got[kueueAnnotationTenantID] != req.TenantID ||
		got[kueueAnnotationWorkloadID] != req.WorkloadID ||
		got[kueueAnnotationProviderID] != req.ProviderID {
		return "JOB_IDENTITY_MISMATCH", true, nil
	}
	pinUID, pinned, err := a.pinnedUIDForName(name)
	if err != nil {
		return "", false, err
	}
	if pinned && pinUID != req.TargetResourceUID {
		return "JOB_UID_REPLACED", true, nil
	}
	intent, ok, err := a.lookupDelete(req)
	if err != nil {
		return "", false, err
	}
	if ok && (intent.UID != req.TargetResourceUID || intent.Name != name || !sameRequest(intent.Request, req)) {
		return "JOB_UID_REPLACED", true, nil
	}
	return "", false, nil
}

type kueueDeleteOptions struct {
	APIVersion    string                   `json:"apiVersion"`
	Kind          string                   `json:"kind"`
	Preconditions kueueDeletePreconditions `json:"preconditions"`
}

type kueueDeletePreconditions struct {
	UID string `json:"uid"`
}

func kueueDeleteResponseAccepted(raw []byte, namespace, name, uid string) bool {
	if len(raw) == 0 || !json.Valid(raw) {
		return false
	}
	// Job status is an object. A metav1.Status response uses the same JSON
	// field as a string. Keep it raw so a deleted Job body still decodes.
	var probe struct {
		Kind     string          `json:"kind"`
		Status   json.RawMessage `json:"status"`
		Metadata kubeMeta        `json:"metadata"`
	}
	if json.Unmarshal(raw, &probe) != nil {
		return false
	}
	if probe.Kind == "Status" {
		var status string
		return json.Unmarshal(probe.Status, &status) == nil && status == "Success"
	}
	if probe.Metadata.Namespace != "" && probe.Metadata.Namespace != namespace {
		return false
	}
	return probe.Metadata.Name == name && probe.Metadata.UID == uid
}

func (a *KueueJobAdapter) deleteJob(ctx context.Context, req Request, name, uid string) (Acknowledgement, error) {
	body, err := json.Marshal(kueueDeleteOptions{
		APIVersion:    "v1",
		Kind:          "DeleteOptions",
		Preconditions: kueueDeletePreconditions{UID: uid},
	})
	if err != nil {
		return Acknowledgement{}, ErrInvalidRequest
	}
	status, raw, err := a.exchange(ctx, http.MethodDelete, a.jobPath(name), body)
	if err != nil {
		if recErr := a.recordDelete(req, name, uid, false); recErr != nil {
			return Acknowledgement{}, uncommittedKueueBinding("DELETE", recErr)
		}
		return Acknowledgement{}, err
	}
	switch status {
	case http.StatusOK, http.StatusAccepted:
		if !kueueDeleteResponseAccepted(raw, a.namespace, name, uid) {
			if recErr := a.recordDelete(req, name, uid, false); recErr != nil {
				return Acknowledgement{}, recErr
			}
			return Acknowledgement{}, fmt.Errorf("%w: job delete response unreadable", ErrAcknowledgementLost)
		}
		if err := a.recordDelete(req, name, uid, true); err != nil {
			return Acknowledgement{}, uncommittedKueueBinding("DELETE", err)
		}
		return Acknowledgement{OperationID: req.OperationID, RequestDigest: req.RequestDigest}, nil
	case http.StatusNotFound:
		// The fenced name is already gone. A successor at this name would be a
		// different UID and a precondition conflict, not a 404.
		if err := a.recordDelete(req, name, uid, true); err != nil {
			return Acknowledgement{}, uncommittedKueueBinding("DELETE", err)
		}
		return Acknowledgement{OperationID: req.OperationID, RequestDigest: req.RequestDigest}, nil
	case http.StatusConflict:
		// UID precondition failed before deletion. The live object was not deleted.
		return Acknowledgement{}, fmt.Errorf("%w: JOB_UID_REPLACED", ErrOperationConflict)
	case http.StatusBadRequest, http.StatusUnauthorized, http.StatusForbidden, http.StatusUnprocessableEntity:
		return Acknowledgement{}, fmt.Errorf("%w: job delete rejected with HTTP %d", ErrInvalidRequest, status)
	default:
		if recErr := a.recordDelete(req, name, uid, false); recErr != nil {
			return Acknowledgement{}, uncommittedKueueBinding("DELETE", recErr)
		}
		return Acknowledgement{}, fmt.Errorf("%w: job delete returned HTTP %d", ErrAcknowledgementLost, status)
	}
}

// applyDelete removes one Job only when its UID is the request target UID.
// A replay after acknowledgement re-reads the name and does not DELETE again,
// including when a new adapter loaded that acknowledgement from the binding
// store, so a successor that reused the name cannot be removed by this
// operation. An unacknowledged tombstone must re-GET and must not delete a
// different UID.
func (a *KueueJobAdapter) applyDelete(ctx context.Context, req Request) (Acknowledgement, error) {
	name, err := a.deleteJobName(req)
	if err != nil {
		return Acknowledgement{}, err
	}
	intent, ok, err := a.lookupDelete(req)
	if err != nil {
		return Acknowledgement{}, err
	}
	if ok && !sameRequest(intent.Request, req) {
		return Acknowledgement{}, ErrOperationConflict
	}
	meta, status, err := a.getJob(ctx, name)
	if err != nil {
		return Acknowledgement{}, err
	}
	switch status {
	case http.StatusNotFound:
		intent, ok, err = a.lookupDelete(req)
		if err != nil {
			return Acknowledgement{}, err
		}
		if !ok || intent.UID != req.TargetResourceUID || intent.Name != name {
			return Acknowledgement{}, fmt.Errorf("%w: delete target is absent", ErrOperationConflict)
		}
		if err = a.recordDelete(req, name, req.TargetResourceUID, true); err != nil {
			return Acknowledgement{}, uncommittedKueueBinding("DELETE", err)
		}
		return Acknowledgement{OperationID: req.OperationID, RequestDigest: req.RequestDigest}, nil
	case http.StatusOK:
		var reason string
		var bad bool
		reason, bad, err = a.deleteConflictReason(req, name, meta)
		if err != nil {
			return Acknowledgement{}, err
		}
		if bad {
			return Acknowledgement{}, fmt.Errorf("%w: %s", ErrOperationConflict, reason)
		}
		intent, ok, err = a.lookupDelete(req)
		if err != nil {
			return Acknowledgement{}, err
		}
		if ok && intent.Acknowledged {
			return Acknowledgement{OperationID: req.OperationID, RequestDigest: req.RequestDigest}, nil
		}
		return a.deleteJob(ctx, req, name, meta.UID)
	default:
		return Acknowledgement{}, fmt.Errorf("%w: job lookup returned HTTP %d", ErrInvalidKueueObservation, status)
	}
}

func (a *KueueJobAdapter) observationQuery(req Request, name, uid string) KubernetesJobQuery {
	return KubernetesJobQuery{
		TenantID: req.TenantID, WorkloadID: req.WorkloadID,
		DesiredGeneration: req.DesiredGeneration,
		Namespace:         a.namespace, JobName: name, ExpectedJobUID: uid,
	}
}

func unboundKueueJobObservation(req Request) operation.Observation {
	return operation.Observation{
		OperationID: req.OperationID, TenantID: req.TenantID,
		WorkloadID: req.WorkloadID, ProviderID: req.ProviderID,
		DesiredGeneration: req.DesiredGeneration,
		ObservedAt:        time.Now().UTC(),
		ResourceVersion:   "kubernetes-job/uid-unbound",
		Result:            operation.OperationInProgress,
	}
}

func conflictObservation(req Request, reason string) operation.Observation {
	return operation.Observation{
		OperationID: req.OperationID, TenantID: req.TenantID,
		WorkloadID: req.WorkloadID, ProviderID: req.ProviderID,
		DesiredGeneration: req.DesiredGeneration, ObservedAt: time.Now().UTC(),
		ResourceVersion: "kubernetes-job/unrecorded",
		Result:          operation.ObservationConflict, ReasonCode: reason,
	}
}

func absentObservation(req Request) operation.Observation {
	return operation.Observation{
		OperationID: req.OperationID, TenantID: req.TenantID,
		WorkloadID: req.WorkloadID, ProviderID: req.ProviderID,
		DesiredGeneration: req.DesiredGeneration, ObservedAt: time.Now().UTC(),
		ResourceVersion:   "kubernetes-job/absent",
		Result:            operation.TargetAbsent,
		TargetResourceID:  req.TargetResourceID,
		TargetResourceUID: req.TargetResourceUID,
	}
}

// Inspect re-reads the Job through the existing UID-pinned collector for
// CREATE. A missing Job on CREATE is a conflict, not authoritative proof that
// CREATE never happened. Every successful stage, including POD_PLACED, becomes
// ResourcePresent for the Job object only. DELETE absence is reported only
// when the binding store has a tombstone for that exact UID. A tombstone is
// not Allocation cleanup or quota release. Without that tombstone, a 404
// stays a conflict.
func (a *KueueJobAdapter) Inspect(ctx context.Context, req Request) (operation.Observation, error) {
	if err := ctx.Err(); err != nil {
		return operation.Observation{}, err
	}
	if a == nil || a.reader == nil {
		return operation.Observation{}, ErrInvalidKueueObservation
	}
	if err := req.Validate(); err != nil {
		return operation.Observation{}, err
	}
	if req.Kind == operation.Delete {
		return a.inspectDelete(ctx, req)
	}
	if req.Kind != operation.Create {
		return operation.Observation{}, ErrInvalidRequest
	}
	name := kueueCreateJobName(req.TenantID, req.OperationID)
	meta, status, err := a.getJob(ctx, name)
	if errors.Is(err, ErrOperationConflict) {
		return conflictObservation(req, "JOB_SPEC_MISMATCH"), nil
	}
	if err != nil {
		return operation.Observation{}, err
	}
	if status == http.StatusNotFound {
		return conflictObservation(req, "MISSING_AUTHORITATIVE_OPERATION"), nil
	}
	if status != http.StatusOK {
		return operation.Observation{}, fmt.Errorf("%w: job lookup returned HTTP %d", ErrInvalidKueueObservation, status)
	}
	if reason, bad, err := a.jobConflictReason(req, name, meta); err != nil {
		return operation.Observation{}, err
	} else if bad {
		uid := meta.UID
		if pin, ok, err := a.lookupPin(req); err != nil {
			return operation.Observation{}, err
		} else if ok && acceptableJobUID(pin.UID) {
			uid = pin.UID
		}
		// Ask the existing reader as well. A local fence failure still wins,
		// including when the reader would accept a looser annotation match.
		if _, err := a.reader.Collect(ctx, a.observationQuery(req, name, uid)); ctx.Err() != nil {
			return operation.Observation{}, ctx.Err()
		} else if err != nil && !errors.Is(err, ErrInvalidKueueObservation) {
			return operation.Observation{}, err
		}
		return conflictObservation(req, reason), nil
	}
	if _, pinned, err := a.lookupPin(req); err != nil {
		return operation.Observation{}, err
	} else if !pinned {
		// A matching live Job is not proof of the original CREATE without a
		// stored pin. Keep persisted UNKNOWN; never return that UID as
		// ResourcePresent.
		return unboundKueueJobObservation(req), nil
	}
	collected, err := a.reader.Collect(ctx, a.observationQuery(req, name, meta.UID))
	if err != nil {
		if errors.Is(err, ErrInvalidKueueObservation) && !strings.Contains(err.Error(), "Kubernetes API returned HTTP") {
			return conflictObservation(req, "KUEUE_OBSERVATION_UNPROVEN"), nil
		}
		return operation.Observation{}, err
	}
	if collected.Evidence.Job.UID != meta.UID || collected.Stage == "" {
		return conflictObservation(req, "KUEUE_OBSERVATION_UNPROVEN"), nil
	}
	if err := a.pin(req, name, meta.UID); err != nil {
		return conflictObservation(req, "JOB_UID_REPLACED"), nil
	}
	// collected.Stage is UNPROVEN, KUEUE_ADMITTED, or POD_PLACED. None of
	// those values is copied onto the operation result. ResourcePresent means
	// the pinned Job object is still the observed object, not Allocation READY.
	return operation.Observation{
		OperationID: req.OperationID, TenantID: req.TenantID,
		WorkloadID: req.WorkloadID, ProviderID: req.ProviderID,
		DesiredGeneration:   req.DesiredGeneration,
		Result:              operation.ResourcePresent,
		ResourceID:          a.namespace + "/" + name,
		ResourceUID:         collected.Evidence.Job.UID,
		ResourceOperationID: req.OperationID,
		ResourceGeneration:  req.DesiredGeneration,
		ResourceVersion:     collected.Evidence.Job.ResourceVersion,
		ObservedAt:          collected.Provenance.ObservedAt,
	}, nil
}

// inspectDelete reports TARGET_ABSENT only when the binding store has a
// tombstone for that UID and the name is now authoritatively gone. A 404
// with no tombstone stays a conflict. The live successor is not adopted.
// TARGET_ABSENT is not Allocation cleanup, quota release, or device release.
func (a *KueueJobAdapter) inspectDelete(ctx context.Context, req Request) (operation.Observation, error) {
	name, err := a.deleteJobName(req)
	if err != nil {
		return operation.Observation{}, err
	}
	meta, status, err := a.getJob(ctx, name)
	if err != nil {
		return operation.Observation{}, err
	}
	intent, intended, err := a.lookupDelete(req)
	if err != nil {
		return operation.Observation{}, err
	}
	if intended && (!sameRequest(intent.Request, req) || intent.Name != name || intent.UID != req.TargetResourceUID) {
		return conflictObservation(req, "OPERATION_FINGERPRINT_MISMATCH"), nil
	}
	switch status {
	case http.StatusNotFound:
		if !intended {
			return conflictObservation(req, "MISSING_AUTHORITATIVE_OPERATION"), nil
		}
		return absentObservation(req), nil
	case http.StatusOK:
		reason, bad, err := a.deleteConflictReason(req, name, meta)
		if err != nil {
			return operation.Observation{}, err
		}
		if bad {
			return conflictObservation(req, reason), nil
		}
		if intended {
			return conflictObservation(req, "DELETE_TARGET_STILL_PRESENT"), nil
		}
		return conflictObservation(req, "OPERATION_STATE_UNCONFIRMED"), nil
	default:
		return operation.Observation{}, fmt.Errorf("%w: job lookup returned HTTP %d", ErrInvalidKueueObservation, status)
	}
}

var _ Adapter = (*KueueJobAdapter)(nil)
