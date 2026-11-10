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
	"sync"
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

	// kueueSuspendedJobImage fills the batch/v1 schema only. The Job is created
	// suspended and without a Kueue queue label, so this slice does not ask
	// Kueue to admit it and does not ask a kubelet to run it.
	kueueSuspendedJobImage = "registry.k8s.io/pause:3.10"
)

var errKueueJobAlreadyExists = errors.New("kubernetes job already exists")

// KueueJobAdapter is a first-cut batch/v1 Job writer. Apply accepts CREATE
// and DELETE. CREATE posts one suspended Job and pins the returned UID in
// memory. DELETE GETs that name and deletes only the pinned target UID.
// CREATE Inspect reads that UID back through KubernetesObservationReader.Collect.
//
// Structural stages UNPROVEN, KUEUE_ADMITTED and POD_PLACED all mean only that
// the pinned Job object was observed. POD_PLACED is not Allocation READY.
// DELETE absence is process-local intent plus a UID-fenced 404. It is not
// Allocation cleanup, quota release, or a durable tombstone: after a process
// restart a bare 404 is still ambiguous. This type does not admit, schedule,
// claim devices, or mount an HTTP handler. Process-local pins are recovered
// from Job annotations after a lost create response; they are not a second database.
type KueueJobAdapter struct {
	namespace string
	apiURL    string
	client    http.Client
	reader    *KubernetesObservationReader

	mu      sync.Mutex
	pins    map[string]kueuePinnedJob
	deletes map[string]kueueDeleteIntent
}

type kueuePinnedJob struct {
	name    string
	uid     string
	request Request
}

// kueueDeleteIntent records that this process issued a UID-fenced DELETE.
// Acknowledged means the API accepted that exact UID or then reported it gone.
// An unacknowledged intent is a lost response: later calls must re-GET and
// must not delete a different UID. This map is not durable across restarts.
type kueueDeleteIntent struct {
	name         string
	uid          string
	request      Request
	acknowledged bool
}

// NewKueueJobAdapter binds CREATE and Inspect to one namespace and an HTTPS
// client the caller has already authenticated. The client is shared with the
// read-only reader. No cluster credential is built here.
func NewKueueJobAdapter(apiURL, namespace string, client *http.Client) (*KueueJobAdapter, error) {
	if !validKubeName(namespace) {
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
		pins:      make(map[string]kueuePinnedJob),
		deletes:   make(map[string]kueueDeleteIntent),
	}, nil
}

// NewServiceAccountKueueJobAdapter uses the same pinned ServiceAccount HTTPS
// client as the read-only observer. Bind a separate Role: create, delete, and
// get batch/v1 Jobs, plus list Pods and Kueue Workloads for Inspect. Do not add
// update, patch, or cluster-admin, and do not add create or delete to the
// observer Role. Provenance from Inspect stays unattested.
func NewServiceAccountKueueJobAdapter(apiURL, caPath, tokenPath, namespace string) (*KueueJobAdapter, error) {
	if !validKubeName(namespace) {
		return nil, ErrInvalidKueueObservation
	}
	client, closeIdle, err := newServiceAccountHTTPClient(caPath, tokenPath)
	if err != nil {
		return nil, err
	}
	adapter, err := NewKueueJobAdapter(apiURL, namespace, client)
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

func kueuePinKey(req Request) string {
	return req.TenantID + "\x00" + req.OperationID
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

func (a *KueueJobAdapter) lookupPin(req Request) (kueuePinnedJob, bool) {
	a.mu.Lock()
	defer a.mu.Unlock()
	pin, ok := a.pins[kueuePinKey(req)]
	return pin, ok
}

func (a *KueueJobAdapter) pin(req Request, name, uid string) error {
	if !validKubeName(name) || !acceptableJobUID(uid) {
		return ErrInvalidKueueObservation
	}
	a.mu.Lock()
	defer a.mu.Unlock()
	key := kueuePinKey(req)
	if prev, ok := a.pins[key]; ok {
		if prev.uid != uid || prev.name != name || !sameRequest(prev.request, req) {
			return ErrOperationConflict
		}
		return nil
	}
	a.pins[key] = kueuePinnedJob{name: name, uid: uid, request: req}
	return nil
}

func (a *KueueJobAdapter) lookupDelete(req Request) (kueueDeleteIntent, bool) {
	a.mu.Lock()
	defer a.mu.Unlock()
	intent, ok := a.deletes[kueuePinKey(req)]
	return intent, ok
}

func (a *KueueJobAdapter) recordDelete(req Request, name, uid string, acknowledged bool) error {
	if !validKubeName(name) || !acceptableJobUID(uid) {
		return ErrInvalidKueueObservation
	}
	a.mu.Lock()
	defer a.mu.Unlock()
	key := kueuePinKey(req)
	if prev, ok := a.deletes[key]; ok {
		if prev.uid != uid || prev.name != name || !sameRequest(prev.request, req) {
			return ErrOperationConflict
		}
		if prev.acknowledged {
			acknowledged = true
		}
	}
	a.deletes[key] = kueueDeleteIntent{
		name: name, uid: uid, request: req, acknowledged: acknowledged,
	}
	return nil
}

func (a *KueueJobAdapter) pinnedUIDForName(name string) (string, bool) {
	a.mu.Lock()
	defer a.mu.Unlock()
	for _, pin := range a.pins {
		if pin.name == name {
			return pin.uid, true
		}
	}
	return "", false
}

// jobConflictReason reports why this Job must not be adopted. A pinned UID is
// never moved onto a successor object that reused the name.
func (a *KueueJobAdapter) jobConflictReason(req Request, name string, meta kubeMeta) (string, bool) {
	if meta.Name != name || meta.Namespace != a.namespace || !validKubernetesRef(refFromKube(meta)) ||
		!acceptableJobUID(meta.UID) {
		return "JOB_IDENTITY_MISMATCH", true
	}
	got := meta.Annotations
	switch {
	case got[kueueAnnotationDesiredGeneration] != strconv.FormatUint(req.DesiredGeneration, 10):
		return "RESOURCE_GENERATION_SUPERSEDED", true
	case got[kueueAnnotationTenantID] != req.TenantID ||
		got[kueueAnnotationWorkloadID] != req.WorkloadID ||
		got[kueueAnnotationProviderID] != req.ProviderID ||
		got[kueueAnnotationOperationID] != req.OperationID ||
		got[kueueAnnotationRequestDigest] != req.RequestDigest ||
		got[kueueAnnotationIdempotencyKey] != req.IdempotencyKey:
		return "JOB_IDENTITY_MISMATCH", true
	}
	if pin, ok := a.lookupPin(req); ok && (pin.uid != meta.UID || pin.name != name || !sameRequest(pin.request, req)) {
		return "JOB_UID_REPLACED", true
	}
	return "", false
}

func (a *KueueJobAdapter) jobsPath() string {
	return "/apis/batch/v1/namespaces/" + url.PathEscape(a.namespace) + "/jobs"
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
	Name  string `json:"name"`
	Image string `json:"image"`
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
			!validKubernetesRef(refFromKube(obj.Metadata)) {
			return kubeMeta{}, ErrAcknowledgementLost
		}
		if reason, bad := a.jobConflictReason(req, name, obj.Metadata); bad {
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
	if err != nil || status != http.StatusOK {
		if err != nil && ctx.Err() != nil {
			return Acknowledgement{}, fmt.Errorf("%w: %w", ErrAcknowledgementLost, ctx.Err())
		}
		return Acknowledgement{}, ErrAcknowledgementLost
	}
	if reason, bad := a.jobConflictReason(req, name, meta); bad {
		return Acknowledgement{}, fmt.Errorf("%w: %s", ErrOperationConflict, reason)
	}
	if err := a.pin(req, name, meta.UID); err != nil {
		return Acknowledgement{}, err
	}
	return Acknowledgement{OperationID: req.OperationID, RequestDigest: req.RequestDigest}, nil
}

// Apply creates one suspended batch/v1 Job for a CREATE operation and pins its
// UID, or deletes that exact UID for a DELETE operation. An identical replay
// does not post again and, once DELETE is acknowledged, does not delete again.
// UPDATE is rejected. A lost or unreadable create or delete response returns
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
	if pin, pinned := a.lookupPin(req); pinned && !sameRequest(pin.request, req) {
		return Acknowledgement{}, ErrOperationConflict
	}
	meta, status, err := a.getJob(ctx, name)
	if err != nil {
		return Acknowledgement{}, err
	}
	switch status {
	case http.StatusOK:
		if reason, bad := a.jobConflictReason(req, name, meta); bad {
			return Acknowledgement{}, fmt.Errorf("%w: %s", ErrOperationConflict, reason)
		}
		if err := a.pin(req, name, meta.UID); err != nil {
			return Acknowledgement{}, err
		}
		return Acknowledgement{OperationID: req.OperationID, RequestDigest: req.RequestDigest}, nil
	case http.StatusNotFound:
		if _, pinned := a.lookupPin(req); pinned {
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
		return Acknowledgement{}, err
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
func (a *KueueJobAdapter) deleteConflictReason(req Request, name string, meta kubeMeta) (string, bool) {
	if meta.Name != name || meta.Namespace != a.namespace || !acceptableJobUID(meta.UID) {
		return "JOB_IDENTITY_MISMATCH", true
	}
	if meta.UID != req.TargetResourceUID {
		return "JOB_UID_REPLACED", true
	}
	got := meta.Annotations
	if got[kueueAnnotationDesiredGeneration] != strconv.FormatUint(req.DesiredGeneration, 10) {
		return "RESOURCE_GENERATION_SUPERSEDED", true
	}
	if got[kueueAnnotationTenantID] != req.TenantID ||
		got[kueueAnnotationWorkloadID] != req.WorkloadID ||
		got[kueueAnnotationProviderID] != req.ProviderID {
		return "JOB_IDENTITY_MISMATCH", true
	}
	if pinUID, ok := a.pinnedUIDForName(name); ok && pinUID != req.TargetResourceUID {
		return "JOB_UID_REPLACED", true
	}
	if intent, ok := a.lookupDelete(req); ok &&
		(intent.uid != req.TargetResourceUID || intent.name != name || !sameRequest(intent.request, req)) {
		return "JOB_UID_REPLACED", true
	}
	return "", false
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
			return Acknowledgement{}, recErr
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
			return Acknowledgement{}, err
		}
		return Acknowledgement{OperationID: req.OperationID, RequestDigest: req.RequestDigest}, nil
	case http.StatusNotFound:
		// The fenced name is already gone. A successor at this name would be a
		// different UID and a precondition conflict, not a 404.
		if err := a.recordDelete(req, name, uid, true); err != nil {
			return Acknowledgement{}, err
		}
		return Acknowledgement{OperationID: req.OperationID, RequestDigest: req.RequestDigest}, nil
	case http.StatusConflict:
		// UID precondition failed before deletion. The live object was not deleted.
		return Acknowledgement{}, fmt.Errorf("%w: JOB_UID_REPLACED", ErrOperationConflict)
	case http.StatusBadRequest, http.StatusUnauthorized, http.StatusForbidden, http.StatusUnprocessableEntity:
		return Acknowledgement{}, fmt.Errorf("%w: job delete rejected with HTTP %d", ErrInvalidRequest, status)
	default:
		if recErr := a.recordDelete(req, name, uid, false); recErr != nil {
			return Acknowledgement{}, recErr
		}
		return Acknowledgement{}, fmt.Errorf("%w: job delete returned HTTP %d", ErrAcknowledgementLost, status)
	}
}

// applyDelete removes one Job only when its UID is the request target UID.
// A replay after acknowledgement re-reads the name and does not DELETE again,
// so a successor that reused the name cannot be removed by this operation.
func (a *KueueJobAdapter) applyDelete(ctx context.Context, req Request) (Acknowledgement, error) {
	name, err := a.deleteJobName(req)
	if err != nil {
		return Acknowledgement{}, err
	}
	if intent, ok := a.lookupDelete(req); ok && !sameRequest(intent.request, req) {
		return Acknowledgement{}, ErrOperationConflict
	}
	meta, status, err := a.getJob(ctx, name)
	if err != nil {
		return Acknowledgement{}, err
	}
	switch status {
	case http.StatusNotFound:
		intent, ok := a.lookupDelete(req)
		if !ok || intent.uid != req.TargetResourceUID || intent.name != name {
			return Acknowledgement{}, fmt.Errorf("%w: delete target is absent", ErrOperationConflict)
		}
		if err := a.recordDelete(req, name, req.TargetResourceUID, true); err != nil {
			return Acknowledgement{}, err
		}
		return Acknowledgement{OperationID: req.OperationID, RequestDigest: req.RequestDigest}, nil
	case http.StatusOK:
		if reason, bad := a.deleteConflictReason(req, name, meta); bad {
			return Acknowledgement{}, fmt.Errorf("%w: %s", ErrOperationConflict, reason)
		}
		if intent, ok := a.lookupDelete(req); ok && intent.acknowledged {
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
// after this process recorded an intended delete of that exact UID.
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
	if err != nil {
		return operation.Observation{}, err
	}
	if status == http.StatusNotFound {
		return conflictObservation(req, "MISSING_AUTHORITATIVE_OPERATION"), nil
	}
	if status != http.StatusOK {
		return operation.Observation{}, fmt.Errorf("%w: job lookup returned HTTP %d", ErrInvalidKueueObservation, status)
	}
	if reason, bad := a.jobConflictReason(req, name, meta); bad {
		uid := meta.UID
		if pin, ok := a.lookupPin(req); ok && acceptableJobUID(pin.uid) {
			uid = pin.uid
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

// inspectDelete reports TARGET_ABSENT only when this process intended to
// delete that UID and the name is now authoritatively gone. A 404 with no
// such intent stays a conflict. The live successor is not adopted.
func (a *KueueJobAdapter) inspectDelete(ctx context.Context, req Request) (operation.Observation, error) {
	name, err := a.deleteJobName(req)
	if err != nil {
		return operation.Observation{}, err
	}
	meta, status, err := a.getJob(ctx, name)
	if err != nil {
		return operation.Observation{}, err
	}
	intent, intended := a.lookupDelete(req)
	if intended && (!sameRequest(intent.request, req) || intent.name != name || intent.uid != req.TargetResourceUID) {
		return conflictObservation(req, "OPERATION_FINGERPRINT_MISMATCH"), nil
	}
	switch status {
	case http.StatusNotFound:
		if !intended {
			return conflictObservation(req, "MISSING_AUTHORITATIVE_OPERATION"), nil
		}
		return absentObservation(req), nil
	case http.StatusOK:
		if reason, bad := a.deleteConflictReason(req, name, meta); bad {
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
