package provider

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"reflect"
	"strings"
	"time"

	"github.com/avaargsh/compute-control-panel/internal/operation"
)

// PDJobSetPlan must be loaded from an independently authorized, durable PD
// Intent. This API deliberately does not accept a tenant-supplied image,
// command, node selector, privileged runtime or mutable ResourceClaim name.
type PDJobSetPlan struct {
	IntentID                  string
	PrefillReplicas           int
	DecodeReplicas            int
	PrefillGPUsPerReplica     int
	DecodeGPUsPerReplica      int
	ResourceClaimTemplateName string
}

func validPDJobSetPlan(p PDJobSetPlan) bool {
	digest, err := hex.DecodeString(p.IntentID)
	return err == nil && len(digest) == sha256.Size && strings.ToLower(p.IntentID) == p.IntentID &&
		p.PrefillReplicas > 0 && p.PrefillReplicas <= 32 &&
		p.DecodeReplicas > 0 && p.DecodeReplicas <= 32 &&
		p.PrefillReplicas+p.DecodeReplicas <= 64 &&
		p.PrefillGPUsPerReplica > 0 && p.PrefillGPUsPerReplica <= 128 &&
		p.DecodeGPUsPerReplica > 0 && p.DecodeGPUsPerReplica <= 128 &&
		validKubeName(p.ResourceClaimTemplateName)
}

func pdJobSetName(req Request) string {
	return strings.Replace(kueueCreateJobName(req.TenantID, req.OperationID), "ccp-", "ccp-pd-", 1)
}

func validPDJobSetCreateRequest(req Request, plan PDJobSetPlan) bool {
	return validPDJobSetPlan(plan) && req.Validate() == nil &&
		req.Kind == operation.Create && req.DesiredGeneration > 1 &&
		req.OperationID == "pd-operation-"+plan.IntentID &&
		req.RequestDigest == "sha256:"+plan.IntentID &&
		req.IdempotencyKey == "pd-create-"+plan.IntentID
}

type pdJobSetMeta struct {
	Name            string            `json:"name"`
	Namespace       string            `json:"namespace"`
	UID             string            `json:"uid,omitempty"`
	ResourceVersion string            `json:"resourceVersion,omitempty"`
	Labels          map[string]string `json:"labels,omitempty"`
	Annotations     map[string]string `json:"annotations,omitempty"`
}

type pdJobSetReplicatedJob struct {
	Name     string `json:"name"`
	Replicas int    `json:"replicas"`
	Template struct {
		Spec struct {
			Parallelism  int                   `json:"parallelism"`
			Completions  int                   `json:"completions"`
			BackoffLimit int                   `json:"backoffLimit"`
			Template     kueueBatchPodTemplate `json:"template"`
		} `json:"spec"`
	} `json:"template"`
}

type pdJobSetSpec struct {
	Suspend        bool                    `json:"suspend"`
	ReplicatedJobs []pdJobSetReplicatedJob `json:"replicatedJobs"`
}

type pdJobSetObject struct {
	APIVersion string       `json:"apiVersion"`
	Kind       string       `json:"kind"`
	Metadata   pdJobSetMeta `json:"metadata"`
	Spec       pdJobSetSpec `json:"spec"`
}

func (a *PDJobSetAdapter) expected(req Request) pdJobSetObject {
	annotations := kueueJobAnnotations(req)
	annotations["compute-control-panel.io/pd-intent-id"] = a.plan.IntentID
	obj := pdJobSetObject{
		APIVersion: "jobset.x-k8s.io/v1alpha2", Kind: "JobSet",
		Metadata: pdJobSetMeta{
			Name: pdJobSetName(req), Namespace: a.wire.namespace,
			Labels:      map[string]string{"kueue.x-k8s.io/queue-name": a.localQueue},
			Annotations: annotations,
		},
		Spec: pdJobSetSpec{Suspend: true},
	}
	for _, phase := range []struct {
		name     string
		replicas int
	}{
		{"prefill", a.plan.PrefillReplicas},
		{"decode", a.plan.DecodeReplicas},
	} {
		job := pdJobSetReplicatedJob{Name: phase.name, Replicas: phase.replicas}
		job.Template.Spec.Parallelism = 1
		job.Template.Spec.Completions = 1
		job.Template.Spec.BackoffLimit = 0
		job.Template.Spec.Template.Spec.RestartPolicy = "Never"
		job.Template.Spec.Template.Spec.ResourceClaims = []kueueBatchPodClaim{{
			Name: "gpu", ResourceClaimTemplateName: a.plan.ResourceClaimTemplateName,
		}}
		job.Template.Spec.Template.Spec.Containers = []kueueBatchContainer{{
			Name: "ccp-pd-inert", Image: kueueSuspendedJobImage,
			Resources: &kueueBatchResources{
				Requests: map[string]string{"cpu": "100m", "memory": "32Mi"},
				Claims:   []kueueBatchContainerClaim{{Name: "gpu"}},
			},
		}}
		obj.Spec.ReplicatedJobs = append(obj.Spec.ReplicatedJobs, job)
	}
	return obj
}

// PDJobSetAdapter creates a single inert, suspended JobSet and binds its
// actual Kubernetes UID using the existing durable binding-store contract.
// It is NOT a live PD deployment writer. In particular, it never unsuspends
// the JobSet or claims GPU readiness, and DELETE is not implemented here.
// Production requires a restricted durable store and trusted Intent loader.
type PDJobSetAdapter struct {
	wire       *KueueJobAdapter
	localQueue string
	plan       PDJobSetPlan
}

// NewPDJobSetAdapterWithBindings is an explicit integration/test constructor.
// The bindings must be durable across workers in any real cluster.
func NewPDJobSetAdapterWithBindings(apiURL, namespace, localQueue string, client *http.Client, bindings KueueJobBindingStore, plan PDJobSetPlan) (*PDJobSetAdapter, error) {
	if !validKubeName(localQueue) || !validPDJobSetPlan(plan) {
		return nil, ErrInvalidRequest
	}
	wire, err := NewKueueJobAdapterWithBindings(apiURL, namespace, client, bindings)
	if err != nil {
		return nil, err
	}
	return &PDJobSetAdapter{wire: wire, localQueue: localQueue, plan: plan}, nil
}

func (a *PDJobSetAdapter) resourcePath(name string) string {
	return "/apis/jobset.x-k8s.io/v1alpha2/namespaces/" + a.wire.namespace + "/jobsets/" + name
}

func (a *PDJobSetAdapter) listPath() string {
	return "/apis/jobset.x-k8s.io/v1alpha2/namespaces/" + a.wire.namespace + "/jobsets"
}

func (a *PDJobSetAdapter) exact(obj pdJobSetObject, req Request) bool {
	want := a.expected(req)
	return obj.APIVersion == want.APIVersion && obj.Kind == want.Kind &&
		obj.Metadata.Name == want.Metadata.Name &&
		obj.Metadata.Namespace == want.Metadata.Namespace &&
		acceptableJobUID(obj.Metadata.UID) && obj.Metadata.ResourceVersion != "" &&
		reflect.DeepEqual(obj.Metadata.Annotations, want.Metadata.Annotations) &&
		reflect.DeepEqual(obj.Metadata.Labels, want.Metadata.Labels) &&
		reflect.DeepEqual(obj.Spec, want.Spec)
}

func parsePDJobSet(raw []byte) (pdJobSetObject, error) {
	var obj pdJobSetObject
	if len(raw) == 0 || len(raw) > 1<<20 || !json.Valid(raw) {
		return pdJobSetObject{}, ErrInvalidKueueObservation
	}
	// Reject unrecognized spec fields: annotations cannot attest a changed
	// container/image, privileged Pod or an extra executable payload.
	var envelope struct {
		Spec json.RawMessage `json:"spec"`
	}
	if json.Unmarshal(raw, &envelope) != nil {
		return pdJobSetObject{}, ErrInvalidKueueObservation
	}
	specDecoder := json.NewDecoder(bytes.NewReader(envelope.Spec))
	specDecoder.DisallowUnknownFields()
	if specDecoder.Decode(&obj.Spec) != nil {
		return pdJobSetObject{}, ErrInvalidKueueObservation
	}
	if json.Unmarshal(raw, &obj) != nil {
		return pdJobSetObject{}, ErrInvalidKueueObservation
	}
	return obj, nil
}

func (a *PDJobSetAdapter) get(ctx context.Context, req Request) (pdJobSetObject, int, error) {
	status, raw, err := a.wire.exchange(ctx, http.MethodGet, a.resourcePath(pdJobSetName(req)), nil)
	if err != nil {
		return pdJobSetObject{}, 0, err
	}
	if status != http.StatusOK {
		return pdJobSetObject{}, status, nil
	}
	obj, err := parsePDJobSet(raw)
	if err != nil || !a.exact(obj, req) {
		return pdJobSetObject{}, 0, ErrOperationConflict
	}
	return obj, status, nil
}

func (a *PDJobSetAdapter) Apply(ctx context.Context, req Request) (Acknowledgement, error) {
	if a == nil || a.wire == nil || !validPDJobSetCreateRequest(req, a.plan) {
		return Acknowledgement{}, ErrInvalidRequest
	}
	if err := ctx.Err(); err != nil {
		return Acknowledgement{}, err
	}
	pin, pinned, err := a.wire.lookupPin(req)
	if err != nil {
		return Acknowledgement{}, err
	}
	if pinned && (pin.Name != pdJobSetName(req) || !sameRequest(pin.Request, req)) {
		return Acknowledgement{}, ErrOperationConflict
	}
	obj, status, err := a.get(ctx, req)
	if err != nil {
		return Acknowledgement{}, err
	}
	switch status {
	case http.StatusOK:
		if !pinned {
			// The response to an earlier POST may have been lost. Matching
			// name and annotations cannot prove the original UID lineage.
			return Acknowledgement{}, ErrAcknowledgementLost
		}
		if obj.Metadata.UID != pin.UID {
			return Acknowledgement{}, ErrOperationConflict
		}
		return Acknowledgement{OperationID: req.OperationID, RequestDigest: req.RequestDigest}, nil
	case http.StatusNotFound:
		if pinned {
			return Acknowledgement{}, ErrOperationConflict
		}
	default:
		return Acknowledgement{}, ErrInvalidKueueObservation
	}

	payload, err := json.Marshal(a.expected(req))
	if err != nil {
		return Acknowledgement{}, err
	}
	status, raw, err := a.wire.exchange(ctx, http.MethodPost, a.listPath(), payload)
	if err != nil {
		return Acknowledgement{}, err
	}
	if status == http.StatusConflict {
		return Acknowledgement{}, ErrAcknowledgementLost
	}
	if status != http.StatusCreated && status != http.StatusOK {
		if status == http.StatusBadRequest || status == http.StatusForbidden || status == http.StatusUnauthorized || status == http.StatusUnprocessableEntity {
			return Acknowledgement{}, ErrInvalidRequest
		}
		return Acknowledgement{}, ErrAcknowledgementLost
	}
	created, err := parsePDJobSet(raw)
	if err != nil || !a.exact(created, req) {
		return Acknowledgement{}, ErrAcknowledgementLost
	}
	if err := a.wire.pin(req, pdJobSetName(req), created.Metadata.UID); err != nil {
		return Acknowledgement{}, uncommittedKueueBinding("PD_JOBSET_CREATE", err)
	}
	return Acknowledgement{OperationID: req.OperationID, RequestDigest: req.RequestDigest}, nil
}

// Inspect resolves only UID-pinned JobSet creation. Unpinned presence is
// OPERATION_IN_PROGRESS (persistent UNKNOWN), not RESOURCE_PRESENT. Absence
// cannot prove OPERATION_NOT_APPLIED_FINAL. Quota and DRA observations remain
// independent and the provider never infers Allocation READY.
func (a *PDJobSetAdapter) Inspect(ctx context.Context, req Request) (operation.Observation, error) {
	if a == nil || a.wire == nil || !validPDJobSetCreateRequest(req, a.plan) {
		return operation.Observation{}, ErrInvalidRequest
	}
	if err := ctx.Err(); err != nil {
		return operation.Observation{}, err
	}
	pin, pinned, err := a.wire.lookupPin(req)
	if err != nil {
		return operation.Observation{}, err
	}
	obj, status, err := a.get(ctx, req)
	if errors.Is(err, ErrOperationConflict) {
		return conflictObservation(req, "PD_JOBSET_SPEC_OR_IDENTITY_DRIFT"), nil
	}
	if err != nil {
		return operation.Observation{}, err
	}
	if status == http.StatusNotFound {
		return conflictObservation(req, "PD_JOBSET_CREATE_OUTCOME_UNPROVEN"), nil
	}
	if status != http.StatusOK {
		return operation.Observation{}, fmt.Errorf("%w: JobSet lookup status %d", ErrInvalidKueueObservation, status)
	}
	if !pinned {
		return unboundKueueJobObservation(req), nil
	}
	if pin.Name != obj.Metadata.Name || pin.UID != obj.Metadata.UID || !sameRequest(pin.Request, req) {
		return conflictObservation(req, "PD_JOBSET_UID_REPLACED"), nil
	}
	return operation.Observation{
		OperationID: req.OperationID, TenantID: req.TenantID,
		WorkloadID: req.WorkloadID, ProviderID: req.ProviderID,
		DesiredGeneration:   req.DesiredGeneration,
		Result:              operation.ResourcePresent,
		ResourceID:          a.wire.namespace + "/" + obj.Metadata.Name,
		ResourceUID:         obj.Metadata.UID,
		ResourceOperationID: req.OperationID,
		ResourceGeneration:  req.DesiredGeneration,
		ResourceVersion:     obj.Metadata.ResourceVersion,
		ObservedAt:          time.Now().UTC(),
	}, nil
}

var _ Adapter = (*PDJobSetAdapter)(nil)
