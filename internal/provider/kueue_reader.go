package provider

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"regexp"
	"strconv"
	"strings"
	"time"
)

// KubernetesObservationReader performs only bounded Kubernetes API GETs.
// The caller must supply a client authenticated to a pinned, trusted cluster
// endpoint with read-only Job, Pod and Kueue Workload permissions. Passing an
// HTTP client alone does not attest the source or make evidence production ready.
type KubernetesObservationReader struct {
	apiURL string
	client http.Client
}

type KubernetesJobQuery struct {
	TenantID          string
	WorkloadID        string
	DesiredGeneration uint64
	Namespace         string
	JobName           string
	ExpectedJobUID    string
}

// Annotation keys the read-only Job reader treats as immutable identity.
// The Job adapter writes the same keys; a mismatch fails closed as UNPROVEN.
const (
	kueueAnnotationTenantID          = "compute-control-panel.io/tenant-id"
	kueueAnnotationWorkloadID        = "compute-control-panel.io/workload-id"
	kueueAnnotationDesiredGeneration = "compute-control-panel.io/desired-generation"
)

var kubeDNSLabel = regexp.MustCompile(`^[a-z0-9]([-a-z0-9]*[a-z0-9])?$`)

func validKubeName(s string) bool {
	return len(s) > 0 && len(s) <= 63 && kubeDNSLabel.MatchString(s)
}

func (q KubernetesJobQuery) validate() error {
	if !validKubeName(q.Namespace) || !validKubeName(q.JobName) ||
		q.TenantID == "" || q.WorkloadID == "" ||
		q.TenantID != strings.TrimSpace(q.TenantID) ||
		q.WorkloadID != strings.TrimSpace(q.WorkloadID) ||
		q.ExpectedJobUID == "" || q.ExpectedJobUID != strings.TrimSpace(q.ExpectedJobUID) ||
		strings.ContainsAny(q.ExpectedJobUID, "/\\ \r\n\t") ||
		q.DesiredGeneration == 0 {
		return ErrInvalidKueueObservation
	}
	return nil
}

func NewKubernetesObservationReader(rawURL string, client *http.Client) (*KubernetesObservationReader, error) {
	parsed, err := url.Parse(rawURL)
	if err != nil || parsed.Scheme != "https" || parsed.Hostname() == "" ||
		parsed.User != nil || parsed.RawQuery != "" || parsed.Fragment != "" ||
		(parsed.Path != "" && parsed.Path != "/") ||
		client == nil || client.Timeout <= 0 || client.Timeout > 30*time.Second {
		return nil, ErrInvalidKueueObservation
	}
	copyClient := *client
	// Never follow an API redirect to a different host (or credentials scope).
	copyClient.CheckRedirect = func(_ *http.Request, _ []*http.Request) error {
		return http.ErrUseLastResponse
	}
	return &KubernetesObservationReader{apiURL: strings.TrimSuffix(parsed.String(), "/"), client: copyClient}, nil
}

type kubeOwner struct {
	APIVersion string `json:"apiVersion"`
	Kind       string `json:"kind"`
	UID        string `json:"uid"`
	Controller bool   `json:"controller"`
}

type kubeMeta struct {
	Namespace       string            `json:"namespace"`
	Name            string            `json:"name"`
	UID             string            `json:"uid"`
	ResourceVersion string            `json:"resourceVersion"`
	Labels          map[string]string `json:"labels"`
	Annotations     map[string]string `json:"annotations"`
	OwnerReferences []kubeOwner       `json:"ownerReferences"`
	Continue        string            `json:"continue"`
}

type kubeCondition struct {
	Type               string `json:"type"`
	Status             string `json:"status"`
	Reason             string `json:"reason"`
	Message            string `json:"message"`
	LastTransitionTime string `json:"lastTransitionTime"`
	ObservedGeneration int64  `json:"observedGeneration"`
}

// Native Kueue API quantities remain strings; this read-only path does not
// parse resource.Quantity into a billing rate or a physical device count.
type kubePodSetAssignment struct {
	Name          string            `json:"name"`
	Flavors       map[string]string `json:"flavors,omitempty"`
	ResourceUsage map[string]string `json:"resourceUsage,omitempty"`
	Count         *int32            `json:"count,omitempty"`
}

type kubeObject struct {
	Metadata kubeMeta `json:"metadata"`
	Spec     struct {
		NodeName    string                `json:"nodeName"`
		Suspend     *bool                 `json:"suspend"`
		Parallelism *int                  `json:"parallelism"`
		Completions *int                  `json:"completions"`
		Template    kueueBatchPodTemplate `json:"template"`
	} `json:"spec"`
	Status struct {
		Conditions []kubeCondition `json:"conditions"`
		Admission  *struct {
			ClusterQueue      string                 `json:"clusterQueue"`
			PodSetAssignments []kubePodSetAssignment `json:"podSetAssignments"`
		} `json:"admission"`
	} `json:"status"`
}

type kubeObjectList struct {
	Metadata kubeMeta     `json:"metadata"`
	Items    []kubeObject `json:"items"`
}

func refFromKube(meta kubeMeta) KubernetesResourceRef {
	return KubernetesResourceRef{
		Namespace: meta.Namespace, Name: meta.Name,
		UID: meta.UID, ResourceVersion: meta.ResourceVersion,
	}
}

func isControlledBy(meta kubeMeta, jobUID string) bool {
	for _, owner := range meta.OwnerReferences {
		if owner.APIVersion == "batch/v1" && owner.Kind == "Job" &&
			owner.Controller && owner.UID == jobUID {
			return true
		}
	}
	return false
}

func conditionIsTrue(conditions []kubeCondition, kind string) (bool, error) {
	seen := false
	result := false
	for _, c := range conditions {
		if c.Type != kind {
			continue
		}
		if seen || (c.Status != "True" && c.Status != "False" && c.Status != "Unknown") {
			return false, ErrInvalidKueueObservation
		}
		seen = true
		result = c.Status == "True"
	}
	return result, nil
}

// nativeKueueCondition preserves False, Unknown and MISSING distinctly.
// A duplicate or malformed condition is never promoted to admission proof.
func nativeKueueCondition(conditions []kubeCondition, kind string) (KueueConditionEvidence, error) {
	result := KueueConditionEvidence{Status: "MISSING"}
	for _, c := range conditions {
		if c.Type != kind {
			continue
		}
		if result.Status != "MISSING" || (c.Status != "True" && c.Status != "False" && c.Status != "Unknown") {
			return KueueConditionEvidence{}, ErrInvalidKueueObservation
		}
		result = KueueConditionEvidence{
			Status: c.Status, Reason: c.Reason, Message: c.Message,
			LastTransitionTime: c.LastTransitionTime, ObservedGeneration: c.ObservedGeneration,
		}
		if !validKueueCondition(result) {
			return KueueConditionEvidence{}, ErrInvalidKueueObservation
		}
	}
	return result, nil
}

// canonicalReservedAssignments requires a complete native reservation payload,
// while retaining extended resource names and raw Kubernetes quantity units.
// This is structural evidence, not a device allocation or cost calculation.
func canonicalReservedAssignments(assignments []kubePodSetAssignment) (string, error) {
	if len(assignments) == 0 || len(assignments) > 18 {
		return "", ErrInvalidKueueObservation
	}
	seen := make(map[string]bool, len(assignments))
	for _, item := range assignments {
		if !validKubeName(item.Name) || seen[item.Name] || len(item.ResourceUsage) == 0 ||
			(item.Count != nil && *item.Count < 0) {
			return "", ErrInvalidKueueObservation
		}
		seen[item.Name] = true
		for key, value := range item.ResourceUsage {
			if key == "" || len(key) > 320 || key != strings.TrimSpace(key) || strings.ContainsAny(key, " \t\r\n\\\x00") ||
				value == "" || len(value) > 128 || value != strings.TrimSpace(value) || strings.ContainsAny(value, " \t\r\n\\\x00") {
				return "", ErrInvalidKueueObservation
			}
		}
		for key, flavor := range item.Flavors {
			if key == "" || len(key) > 320 || key != strings.TrimSpace(key) || strings.ContainsAny(key, " \t\r\n\\\x00") ||
				flavor == "" || len(flavor) > 253 || flavor != strings.TrimSpace(flavor) || strings.ContainsAny(flavor, " \t\r\n\\\x00") {
				return "", ErrInvalidKueueObservation
			}
		}
	}
	serialized, err := json.Marshal(assignments)
	if err != nil {
		return "", ErrInvalidKueueObservation
	}
	return string(serialized), nil
}

func (r *KubernetesObservationReader) read(ctx context.Context, path string, into any) error {
	if r == nil {
		return ErrInvalidKueueObservation
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, r.apiURL+path, nil)
	if err != nil {
		return fmt.Errorf("build Kubernetes read: %w", err)
	}
	req.Header.Set("Accept", "application/json")
	resp, err := r.client.Do(req)
	if err != nil {
		return fmt.Errorf("read Kubernetes API: %w", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("%w: Kubernetes API returned HTTP %d", ErrInvalidKueueObservation, resp.StatusCode)
	}
	body, err := io.ReadAll(io.LimitReader(resp.Body, 1<<20+1))
	if err != nil {
		return fmt.Errorf("read bounded Kubernetes response: %w", err)
	}
	if len(body) > 1<<20 || json.Unmarshal(body, into) != nil {
		return ErrInvalidKueueObservation
	}
	return nil
}

// ObserveJob reads one known, UID-pinned Job and its Kueue/Pod dependents.
// It is a structural, read-only observation, not an authenticated readiness,
// release, capacity, placement-policy or device allocation attestation.
//
// This v0.1 reader intentionally handles single-Pod Jobs only. Multiple
// owned Kueue Workloads or Pods fail closed until a multi-Pod policy exists.
func (r *KubernetesObservationReader) ObserveJob(
	ctx context.Context, q KubernetesJobQuery,
) (KueueJobEvidence, KueueObservationStage, error) {
	reject := func(err error) (KueueJobEvidence, KueueObservationStage, error) {
		return KueueJobEvidence{}, KueueUnproven, err
	}
	if err := q.validate(); err != nil {
		return reject(err)
	}
	jobPath := fmt.Sprintf("/apis/batch/v1/namespaces/%s/jobs/%s", q.Namespace, q.JobName)
	var job kubeObject
	if err := r.read(ctx, jobPath, &job); err != nil {
		return reject(err)
	}
	jobRef := refFromKube(job.Metadata)
	if !validKubernetesRef(jobRef) || jobRef.Namespace != q.Namespace ||
		jobRef.Name != q.JobName || jobRef.UID != q.ExpectedJobUID ||
		job.Metadata.Annotations[kueueAnnotationTenantID] != q.TenantID ||
		job.Metadata.Annotations[kueueAnnotationWorkloadID] != q.WorkloadID ||
		job.Metadata.Annotations[kueueAnnotationDesiredGeneration] != strconv.FormatUint(q.DesiredGeneration, 10) {
		return reject(ErrInvalidKueueObservation)
	}
	e := KueueJobEvidence{
		TenantID: q.TenantID, WorkloadID: q.WorkloadID,
		DesiredGeneration: q.DesiredGeneration,
		Job:               jobRef, JobTenantID: q.TenantID, JobWorkloadID: q.WorkloadID,
		JobDesiredGeneration: q.DesiredGeneration,
	}

	var kueueList kubeObjectList
	if err := r.read(ctx, fmt.Sprintf("/apis/kueue.x-k8s.io/v1beta1/namespaces/%s/workloads", q.Namespace), &kueueList); err != nil {
		return reject(err)
	}
	if kueueList.Metadata.Continue != "" {
		return reject(ErrInvalidKueueObservation)
	}
	for _, workload := range kueueList.Items {
		if !isControlledBy(workload.Metadata, jobRef.UID) {
			continue
		}
		if presentKubernetesRef(e.KueueWorkload) || !validKubernetesRef(refFromKube(workload.Metadata)) ||
			workload.Metadata.Namespace != q.Namespace {
			return reject(ErrInvalidKueueObservation)
		}
		e.KueueWorkload = refFromKube(workload.Metadata)
		e.KueueOwnerJobUID = jobRef.UID
		quotaCondition, err := nativeKueueCondition(workload.Status.Conditions, "QuotaReserved")
		if err != nil {
			return reject(err)
		}
		admissionCondition, err := nativeKueueCondition(workload.Status.Conditions, "Admitted")
		if err != nil {
			return reject(err)
		}
		evictedCondition, err := nativeKueueCondition(workload.Status.Conditions, "Evicted")
		if err != nil {
			return reject(err)
		}
		preemptedCondition, err := nativeKueueCondition(workload.Status.Conditions, "Preempted")
		if err != nil {
			return reject(err)
		}
		reserved := quotaCondition.Status == "True"
		admitted := admissionCondition.Status == "True"
		// An Admitted=True without the independent quota reservation is
		// contradictory, not evidence that the queue has available resources.
		if admitted && !reserved {
			return reject(ErrInvalidKueueObservation)
		}
		if reserved {
			if workload.Status.Admission == nil || workload.Status.Admission.ClusterQueue == "" {
				return reject(ErrInvalidKueueObservation)
			}
			assignments, err := canonicalReservedAssignments(workload.Status.Admission.PodSetAssignments)
			if err != nil {
				return reject(err)
			}
			e.KueueClusterQueue = workload.Status.Admission.ClusterQueue
			e.KueuePodSetAssignmentsJSON = assignments
		}
		e.KueueQuotaCondition = quotaCondition
		e.KueueAdmissionCondition = admissionCondition
		e.KueueEvictedCondition = evictedCondition
		e.KueuePreemptedCondition = preemptedCondition
		e.KueueQuotaReserved = reserved
		e.KueueAdmitted = admitted
		e.KueueEvicted = evictedCondition.Status == "True"
		e.KueuePreempted = preemptedCondition.Status == "True"
	}
	if e.KueueAdmitted {
		var pods kubeObjectList
		selector := url.QueryEscape("batch.kubernetes.io/controller-uid=" + jobRef.UID)
		if err := r.read(ctx, fmt.Sprintf("/api/v1/namespaces/%s/pods?labelSelector=%s", q.Namespace, selector), &pods); err != nil {
			return reject(err)
		}
		if pods.Metadata.Continue != "" || len(pods.Items) > 1 {
			return reject(ErrInvalidKueueObservation)
		}
		if len(pods.Items) == 1 {
			pod := pods.Items[0]
			if !validKubernetesRef(refFromKube(pod.Metadata)) ||
				pod.Metadata.Namespace != q.Namespace ||
				pod.Metadata.Labels["batch.kubernetes.io/controller-uid"] != jobRef.UID ||
				!isControlledBy(pod.Metadata, jobRef.UID) {
				return reject(ErrInvalidKueueObservation)
			}
			scheduled, err := conditionIsTrue(pod.Status.Conditions, "PodScheduled")
			if err != nil {
				return reject(err)
			}
			e.Pod = refFromKube(pod.Metadata)
			e.PodOwnerJobUID = jobRef.UID
			e.PodScheduled = scheduled
			if scheduled {
				e.PodNodeName = pod.Spec.NodeName
			}
		}
	}

	// Catch Job name reuse or changes while Kueue and Pod were read. These
	// sequential GETs are not a transactional Kubernetes snapshot.
	var current kubeObject
	if err := r.read(ctx, jobPath, &current); err != nil {
		return reject(err)
	}
	if current.Metadata.UID != jobRef.UID ||
		current.Metadata.ResourceVersion != jobRef.ResourceVersion {
		return reject(ErrInvalidKueueObservation)
	}
	stage, err := AssessKueueJobObservation(e)
	if err != nil {
		return reject(err)
	}
	return e, stage, nil
}

// Collect adapts the bounded HTTPS Kubernetes reader to the shared read-only
// Collector contract. Provenance labels and local observation time identify
// this code path; they are NOT authenticated cluster evidence or a GPU device
// attestation. No Kubernetes write, Allocation mutation or scheduling occurs.
func (r *KubernetesObservationReader) Collect(
	ctx context.Context, q KubernetesJobQuery,
) (KueueJobCollection, error) {
	observed, _, err := r.ObserveJob(ctx, q)
	if err != nil {
		return KueueJobCollection{Stage: KueueUnproven}, err
	}
	return bindKueueJobCollection(q, observed, KueueEvidenceProvenance{
		Source:         "kubernetes-https-readonly-unattested",
		AdapterVersion: "kueue-v1beta1-single-pod-v1",
		ObservedAt:     time.Now().UTC(),
	})
}

var _ Collector = (*KubernetesObservationReader)(nil)
