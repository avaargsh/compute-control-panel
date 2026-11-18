package provider

import (
	"context"
	"encoding/hex"
	"fmt"
	"reflect"
	"strconv"
	"strings"
)

// PDPhaseQuota is the independently authorized, digest-bound phase shape.
// Each replicated Job has exactly one Pod in this adapter's first version.
type PDPhaseQuota struct {
	Replicas, GPUsPerReplica int
}

// PDJobSetQuery must be built from trusted durable intent and provider
// resource identity, not caller-supplied annotations or Kubernetes status.
type PDJobSetQuery struct {
	TenantID, WorkloadID, IntentID        string
	Generation                            uint64
	Namespace, JobSetName, JobSetUID      string
	KueueName, KueueUID                   string
	LocalQueue, ClusterQueue, GPUResource string
	Prefill, Decode                       PDPhaseQuota
}

// PDJobSetAdmission attests no physical readiness. Quota admission for both
// PodSets is a structural observation, not a Pod, DRA or driver receipt.
type PDJobSetAdmission struct {
	JobSet, Workload KubernetesResourceRef
	Stage            string
	ReservedGPUCount int
	AssignmentsJSON  string
	PhysicalReady    bool
}

type pdNativeMeta struct {
	kubeMeta
	Generation        int64   `json:"generation"`
	DeletionTimestamp *string `json:"deletionTimestamp"`
}

type pdNativeJobSet struct {
	Metadata pdNativeMeta `json:"metadata"`
	Spec     struct {
		Suspend        *bool `json:"suspend"`
		ReplicatedJobs []struct {
			Name     string `json:"name"`
			Replicas int    `json:"replicas"`
			Template struct {
				Spec struct {
					Parallelism int `json:"parallelism"`
					Completions int `json:"completions"`
				} `json:"spec"`
			} `json:"template"`
		} `json:"replicatedJobs"`
	} `json:"spec"`
}

type pdNativeWorkload struct {
	Metadata pdNativeMeta `json:"metadata"`
	Spec     struct {
		QueueName string `json:"queueName"`
		PodSets   []struct {
			Name  string `json:"name"`
			Count int    `json:"count"`
		} `json:"podSets"`
	} `json:"spec"`
	Status struct {
		Conditions []kubeCondition `json:"conditions"`
		Admission  *struct {
			ClusterQueue      string                 `json:"clusterQueue"`
			PodSetAssignments []kubePodSetAssignment `json:"podSetAssignments"`
		} `json:"admission"`
	} `json:"status"`
}

func (q PDJobSetQuery) validate() error {
	for _, name := range []string{q.Namespace, q.JobSetName, q.KueueName, q.LocalQueue, q.ClusterQueue} {
		if !validKubeName(name) {
			return ErrInvalidKueueObservation
		}
	}
	for _, s := range []string{q.TenantID, q.WorkloadID, q.JobSetUID, q.KueueUID} {
		if s == "" || len(s) > 256 || s != strings.TrimSpace(s) || strings.ContainsAny(s, "/\\ \r\n\t\x00") {
			return ErrInvalidKueueObservation
		}
	}
	_, err := hex.DecodeString(q.IntentID)
	if len(q.IntentID) != 64 || strings.ToLower(q.IntentID) != q.IntentID || err != nil ||
		q.Generation == 0 || q.JobSetUID == q.KueueUID {
		return ErrInvalidKueueObservation
	}
	parts := strings.Split(q.GPUResource, "/")
	if len(parts) != 2 || len(parts[0]) > 253 || !validKubeName(parts[1]) {
		return ErrInvalidKueueObservation
	}
	for _, part := range strings.Split(parts[0], ".") {
		if !validKubeName(part) {
			return ErrInvalidKueueObservation
		}
	}
	for _, p := range []PDPhaseQuota{q.Prefill, q.Decode} {
		if p.Replicas < 1 || p.Replicas > 10000 || p.GPUsPerReplica < 1 || p.GPUsPerReplica > 1024 {
			return ErrInvalidKueueObservation
		}
	}
	return nil
}

func pdPhase(q PDJobSetQuery, name string) (PDPhaseQuota, bool) {
	switch name {
	case "prefill":
		return q.Prefill, true
	case "decode":
		return q.Decode, true
	default:
		return PDPhaseQuota{}, false
	}
}

func pdExactRef(meta pdNativeMeta, ns, name, uid string) bool {
	ref := refFromKube(meta.kubeMeta)
	return validKubernetesRef(ref) && ref.Namespace == ns && ref.Name == name &&
		ref.UID == uid && meta.Generation > 0 && meta.DeletionTimestamp == nil
}

func pdConditionsCurrent(w pdNativeWorkload) bool {
	seen := map[string]bool{}
	for _, c := range w.Status.Conditions {
		if seen[c.Type] || (c.Status != "True" && c.Status != "False" && c.Status != "Unknown") {
			return false
		}
		seen[c.Type] = true
		switch c.Type {
		case "Admitted", "QuotaReserved":
			if c.Status != "True" || c.ObservedGeneration != w.Metadata.Generation {
				return false
			}
		case "Evicted", "Preempted", "Finished", "DeactivationTarget":
			if c.Status != "False" || c.ObservedGeneration != w.Metadata.Generation {
				return false
			}
		}
	}
	return seen["Admitted"] && seen["QuotaReserved"]
}

func assessPDJobSet(q PDJobSetQuery, j pdNativeJobSet, w pdNativeWorkload) (PDJobSetAdmission, error) {
	reject := func() (PDJobSetAdmission, error) { return PDJobSetAdmission{}, ErrInvalidKueueObservation }
	if !pdExactRef(j.Metadata, q.Namespace, q.JobSetName, q.JobSetUID) ||
		!pdExactRef(w.Metadata, q.Namespace, q.KueueName, q.KueueUID) ||
		j.Metadata.Labels["kueue.x-k8s.io/queue-name"] != q.LocalQueue ||
		j.Metadata.Annotations[kueueAnnotationTenantID] != q.TenantID ||
		j.Metadata.Annotations[kueueAnnotationWorkloadID] != q.WorkloadID ||
		j.Metadata.Annotations[kueueAnnotationDesiredGeneration] != strconv.FormatUint(q.Generation, 10) ||
		j.Metadata.Annotations["compute-control-panel.io/pd-intent-id"] != q.IntentID ||
		j.Spec.Suspend == nil || *j.Spec.Suspend || w.Spec.QueueName != q.LocalQueue ||
		len(j.Spec.ReplicatedJobs) != 2 || len(w.Spec.PodSets) != 2 ||
		w.Status.Admission == nil || w.Status.Admission.ClusterQueue != q.ClusterQueue ||
		len(w.Status.Admission.PodSetAssignments) != 2 || !pdConditionsCurrent(w) {
		return reject()
	}
	controllers := 0
	for _, o := range w.Metadata.OwnerReferences {
		if o.Controller {
			controllers++
			if o.APIVersion != "jobset.x-k8s.io/v1alpha2" || o.Kind != "JobSet" || o.UID != q.JobSetUID {
				return reject()
			}
		}
	}
	if controllers != 1 {
		return reject()
	}
	seen := map[string]bool{}
	for _, job := range j.Spec.ReplicatedJobs {
		p, ok := pdPhase(q, job.Name)
		if !ok || seen[job.Name] || job.Replicas != p.Replicas ||
			job.Template.Spec.Parallelism != 1 || job.Template.Spec.Completions != 1 {
			return reject()
		}
		seen[job.Name] = true
	}
	seen = map[string]bool{}
	for _, set := range w.Spec.PodSets {
		p, ok := pdPhase(q, set.Name)
		if !ok || seen[set.Name] || set.Count != p.Replicas {
			return reject()
		}
		seen[set.Name] = true
	}
	seen = map[string]bool{}
	total := 0
	for _, a := range w.Status.Admission.PodSetAssignments {
		p, ok := pdPhase(q, a.Name)
		expected := p.Replicas * p.GPUsPerReplica
		if !ok || seen[a.Name] || a.Count == nil || int(*a.Count) != p.Replicas ||
			a.ResourceUsage[q.GPUResource] != strconv.Itoa(expected) ||
			a.Flavors[q.GPUResource] == "" {
			return reject()
		}
		seen[a.Name] = true
		total += expected
	}
	assignments, err := canonicalReservedAssignments(w.Status.Admission.PodSetAssignments)
	if err != nil {
		return reject()
	}
	return PDJobSetAdmission{JobSet: refFromKube(j.Metadata.kubeMeta), Workload: refFromKube(w.Metadata.kubeMeta),
		Stage: "PAIRED_QUOTA_ADMITTED", ReservedGPUCount: total, AssignmentsJSON: assignments}, nil
}

// ObservePairedPDAdmission uses the existing bounded HTTPS reader and requires
// both native objects to stay identical across two sweeps. It never creates or
// unsuspends a JobSet, reserves quota, or transitions an Allocation to READY.
// Sequential GETs are not an atomic Kubernetes snapshot.
func (r *KubernetesObservationReader) ObservePairedPDAdmission(ctx context.Context, q PDJobSetQuery) (PDJobSetAdmission, error) {
	if err := q.validate(); err != nil {
		return PDJobSetAdmission{}, err
	}
	jp := fmt.Sprintf("/apis/jobset.x-k8s.io/v1alpha2/namespaces/%s/jobsets/%s", q.Namespace, q.JobSetName)
	wp := fmt.Sprintf("/apis/kueue.x-k8s.io/v1beta1/namespaces/%s/workloads/%s", q.Namespace, q.KueueName)
	var first, second pdNativeJobSet
	var one, two pdNativeWorkload
	for _, read := range []struct {
		path string
		into any
	}{{jp, &first}, {wp, &one}, {jp, &second}, {wp, &two}} {
		if err := r.read(ctx, read.path, read.into); err != nil {
			return PDJobSetAdmission{}, err
		}
	}
	if !reflect.DeepEqual(first, second) || !reflect.DeepEqual(one, two) {
		return PDJobSetAdmission{}, ErrInvalidKueueObservation
	}
	return assessPDJobSet(q, second, two)
}
