package provider

import (
	"context"
	"fmt"
	"reflect"
	"strconv"
)

// PDReplicaClaimQuery pins one replica's child Job and generated per-Pod
// ResourceClaim. The caller must independently authenticate every identity.
// The existing single-device claim reader limits this first integration to
// one whole, non-shared DRA device per replica.
type PDReplicaClaimQuery struct {
	Phase  string
	Index  int
	JobUID string
	Pod    DRAPodClaimQuery
}

type PDReplicaClaimEvidence struct {
	Phase string
	Index int
	Job   KubernetesResourceRef
	Pod   DRAPodClaimObservation
}

type PDJobSetDRAEvidence struct {
	Admission PDJobSetAdmission
	Replicas  []PDReplicaClaimEvidence
}

func validPDReplicaClaims(q PDJobSetQuery, replicas []PDReplicaClaimQuery) bool {
	if q.validate() != nil || q.Prefill.GPUsPerReplica != 1 || q.Decode.GPUsPerReplica != 1 ||
		len(replicas) > 128 || len(replicas) != q.Prefill.Replicas+q.Decode.Replicas {
		return false
	}
	type ordinal struct {
		phase string
		index int
	}
	ordinals := map[ordinal]bool{}
	jobs, pods, claims := map[string]bool{}, map[string]bool{}, map[string]bool{}
	type device struct{ driver, pool, name string }
	devices := map[device]bool{}
	for _, r := range replicas {
		p, ok := pdPhase(q, r.Phase)
		c := r.Pod.Claim
		k := ordinal{r.Phase, r.Index}
		d := device{c.Driver, c.Pool, c.Device}
		if !ok || r.Index < 0 || r.Index >= p.Replicas || ordinals[k] ||
			r.Pod.validate() != nil || !validDRAIdentity(r.JobUID) || r.Pod.ExpectedJobUID != r.JobUID ||
			c.Namespace != q.Namespace || r.Pod.ClaimTemplateName == "" || c.ShareID != "" ||
			jobs[r.JobUID] || pods[c.ExpectedPodUID] || claims[c.ExpectedClaimUID] || devices[d] {
			return false
		}
		ordinals[k], jobs[r.JobUID], pods[c.ExpectedPodUID], claims[c.ExpectedClaimUID], devices[d] = true, true, true, true, true
	}
	return true
}

func (r *KubernetesObservationReader) observePDReplica(ctx context.Context, q PDJobSetQuery, pin PDReplicaClaimQuery) (PDReplicaClaimEvidence, error) {
	reject := func() (PDReplicaClaimEvidence, error) { return PDReplicaClaimEvidence{}, ErrInvalidDRAObservation }
	name := fmt.Sprintf("%s-%s-%d", q.JobSetName, pin.Phase, pin.Index)
	if !validKubeName(name) {
		return reject()
	}
	path := fmt.Sprintf("/apis/batch/v1/namespaces/%s/jobs/%s", q.Namespace, name)
	var first, second struct {
		Metadata pdNativeMeta `json:"metadata"`
		Spec     struct {
			Suspend     *bool `json:"suspend"`
			Parallelism int   `json:"parallelism"`
			Completions int   `json:"completions"`
		} `json:"spec"`
	}
	if r.read(ctx, path, &first) != nil || !pdExactRef(first.Metadata, q.Namespace, name, pin.JobUID) ||
		first.Spec.Suspend == nil || *first.Spec.Suspend || first.Spec.Parallelism != 1 || first.Spec.Completions != 1 {
		return reject()
	}
	labels := first.Metadata.Labels
	if labels["jobset.sigs.k8s.io/jobset-name"] != q.JobSetName ||
		labels["jobset.sigs.k8s.io/jobset-uid"] != q.JobSetUID ||
		labels["jobset.sigs.k8s.io/replicatedjob-name"] != pin.Phase ||
		labels["jobset.sigs.k8s.io/job-index"] != strconv.Itoa(pin.Index) {
		return reject()
	}
	controllers := 0
	for _, o := range first.Metadata.OwnerReferences {
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
	pod, err := r.ObserveDRAPodClaim(ctx, pin.Pod)
	if err != nil || r.read(ctx, path, &second) != nil || !reflect.DeepEqual(first, second) {
		return reject()
	}
	return PDReplicaClaimEvidence{Phase: pin.Phase, Index: pin.Index, Job: refFromKube(first.Metadata.kubeMeta), Pod: pod}, nil
}

// ObservePairedPDClaims joins a valid paired quota observation to all expected
// child Job/Pod/Claim identities and rejects duplicate device tuples or drift.
// This is scheduler-side evidence only: it cannot prove device preparation,
// physical UID membership, KV transfer, release, readiness or accounting.
// Multi-device replicas are intentionally rejected until their reader exists.
func (r *KubernetesObservationReader) ObservePairedPDClaims(ctx context.Context, q PDJobSetQuery, pins []PDReplicaClaimQuery) (PDJobSetDRAEvidence, error) {
	reject := func() (PDJobSetDRAEvidence, error) { return PDJobSetDRAEvidence{}, ErrInvalidDRAObservation }
	if !validPDReplicaClaims(q, pins) {
		return reject()
	}
	admission, err := r.ObservePairedPDAdmission(ctx, q)
	if err != nil {
		return reject()
	}
	replicas := make([]PDReplicaClaimEvidence, len(pins))
	for i, pin := range pins {
		replicas[i], err = r.observePDReplica(ctx, q, pin)
		if err != nil {
			return reject()
		}
	}
	for i, pin := range pins {
		again, err := r.observePDReplica(ctx, q, pin)
		if err != nil || !reflect.DeepEqual(replicas[i], again) {
			return reject()
		}
	}
	again, err := r.ObservePairedPDAdmission(ctx, q)
	if err != nil || again != admission {
		return reject()
	}
	return PDJobSetDRAEvidence{Admission: admission, Replicas: replicas}, nil
}
