package provider

import (
	"context"
	"fmt"
	"strings"
)

// KueueQueueBindingQuery comes from an already-authorized queue mapping,
// not an untrusted tenant request. Until a server-owned QuotaBinding resolves
// its UID fences, this reader remains a structural, non-production proof.
type KueueQueueBindingQuery struct {
	Job              KubernetesJobQuery
	LocalQueueName   string
	LocalQueueUID    string
	ClusterQueueName string
	ClusterQueueUID  string
}

// KueueQueueBindingEvidence proves the observed Kueue routing object chain,
// not GPU allocation, tenant policy enforcement, quota-release finality or
// the authority to mark an Allocation READY/RELEASED.
type KueueQueueBindingEvidence struct {
	Job          KueueJobEvidence
	Stage        KueueObservationStage
	LocalQueue   KubernetesResourceRef
	ClusterQueue KubernetesResourceRef
}

type kueueQueueObject struct {
	Metadata kubeMeta `json:"metadata"`
	Spec     struct {
		QueueName    string `json:"queueName"`
		ClusterQueue string `json:"clusterQueue"`
	} `json:"spec"`
}

func validPinnedQueueUID(uid string) bool {
	return uid != "" && uid == strings.TrimSpace(uid) && !strings.ContainsAny(uid, "/\\ \t\r\n\x00")
}

func (q KueueQueueBindingQuery) validate() error {
	if q.Job.validate() != nil || !validKubeName(q.LocalQueueName) ||
		!validKubeName(q.ClusterQueueName) || !validPinnedQueueUID(q.LocalQueueUID) ||
		!validPinnedQueueUID(q.ClusterQueueUID) {
		return ErrInvalidKueueObservation
	}
	return nil
}

func sameKueueObjectIdentity(a, b kubeMeta) bool {
	return a.Namespace == b.Namespace && a.Name == b.Name &&
		a.UID == b.UID && a.ResourceVersion == b.ResourceVersion
}

// ObserveQueueBinding resolves one immutable Job -> Kueue Workload ->
// LocalQueue -> ClusterQueue path. Reads are not a Kubernetes transaction:
// all observed queue objects are re-GETed and must remain on the same UID/RV.
// A stale, missing, untrusted, or conflicting source returns zero evidence.
// This optional observer is intentionally not wired into Allocation state.
func (r *KubernetesObservationReader) ObserveQueueBinding(
	ctx context.Context, q KueueQueueBindingQuery,
) (KueueQueueBindingEvidence, error) {
	reject := func() (KueueQueueBindingEvidence, error) {
		return KueueQueueBindingEvidence{}, ErrInvalidKueueObservation
	}
	if q.validate() != nil {
		return reject()
	}
	jobEvidence, stage, err := r.ObserveJob(ctx, q.Job)
	if err != nil || !validKubernetesRef(jobEvidence.KueueWorkload) {
		return reject()
	}
	if jobEvidence.KueueQuotaReserved && jobEvidence.KueueClusterQueue != q.ClusterQueueName {
		return reject()
	}

	workloadPath := fmt.Sprintf("/apis/kueue.x-k8s.io/v1beta1/namespaces/%s/workloads/%s",
		q.Job.Namespace, jobEvidence.KueueWorkload.Name)
	localPath := fmt.Sprintf("/apis/kueue.x-k8s.io/v1beta1/namespaces/%s/localqueues/%s",
		q.Job.Namespace, q.LocalQueueName)
	clusterPath := fmt.Sprintf("/apis/kueue.x-k8s.io/v1beta1/clusterqueues/%s", q.ClusterQueueName)

	var wl, local, cluster kueueQueueObject
	if r.read(ctx, workloadPath, &wl) != nil || r.read(ctx, localPath, &local) != nil ||
		r.read(ctx, clusterPath, &cluster) != nil {
		return reject()
	}
	if !sameKueueObjectIdentity(wl.Metadata, kubeMeta{
		Namespace:       jobEvidence.KueueWorkload.Namespace,
		Name:            jobEvidence.KueueWorkload.Name,
		UID:             jobEvidence.KueueWorkload.UID,
		ResourceVersion: jobEvidence.KueueWorkload.ResourceVersion,
	}) || wl.Spec.QueueName != q.LocalQueueName {
		return reject()
	}
	localRef := refFromKube(local.Metadata)
	if !validKubernetesRef(localRef) || localRef.Namespace != q.Job.Namespace ||
		localRef.Name != q.LocalQueueName || localRef.UID != q.LocalQueueUID ||
		local.Spec.ClusterQueue != q.ClusterQueueName {
		return reject()
	}
	clusterRef := refFromKube(cluster.Metadata)
	if clusterRef.Namespace != "" || clusterRef.Name != q.ClusterQueueName ||
		clusterRef.UID != q.ClusterQueueUID || clusterRef.ResourceVersion == "" ||
		clusterRef.ResourceVersion != strings.TrimSpace(clusterRef.ResourceVersion) ||
		strings.ContainsRune(clusterRef.ResourceVersion, 0) {
		return reject()
	}
	// Re-list exact resources to fence name reuse, altered queue mappings, or
	// a Workload changing admission while the dependent queues are observed.
	var wlAgain, localAgain, clusterAgain kueueQueueObject
	if r.read(ctx, workloadPath, &wlAgain) != nil ||
		r.read(ctx, localPath, &localAgain) != nil ||
		r.read(ctx, clusterPath, &clusterAgain) != nil {
		return reject()
	}
	if !sameKueueObjectIdentity(wl.Metadata, wlAgain.Metadata) ||
		!sameKueueObjectIdentity(local.Metadata, localAgain.Metadata) ||
		!sameKueueObjectIdentity(cluster.Metadata, clusterAgain.Metadata) ||
		wlAgain.Spec.QueueName != q.LocalQueueName ||
		localAgain.Spec.ClusterQueue != q.ClusterQueueName {
		return reject()
	}
	// Confirm the pinned Job has not been replaced after queue reads.
	var jobAgain kubeObject
	jobPath := fmt.Sprintf("/apis/batch/v1/namespaces/%s/jobs/%s",
		q.Job.Namespace, q.Job.JobName)
	if r.read(ctx, jobPath, &jobAgain) != nil ||
		!sameKueueObjectIdentity(jobAgain.Metadata, kubeMeta{
			Namespace: jobEvidence.Job.Namespace, Name: jobEvidence.Job.Name,
			UID: jobEvidence.Job.UID, ResourceVersion: jobEvidence.Job.ResourceVersion,
		}) {
		return reject()
	}
	return KueueQueueBindingEvidence{
		Job: jobEvidence, Stage: stage, LocalQueue: localRef, ClusterQueue: clusterRef,
	}, nil
}
