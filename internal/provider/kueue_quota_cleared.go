package provider

import (
	"context"
	"fmt"
	"reflect"
	"strings"
)

// KueueQuotaClearedObservation proves only an idle ClusterQueue and absence
// of the previously observed Workload in two bounded, consistent API reads.
// It is NOT an authenticated release receipt, tenant-specific reservation
// accounting, Pod/Job disappearance, GPU/device release or Allocation RELEASED.
type KueueQuotaClearedObservation struct {
	PreviousWorkload           KubernetesResourceRef
	WorkloadListResourceVersion string
	LocalQueue                 KubernetesResourceRef
	ClusterQueue               KubernetesResourceRef
}

type kueueQuotaQueueObject struct {
	Metadata kubeMeta `json:"metadata"`
	Spec     struct {
		ClusterQueue string `json:"clusterQueue"`
	} `json:"spec"`
	Status struct {
		ReservingWorkloads *int32 `json:"reservingWorkloads"`
		AdmittedWorkloads  *int32 `json:"admittedWorkloads"`
	} `json:"status"`
}

func validPreviousKueueReservation(prior KueueQueueBindingEvidence) bool {
	old := prior.Job
	if (prior.Stage != KueueAdmitted && prior.Stage != KueuePodPlaced) ||
		!old.KueueQuotaReserved || old.KueueQuotaCondition.Status != "True" ||
		!validKubernetesRef(old.Job) || !validKubernetesRef(old.KueueWorkload) ||
		!validKubernetesRef(prior.LocalQueue) ||
		!validPinnedQueueUID(old.Job.UID) ||
		!validPinnedQueueUID(old.KueueWorkload.UID) ||
		!validPinnedQueueUID(prior.LocalQueue.UID) ||
		!validPinnedQueueUID(prior.ClusterQueue.UID) ||
		!validKubeName(prior.ClusterQueue.Name) ||
		prior.ClusterQueue.Namespace != "" ||
		prior.ClusterQueue.ResourceVersion == "" ||
		prior.ClusterQueue.ResourceVersion != strings.TrimSpace(prior.ClusterQueue.ResourceVersion) ||
		old.Job.Namespace != old.KueueWorkload.Namespace ||
		old.Job.Namespace != prior.LocalQueue.Namespace ||
		old.KueueOwnerJobUID != old.Job.UID ||
		old.KueueClusterQueue != prior.ClusterQueue.Name {
		return false
	}
	return true
}

func kueueWorkloadUIDAbsent(list kubeObjectList, prior KueueQueueBindingEvidence) bool {
	if list.Metadata.Continue != "" || list.Metadata.ResourceVersion == "" ||
		list.Metadata.ResourceVersion != strings.TrimSpace(list.Metadata.ResourceVersion) {
		return false
	}
	old := prior.Job
	for _, workload := range list.Items {
		ref := refFromKube(workload.Metadata)
		if !validKubernetesRef(ref) || ref.Namespace != old.KueueWorkload.Namespace ||
			ref.UID == old.KueueWorkload.UID || ref.Name == old.KueueWorkload.Name ||
			isControlledBy(workload.Metadata, old.Job.UID) {
			return false
		}
	}
	return true
}

func kueueQueueCleared(local, cluster kueueQuotaQueueObject, prior KueueQueueBindingEvidence) bool {
	lq := refFromKube(local.Metadata)
	cq := refFromKube(cluster.Metadata)
	if !validKubernetesRef(lq) || lq.Namespace != prior.LocalQueue.Namespace ||
		lq.Name != prior.LocalQueue.Name || lq.UID != prior.LocalQueue.UID ||
		local.Spec.ClusterQueue != prior.ClusterQueue.Name ||
		cq.Namespace != "" || cq.Name != prior.ClusterQueue.Name ||
		cq.UID != prior.ClusterQueue.UID || !validPinnedQueueUID(cq.UID) ||
		cq.ResourceVersion == "" || cq.ResourceVersion != strings.TrimSpace(cq.ResourceVersion) ||
		cluster.Status.ReservingWorkloads == nil || cluster.Status.AdmittedWorkloads == nil {
		return false
	}
	// The zero counters are for the *entire* ClusterQueue, not per tenant.
	// Shared active queues intentionally remain unproven in this strict slice.
	return *cluster.Status.ReservingWorkloads == 0 && *cluster.Status.AdmittedWorkloads == 0
}

// ObserveKueueQuotaCleared is deliberately a strict, read-only observation
// for a fully idle, already UID-pinned Kueue ClusterQueue. It does not derive
// per-Workload release from a global counter alone: the complete namespace
// Workload listing must also omit the previous UID/name/Job owner.
//
// Previous identity and API provenance MUST be authenticated elsewhere. Two
// sequential reads cannot provide transactional absence finality. Missing
// ClusterQueue counters, pagination, RBAC errors and any UID/RV drift fail
// closed. Do not use this result to authorize Allocation RELEASED or billing.
func (r *KubernetesObservationReader) ObserveKueueQuotaCleared(
	ctx context.Context, prior KueueQueueBindingEvidence,
) (KueueQuotaClearedObservation, error) {
	reject := func() (KueueQuotaClearedObservation, error) {
		return KueueQuotaClearedObservation{}, ErrInvalidKueueObservation
	}
	if r == nil || !validPreviousKueueReservation(prior) {
		return reject()
	}
	listPath := fmt.Sprintf("/apis/kueue.x-k8s.io/v1beta1/namespaces/%s/workloads",
		prior.Job.KueueWorkload.Namespace)
	localPath := fmt.Sprintf("/apis/kueue.x-k8s.io/v1beta1/namespaces/%s/localqueues/%s",
		prior.LocalQueue.Namespace, prior.LocalQueue.Name)
	clusterPath := fmt.Sprintf("/apis/kueue.x-k8s.io/v1beta1/clusterqueues/%s",
		prior.ClusterQueue.Name)

	var firstWL, secondWL kubeObjectList
	var firstLQ, secondLQ, firstCQ, secondCQ kueueQuotaQueueObject
	if r.read(ctx, listPath, &firstWL) != nil ||
		!kueueWorkloadUIDAbsent(firstWL, prior) ||
		r.read(ctx, localPath, &firstLQ) != nil ||
		r.read(ctx, clusterPath, &firstCQ) != nil ||
		!kueueQueueCleared(firstLQ, firstCQ, prior) {
		return reject()
	}
	if r.read(ctx, listPath, &secondWL) != nil ||
		!kueueWorkloadUIDAbsent(secondWL, prior) ||
		r.read(ctx, localPath, &secondLQ) != nil ||
		r.read(ctx, clusterPath, &secondCQ) != nil ||
		!kueueQueueCleared(secondLQ, secondCQ, prior) {
		return reject()
	}
	if firstWL.Metadata.ResourceVersion != secondWL.Metadata.ResourceVersion ||
		!reflect.DeepEqual(firstWL.Items, secondWL.Items) ||
		!sameKueueObjectIdentity(firstLQ.Metadata, secondLQ.Metadata) ||
		!sameKueueObjectIdentity(firstCQ.Metadata, secondCQ.Metadata) {
		return reject()
	}
	return KueueQuotaClearedObservation{
		PreviousWorkload: prior.Job.KueueWorkload,
		WorkloadListResourceVersion: firstWL.Metadata.ResourceVersion,
		LocalQueue: refFromKube(firstLQ.Metadata),
		ClusterQueue: refFromKube(firstCQ.Metadata),
	}, nil
}
