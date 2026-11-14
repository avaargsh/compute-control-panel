package provider

import "context"

// KubernetesReleaseSnapshot contains correlated *API-object observations*
// from one read-only Kubernetes client. The three component observations are
// collected and rechecked in a second sweep so an intervening resourceVersion
// change cannot be silently joined as one stable snapshot.
//
// This is NOT an atomic snapshot, source attestation, physical device release,
// Pod termination, Provider finality, or an Allocation RELEASED decision.
type KubernetesReleaseSnapshot struct {
	PodAbsent        KubernetesPodAbsentObservation
	QuotaCleared     KueueQuotaClearedObservation
	ClaimUnallocated DRAClaimUnallocatedObservation
}

// ObserveKubernetesReleaseSnapshot reads the historical Pod, Kueue queue and
// DRA Claim using the same caller-supplied KubernetesObservationReader.
// Every source gets two consistent reads per sweep; the second sweep must
// agree exactly with the first. No Kubernetes writes are performed.
//
// The historical evidence and the HTTPS transport authority must be
// independently authenticated. Callers must not promote the returned
// structural facts to physical GPU deallocation, cost finality or RELEASED.
func (r *KubernetesObservationReader) ObserveKubernetesReleaseSnapshot(
	ctx context.Context,
	queue KueueQueueBindingEvidence,
	podClaim DRAPodClaimObservation,
) (KubernetesReleaseSnapshot, error) {
	reject := func() (KubernetesReleaseSnapshot, error) {
		return KubernetesReleaseSnapshot{}, ErrUncorrelatedReleaseEvidence
	}
	job := queue.Job
	if r == nil || queue.Stage != KueuePodPlaced ||
		!validPreviousKueueReservation(queue) ||
		!validKubernetesRef(job.Pod) ||
		!job.PodScheduled || !validDRAIdentity(job.PodNodeName) ||
		podClaim.Pod != job.Pod ||
		podClaim.NodeName != job.PodNodeName ||
		!validKubeName(podClaim.PodClaimAlias) ||
		!validKubeName(podClaim.ConsumerContainer) ||
		!validPriorDRAAllocation(podClaim.Claim) ||
		podClaim.Claim.PodUID != job.Pod.UID ||
		podClaim.Claim.Claim.Namespace != job.Pod.Namespace {
		return reject()
	}
	collect := func() (KubernetesReleaseSnapshot, error) {
		pod, err := r.ObservePodUIDAbsent(ctx, job.Pod)
		if err != nil {
			return KubernetesReleaseSnapshot{}, err
		}
		quota, err := r.ObserveKueueQuotaCleared(ctx, queue)
		if err != nil {
			return KubernetesReleaseSnapshot{}, err
		}
		claim, err := r.ObserveDRAClaimUnallocated(ctx, podClaim.Claim)
		if err != nil {
			return KubernetesReleaseSnapshot{}, err
		}
		return KubernetesReleaseSnapshot{
			PodAbsent:        pod,
			QuotaCleared:     quota,
			ClaimUnallocated: claim,
		}, nil
	}
	first, err := collect()
	if err != nil {
		return reject()
	}
	second, err := collect()
	if err != nil || first != second {
		return reject()
	}
	return first, nil
}
