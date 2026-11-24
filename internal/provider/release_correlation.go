package provider

import (
	"errors"
	"strings"

	"github.com/avaargsh/compute-control-panel/internal/operation"
)

// ErrUncorrelatedReleaseEvidence means independent source observations cannot
// be joined to the same durable Allocation and exact Kubernetes identities.
// It does NOT establish that a GPU or Provider resource is still present.
var ErrUncorrelatedReleaseEvidence = errors.New("release evidence sources do not correlate")

// ReleaseEvidenceCandidate contains separately collected, caller-attested
// source records for a single historical Allocation. The trusted repository
// and identity/policy authority MUST authenticate these records and their
// provenance before any production use. An API caller must not construct it.
//
// This does not prove source finality, physical release or accurate metering.
type ReleaseEvidenceCandidate struct {
	Create           operation.Operation
	Delete           operation.Operation
	ProviderAbsence  operation.Observation
	PodAbsent        KubernetesPodAbsentObservation
	PriorQueue       KueueQueueBindingEvidence
	QuotaCleared     KueueQuotaClearedObservation
	PriorPodClaim    DRAPodClaimObservation
	ClaimUnallocated DRAClaimUnallocatedObservation
	// ClaimAbsent is the alternative to a still-existing unallocated Claim.
	// It is a Kubernetes API observation, never a driver release receipt.
	ClaimAbsent *DRAClaimUIDAbsentObservation

	// PreDeleteSnapshot is the durable immutable CREATE/DELETE allocation
	// history read from the binding store. It is required before a signed
	// NodeUnprepare receipt can be correlated; it is structural history,
	// not an authenticated source attestation.
	PreDeleteSnapshot *KueuePreDeleteDRASnapshot

	// ExpectedAllocationID is the historical Allocation identity the trusted
	// durable-state caller intends to correlate. It is only a structural
	// cross-check: callers must independently authenticate it. A newer
	// DELETE cannot carry the older Allocation's FK under migration 0003.
	ExpectedAllocationID string
}

// ReleaseEvidenceCorrelation contains only identities that matched across
// supplied observations. It deliberately has NO verified/released/ready/final
// flag and does not permit an Allocation or accounting state transition.
type ReleaseEvidenceCorrelation struct {
	TenantID          string
	WorkloadID        string
	AllocationID      string
	ProviderID        string
	DeleteOperationID string
	Job               KubernetesResourceRef
	Pod               KubernetesResourceRef
	Claim             KubernetesResourceRef
	Device            DRAAllocatedDevice
}

func sameKubeResourceIdentity(a, b KubernetesResourceRef) bool {
	return a.Namespace == b.Namespace && a.Name == b.Name && a.UID == b.UID
}

func validReleaseDeletePair(c, d operation.Operation, obs operation.Observation, expectedAllocationID string) bool {
	if c.Validate() != nil || d.Validate() != nil ||
		c.Kind != operation.Create ||
		(c.Status != operation.Succeeded &&
			!(c.Status == operation.Superseded && c.CleanupRequired)) ||
		d.Kind != operation.Delete || d.Status != operation.Succeeded ||
		c.ID == d.ID || c.AllocationID == "" ||
		expectedAllocationID == "" || expectedAllocationID != strings.TrimSpace(expectedAllocationID) ||
		strings.ContainsRune(expectedAllocationID, 0) ||
		c.AllocationID != expectedAllocationID || d.AllocationID != "" ||
		c.TenantID != d.TenantID || c.WorkloadID != d.WorkloadID ||
		c.ProviderID != d.ProviderID ||
		// DELETE must represent a newer desired transition than the CREATE.
		d.Generation <= c.Generation || c.ProviderResourceID == "" ||
		c.ProviderResourceUID == "" ||
		d.TargetResourceID != c.ProviderResourceID ||
		d.TargetResourceUID != c.ProviderResourceUID {
		return false
	}
	return obs.OperationID == d.ID &&
		obs.TenantID == d.TenantID &&
		obs.WorkloadID == d.WorkloadID &&
		obs.ProviderID == d.ProviderID &&
		obs.DesiredGeneration == d.Generation &&
		obs.Result == operation.TargetAbsent &&
		obs.TargetResourceID == d.TargetResourceID &&
		obs.TargetResourceUID == d.TargetResourceUID &&
		obs.ResourceID == "" && obs.ResourceUID == "" &&
		obs.ResourceOperationID == "" && obs.ResourceGeneration == 0 &&
		obs.ErrorCode == "" && obs.ReasonCode == "" &&
		obs.ResourceVersion != "" &&
		obs.ResourceVersion == strings.TrimSpace(obs.ResourceVersion) &&
		!strings.ContainsRune(obs.ResourceVersion, 0) &&
		!obs.ObservedAt.IsZero()
}

func validReleaseQueueCorrelation(e ReleaseEvidenceCandidate) bool {
	q := e.PriorQueue
	p := q.Job
	c := e.Create
	d := e.Delete
	qc := e.QuotaCleared
	if !validPreviousKueueReservation(q) || q.Stage != KueuePodPlaced ||
		// A historical node-bound Pod is not a legitimate Kueue-placed
		// snapshot without native Admitted=True. QuotaReserved alone is
		// insufficient, and an UNKNOWN/absent condition is not a true one.
		!p.KueueAdmitted || p.KueueAdmissionCondition.Status != "True" ||
		!validKueueCondition(p.KueueAdmissionCondition) ||
		!validKueueCondition(p.KueueQuotaCondition) ||
		p.TenantID != c.TenantID || p.WorkloadID != c.WorkloadID ||
		p.JobTenantID != c.TenantID || p.JobWorkloadID != c.WorkloadID ||
		p.DesiredGeneration != c.Generation ||
		p.JobDesiredGeneration != c.Generation ||
		!validKubernetesRef(p.Job) || !validKubernetesRef(p.Pod) ||
		!validKubeName(p.Job.Namespace) || !validKubeName(p.Job.Name) ||
		!validKubeName(p.Pod.Name) ||
		p.Job.Namespace != p.Pod.Namespace ||
		p.Job.Namespace+"/"+p.Job.Name != d.TargetResourceID ||
		p.Job.UID != d.TargetResourceUID ||
		p.PodOwnerJobUID != p.Job.UID || !p.PodScheduled ||
		!validDRAIdentity(p.PodNodeName) {
		return false
	}
	if qc.PreviousWorkload != p.KueueWorkload ||
		!sameKubeResourceIdentity(qc.LocalQueue, q.LocalQueue) ||
		!validKubernetesRef(qc.LocalQueue) ||
		qc.ClusterQueue.Namespace != "" || qc.ClusterQueue.Name != q.ClusterQueue.Name ||
		qc.ClusterQueue.UID != q.ClusterQueue.UID ||
		qc.ClusterQueue.ResourceVersion == "" ||
		qc.ClusterQueue.ResourceVersion != strings.TrimSpace(qc.ClusterQueue.ResourceVersion) ||
		strings.ContainsRune(qc.ClusterQueue.ResourceVersion, 0) ||
		qc.WorkloadListResourceVersion == "" ||
		qc.WorkloadListResourceVersion != strings.TrimSpace(qc.WorkloadListResourceVersion) ||
		strings.ContainsRune(qc.WorkloadListResourceVersion, 0) {
		return false
	}
	return true
}

func validReleaseClaimCorrelation(e ReleaseEvidenceCandidate) bool {
	p := e.PriorQueue.Job
	d := e.PriorPodClaim
	u := e.ClaimUnallocated
	a := e.PodAbsent
	if a.PreviousPod != p.Pod ||
		!validKubernetesRef(a.PreviousPod) ||
		a.PodListResourceVersion == "" ||
		a.PodListResourceVersion != strings.TrimSpace(a.PodListResourceVersion) ||
		strings.ContainsRune(a.PodListResourceVersion, 0) ||
		!validKubernetesRef(d.Pod) || d.Pod != p.Pod ||
		d.NodeName != p.PodNodeName ||
		!validKubeName(d.PodClaimAlias) ||
		!validKubeName(d.ConsumerContainer) ||
		!validPriorDRAAllocation(d.Claim) ||
		d.Claim.Claim.Namespace != d.Pod.Namespace ||
		d.Claim.PodUID != d.Pod.UID {
		return false
	}
	// A deleted Claim cannot also be asserted as a still-existing,
	// unallocated Claim. Never combine competing snapshots.
	if e.ClaimAbsent != nil {
		absent := e.ClaimAbsent
		return u == (DRAClaimUnallocatedObservation{}) &&
			absent.PreviousClaim == d.Claim.Claim &&
			absent.ClaimListResourceVersion != "" &&
			absent.ClaimListResourceVersion == strings.TrimSpace(absent.ClaimListResourceVersion) &&
			!strings.ContainsRune(absent.ClaimListResourceVersion, 0)
	}
	if u.PreviousClaim != d.Claim.Claim ||
		u.PreviousPodUID != d.Claim.PodUID ||
		u.PreviousDevice != d.Claim.Device ||
		!validKubernetesRef(u.CurrentClaim) ||
		!sameKubeResourceIdentity(u.CurrentClaim, u.PreviousClaim) ||
		u.CurrentClaim.ResourceVersion == u.PreviousClaim.ResourceVersion {
		return false
	}
	return true
}

// CorrelateReleaseEvidence only checks the historical CREATE Allocation and a
// newer DELETE's exact resource/operation identities, plus cross-source
// structural consistency, including an independent
// API observation that the historical Pod UID/name is absent. Inputs must originate
// from separate, authenticated stores/collectors; this is not a trust
// resolver. Even a correlated PodList absence snapshot cannot establish Pod
// deletion finality, kubelet/driver physical deallocation,
// Provider final absence, or the end of GPU cost liability.
//
// No backend mutation, state transition, capability grant or cost posting.
func CorrelateReleaseEvidence(e ReleaseEvidenceCandidate) (ReleaseEvidenceCorrelation, error) {
	if !validReleaseDeletePair(e.Create, e.Delete, e.ProviderAbsence, e.ExpectedAllocationID) ||
		!validReleaseQueueCorrelation(e) ||
		!validReleaseClaimCorrelation(e) {
		return ReleaseEvidenceCorrelation{}, ErrUncorrelatedReleaseEvidence
	}
	claim := e.ClaimUnallocated.CurrentClaim
	if e.ClaimAbsent != nil {
		claim = e.ClaimAbsent.PreviousClaim
	}
	return ReleaseEvidenceCorrelation{
		TenantID:          e.Delete.TenantID,
		WorkloadID:        e.Delete.WorkloadID,
		AllocationID:      e.Create.AllocationID,
		ProviderID:        e.Delete.ProviderID,
		DeleteOperationID: e.Delete.ID,
		Job:               e.PriorQueue.Job.Job,
		Pod:               e.PriorPodClaim.Pod,
		Claim:             claim,
		Device:            e.PriorPodClaim.Claim.Device,
	}, nil
}
