package provider

import (
	"errors"
)

// RelistAttemptLimit is the maximum number of API re-list attempts a fresh
// process may make before it stops. Reconciliation is bounded. It does not
// retry until a ready or released Allocation appears.
const RelistAttemptLimit = 20

// ErrStaleKueueRelist means an observation for a UID came from an earlier CCP
// read than the fenced one and does not repeat it. The fenced read is kept.
// Kubernetes resourceVersion is not the order.
var ErrStaleKueueRelist = errors.New("stale kueue relist observation")

// ErrCrossKueueRelist means an observation for one Job or Workload UID was
// applied to a fence for a different UID. A same-name successor is not the
// original object.
var ErrCrossKueueRelist = errors.New("kueue relist projection crossed identities")

// ErrRelistInconclusive means this attempt did not prove the current object.
// It is not release, and it must not replace a newer conclusive fence.
var ErrRelistInconclusive = errors.New("kueue relist evidence inconclusive")

// RelistProjection is a fail-closed reading of one re-listed Job. NOT_READY
// means the Job and its Kueue Workload were read and match. UNKNOWN means the
// read was missing or inconclusive. Neither value is Allocation allocated,
// active, READY, costable, or RELEASED. A Kueue controller restart is not one
// of those transitions.
type RelistProjection struct {
	Phase                   string
	Stage                   KueueObservationStage
	Inconclusive            bool
	QuotaReserved           bool
	Admitted                bool
	ClusterQueue            string
	WorkloadNamespace       string
	WorkloadName            string
	WorkloadUID             string
	WorkloadResourceVersion string
	JobNamespace            string
	JobName                 string
	JobUID                  string
	JobResourceVersion      string
	PodUID                  string
	PodResourceVersion      string
	PodScheduled            bool
	NodeName                string
	// ReadSequence is the monotonic order CCP assigned when it performed this
	// re-list. It is not a Kubernetes resourceVersion and must not be derived
	// from one. Zero means the caller did not order the read.
	ReadSequence uint64
	Allocated    bool
	Active       bool
	Ready        bool
	Costable     bool
	Released     bool
}

// RelistFence is the accepted re-list for one Job UID. ResourceVersions are
// opaque strings compared only for equality. ReadSequence is the CCP order.
type RelistFence struct {
	JobUID                  string
	JobResourceVersion      string
	WorkloadUID             string
	WorkloadResourceVersion string
	PodUID                  string
	PodResourceVersion      string
	Phase                   string
	ReadSequence            uint64
}

func inactiveRelistProjection(p RelistProjection) bool {
	return !p.Allocated && !p.Active && !p.Ready && !p.Costable && !p.Released
}

func relistIdentityDistinct(p RelistProjection) bool {
	if p.JobUID == "" || p.JobUID == p.WorkloadUID {
		return false
	}
	if p.PodUID != "" && (p.PodUID == p.JobUID || p.PodUID == p.WorkloadUID) {
		return false
	}
	return true
}

// ProjectRelistSnapshot maps one collected Job snapshot to NOT_READY or
// UNKNOWN. Placed and unplaced pods both stay NOT_READY when the Workload is
// admitted. Missing or unknown admission stays UNKNOWN. Ready, allocated,
// active, costable, and released stay false.
func ProjectRelistSnapshot(e KueueJobEvidence) (RelistProjection, error) {
	reject := func() (RelistProjection, error) {
		return RelistProjection{}, ErrInvalidKueueObservation
	}
	stage, err := AssessKueueJobObservation(e)
	if err != nil || !validKubernetesRef(e.Job) {
		return reject()
	}
	if !opaqueResourceVersion(e.Job.ResourceVersion) {
		return reject()
	}
	out := RelistProjection{
		Stage:              stage,
		JobNamespace:       e.Job.Namespace,
		JobName:            e.Job.Name,
		JobUID:             e.Job.UID,
		JobResourceVersion: e.Job.ResourceVersion,
	}
	switch stage {
	case KueueAdmitted, KueuePodPlaced:
		if !e.KueueQuotaReserved || !e.KueueAdmitted || e.KueueClusterQueue == "" ||
			e.KueuePodSetAssignmentsJSON == "" || e.KueueOwnerJobUID != e.Job.UID ||
			e.KueueQuotaCondition.Status != "True" || e.KueueAdmissionCondition.Status != "True" ||
			!validKubernetesRef(e.KueueWorkload) || e.KueueWorkload.Namespace != e.Job.Namespace {
			return reject()
		}
		if !opaqueResourceVersion(e.KueueWorkload.ResourceVersion) {
			return reject()
		}
		out.Phase = "NOT_READY"
		out.QuotaReserved = true
		out.Admitted = true
		out.ClusterQueue = e.KueueClusterQueue
		out.WorkloadNamespace = e.KueueWorkload.Namespace
		out.WorkloadName = e.KueueWorkload.Name
		out.WorkloadUID = e.KueueWorkload.UID
		out.WorkloadResourceVersion = e.KueueWorkload.ResourceVersion
		if presentKubernetesRef(e.Pod) {
			if !validKubernetesRef(e.Pod) || e.Pod.Namespace != e.Job.Namespace ||
				e.PodOwnerJobUID != e.Job.UID {
				return reject()
			}
			if !opaqueResourceVersion(e.Pod.ResourceVersion) {
				return reject()
			}
			out.PodUID = e.Pod.UID
			out.PodResourceVersion = e.Pod.ResourceVersion
			out.PodScheduled = e.PodScheduled
			out.NodeName = e.PodNodeName
		} else if e.PodOwnerJobUID != "" || e.PodScheduled || e.PodNodeName != "" || stage == KueuePodPlaced {
			return reject()
		}
		if stage == KueuePodPlaced && (!out.PodScheduled || out.NodeName == "" || out.PodUID == "") {
			return reject()
		}
		if stage == KueueAdmitted && (out.PodScheduled || out.NodeName != "") {
			return reject()
		}
	case KueueUnproven:
		out.Phase = "UNKNOWN"
		out.Inconclusive = true
		if presentKubernetesRef(e.KueueWorkload) {
			if !validKubernetesRef(e.KueueWorkload) || e.KueueWorkload.Namespace != e.Job.Namespace ||
				e.KueueOwnerJobUID != e.Job.UID || e.KueueAdmitted || e.KueueQuotaReserved ||
				e.KueueClusterQueue != "" || e.KueuePodSetAssignmentsJSON != "" {
				return reject()
			}
			if !opaqueResourceVersion(e.KueueWorkload.ResourceVersion) {
				return reject()
			}
			out.WorkloadNamespace = e.KueueWorkload.Namespace
			out.WorkloadName = e.KueueWorkload.Name
			out.WorkloadUID = e.KueueWorkload.UID
			out.WorkloadResourceVersion = e.KueueWorkload.ResourceVersion
		} else if e.KueueOwnerJobUID != "" || e.KueueAdmitted || e.KueueQuotaReserved || e.KueueClusterQueue != "" {
			return reject()
		}
		if presentKubernetesRef(e.Pod) || e.PodOwnerJobUID != "" || e.PodScheduled || e.PodNodeName != "" {
			return reject()
		}
	default:
		return reject()
	}
	if !inactiveRelistProjection(out) || !relistIdentityDistinct(out) ||
		(out.Phase != "NOT_READY" && out.Phase != "UNKNOWN") ||
		(out.Phase == "UNKNOWN" && !out.Inconclusive) ||
		(out.Phase == "NOT_READY" && (out.Inconclusive || out.WorkloadUID == "")) {
		return reject()
	}
	return out, nil
}

func (p RelistProjection) fence() (RelistFence, error) {
	if !inactiveRelistProjection(p) || p.Ready || p.Released || p.Allocated || p.Active || p.Costable {
		return RelistFence{}, ErrInvalidKueueObservation
	}
	if p.JobUID == "" || !relistIdentityDistinct(p) || p.ReadSequence == 0 {
		return RelistFence{}, ErrInvalidKueueObservation
	}
	if !opaqueResourceVersion(p.JobResourceVersion) {
		return RelistFence{}, ErrInvalidKueueObservation
	}
	switch p.Phase {
	case "NOT_READY":
		if p.Inconclusive || p.WorkloadUID == "" || !p.Admitted || !p.QuotaReserved || p.ClusterQueue == "" {
			return RelistFence{}, ErrInvalidKueueObservation
		}
	case "UNKNOWN":
		if !p.Inconclusive || p.Admitted || p.QuotaReserved || p.ClusterQueue != "" || p.PodScheduled || p.NodeName != "" {
			return RelistFence{}, ErrInvalidKueueObservation
		}
	default:
		return RelistFence{}, ErrInvalidKueueObservation
	}
	if p.WorkloadUID != "" {
		if !opaqueResourceVersion(p.WorkloadResourceVersion) {
			return RelistFence{}, ErrInvalidKueueObservation
		}
	} else if p.WorkloadResourceVersion != "" {
		return RelistFence{}, ErrInvalidKueueObservation
	}
	if p.PodUID != "" {
		if !opaqueResourceVersion(p.PodResourceVersion) {
			return RelistFence{}, ErrInvalidKueueObservation
		}
	} else if p.PodResourceVersion != "" || p.PodScheduled || p.NodeName != "" {
		return RelistFence{}, ErrInvalidKueueObservation
	}
	return RelistFence{
		JobUID:                  p.JobUID,
		JobResourceVersion:      p.JobResourceVersion,
		WorkloadUID:             p.WorkloadUID,
		WorkloadResourceVersion: p.WorkloadResourceVersion,
		PodUID:                  p.PodUID,
		PodResourceVersion:      p.PodResourceVersion,
		Phase:                   p.Phase,
		ReadSequence:            p.ReadSequence,
	}, nil
}

func (f RelistFence) empty() bool {
	return f == (RelistFence{})
}

func relistContentEqual(prior, observed RelistFence) bool {
	return prior.JobUID == observed.JobUID &&
		prior.JobResourceVersion == observed.JobResourceVersion &&
		prior.WorkloadUID == observed.WorkloadUID &&
		prior.WorkloadResourceVersion == observed.WorkloadResourceVersion &&
		prior.PodUID == observed.PodUID &&
		prior.PodResourceVersion == observed.PodResourceVersion &&
		prior.Phase == observed.Phase
}

// AdvanceRelistFence accepts next when it repeats the fenced UID and
// resourceVersion, or when the same UID carries a different resourceVersion
// from a strictly later CCP read. ResourceVersions are compared only for
// equality. An earlier read with a different resourceVersion returns the prior
// fence and ErrStaleKueueRelist. A different UID returns the prior fence and
// ErrCrossKueueRelist. An inconclusive later read does not replace a NOT_READY
// fence and does not report release.
func AdvanceRelistFence(prior RelistFence, next RelistProjection) (RelistFence, error) {
	keep := func(err error) (RelistFence, error) {
		return prior, err
	}
	observed, err := next.fence()
	if err != nil {
		return keep(err)
	}
	if prior.empty() {
		return observed, nil
	}
	if prior.JobUID != observed.JobUID {
		return keep(errors.Join(ErrInvalidKueueObservation, ErrCrossKueueRelist))
	}
	if prior.WorkloadUID != "" && observed.WorkloadUID != "" && prior.WorkloadUID != observed.WorkloadUID {
		return keep(errors.Join(ErrInvalidKueueObservation, ErrCrossKueueRelist))
	}
	if prior.WorkloadUID != "" && observed.WorkloadUID == "" {
		return keep(errors.Join(ErrInvalidKueueObservation, ErrStaleKueueRelist))
	}
	if prior.ReadSequence == 0 || prior.Phase == "" || !opaqueResourceVersion(prior.JobResourceVersion) {
		return keep(ErrInvalidKueueObservation)
	}
	later := observed.ReadSequence > prior.ReadSequence
	if relistContentEqual(prior, observed) {
		if later {
			prior.ReadSequence = observed.ReadSequence
		}
		return prior, nil
	}
	if !later {
		if prior.PodUID != "" && observed.PodUID != "" && observed.PodUID != prior.PodUID {
			return keep(errors.Join(ErrInvalidKueueObservation, ErrCrossKueueRelist))
		}
		return keep(errors.Join(ErrInvalidKueueObservation, ErrStaleKueueRelist))
	}
	if prior.Phase == "NOT_READY" && observed.Phase == "UNKNOWN" {
		return keep(errors.Join(ErrInvalidKueueObservation, ErrRelistInconclusive))
	}
	return observed, nil
}

// ConvergeRelist rebuilds a fence from read, which must be a full API re-list.
// It does not take a previous in-memory observation. afterSequence is the CCP
// read already recorded; this process's reads are numbered strictly after it.
// expectJobUID, when set, rejects any other Job UID. The attempt count stops
// at maxAttempts.
func ConvergeRelist(maxAttempts int, expectJobUID string, afterSequence uint64, read func() (KueueJobEvidence, error)) (RelistFence, int, error) {
	if read == nil || maxAttempts < 1 || maxAttempts > RelistAttemptLimit {
		return RelistFence{}, 0, ErrInvalidKueueObservation
	}
	var fence RelistFence
	used := 0
	for used < maxAttempts {
		used++
		sequence := afterSequence + uint64(used)
		if sequence <= afterSequence {
			return fence, used, ErrInvalidKueueObservation
		}
		evidence, err := read()
		if err != nil {
			continue
		}
		projected, err := ProjectRelistSnapshot(evidence)
		if err != nil {
			continue
		}
		projected.ReadSequence = sequence
		if expectJobUID != "" && projected.JobUID != expectJobUID {
			return fence, used, errors.Join(ErrInvalidKueueObservation, ErrCrossKueueRelist)
		}
		next, err := AdvanceRelistFence(fence, projected)
		if err != nil {
			if errors.Is(err, ErrCrossKueueRelist) {
				return fence, used, err
			}
			continue
		}
		fence = next
		if fence.Phase == "NOT_READY" && fence.JobUID != "" && fence.WorkloadUID != "" {
			return fence, used, nil
		}
	}
	if !fence.empty() && (fence.Phase == "NOT_READY" || fence.Phase == "UNKNOWN") {
		return fence, used, nil
	}
	return RelistFence{}, used, ErrRelistInconclusive
}
