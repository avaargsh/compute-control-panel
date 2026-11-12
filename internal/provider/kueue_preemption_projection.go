package provider

import (
	"errors"
	"strconv"
	"strings"
)

// ErrStaleKueuePreemption means an observation for a UID is older than the
// fenced resourceVersion. The caller must keep the newer fence.
var ErrStaleKueuePreemption = errors.New("stale kueue preemption resourceVersion")

// ErrCrossKueuePreemption means an observation for one Job or Workload UID was
// applied to a fence for a different UID.
var ErrCrossKueuePreemption = errors.New("kueue preemption projection crossed identities")

// KueuePreemptionProjection is a fail-closed reading of one Job and its Kueue
// Workload. ADMITTED means quota is reserved and the Workload is admitted.
// NOT_READY / PREEMPTED means Kueue reported Evicted reason Preempted and
// Preempted reason InClusterQueue, with QuotaReserved and Admitted both false.
// Neither value is Allocation allocated, active, READY, costable, or RELEASED.
// Preemption is not release.
type KueuePreemptionProjection struct {
	Phase                   string
	Disposition             string
	Stage                   KueueObservationStage
	Evicted                 bool
	EvictedReason           string
	Preempted               bool
	PreemptedReason         string
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
	PodScheduled            bool
	NodeName                string
	Allocated               bool
	Active                  bool
	Ready                   bool
	Costable                bool
	Released                bool
}

// KueuePreemptionFence is the newest accepted projection for one Job UID and
// its Workload UID. ResourceVersions are compared only within the same object.
type KueuePreemptionFence struct {
	JobUID                  string
	JobResourceVersion      string
	WorkloadUID             string
	WorkloadResourceVersion string
	Phase                   string
	Disposition             string
}

func inactivePreemptionProjection(p KueuePreemptionProjection) bool {
	return !p.Allocated && !p.Active && !p.Ready && !p.Costable && !p.Released
}

func kubePositiveResourceVersion(v string) (uint64, bool) {
	if v == "" || len(v) > 20 || strings.TrimLeft(v, "0") != v {
		return 0, false
	}
	n, err := strconv.ParseUint(v, 10, 64)
	if err != nil || n == 0 {
		return 0, false
	}
	return n, true
}

func conditionObserved(c KueueConditionEvidence) bool {
	return c.Status != "" && validKueueCondition(c)
}

func evictedOrPreempted(e KueueJobEvidence) bool {
	return e.KueueEvicted || e.KueuePreempted ||
		e.KueueEvictedCondition.Status == "True" ||
		e.KueuePreemptedCondition.Status == "True"
}

// ProjectKueueAdmissionBeforePreemption accepts one admitted Workload. The
// Pod may be absent or placed. Placement does not make the projection
// allocated, active, READY, costable, or RELEASED. Evicted or Preempted
// evidence is rejected.
func ProjectKueueAdmissionBeforePreemption(e KueueJobEvidence) (KueuePreemptionProjection, error) {
	reject := func() (KueuePreemptionProjection, error) {
		return KueuePreemptionProjection{}, ErrInvalidKueueObservation
	}
	stage, err := AssessKueueJobObservation(e)
	if err != nil || (stage != KueueAdmitted && stage != KueuePodPlaced) {
		return reject()
	}
	if !e.KueueQuotaReserved || !e.KueueAdmitted || e.KueueClusterQueue == "" ||
		e.KueuePodSetAssignmentsJSON == "" || e.KueueOwnerJobUID != e.Job.UID ||
		e.KueueQuotaCondition.Status != "True" || e.KueueAdmissionCondition.Status != "True" {
		return reject()
	}
	if !conditionObserved(e.KueueEvictedCondition) || !conditionObserved(e.KueuePreemptedCondition) ||
		evictedOrPreempted(e) {
		return reject()
	}
	if e.KueueEvictedCondition.Status == "Unknown" || e.KueuePreemptedCondition.Status == "Unknown" {
		return reject()
	}
	if !validKubernetesRef(e.KueueWorkload) || !validKubernetesRef(e.Job) ||
		e.KueueWorkload.Namespace != e.Job.Namespace || e.Job.UID == e.KueueWorkload.UID {
		return reject()
	}
	if _, ok := kubePositiveResourceVersion(e.Job.ResourceVersion); !ok {
		return reject()
	}
	if _, ok := kubePositiveResourceVersion(e.KueueWorkload.ResourceVersion); !ok {
		return reject()
	}
	podUID := ""
	if presentKubernetesRef(e.Pod) {
		if !validKubernetesRef(e.Pod) || e.Pod.Namespace != e.Job.Namespace ||
			e.PodOwnerJobUID != e.Job.UID || e.Pod.UID == e.Job.UID || e.Pod.UID == e.KueueWorkload.UID {
			return reject()
		}
		podUID = e.Pod.UID
	} else if e.PodOwnerJobUID != "" || e.PodScheduled || e.PodNodeName != "" || stage == KueuePodPlaced {
		return reject()
	}
	out := KueuePreemptionProjection{
		Phase:                   "ADMITTED",
		Disposition:             "ADMITTED",
		Stage:                   stage,
		Evicted:                 false,
		Preempted:               false,
		QuotaReserved:           true,
		Admitted:                true,
		ClusterQueue:            e.KueueClusterQueue,
		WorkloadNamespace:       e.KueueWorkload.Namespace,
		WorkloadName:            e.KueueWorkload.Name,
		WorkloadUID:             e.KueueWorkload.UID,
		WorkloadResourceVersion: e.KueueWorkload.ResourceVersion,
		JobNamespace:            e.Job.Namespace,
		JobName:                 e.Job.Name,
		JobUID:                  e.Job.UID,
		JobResourceVersion:      e.Job.ResourceVersion,
		PodUID:                  podUID,
		PodScheduled:            e.PodScheduled,
		NodeName:                e.PodNodeName,
	}
	if !inactivePreemptionProjection(out) || out.Ready || out.Released {
		return reject()
	}
	return out, nil
}

// ProjectKueuePreemptedWorkload accepts one Workload whose live conditions are
// Evicted=True reason Preempted, Preempted=True reason InClusterQueue,
// QuotaReserved=False, and Admitted=False. The phase is NOT_READY. It is not
// release, and it is not a ready or costable Allocation.
func ProjectKueuePreemptedWorkload(e KueueJobEvidence) (KueuePreemptionProjection, error) {
	reject := func() (KueuePreemptionProjection, error) {
		return KueuePreemptionProjection{}, ErrInvalidKueueObservation
	}
	stage, err := AssessKueueJobObservation(e)
	if err != nil || stage != KueueUnproven {
		return reject()
	}
	if !conditionObserved(e.KueueEvictedCondition) || !conditionObserved(e.KueuePreemptedCondition) ||
		!conditionObserved(e.KueueQuotaCondition) || !conditionObserved(e.KueueAdmissionCondition) {
		return reject()
	}
	if e.KueueEvictedCondition.Status != "True" || e.KueueEvictedCondition.Reason != "Preempted" ||
		!e.KueueEvicted || e.KueuePreemptedCondition.Status != "True" ||
		e.KueuePreemptedCondition.Reason != "InClusterQueue" || !e.KueuePreempted {
		return reject()
	}
	if e.KueueQuotaCondition.Status != "False" || e.KueueQuotaReserved ||
		e.KueueAdmissionCondition.Status != "False" || e.KueueAdmitted ||
		e.KueueClusterQueue != "" || e.KueuePodSetAssignmentsJSON != "" {
		return reject()
	}
	if presentKubernetesRef(e.Pod) || e.PodOwnerJobUID != "" || e.PodScheduled || e.PodNodeName != "" {
		return reject()
	}
	if !validKubernetesRef(e.KueueWorkload) || !validKubernetesRef(e.Job) ||
		e.KueueWorkload.Namespace != e.Job.Namespace || e.KueueOwnerJobUID != e.Job.UID ||
		e.Job.UID == e.KueueWorkload.UID {
		return reject()
	}
	if _, ok := kubePositiveResourceVersion(e.Job.ResourceVersion); !ok {
		return reject()
	}
	if _, ok := kubePositiveResourceVersion(e.KueueWorkload.ResourceVersion); !ok {
		return reject()
	}
	out := KueuePreemptionProjection{
		Phase:                   "NOT_READY",
		Disposition:             "PREEMPTED",
		Stage:                   stage,
		Evicted:                 true,
		EvictedReason:           "Preempted",
		Preempted:               true,
		PreemptedReason:         "InClusterQueue",
		QuotaReserved:           false,
		Admitted:                false,
		WorkloadNamespace:       e.KueueWorkload.Namespace,
		WorkloadName:            e.KueueWorkload.Name,
		WorkloadUID:             e.KueueWorkload.UID,
		WorkloadResourceVersion: e.KueueWorkload.ResourceVersion,
		JobNamespace:            e.Job.Namespace,
		JobName:                 e.Job.Name,
		JobUID:                  e.Job.UID,
		JobResourceVersion:      e.Job.ResourceVersion,
	}
	if !inactivePreemptionProjection(out) || out.Phase != "NOT_READY" || out.Released || out.Ready {
		return reject()
	}
	return out, nil
}

func (p KueuePreemptionProjection) fence() (KueuePreemptionFence, error) {
	if !inactivePreemptionProjection(p) || p.Ready || p.Released || p.Allocated || p.Active || p.Costable {
		return KueuePreemptionFence{}, ErrInvalidKueueObservation
	}
	switch {
	case p.Phase == "ADMITTED" && p.Disposition == "ADMITTED" && p.Admitted && p.QuotaReserved && !p.Evicted && !p.Preempted:
	case p.Phase == "NOT_READY" && p.Disposition == "PREEMPTED" && !p.Admitted && !p.QuotaReserved &&
		p.Evicted && p.EvictedReason == "Preempted" && p.Preempted && p.PreemptedReason == "InClusterQueue":
	default:
		return KueuePreemptionFence{}, ErrInvalidKueueObservation
	}
	if p.JobUID == "" || p.WorkloadUID == "" || p.JobUID == p.WorkloadUID {
		return KueuePreemptionFence{}, ErrInvalidKueueObservation
	}
	if _, ok := kubePositiveResourceVersion(p.JobResourceVersion); !ok {
		return KueuePreemptionFence{}, ErrInvalidKueueObservation
	}
	if _, ok := kubePositiveResourceVersion(p.WorkloadResourceVersion); !ok {
		return KueuePreemptionFence{}, ErrInvalidKueueObservation
	}
	return KueuePreemptionFence{
		JobUID:                  p.JobUID,
		JobResourceVersion:      p.JobResourceVersion,
		WorkloadUID:             p.WorkloadUID,
		WorkloadResourceVersion: p.WorkloadResourceVersion,
		Phase:                   p.Phase,
		Disposition:             p.Disposition,
	}, nil
}

func (f KueuePreemptionFence) empty() bool {
	return f == (KueuePreemptionFence{})
}

// AdvanceKueuePreemptionFence records next only when it is the same Job and
// Workload as prior and both resourceVersions move forward, or when it repeats
// the fenced UID and resourceVersion exactly. An older resourceVersion returns
// the prior fence and ErrStaleKueuePreemption. A different UID returns the
// prior fence and ErrCrossKueuePreemption. On error the returned fence is the
// one the caller must keep.
func AdvanceKueuePreemptionFence(prior KueuePreemptionFence, next KueuePreemptionProjection) (KueuePreemptionFence, error) {
	keep := func(err error) (KueuePreemptionFence, error) {
		return prior, err
	}
	observed, err := next.fence()
	if err != nil {
		return keep(err)
	}
	if prior.empty() {
		if observed.Phase != "ADMITTED" {
			return keep(ErrInvalidKueueObservation)
		}
		return observed, nil
	}
	if prior.JobUID != observed.JobUID || prior.WorkloadUID != observed.WorkloadUID {
		return keep(fmtWrapCross())
	}
	priorJob, okJob := kubePositiveResourceVersion(prior.JobResourceVersion)
	priorWorkload, okWorkload := kubePositiveResourceVersion(prior.WorkloadResourceVersion)
	nextJob, okNextJob := kubePositiveResourceVersion(observed.JobResourceVersion)
	nextWorkload, okNextWorkload := kubePositiveResourceVersion(observed.WorkloadResourceVersion)
	if !okJob || !okWorkload || !okNextJob || !okNextWorkload ||
		prior.Phase == "" || prior.Disposition == "" {
		return keep(ErrInvalidKueueObservation)
	}
	if nextJob < priorJob || nextWorkload < priorWorkload {
		return keep(fmtWrapStale())
	}
	if nextJob == priorJob && nextWorkload == priorWorkload {
		if prior.Phase != observed.Phase || prior.Disposition != observed.Disposition {
			return keep(ErrInvalidKueueObservation)
		}
		return prior, nil
	}
	if nextWorkload <= priorWorkload || nextJob < priorJob {
		return keep(ErrInvalidKueueObservation)
	}
	if prior.Disposition == "PREEMPTED" && observed.Disposition != "PREEMPTED" {
		return keep(ErrInvalidKueueObservation)
	}
	if prior.Disposition == "ADMITTED" && observed.Disposition == "PREEMPTED" && nextWorkload <= priorWorkload {
		return keep(ErrInvalidKueueObservation)
	}
	return observed, nil
}

func fmtWrapStale() error {
	return errors.Join(ErrInvalidKueueObservation, ErrStaleKueuePreemption)
}

func fmtWrapCross() error {
	return errors.Join(ErrInvalidKueueObservation, ErrCrossKueuePreemption)
}
