package provider

// AdmissionPlacementProjection separates Kueue admission and quota reservation
// from Job/Pod identity and node binding. Phase is NOT_READY. This value is
// not an Allocation transition: allocated, active, READY, costable, and
// RELEASED stay false. Node binding is represented only when observed;
// independent device assignment is never established here.
type AdmissionPlacementProjection struct {
	Phase                   string
	Stage                   KueueObservationStage
	QuotaReserved           bool
	Admitted                bool
	ClusterQueue            string
	PodSetAssignmentsJSON   string
	WorkloadNamespace       string
	WorkloadName            string
	WorkloadUID             string
	WorkloadResourceVersion string
	JobNamespace            string
	JobName                 string
	JobUID                  string
	JobResourceVersion      string
	PodNamespace            string
	PodName                 string
	PodUID                  string
	PodResourceVersion      string
	PodScheduled            bool
	NodeBound               bool
	NodeName                string
	Allocated               bool
	Active                  bool
	Ready                   bool
	Costable                bool
	Released                bool
}

// ProjectAdmissionWithoutPlacement records an admitted, quota-reserved
// Workload before placement. A Pod may be absent or may have a UID without
// a node binding; an absent Pod retains empty Pod identity fields. A placed
// Pod or contradictory evidence returns no projection. The returned phase is
// NOT_READY, never allocated, active, READY, costable, or RELEASED.
func ProjectAdmissionWithoutPlacement(e KueueJobEvidence) (AdmissionPlacementProjection, error) {
	reject := func() (AdmissionPlacementProjection, error) {
		return AdmissionPlacementProjection{}, ErrInvalidKueueObservation
	}
	stage, err := AssessKueueJobObservation(e)
	if err != nil || stage != KueueAdmitted {
		return reject()
	}
	if !e.KueueQuotaReserved || !e.KueueAdmitted || e.KueueClusterQueue == "" ||
		e.KueuePodSetAssignmentsJSON == "" || e.KueueOwnerJobUID != e.Job.UID {
		return reject()
	}
	// The reader preserves absent native conditions as explicit MISSING.
	// Do not accept zero-value, malformed, or future-unknown condition data
	// as proof that this admitted Workload has not been evicted/preempted.
	if !conditionObserved(e.KueueEvictedCondition) ||
		!conditionObserved(e.KueuePreemptedCondition) ||
		evictedOrPreempted(e) ||
		e.KueueEvictedCondition.Status == "Unknown" ||
		e.KueuePreemptedCondition.Status == "Unknown" {
		return reject()
	}
	podPresent := presentKubernetesRef(e.Pod)
	if !validKubernetesRef(e.KueueWorkload) || !validKubernetesRef(e.Job) ||
		e.KueueWorkload.Namespace != e.Job.Namespace || e.PodScheduled || e.PodNodeName != "" ||
		e.Job.UID == e.KueueWorkload.UID {
		return reject()
	}
	if podPresent {
		if !validKubernetesRef(e.Pod) || e.Pod.Namespace != e.Job.Namespace ||
			e.PodOwnerJobUID != e.Job.UID || e.Job.UID == e.Pod.UID ||
			e.Pod.UID == e.KueueWorkload.UID {
			return reject()
		}
	} else if e.PodOwnerJobUID != "" {
		return reject()
	}
	return AdmissionPlacementProjection{
		Phase:                   "NOT_READY",
		Stage:                   stage,
		QuotaReserved:           true,
		Admitted:                true,
		ClusterQueue:            e.KueueClusterQueue,
		PodSetAssignmentsJSON:   e.KueuePodSetAssignmentsJSON,
		WorkloadNamespace:       e.KueueWorkload.Namespace,
		WorkloadName:            e.KueueWorkload.Name,
		WorkloadUID:             e.KueueWorkload.UID,
		WorkloadResourceVersion: e.KueueWorkload.ResourceVersion,
		JobNamespace:            e.Job.Namespace,
		JobName:                 e.Job.Name,
		JobUID:                  e.Job.UID,
		JobResourceVersion:      e.Job.ResourceVersion,
		PodNamespace:            e.Pod.Namespace,
		PodName:                 e.Pod.Name,
		PodUID:                  e.Pod.UID,
		PodResourceVersion:      e.Pod.ResourceVersion,
		PodScheduled:            false,
		NodeBound:               false,
		NodeName:                "",
		Allocated:               false,
		Active:                  false,
		Ready:                   false,
		Costable:                false,
		Released:                false,
	}, nil
}

// ProjectPlacementWithoutDeviceProof exposes a read-only, node-bound Pod
// observation without pretending that Kueue admission or Kubernetes binding
// establishes a DRA/device-plugin assignment. It is intentionally NOT_READY,
// even though PodScheduled and NodeBound are true.
//
// The lack of device proof is UNKNOWN, not evidence of device absence. This
// method accepts no device evidence and cannot transition an Allocation,
// authorize GPU work, finalize release or make any interval costable. Callers
// must separately authenticate and correlate actual device assignment to the
// exact historical Pod/Claim, as well as handle a later preemption or UID swap.
func ProjectPlacementWithoutDeviceProof(e KueueJobEvidence) (AdmissionPlacementProjection, error) {
	reject := func() (AdmissionPlacementProjection, error) {
		return AdmissionPlacementProjection{}, ErrInvalidKueueObservation
	}
	stage, err := AssessKueueJobObservation(e)
	if err != nil || stage != KueuePodPlaced {
		return reject()
	}
	if !e.KueueQuotaReserved || !e.KueueAdmitted ||
		e.KueueQuotaCondition.Status != "True" ||
		e.KueueAdmissionCondition.Status != "True" ||
		e.KueueClusterQueue == "" || e.KueuePodSetAssignmentsJSON == "" ||
		e.KueueOwnerJobUID != e.Job.UID ||
		!e.PodScheduled || !validDRAIdentity(e.PodNodeName) ||
		!validKubernetesRef(e.Job) || !validKubernetesRef(e.KueueWorkload) ||
		!validKubernetesRef(e.Pod) ||
		e.Job.Namespace != e.KueueWorkload.Namespace ||
		e.Pod.Namespace != e.Job.Namespace || e.PodOwnerJobUID != e.Job.UID ||
		e.Job.UID == e.KueueWorkload.UID || e.Job.UID == e.Pod.UID ||
		e.KueueWorkload.UID == e.Pod.UID ||
		evictedOrPreempted(e) ||
		e.KueueEvictedCondition.Status == "Unknown" ||
		e.KueuePreemptedCondition.Status == "Unknown" {
		return reject()
	}
	return AdmissionPlacementProjection{
		Phase:                   "NOT_READY",
		Stage:                   stage,
		QuotaReserved:           true,
		Admitted:                true,
		ClusterQueue:            e.KueueClusterQueue,
		PodSetAssignmentsJSON:   e.KueuePodSetAssignmentsJSON,
		WorkloadNamespace:       e.KueueWorkload.Namespace,
		WorkloadName:            e.KueueWorkload.Name,
		WorkloadUID:             e.KueueWorkload.UID,
		WorkloadResourceVersion: e.KueueWorkload.ResourceVersion,
		JobNamespace:            e.Job.Namespace,
		JobName:                 e.Job.Name,
		JobUID:                  e.Job.UID,
		JobResourceVersion:      e.Job.ResourceVersion,
		PodNamespace:            e.Pod.Namespace,
		PodName:                 e.Pod.Name,
		PodUID:                  e.Pod.UID,
		PodResourceVersion:      e.Pod.ResourceVersion,
		PodScheduled:            true,
		NodeBound:               true,
		NodeName:                e.PodNodeName,
		Allocated:               false,
		Active:                  false,
		Ready:                   false,
		Costable:                false,
		Released:                false,
	}, nil
}
