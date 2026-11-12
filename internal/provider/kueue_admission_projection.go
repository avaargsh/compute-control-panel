package provider

// AdmissionPlacementProjection separates Kueue admission and quota reservation
// from Job/Pod identity and node binding. Phase is NOT_READY. This value is
// not an Allocation transition: allocated, active, READY, costable, and
// RELEASED stay false. Pod placement and device assignment are not decided here.
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

// ProjectAdmissionWithoutPlacement records one admitted, quota-reserved
// Workload whose Pod has a UID and no node binding. A placed Pod, a missing
// Pod, or contradictory evidence returns no projection. The returned phase is
// NOT_READY. It never reports the Allocation as allocated, active, READY,
// costable, or RELEASED.
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
	if !validKubernetesRef(e.KueueWorkload) || !validKubernetesRef(e.Job) || !validKubernetesRef(e.Pod) {
		return reject()
	}
	if e.KueueWorkload.Namespace != e.Job.Namespace || e.Pod.Namespace != e.Job.Namespace ||
		e.PodOwnerJobUID != e.Job.UID || e.PodScheduled || e.PodNodeName != "" {
		return reject()
	}
	if e.Job.UID == e.Pod.UID || e.Job.UID == e.KueueWorkload.UID || e.Pod.UID == e.KueueWorkload.UID {
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
