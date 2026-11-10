// Package provider contains provider-neutral operation contracts and strictly
// bounded observation helpers. This file does not perform Kubernetes I/O.
package provider

import (
	"errors"
	"strings"
)

// ErrInvalidKueueObservation means the supplied snapshot contradicts the
// expected, immutable Workload or Kubernetes resource identity. It does not
// establish whether any external resource actually exists.
var ErrInvalidKueueObservation = errors.New("inconsistent Kueue Job observation")

// KubernetesResourceRef is an observed Kubernetes API object identity.
// ResourceVersion is opaque and must not be compared across object kinds.
type KubernetesResourceRef struct {
	Namespace       string
	Name            string
	UID             string
	ResourceVersion string
}

// KueueJobEvidence contains read-only facts collected for the built-in Kueue
// Job integration. The caller must separately authenticate source, freshness,
// namespace/tenant binding and controller owner references before populating
// these fields. Untrusted HTTP request data is not evidence.
//
// No field claims device assignment, actual GPU execution, allocation
// readiness, quota-release finality or authority to mutate Kubernetes.
type KueueJobEvidence struct {
	TenantID          string
	WorkloadID        string
	DesiredGeneration uint64

	Job                  KubernetesResourceRef
	JobTenantID          string
	JobWorkloadID        string
	JobDesiredGeneration uint64

	KueueWorkload      KubernetesResourceRef
	KueueOwnerJobUID   string
	KueueQuotaReserved bool
	KueueClusterQueue  string
	KueueAdmitted      bool

	Pod            KubernetesResourceRef
	PodOwnerJobUID string
	PodScheduled   bool
	PodNodeName    string
}

// KueueObservationStage is a structural observation, NOT a Workload or
// Allocation state. Even KueuePodPlaced does not prove device assignment.
type KueueObservationStage string

const (
	KueueUnproven  KueueObservationStage = "UNPROVEN"
	KueueAdmitted  KueueObservationStage = "KUEUE_ADMITTED"
	KueuePodPlaced KueueObservationStage = "POD_PLACED"
)

func presentKubernetesRef(ref KubernetesResourceRef) bool {
	return ref != (KubernetesResourceRef{})
}

func validKubernetesRef(ref KubernetesResourceRef) bool {
	for _, field := range []string{ref.Namespace, ref.Name, ref.UID, ref.ResourceVersion} {
		if field == "" || field != strings.TrimSpace(field) || strings.ContainsRune(field, 0) {
			return false
		}
	}
	return true
}

// AssessKueueJobObservation conservatively classifies a single Job/Pod
// snapshot. It cannot mark a Compute Allocation READY, establish multi-Pod
// readiness, validate real device allocation, or finalize a deletion. On an
// identity contradiction it always returns UNPROVEN and an error.
func AssessKueueJobObservation(e KueueJobEvidence) (KueueObservationStage, error) {
	reject := func() (KueueObservationStage, error) {
		return KueueUnproven, ErrInvalidKueueObservation
	}
	if e.TenantID == "" || e.WorkloadID == "" || e.DesiredGeneration == 0 ||
		e.TenantID != strings.TrimSpace(e.TenantID) ||
		e.WorkloadID != strings.TrimSpace(e.WorkloadID) {
		return reject()
	}

	if !presentKubernetesRef(e.Job) {
		if e.JobTenantID != "" || e.JobWorkloadID != "" || e.JobDesiredGeneration != 0 ||
			presentKubernetesRef(e.KueueWorkload) || e.KueueOwnerJobUID != "" ||
			e.KueueAdmitted || e.KueueQuotaReserved || e.KueueClusterQueue != "" || presentKubernetesRef(e.Pod) ||
			e.PodOwnerJobUID != "" || e.PodScheduled || e.PodNodeName != "" {
			return reject()
		}
		return KueueUnproven, nil
	}
	if !validKubernetesRef(e.Job) || e.JobTenantID != e.TenantID ||
		e.JobWorkloadID != e.WorkloadID || e.JobDesiredGeneration != e.DesiredGeneration {
		return reject()
	}

	if !presentKubernetesRef(e.KueueWorkload) {
		if e.KueueOwnerJobUID != "" || e.KueueAdmitted || e.KueueQuotaReserved || e.KueueClusterQueue != "" ||
			presentKubernetesRef(e.Pod) || e.PodOwnerJobUID != "" ||
			e.PodScheduled || e.PodNodeName != "" {
			return reject()
		}
		return KueueUnproven, nil
	}
	if !validKubernetesRef(e.KueueWorkload) ||
		e.KueueWorkload.Namespace != e.Job.Namespace ||
		e.KueueOwnerJobUID != e.Job.UID {
		return reject()
	}
	if e.KueueQuotaReserved {
		if e.KueueClusterQueue == "" || e.KueueClusterQueue != strings.TrimSpace(e.KueueClusterQueue) ||
			strings.ContainsRune(e.KueueClusterQueue, 0) {
			return reject()
		}
	} else if e.KueueClusterQueue != "" || e.KueueAdmitted {
		return reject()
	}
	if !e.KueueAdmitted {
		if presentKubernetesRef(e.Pod) || e.PodOwnerJobUID != "" ||
			e.PodScheduled || e.PodNodeName != "" {
			return reject()
		}
		return KueueUnproven, nil
	}

	if !presentKubernetesRef(e.Pod) {
		if e.PodOwnerJobUID != "" || e.PodScheduled || e.PodNodeName != "" {
			return reject()
		}
		return KueueAdmitted, nil
	}
	if !validKubernetesRef(e.Pod) || e.Pod.Namespace != e.Job.Namespace ||
		e.PodOwnerJobUID != e.Job.UID {
		return reject()
	}
	if !e.PodScheduled {
		if e.PodNodeName != "" {
			return reject()
		}
		return KueueAdmitted, nil
	}
	if e.PodNodeName == "" || e.PodNodeName != strings.TrimSpace(e.PodNodeName) {
		return reject()
	}
	return KueuePodPlaced, nil
}
