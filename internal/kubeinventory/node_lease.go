package kubeinventory

import "time"

// nodeLease is a deliberately narrow read model of a kubelet's coordination.k8s.io
// Lease. It is not a HostIdentity or a signed/authoritative GPU health record.
type nodeLease struct {
	APIVersion string `json:"apiVersion"`
	Kind       string `json:"kind"`
	Metadata   struct {
		Namespace         string     `json:"namespace"`
		Name              string     `json:"name"`
		UID               string     `json:"uid"`
		ResourceVersion   string     `json:"resourceVersion"`
		DeletionTimestamp *time.Time `json:"deletionTimestamp"`
		OwnerReferences   []struct {
			APIVersion string `json:"apiVersion"`
			Kind       string `json:"kind"`
			Name       string `json:"name"`
			UID        string `json:"uid"`
		} `json:"ownerReferences"`
	} `json:"metadata"`
	Spec struct {
		HolderIdentity       string     `json:"holderIdentity"`
		RenewTime            *time.Time `json:"renewTime"`
		LeaseDurationSeconds *int32     `json:"leaseDurationSeconds"`
	} `json:"spec"`
}

type nodeLeaseState string

const (
	nodeLeaseFresh   nodeLeaseState = "FRESH"
	nodeLeaseExpired nodeLeaseState = "EXPIRED"
	nodeLeaseUnknown nodeLeaseState = "UNKNOWN"
)

type nodeLeaseAssessment struct {
	State      nodeLeaseState
	ReasonCode string
	ExpiresAt  time.Time
}

// assessNodeLease evaluates a single, already obtained snapshot. A FRESH result
// is evidence of a recent Kubernetes Lease only. It does not authenticate a
// physical host, prove GPU presence, or grant scheduling/admission authority.
// Caller must independently authenticate the source and recheck Node UID and
// current Lease before any protected resource operation.
func assessNodeLease(nodeName, nodeUID string, lease nodeLease, now time.Time, maxAge time.Duration) nodeLeaseAssessment {
	unknown := func(reason string) nodeLeaseAssessment {
		return nodeLeaseAssessment{State: nodeLeaseUnknown, ReasonCode: reason}
	}
	if now.IsZero() || maxAge <= 0 {
		return unknown("LEASE_POLICY_INVALID")
	}
	if nodeName == "" || nodeUID == "" || lease.APIVersion != "coordination.k8s.io/v1" ||
		lease.Kind != "Lease" || lease.Metadata.Namespace != "kube-node-lease" ||
		lease.Metadata.Name != nodeName || lease.Metadata.UID == "" ||
		lease.Metadata.ResourceVersion == "" || lease.Metadata.DeletionTimestamp != nil ||
		lease.Spec.HolderIdentity != nodeName {
		return unknown("LEASE_IDENTITY_INVALID")
	}
	if len(lease.Metadata.OwnerReferences) != 1 {
		return unknown("LEASE_OWNER_MISMATCH")
	}
	owner := lease.Metadata.OwnerReferences[0]
	if owner.APIVersion != "v1" || owner.Kind != "Node" ||
		owner.Name != nodeName || owner.UID != nodeUID {
		return unknown("LEASE_OWNER_MISMATCH")
	}
	if lease.Spec.LeaseDurationSeconds == nil || *lease.Spec.LeaseDurationSeconds <= 0 {
		return unknown("LEASE_DURATION_INVALID")
	}
	if lease.Spec.RenewTime == nil || lease.Spec.RenewTime.IsZero() ||
		lease.Spec.RenewTime.After(now) {
		return unknown("LEASE_RENEWAL_INVALID")
	}
	duration := time.Duration(*lease.Spec.LeaseDurationSeconds) * time.Second
	if duration > maxAge {
		duration = maxAge
	}
	expires := lease.Spec.RenewTime.Add(duration)
	if !now.Before(expires) {
		return nodeLeaseAssessment{State: nodeLeaseExpired, ReasonCode: "LEASE_EXPIRED", ExpiresAt: expires}
	}
	return nodeLeaseAssessment{State: nodeLeaseFresh, ExpiresAt: expires}
}
