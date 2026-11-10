package provider

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"strings"
)

// DRAClaimUnallocatedObservation records one exact ResourceClaim UID that
// has no scheduler allocation, Pod reservation or reported driver device
// status in two consecutive API reads. PreviousClaim links the same UID to
// an earlier allocation observation, but is not independently authenticated.
//
// This is NOT a physical GPU deallocation, kubelet/driver receipt, Provider
// absence proof, allocation RELEASED decision or final accounting evidence.
type DRAClaimUnallocatedObservation struct {
	PreviousClaim  KubernetesResourceRef
	CurrentClaim   KubernetesResourceRef
	PreviousPodUID string
	PreviousDevice DRAAllocatedDevice
}

// draUnallocatedClaimState retains only fields needed for the narrowed,
// read-only release-side check. A missing status object is not proof of
// transition; pending deletion is not proof of deallocation.
type draUnallocatedClaimState struct {
	APIVersion string `json:"apiVersion"`
	Kind       string `json:"kind"`
	Metadata   struct {
		Namespace         string  `json:"namespace"`
		Name              string  `json:"name"`
		UID               string  `json:"uid"`
		ResourceVersion   string  `json:"resourceVersion"`
		DeletionTimestamp *string `json:"deletionTimestamp"`
	} `json:"metadata"`
	Status *struct {
		Allocation  json.RawMessage   `json:"allocation"`
		ReservedFor []json.RawMessage `json:"reservedFor"`
		Devices     []json.RawMessage `json:"devices"`
	} `json:"status"`
}

func (c draUnallocatedClaimState) claimRef() KubernetesResourceRef {
	return KubernetesResourceRef{
		Namespace: c.Metadata.Namespace,
		Name: c.Metadata.Name,
		UID: c.Metadata.UID,
		ResourceVersion: c.Metadata.ResourceVersion,
	}
}

func validPriorDRAAllocation(prior DRAClaimObservation) bool {
	if !validKubeName(prior.Claim.Namespace) || !validKubeName(prior.Claim.Name) ||
		!validKubernetesRef(prior.Claim) ||
		!validDRAIdentity(prior.Claim.UID) ||
		strings.ContainsRune(prior.Claim.UID, '/') ||
		!validDRAIdentity(prior.PodUID) ||
		!validKubeName(prior.Device.Request) {
		return false
	}
	for _, value := range []string{prior.Device.Driver, prior.Device.Pool, prior.Device.Device} {
		if !validDRAIdentity(value) {
			return false
		}
	}
	return prior.Device.ShareID == "" || validDRAIdentity(prior.Device.ShareID)
}

func (c draUnallocatedClaimState) clearFor(prior DRAClaimObservation) bool {
	ref := c.claimRef()
	if c.APIVersion != "resource.k8s.io/v1" || c.Kind != "ResourceClaim" ||
		!validKubernetesRef(ref) ||
		ref.Namespace != prior.Claim.Namespace || ref.Name != prior.Claim.Name ||
		ref.UID != prior.Claim.UID ||
		ref.ResourceVersion == prior.Claim.ResourceVersion ||
		c.Metadata.DeletionTimestamp != nil || c.Status == nil ||
		len(c.Status.ReservedFor) != 0 || len(c.Status.Devices) != 0 {
		return false
	}
	// A JSON null or an omitted allocation means no scheduler allocation.
	// An empty object is not the same thing and must fail closed.
	allocation := bytes.TrimSpace(c.Status.Allocation)
	return len(allocation) == 0 || bytes.Equal(allocation, []byte("null"))
}

// ObserveDRAClaimUnallocated reads a still-existing, exact-UID Claim with an
// earlier allocated observation, then repeats the GET to fence replacement
// and concurrent status/resourceVersion drift. HTTP 404 is NOT accepted as
// device-release proof because the old UID cannot be inspected via name.
// The caller must separately authorize/attest the prior observation and the
// Kubernetes source, then independently verify driver/kubelet release.
//
// This method performs no writes, has no Allocation state transition and must
// not be used to close Provider liability or finalize GPU cost.
func (r *KubernetesObservationReader) ObserveDRAClaimUnallocated(
	ctx context.Context, prior DRAClaimObservation,
) (DRAClaimUnallocatedObservation, error) {
	reject := func() (DRAClaimUnallocatedObservation, error) {
		return DRAClaimUnallocatedObservation{}, ErrInvalidDRAObservation
	}
	if r == nil || !validPriorDRAAllocation(prior) {
		return reject()
	}
	path := fmt.Sprintf("/apis/resource.k8s.io/v1/namespaces/%s/resourceclaims/%s",
		prior.Claim.Namespace, prior.Claim.Name)
	var first draUnallocatedClaimState
	if err := r.read(ctx, path, &first); err != nil || !first.clearFor(prior) {
		return reject()
	}
	var second draUnallocatedClaimState
	if err := r.read(ctx, path, &second); err != nil || !second.clearFor(prior) ||
		second.claimRef() != first.claimRef() {
		return reject()
	}
	return DRAClaimUnallocatedObservation{
		PreviousClaim: prior.Claim,
		CurrentClaim: first.claimRef(),
		PreviousPodUID: prior.PodUID,
		PreviousDevice: prior.Device,
	}, nil
}
