package provider

import (
	"context"
	"fmt"
	"reflect"
	"strings"
)

// DRAClaimUIDAbsentObservation is a read-only Kubernetes API LIST observation
// that excludes a historically allocated ResourceClaim by exact UID and name.
// It does not prove same-UID unallocation, kubelet NodeUnprepare, device
// deallocation, Provider liability finality, Allocation RELEASED or cost FINAL.
type DRAClaimUIDAbsentObservation struct {
	PreviousClaim            KubernetesResourceRef
	ClaimListResourceVersion string
}

type draClaimListSnapshot struct {
	APIVersion string   `json:"apiVersion"`
	Kind       string   `json:"kind"`
	Metadata   kubeMeta `json:"metadata"`
	Items      []struct {
		Metadata kubeMeta `json:"metadata"`
	} `json:"items"`
}

func (s draClaimListSnapshot) excludesPriorClaim(prior KubernetesResourceRef) bool {
	if s.APIVersion != "resource.k8s.io/v1" || s.Kind != "ResourceClaimList" ||
		s.Items == nil || s.Metadata.Continue != "" ||
		s.Metadata.ResourceVersion == "" ||
		s.Metadata.ResourceVersion != strings.TrimSpace(s.Metadata.ResourceVersion) ||
		strings.ContainsRune(s.Metadata.ResourceVersion, 0) {
		return false
	}
	seenUID := make(map[string]bool, len(s.Items))
	seenName := make(map[string]bool, len(s.Items))
	for _, item := range s.Items {
		ref := refFromKube(item.Metadata)
		if !validKubernetesRef(ref) || ref.Namespace != prior.Namespace ||
			ref.UID == prior.UID || ref.Name == prior.Name ||
			seenUID[ref.UID] || seenName[ref.Name] {
			return false
		}
		seenUID[ref.UID] = true
		seenName[ref.Name] = true
	}
	return true
}

// ObserveDRAClaimUIDAbsent performs two complete, stable namespace ResourceClaim
// LIST reads and fences old UID/name reuse, incomplete pagination and RV drift.
// Unlike ObserveDRAClaimUnallocated, it explicitly permits the old Claim to be
// absent. A Claim LIST absence is NOT a NodeUnprepare receipt or evidence that
// the historically allocated device was deallocated. HTTP 404 for the Claim
// name alone is never sufficient. Source provenance is authenticated elsewhere.
func (r *KubernetesObservationReader) ObserveDRAClaimUIDAbsent(
	ctx context.Context, prior DRAClaimObservation,
) (DRAClaimUIDAbsentObservation, error) {
	reject := func() (DRAClaimUIDAbsentObservation, error) {
		return DRAClaimUIDAbsentObservation{}, ErrInvalidDRAObservation
	}
	if r == nil || !validPriorDRAAllocation(prior) {
		return reject()
	}
	path := fmt.Sprintf("/apis/resource.k8s.io/v1/namespaces/%s/resourceclaims",
		prior.Claim.Namespace)
	var first, second draClaimListSnapshot
	if r.read(ctx, path, &first) != nil || !first.excludesPriorClaim(prior.Claim) ||
		r.read(ctx, path, &second) != nil || !second.excludesPriorClaim(prior.Claim) ||
		first.Metadata.ResourceVersion != second.Metadata.ResourceVersion ||
		!reflect.DeepEqual(first.Items, second.Items) {
		return reject()
	}
	return DRAClaimUIDAbsentObservation{
		PreviousClaim:            prior.Claim,
		ClaimListResourceVersion: first.Metadata.ResourceVersion,
	}, nil
}
