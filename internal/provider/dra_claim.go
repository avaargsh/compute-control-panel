package provider

import (
	"context"
	"errors"
	"fmt"
	"strings"
)

// ErrInvalidDRAObservation is returned when a DRA ResourceClaim is absent,
// ambiguous or disagrees with a previously authenticated Pod/device identity.
// It is not evidence of device release.
var ErrInvalidDRAObservation = errors.New("unproven DRA ResourceClaim identity")

// DRAClaimQuery is the expected, immutable identity for one claim, one Pod
// reservation and one allocated device in resource.k8s.io/v1. The caller must
// independently obtain and authenticate the Pod and expected device identities.
// This does not authorize any Kubernetes write or Allocation transition.
type DRAClaimQuery struct {
	Namespace        string
	ClaimName        string
	ExpectedClaimUID string
	PodName          string
	ExpectedPodUID   string
	Request          string
	Driver           string
	Pool             string
	Device           string
	ShareID          string
}

// DRAAllocatedDevice identifies a scheduler-side allocation result. The
// driver/pool/device/share tuple is NOT a physical-device health, kubelet
// preparation, usage, release, or GPU READY attestation.
type DRAAllocatedDevice struct {
	Request string `json:"request"`
	Driver  string `json:"driver"`
	Pool    string `json:"pool"`
	Device  string `json:"device"`
	ShareID string `json:"shareID"`
}

// DRAClaimObservation is a bounded read-only Claim/Pod/device identity tuple.
// It deliberately has no READY, RELEASED, device-attested or cost-finality flag.
type DRAClaimObservation struct {
	Claim  KubernetesResourceRef
	PodUID string
	Device DRAAllocatedDevice
}

func validDRAIdentity(value string) bool {
	if len(value) == 0 || len(value) > 253 || value != strings.TrimSpace(value) {
		return false
	}
	return strings.IndexFunc(value, func(char rune) bool {
		return char <= 32 || char == 92
	}) == -1
}

func (q DRAClaimQuery) validate() error {
	if !validKubeName(q.Namespace) || !validKubeName(q.ClaimName) ||
		!validKubeName(q.PodName) || !validKubeName(q.Request) {
		return ErrInvalidDRAObservation
	}
	for _, v := range []string{
		q.ExpectedClaimUID, q.ExpectedPodUID,
		q.Driver, q.Pool, q.Device,
	} {
		if !validDRAIdentity(v) {
			return ErrInvalidDRAObservation
		}
	}
	if (q.ShareID != "" && !validDRAIdentity(q.ShareID)) ||
		strings.ContainsAny(q.ExpectedClaimUID, "/") ||
		strings.ContainsAny(q.ExpectedPodUID, "/") {
		return ErrInvalidDRAObservation
	}
	return nil
}

type draClaimV1 struct {
	APIVersion string   `json:"apiVersion"`
	Kind       string   `json:"kind"`
	Metadata   kubeMeta `json:"metadata"`
	Status     struct {
		ReservedFor []struct {
			APIGroup string `json:"apiGroup"`
			Resource string `json:"resource"`
			Name     string `json:"name"`
			UID      string `json:"uid"`
		} `json:"reservedFor"`
		Allocation *struct {
			Devices struct {
				Results []DRAAllocatedDevice `json:"results"`
			} `json:"devices"`
		} `json:"allocation"`
	} `json:"status"`
}

// ObserveDRAClaim only uses the existing bounded TLS Kubernetes GET reader.
// One exact v1 ResourceClaim UID + one Pod reservation + one device result is
// necessary for this *single device* P0 slice. Unobserved, shared, missing,
// forbidden, changed or ambiguous claims fail closed and never prove release.
//
// A positive response shows only a scheduler-side Claim status snapshot.
// Driver preparation, kubelet PodResources, physical GPU identity and release
// still require independent source-specific verification.
func (r *KubernetesObservationReader) ObserveDRAClaim(
	ctx context.Context, q DRAClaimQuery,
) (DRAClaimObservation, error) {
	if err := q.validate(); err != nil {
		return DRAClaimObservation{}, err
	}
	if r == nil {
		return DRAClaimObservation{}, ErrInvalidDRAObservation
	}
	var claim draClaimV1
	path := fmt.Sprintf("/apis/resource.k8s.io/v1/namespaces/%s/resourceclaims/%s", q.Namespace, q.ClaimName)
	if err := r.read(ctx, path, &claim); err != nil {
		return DRAClaimObservation{}, fmt.Errorf("%w: %v", ErrInvalidDRAObservation, err)
	}
	if claim.APIVersion != "resource.k8s.io/v1" || claim.Kind != "ResourceClaim" ||
		!validKubernetesRef(refFromKube(claim.Metadata)) ||
		claim.Metadata.Namespace != q.Namespace ||
		claim.Metadata.Name != q.ClaimName ||
		claim.Metadata.UID != q.ExpectedClaimUID ||
		len(claim.Status.ReservedFor) != 1 ||
		claim.Status.Allocation == nil ||
		len(claim.Status.Allocation.Devices.Results) != 1 {
		return DRAClaimObservation{}, ErrInvalidDRAObservation
	}
	owner := claim.Status.ReservedFor[0]
	if owner.APIGroup != "" || owner.Resource != "pods" ||
		owner.Name != q.PodName || owner.UID != q.ExpectedPodUID {
		return DRAClaimObservation{}, ErrInvalidDRAObservation
	}
	device := claim.Status.Allocation.Devices.Results[0]
	if device.Request != q.Request || device.Driver != q.Driver ||
		device.Pool != q.Pool || device.Device != q.Device ||
		device.ShareID != q.ShareID {
		return DRAClaimObservation{}, ErrInvalidDRAObservation
	}
	return DRAClaimObservation{
		Claim:  refFromKube(claim.Metadata),
		PodUID: owner.UID,
		Device: device,
	}, nil
}
