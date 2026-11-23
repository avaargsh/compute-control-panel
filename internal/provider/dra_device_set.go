package provider

import (
	"context"
	"encoding/json"
	"fmt"
	"reflect"
	"sort"
)

// DRADeviceSetQuery pins a generated per-Pod Claim with one exact direct
// request and the complete expected device set. The existing Pod query's
// device tuple must be a member; it is not a substitute for the full set.
type DRADeviceSetQuery struct {
	Pod     DRAPodClaimQuery
	Devices []DRAAllocatedDevice
}

type DRADeviceSetObservation struct {
	Pod, Claim        KubernetesResourceRef
	NodeName          string
	PodClaimAlias     string
	ConsumerContainer string
	Devices           []DRAAllocatedDevice
}

const maxDRADeviceSet = 128

type draDeviceAddress struct {
	driver, pool, device string
}

func deviceAddress(d DRAAllocatedDevice) draDeviceAddress {
	return draDeviceAddress{d.Driver, d.Pool, d.Device}
}

func expectedDevice(d DRAClaimQuery) DRAAllocatedDevice {
	return DRAAllocatedDevice{Request: d.Request, Driver: d.Driver, Pool: d.Pool, Device: d.Device, ShareID: d.ShareID}
}

// canonicalDRADeviceSet deliberately supports only non-shared devices of
// one direct request, not alternatives, fractional resources or admin access.
func canonicalDRADeviceSet(devices []DRAAllocatedDevice, request string) ([]DRAAllocatedDevice, bool) {
	if len(devices) == 0 || len(devices) > maxDRADeviceSet || !validKubeName(request) {
		return nil, false
	}
	seen := make(map[draDeviceAddress]bool, len(devices))
	out := append([]DRAAllocatedDevice(nil), devices...)
	for _, d := range out {
		a := deviceAddress(d)
		if d.Request != request || d.ShareID != "" || !validDRAIdentity(d.Driver) ||
			!validDRAIdentity(d.Pool) || !validDRAIdentity(d.Device) || seen[a] {
			return nil, false
		}
		seen[a] = true
	}
	sort.Slice(out, func(i, j int) bool {
		a, b := deviceAddress(out[i]), deviceAddress(out[j])
		if a.driver != b.driver {
			return a.driver < b.driver
		}
		if a.pool != b.pool {
			return a.pool < b.pool
		}
		return a.device < b.device
	})
	return out, true
}

func (q DRADeviceSetQuery) validate() ([]DRAAllocatedDevice, bool) {
	if q.Pod.validate() != nil || q.Pod.ClaimTemplateName == "" || q.Pod.Claim.ShareID != "" {
		return nil, false
	}
	set, ok := canonicalDRADeviceSet(q.Devices, q.Pod.Claim.Request)
	if !ok {
		return nil, false
	}
	for _, d := range set {
		if d == expectedDevice(q.Pod.Claim) {
			return set, true
		}
	}
	return nil, false
}

// draDeviceReadMeta deliberately avoids depending on the PD JobSet writer's
// metadata structs. The read-only DRA inventory module must compile without
// importing or promoting its mutating control-plane implementation.
type draDeviceReadMeta struct {
	kubeMeta
	DeletionTimestamp *string `json:"deletionTimestamp"`
}

type draDeviceSetClaim struct {
	APIVersion string            `json:"apiVersion"`
	Kind       string            `json:"kind"`
	Metadata   draDeviceReadMeta `json:"metadata"`
	Status     struct {
		ReservedFor []struct {
			APIGroup string `json:"apiGroup"`
			Resource string `json:"resource"`
			Name     string `json:"name"`
			UID      string `json:"uid"`
		} `json:"reservedFor"`
		Allocation *struct {
			Devices struct {
				Results []struct {
					DRAAllocatedDevice
					AdminAccess      bool                       `json:"adminAccess"`
					ConsumedCapacity map[string]json.RawMessage `json:"consumedCapacity"`
				} `json:"results"`
			} `json:"devices"`
		} `json:"allocation"`
	} `json:"status"`
}

func assessDRADeviceSetClaim(c draDeviceSetClaim, q DRAClaimQuery, want []DRAAllocatedDevice) (KubernetesResourceRef, bool) {
	ref := refFromKube(c.Metadata.kubeMeta)
	if c.APIVersion != "resource.k8s.io/v1" || c.Kind != "ResourceClaim" ||
		!validKubernetesRef(ref) || c.Metadata.DeletionTimestamp != nil ||
		ref.Namespace != q.Namespace || ref.Name != q.ClaimName || ref.UID != q.ExpectedClaimUID ||
		len(c.Status.ReservedFor) != 1 || c.Status.Allocation == nil ||
		len(c.Status.Allocation.Devices.Results) != len(want) {
		return KubernetesResourceRef{}, false
	}
	owner := c.Status.ReservedFor[0]
	if owner.APIGroup != "" || owner.Resource != "pods" || owner.Name != q.PodName || owner.UID != q.ExpectedPodUID {
		return KubernetesResourceRef{}, false
	}
	devices := make([]DRAAllocatedDevice, 0, len(want))
	for _, result := range c.Status.Allocation.Devices.Results {
		if result.AdminAccess || len(result.ConsumedCapacity) != 0 {
			return KubernetesResourceRef{}, false
		}
		devices = append(devices, result.DRAAllocatedDevice)
	}
	got, ok := canonicalDRADeviceSet(devices, q.Request)
	if !ok || !reflect.DeepEqual(got, want) {
		return KubernetesResourceRef{}, false
	}
	return ref, true
}

type draDeviceSetPod struct {
	draPodObject
	Metadata draDeviceReadMeta `json:"metadata"`
}

func (p draDeviceSetPod) consumer(q DRAPodClaimQuery) (string, bool) {
	if p.Metadata.DeletionTimestamp != nil {
		return "", false
	}
	controllers := 0
	for _, o := range p.Metadata.OwnerReferences {
		if o.Controller {
			controllers++
			if o.APIVersion != "batch/v1" || o.Kind != "Job" || o.UID != q.ExpectedJobUID {
				return "", false
			}
		}
	}
	if controllers != 1 {
		return "", false
	}
	p.draPodObject.Metadata = p.Metadata.kubeMeta
	return exactDRAPodConsumer(p.draPodObject, q)
}

// ObserveDRAPodDeviceSet reads the Pod, two exact Claim snapshots and the Pod
// again. The order-independent device set, every identity and opaque RV must
// match. It is scheduler-side evidence only: no physical GPU/prepare/release
// or readiness/finance authority is implied.
func (r *KubernetesObservationReader) ObserveDRAPodDeviceSet(ctx context.Context, q DRADeviceSetQuery) (DRADeviceSetObservation, error) {
	reject := func() (DRADeviceSetObservation, error) { return DRADeviceSetObservation{}, ErrInvalidDRAObservation }
	want, ok := q.validate()
	if !ok || r == nil {
		return reject()
	}
	c := q.Pod.Claim
	podPath := fmt.Sprintf("/api/v1/namespaces/%s/pods/%s", c.Namespace, c.PodName)
	claimPath := fmt.Sprintf("/apis/resource.k8s.io/v1/namespaces/%s/resourceclaims/%s", c.Namespace, c.ClaimName)
	var first, second draDeviceSetPod
	if r.read(ctx, podPath, &first) != nil {
		return reject()
	}
	consumer, ok := first.consumer(q.Pod)
	if !ok {
		return reject()
	}
	var prior KubernetesResourceRef
	for i := 0; i < 2; i++ {
		var snapshot draDeviceSetClaim
		if r.read(ctx, claimPath, &snapshot) != nil {
			return reject()
		}
		ref, ok := assessDRADeviceSetClaim(snapshot, c, want)
		if !ok || (i != 0 && ref != prior) {
			return reject()
		}
		prior = ref
	}
	if r.read(ctx, podPath, &second) != nil || !reflect.DeepEqual(first, second) {
		return reject()
	}
	if again, ok := second.consumer(q.Pod); !ok || again != consumer {
		return reject()
	}
	return DRADeviceSetObservation{Pod: refFromKube(first.Metadata.kubeMeta), Claim: prior,
		NodeName: first.Spec.NodeName, PodClaimAlias: q.Pod.PodClaimAlias, ConsumerContainer: consumer, Devices: want}, nil
}
