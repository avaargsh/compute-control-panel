package provider

import (
	"context"
	"encoding/json"
	"reflect"
	"sort"
)

// DRAPoolInventoryQuery comes from a separately authorized, UID-pinned
// Job/Pod/Claim request. It is not caller-supplied device admission authority.
type DRAPoolInventoryQuery struct {
	Pod DRAPodClaimQuery
}

// DRAPoolInventoryObservation is scheduler/driver-published ResourceSlice
// inventory structural evidence only. No field attests kubelet preparation,
// physical GPU health, device use, device release or Allocation READY/RELEASED.
type DRAPoolInventoryObservation struct {
	PodClaim               DRAPodClaimObservation
	Slice                  KubernetesResourceRef
	PoolGeneration         int64
	PoolResourceSliceCount int
}

type draResourceSlice struct {
	APIVersion string   `json:"apiVersion"`
	Kind       string   `json:"kind"`
	Metadata   kubeMeta `json:"metadata"`
	Spec       struct {
		Driver                 string          `json:"driver"`
		NodeName               string          `json:"nodeName"`
		NodeSelector           json.RawMessage `json:"nodeSelector"`
		AllNodes               bool            `json:"allNodes"`
		PerDeviceNodeSelection bool            `json:"perDeviceNodeSelection"`
		Pool                   struct {
			Name               string `json:"name"`
			Generation         int64  `json:"generation"`
			ResourceSliceCount int    `json:"resourceSliceCount"`
		} `json:"pool"`
		Devices []struct {
			Name string `json:"name"`
		} `json:"devices"`
	} `json:"spec"`
}

type draResourceSliceList struct {
	APIVersion string `json:"apiVersion"`
	Kind       string `json:"kind"`
	Metadata   struct {
		ResourceVersion string `json:"resourceVersion"`
		Continue        string `json:"continue"`
	} `json:"metadata"`
	Items []draResourceSlice `json:"items"`
}

// currentDRAPool rejects incomplete generations, duplicated inventory identities,
// and unsupported placement modes. The highest generation (not the first matching
// slice) is authoritative. No device quantity or GPU readiness is derived.
func currentDRAPool(
	list draResourceSliceList, query DRAPoolInventoryQuery,
) ([]draResourceSlice, KubernetesResourceRef, int64, int, error) {
	reject := func() ([]draResourceSlice, KubernetesResourceRef, int64, int, error) {
		return nil, KubernetesResourceRef{}, 0, 0, ErrInvalidDRAObservation
	}
	if list.APIVersion != "resource.k8s.io/v1" || list.Kind != "ResourceSliceList" ||
		list.Metadata.ResourceVersion == "" || list.Metadata.Continue != "" ||
		len(list.Items) == 0 {
		return reject()
	}
	driver := query.Pod.Claim.Driver
	poolName := query.Pod.Claim.Pool
	deviceName := query.Pod.Claim.Device
	var newest int64 = -1
	for _, item := range list.Items {
		if item.Spec.Driver == driver && item.Spec.Pool.Name == poolName &&
			item.Spec.Pool.Generation > newest {
			newest = item.Spec.Pool.Generation
		}
	}
	if newest < 0 {
		return reject()
	}
	var pool []draResourceSlice
	expectedCount := -1
	deviceNames := make(map[string]bool)
	sliceNames := make(map[string]bool)
	sliceUIDs := make(map[string]bool)
	var selected KubernetesResourceRef
	found := 0
	for _, item := range list.Items {
		if item.Spec.Driver != driver || item.Spec.Pool.Name != poolName ||
			item.Spec.Pool.Generation != newest {
			continue
		}
		ref := refFromKube(item.Metadata)
		if item.APIVersion != "resource.k8s.io/v1" || item.Kind != "ResourceSlice" ||
			item.Metadata.Namespace != "" || !validKubeName(ref.Name) ||
			!validDRAIdentity(ref.UID) || !validDRAIdentity(ref.ResourceVersion) ||
			sliceNames[ref.Name] || sliceUIDs[ref.UID] ||
			item.Spec.Pool.ResourceSliceCount <= 0 ||
			(expectedCount >= 0 && expectedCount != item.Spec.Pool.ResourceSliceCount) {
			return reject()
		}
		expectedCount = item.Spec.Pool.ResourceSliceCount
		sliceNames[ref.Name] = true
		sliceUIDs[ref.UID] = true
		for _, device := range item.Spec.Devices {
			if !validKubeName(device.Name) || deviceNames[device.Name] {
				return reject()
			}
			deviceNames[device.Name] = true
			if device.Name != deviceName {
				continue
			}
			// First slice deliberately supports explicit nodeName only. A
			// device exposed through allNodes, nodeSelector or per-device
			// selection needs a different independent topology proof.
			if item.Spec.NodeName != query.Pod.ExpectedNodeName ||
				item.Spec.AllNodes || item.Spec.PerDeviceNodeSelection ||
				len(item.Spec.NodeSelector) != 0 {
				return reject()
			}
			selected = ref
			found++
		}
		pool = append(pool, item)
	}
	if found != 1 || expectedCount != len(pool) {
		return reject()
	}
	sort.Slice(pool, func(i, j int) bool { return pool[i].Metadata.Name < pool[j].Metadata.Name })
	return pool, selected, newest, expectedCount, nil
}

// ObserveDRAPoolInventory verifies a live Pod-to-Claim join, two bounded
// ResourceSlice lists with identical latest-generation identities, then the
// live Pod-to-Claim join again. No Kubernetes write is performed.
//
// These sequential reads are NOT a cluster snapshot transaction. Even a
// positive result cannot attest kubelet or GPU driver preparation/release.
func (r *KubernetesObservationReader) ObserveDRAPoolInventory(
	ctx context.Context, query DRAPoolInventoryQuery,
) (DRAPoolInventoryObservation, error) {
	reject := func() (DRAPoolInventoryObservation, error) {
		return DRAPoolInventoryObservation{}, ErrInvalidDRAObservation
	}
	if r == nil || query.Pod.validate() != nil {
		return reject()
	}
	podClaim, err := r.ObserveDRAPodClaim(ctx, query.Pod)
	if err != nil {
		return reject()
	}
	var first, second draResourceSliceList
	const endpoint = "/apis/resource.k8s.io/v1/resourceslices"
	if r.read(ctx, endpoint, &first) != nil {
		return reject()
	}
	current, slice, generation, count, err := currentDRAPool(first, query)
	if err != nil || r.read(ctx, endpoint, &second) != nil {
		return reject()
	}
	again, secondSlice, secondGeneration, secondCount, err := currentDRAPool(second, query)
	if err != nil || slice != secondSlice || generation != secondGeneration ||
		count != secondCount || !reflect.DeepEqual(current, again) {
		return reject()
	}
	finalPodClaim, err := r.ObserveDRAPodClaim(ctx, query.Pod)
	if err != nil || finalPodClaim != podClaim {
		return reject()
	}
	return DRAPoolInventoryObservation{
		PodClaim:               podClaim,
		Slice:                  slice,
		PoolGeneration:         generation,
		PoolResourceSliceCount: count,
	}, nil
}
