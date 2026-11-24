package provider

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"reflect"
	"sort"
	"strings"
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
	Metadata   draDeviceReadMeta `json:"metadata"`
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
		Devices []draResourceSliceDevice `json:"devices"`
	} `json:"spec"`
}

// Keep each complete device object for the second-read comparison. Inspecting
// only name would discard capacity, attributes and future API fields before
// comparing snapshots. These fields remain opaque inventory, not readiness.
type draResourceSliceDevice struct {
	Name     string `json:"name"`
	contents map[string]any
}

func (d *draResourceSliceDevice) UnmarshalJSON(data []byte) error {
	var contents map[string]any
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.UseNumber()
	if err := decoder.Decode(&contents); err != nil {
		return err
	}
	name, ok := contents["name"].(string)
	if !ok {
		return ErrInvalidDRAObservation
	}
	d.Name = name
	d.contents = contents
	return nil
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

// ResourceSlice names are Kubernetes DNS subdomains (up to 253 bytes), not
// one 63-byte DNS label. The example driver publishes names containing its
// dotted domain, such as node-gpu.example.com-suffix. Keep each label strict.
func validResourceSliceName(name string) bool {
	if len(name) == 0 || len(name) > 253 {
		return false
	}
	for _, label := range strings.Split(name, ".") {
		if !validKubeName(label) {
			return false
		}
	}
	return true
}

// currentDRAPool rejects incomplete generations, duplicated inventory identities,
// and unsupported placement modes. The highest generation (not the first matching
// slice) is authoritative. No device quantity or GPU readiness is derived.
func currentDRAPool(
	list draResourceSliceList, query DRAPoolInventoryQuery,
) ([]draResourceSlice, KubernetesResourceRef, int64, int, error) {
	reject := func(reason string) ([]draResourceSlice, KubernetesResourceRef, int64, int, error) {
		return nil, KubernetesResourceRef{}, 0, 0, fmt.Errorf("%w: ResourceSlice pool %s", ErrInvalidDRAObservation, reason)
	}
	if list.APIVersion != "resource.k8s.io/v1" || list.Kind != "ResourceSliceList" ||
		list.Metadata.ResourceVersion == "" || list.Metadata.Continue != "" ||
		len(list.Items) == 0 {
		return reject("invalid list header or pagination")
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
		return reject("driver/pool not found")
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
		// A terminating selected or sibling slice is not authoritative current
		// inventory, even if its generation/count and both reads agree.
		if item.Metadata.DeletionTimestamp != nil {
			return reject("terminating ResourceSlice in latest pool generation")
		}
		ref := refFromKube(item.Metadata.kubeMeta)
		// The typed ResourceSliceList returned by this fixed API endpoint
		// determines each item's resource kind. Kubernetes list items may omit
		// optional TypeMeta; explicit contradictory GVK remains invalid.
		if (item.APIVersion != "" && item.APIVersion != "resource.k8s.io/v1") ||
			(item.Kind != "" && item.Kind != "ResourceSlice") {
			return reject("unexpected ResourceSlice item API version/kind")
		}
		if item.Metadata.Namespace != "" {
			return reject("unexpected namespace on cluster-scoped ResourceSlice")
		}
		if !validResourceSliceName(ref.Name) {
			return reject("invalid ResourceSlice DNS subdomain name")
		}
		if !validDRAIdentity(ref.UID) || !validDRAIdentity(ref.ResourceVersion) {
			return reject("invalid ResourceSlice UID or resourceVersion")
		}
		if sliceNames[ref.Name] || sliceUIDs[ref.UID] {
			return reject("duplicate ResourceSlice name or UID")
		}
		if item.Spec.Pool.ResourceSliceCount <= 0 ||
			(expectedCount >= 0 && expectedCount != item.Spec.Pool.ResourceSliceCount) {
			return reject("ResourceSlice count mismatch")
		}
		expectedCount = item.Spec.Pool.ResourceSliceCount
		sliceNames[ref.Name] = true
		sliceUIDs[ref.UID] = true
		for _, device := range item.Spec.Devices {
			if !validKubeName(device.Name) || deviceNames[device.Name] {
				return reject("duplicate or invalid device name")
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
				return reject("device node topology unsupported or mismatched")
			}
			selected = ref
			found++
		}
		pool = append(pool, item)
	}
	if found != 1 || expectedCount != len(pool) {
		return reject("allocated device absent/ambiguous or pool generation incomplete")
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
		return DRAPoolInventoryObservation{}, fmt.Errorf("%w: initial Pod/Claim association: %v", ErrInvalidDRAObservation, err)
	}
	var first, second draResourceSliceList
	const endpoint = "/apis/resource.k8s.io/v1/resourceslices"
	if err := r.read(ctx, endpoint, &first); err != nil {
		return DRAPoolInventoryObservation{}, fmt.Errorf("%w: first ResourceSlice list read: %v", ErrInvalidDRAObservation, err)
	}
	current, slice, generation, count, err := currentDRAPool(first, query)
	if err != nil {
		return DRAPoolInventoryObservation{}, fmt.Errorf("%w: first pool: %v", ErrInvalidDRAObservation, err)
	}
	if err := r.read(ctx, endpoint, &second); err != nil {
		return DRAPoolInventoryObservation{}, fmt.Errorf("%w: second ResourceSlice list read: %v", ErrInvalidDRAObservation, err)
	}
	again, secondSlice, secondGeneration, secondCount, err := currentDRAPool(second, query)
	if err != nil {
		return DRAPoolInventoryObservation{}, fmt.Errorf("%w: second pool: %v", ErrInvalidDRAObservation, err)
	}
	if slice != secondSlice || generation != secondGeneration ||
		count != secondCount || !reflect.DeepEqual(current, again) {
		return DRAPoolInventoryObservation{}, fmt.Errorf("%w: ResourceSlice list changed between reads", ErrInvalidDRAObservation)
	}
	finalPodClaim, err := r.ObserveDRAPodClaim(ctx, query.Pod)
	if err != nil || finalPodClaim != podClaim {
		return DRAPoolInventoryObservation{}, fmt.Errorf("%w: Pod/Claim changed after inventory read: %v", ErrInvalidDRAObservation, err)
	}
	return DRAPoolInventoryObservation{
		PodClaim:               podClaim,
		Slice:                  slice,
		PoolGeneration:         generation,
		PoolResourceSliceCount: count,
	}, nil
}
