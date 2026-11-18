package provider

import (
	"context"
	"reflect"
)

// DRAMultiSliceQuery must come from a pinned Provider/Pod/Claim identity.
// Node UID is independently pinned from an authorized source, never inferred
// from a Node name. It supports only a single driver and pool per Claim.
type DRAMultiSliceQuery struct {
	Devices         DRADeviceSetQuery
	ExpectedNodeUID string
}

// DRAMultiSliceDevice records the exact scheduler-published ResourceSlice
// source for one DRA device. These are not physical UUIDs or driver receipts.
type DRAMultiSliceDevice struct {
	DRAAllocatedDevice
	NodeUID              string
	SliceUID             string
	SliceResourceVersion string
}

// DRAMultiSliceObservation is a Kubernetes scheduler-side identity join.
// PhysicalPrepared is always false: driver/kubelet Prepare and device health
// require independent evidence.
type DRAMultiSliceObservation struct {
	PodClaim               DRADeviceSetObservation
	NodeUID                string
	NodeResourceVersion    string
	PoolGeneration         int64
	PoolResourceSliceCount int
	Devices                []DRAMultiSliceDevice
	PhysicalPrepared       bool
}

type draNodeRead struct {
	APIVersion string       `json:"apiVersion"`
	Kind       string       `json:"kind"`
	Metadata   pdNativeMeta `json:"metadata"`
}

func (q DRAMultiSliceQuery) validate() ([]DRAAllocatedDevice, bool) {
	devices, ok := q.Devices.validate()
	if !ok || !validDRAIdentity(q.ExpectedNodeUID) ||
		!validResourceSliceName(q.Devices.Pod.ExpectedNodeName) {
		return nil, false
	}
	primary := devices[0]
	for _, device := range devices {
		if device.Driver != primary.Driver || device.Pool != primary.Pool {
			return nil, false
		}
	}
	return devices, true
}

func (r *KubernetesObservationReader) readPinnedDRANode(ctx context.Context, q DRAMultiSliceQuery) (string, error) {
	var node draNodeRead
	name := q.Devices.Pod.ExpectedNodeName
	if err := r.read(ctx, "/api/v1/nodes/"+name, &node); err != nil {
		return "", err
	}
	m := node.Metadata
	if node.APIVersion != "v1" || node.Kind != "Node" ||
		m.Name != name || m.Namespace != "" || m.UID != q.ExpectedNodeUID ||
		!validDRAIdentity(m.ResourceVersion) || m.DeletionTimestamp != nil {
		return "", ErrInvalidDRAObservation
	}
	return m.ResourceVersion, nil
}

func (r *KubernetesObservationReader) observeMultiSlicePool(
	list draResourceSliceList, q DRAMultiSliceQuery, devices []DRAAllocatedDevice,
) ([]DRAMultiSliceDevice, int64, int, error) {
	var result []DRAMultiSliceDevice
	var generation int64
	var count int
	for idx, d := range devices {
		single := q.Devices.Pod
		single.Claim.Device = d.Device
		_, slice, gen, total, err := currentDRAPool(list, DRAPoolInventoryQuery{Pod: single})
		if err != nil || !validDRAIdentity(slice.UID) || !validDRAIdentity(slice.ResourceVersion) {
			return nil, 0, 0, ErrInvalidDRAObservation
		}
		if idx != 0 && (gen != generation || total != count) {
			return nil, 0, 0, ErrInvalidDRAObservation
		}
		generation, count = gen, total
		result = append(result, DRAMultiSliceDevice{
			DRAAllocatedDevice:   d,
			NodeUID:              q.ExpectedNodeUID,
			SliceUID:             slice.UID,
			SliceResourceVersion: slice.ResourceVersion,
		})
	}
	return result, generation, count, nil
}

// ObserveDRAMultiSlice collects exact Pod/Claim identities, the live Node UID
// and complete current ResourceSlice pool generation, each twice. It rejects
// absent/mismatched devices, Node successor, source RV or generation drift.
// Sequential Kubernetes GETs are not transactional and cannot prove physical
// GPU use, device preparation, release or accounting finality.
func (r *KubernetesObservationReader) ObserveDRAMultiSlice(ctx context.Context, q DRAMultiSliceQuery) (DRAMultiSliceObservation, error) {
	reject := func() (DRAMultiSliceObservation, error) {
		return DRAMultiSliceObservation{}, ErrInvalidDRAObservation
	}
	devices, ok := q.validate()
	if !ok || r == nil {
		return reject()
	}
	pod, err := r.ObserveDRAPodDeviceSet(ctx, q.Devices)
	if err != nil {
		return reject()
	}
	nodeVersion, err := r.readPinnedDRANode(ctx, q)
	if err != nil {
		return reject()
	}
	const path = "/apis/resource.k8s.io/v1/resourceslices"
	var first, second draResourceSliceList
	if err = r.read(ctx, path, &first); err != nil {
		return reject()
	}
	mapped, gen, count, err := r.observeMultiSlicePool(first, q, devices)
	if err != nil {
		return reject()
	}
	if err = r.read(ctx, path, &second); err != nil ||
		!reflect.DeepEqual(first, second) {
		return reject()
	}
	again, secondGen, secondCount, err := r.observeMultiSlicePool(second, q, devices)
	if err != nil || gen != secondGen || count != secondCount ||
		!reflect.DeepEqual(mapped, again) {
		return reject()
	}
	latestNodeVersion, err := r.readPinnedDRANode(ctx, q)
	if err != nil || latestNodeVersion != nodeVersion {
		return reject()
	}
	currentPod, err := r.ObserveDRAPodDeviceSet(ctx, q.Devices)
	if err != nil || !reflect.DeepEqual(pod, currentPod) {
		return reject()
	}
	return DRAMultiSliceObservation{
		PodClaim: pod, NodeUID: q.ExpectedNodeUID,
		NodeResourceVersion: nodeVersion,
		PoolGeneration:      gen, PoolResourceSliceCount: count, Devices: mapped,
	}, nil
}
