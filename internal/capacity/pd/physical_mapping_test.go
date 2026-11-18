package pd

import (
	"strings"
	"testing"
)

func TestBindScheduledDevicesAuthenticatesExactPhysicalUIDAndResourceSlice(t *testing.T) {
	now, demand, _, _, _, _ := fixture(t)
	e, policy, inventoryKey, fabricKey := topologyFixture(t, now, demand.Scope)
	e.Inventory.Devices[0].State = "RESERVED"
	e.Inventory.Devices[1].State = "RESERVED"
	signTopology(t, &e, inventoryKey, fabricKey)
	one := e.Inventory.Devices[0]
	two := e.Inventory.Devices[1]
	observed := []ScheduledDevice{
		{Driver: two.Driver, Pool: two.Pool, Device: two.Device, ClaimUID: "claim-uid", PodUID: "pod-uid", NodeUID: two.NodeUID, SliceUID: two.SliceUID, SliceResourceVersion: two.SliceResourceVersion},
		{Driver: one.Driver, Pool: one.Pool, Device: one.Device, ClaimUID: "claim-uid", PodUID: "pod-uid", NodeUID: one.NodeUID, SliceUID: one.SliceUID, SliceResourceVersion: one.SliceResourceVersion},
	}
	mapped, err := BindScheduledDevices(now, e, policy, observed)
	if err != nil || len(mapped.Bindings) != 2 || mapped.Bindings[0].PhysicalUID != "gpu-1" || mapped.Bindings[1].PhysicalUID != "gpu-2" ||
		mapped.Bindings[0].IslandID != "a" || mapped.EvidenceSHA256 == "" || mapped.PhysicalReady {
		t.Fatalf("mapped=%+v err=%v", mapped, err)
	}
	again, err := BindScheduledDevices(now, e, policy, []ScheduledDevice{observed[1], observed[0]})
	if err != nil || mapped.EvidenceSHA256 != again.EvidenceSHA256 {
		t.Fatalf("input order changed identity: %+v %v", again, err)
	}
}

func TestBindScheduledDevicesRejectsCrossSourceDrift(t *testing.T) {
	now, demand, _, _, _, _ := fixture(t)
	e, policy, inventoryKey, fabricKey := topologyFixture(t, now, demand.Scope)
	e.Inventory.Devices[0].State = "ACTIVE"
	e.Inventory.Devices[1].State = "RESERVED"
	signTopology(t, &e, inventoryKey, fabricKey)
	devices := e.Inventory.Devices
	one, two := devices[0], devices[1]
	makeDevice := func(g PhysicalGPU, claim, pod string) ScheduledDevice {
		return ScheduledDevice{
			Driver: g.Driver, Pool: g.Pool, Device: g.Device,
			ClaimUID: claim, PodUID: pod, NodeUID: g.NodeUID,
			SliceUID: g.SliceUID, SliceResourceVersion: g.SliceResourceVersion,
		}
	}
	valid := []ScheduledDevice{makeDevice(one, "claim-1", "pod-1"), makeDevice(two, "claim-2", "pod-2")}
	for name, change := range map[string]func(*TopologyEvidence, *[]ScheduledDevice){
		"wrong node":               func(_ *TopologyEvidence, d *[]ScheduledDevice) { (*d)[0].NodeUID = "successor-node" },
		"wrong ResourceSlice UID":  func(_ *TopologyEvidence, d *[]ScheduledDevice) { (*d)[0].SliceUID = "other-slice" },
		"ResourceSlice RV changed": func(_ *TopologyEvidence, d *[]ScheduledDevice) { (*d)[0].SliceResourceVersion = "124" },
		"unlisted GPU":             func(_ *TopologyEvidence, d *[]ScheduledDevice) { (*d)[0].Device = "unregistered-gpu" },
		"duplicate GPU":            func(_ *TopologyEvidence, d *[]ScheduledDevice) { (*d)[1] = (*d)[0] },
		"cross Pod Claim reuse":    func(_ *TopologyEvidence, d *[]ScheduledDevice) { (*d)[1].ClaimUID = (*d)[0].ClaimUID },
		"missing Pod UID":          func(_ *TopologyEvidence, d *[]ScheduledDevice) { (*d)[1].PodUID = "" },
		"GPU still FREE":           func(e *TopologyEvidence, _ *[]ScheduledDevice) { e.Inventory.Devices[0].State = "FREE" },
		"GPU unhealthy":            func(e *TopologyEvidence, _ *[]ScheduledDevice) { e.Inventory.Devices[0].Healthy = false },
		"GPU draining":             func(e *TopologyEvidence, _ *[]ScheduledDevice) { e.Inventory.Devices[0].State = "DRAINING" },
		"GPU unknown":              func(e *TopologyEvidence, _ *[]ScheduledDevice) { e.Inventory.Devices[0].State = "UNKNOWN" },
		"stale inventory":          func(e *TopologyEvidence, _ *[]ScheduledDevice) { e.Inventory.Source.ObservedAtMillis -= 61000 },
		"signature replay": func(e *TopologyEvidence, _ *[]ScheduledDevice) {
			e.Inventory.Devices[0].PhysicalUID = strings.Repeat("z", 20)
		},
	} {
		t.Run(name, func(t *testing.T) {
			in := e
			in.Inventory.Devices = append([]PhysicalGPU(nil), e.Inventory.Devices...)
			raw := append([]ScheduledDevice(nil), valid...)
			change(&in, &raw)
			if name != "signature replay" {
				signTopology(t, &in, inventoryKey, fabricKey)
			}
			got, err := BindScheduledDevices(now, in, policy, raw)
			if err == nil || len(got.Bindings) != 0 || got.EvidenceSHA256 != "" {
				t.Fatalf("invalid binding accepted: %+v %v", got, err)
			}
		})
	}
}
