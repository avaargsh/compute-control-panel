package pd

import (
	"crypto/ed25519"
	"strings"
	"testing"
	"time"
)

func topologyFixture(t *testing.T, now time.Time, scope Scope) (TopologyEvidence, TopologyPolicy, ed25519.PrivateKey, ed25519.PrivateKey) {
	t.Helper()
	iPub, iPriv, err := ed25519.GenerateKey(nil)
	if err != nil {
		t.Fatal(err)
	}
	fPub, fPriv, err := ed25519.GenerateKey(nil)
	if err != nil {
		t.Fatal(err)
	}
	base := SourceMeta{Scope: scope, ClusterUID: "cluster-uid", ProviderID: "provider", PoolRevision: 7, TopologyRevision: 9, ObservedAtMillis: now.Add(-4 * time.Second).UnixMilli(), KeyID: "inventory-key", AdapterID: "device-reader/v1"}
	proof := strings.Repeat("a", 64)
	gpu := func(uid, address, island, node, state string) PhysicalGPU {
		return PhysicalGPU{PhysicalUID: uid, Driver: "gpu.example.com", Pool: "dra-pool", Device: address, SliceUID: "slice-" + node, SliceResourceVersion: "123", NodeUID: node, IslandID: island, State: state, WholeDevice: true, Healthy: true, OccupancySHA256: proof, HealthSHA256: proof, ConnectivitySHA256: proof}
	}
	inv := InventoryClaim{Source: base, Complete: true, Devices: []PhysicalGPU{gpu("gpu-1", "device-1", "a", "node-a", "FREE"), gpu("gpu-2", "device-2", "a", "node-a", "FREE"), gpu("gpu-3", "device-3", "b", "node-b", "FREE"), gpu("gpu-4", "device-4", "b", "node-b", "RESERVED")}}
	fm := base
	fm.KeyID, fm.AdapterID, fm.ObservedAtMillis = "fabric-key", "fabric-probe/v1", now.UnixMilli()
	link := FabricClaim{Source: fm, BottleneckID: "shared-link", CoveredIslands: []string{"a", "b"}, Windows: []LinkWindow{{StartMS: now.Add(-2 * time.Second).UnixMilli(), EndMS: now.Add(-time.Second).UnixMilli(), MeasuredBytes: 1000, ReservedBytesPerSecond: 100, HeadroomBytesPerSecond: 100}, {StartMS: now.Add(-time.Second).UnixMilli(), EndMS: now.UnixMilli(), MeasuredBytes: 700, ReservedBytesPerSecond: 100, HeadroomBytesPerSecond: 100}}}
	policy := TopologyPolicy{Inventory: SourcePolicy{Scope: scope, ClusterUID: base.ClusterUID, ProviderID: base.ProviderID, KeyID: base.KeyID, AdapterID: base.AdapterID, PublicKey: iPub, ValidFrom: now.Add(-time.Hour), ValidUntil: now.Add(time.Hour), MaxAge: time.Minute}, Fabric: SourcePolicy{Scope: scope, ClusterUID: fm.ClusterUID, ProviderID: fm.ProviderID, KeyID: fm.KeyID, AdapterID: fm.AdapterID, PublicKey: fPub, ValidFrom: now.Add(-time.Hour), ValidUntil: now.Add(time.Hour), MaxAge: time.Minute}, MinimumWindows: 2, MinimumWindowMS: 1000, MinimumHeadroomBytesPerSecond: 100}
	e := TopologyEvidence{Inventory: inv, Fabric: link}
	signTopology(t, &e, iPriv, fPriv)
	return e, policy, iPriv, fPriv
}

func signTopology(t *testing.T, e *TopologyEvidence, inventoryKey, fabricKey ed25519.PrivateKey) {
	t.Helper()
	b, err := InventorySigningBytes(e.Inventory)
	if err != nil {
		t.Fatal(err)
	}
	e.InventorySignature = ed25519.Sign(inventoryKey, b)
	e.Fabric.InventorySHA256 = hashBytes(b)
	b, err = FabricSigningBytes(e.Fabric)
	if err != nil {
		t.Fatal(err)
	}
	e.FabricSignature = ed25519.Sign(fabricKey, b)
}

func TestVerifyTopologyDerivesUniqueFreeDevicesAndMeasuredBottleneck(t *testing.T) {
	now, d, _, _, _, _ := fixture(t)
	e, policy, _, _ := topologyFixture(t, now, d.Scope)
	v, err := VerifyTopology(now, e, policy)
	if err != nil || v.observation.FreeGPUs != 3 || v.observation.MaxGPUsPerReplica != 2 || v.observation.FreeKVBytesPerSecond != 500 || v.digest == "" || len(v.observation.Islands) != 2 || v.observation.Islands[0] != (Island{ID: "a", FreeGPUs: 2}) || v.observation.Islands[1] != (Island{ID: "b", FreeGPUs: 1}) {
		t.Fatalf("%+v %v", v, err)
	}
}

func TestVerifyTopologyRejectsSignedContradictions(t *testing.T) {
	now, d, _, _, _, _ := fixture(t)
	for name, change := range map[string]func(*TopologyEvidence, *TopologyPolicy){
		"duplicate physical UID": func(e *TopologyEvidence, _ *TopologyPolicy) {
			e.Inventory.Devices[1].PhysicalUID = e.Inventory.Devices[0].PhysicalUID
		},
		"duplicate DRA address": func(e *TopologyEvidence, _ *TopologyPolicy) {
			e.Inventory.Devices[1].Device = e.Inventory.Devices[0].Device
		},
		"island crosses node":   func(e *TopologyEvidence, _ *TopologyPolicy) { e.Inventory.Devices[1].NodeUID = "other" },
		"missing occupancy":     func(e *TopologyEvidence, _ *TopologyPolicy) { e.Inventory.Devices[0].OccupancySHA256 = "" },
		"unhealthy free GPU":    func(e *TopologyEvidence, _ *TopologyPolicy) { e.Inventory.Devices[0].Healthy = false },
		"not whole device":      func(e *TopologyEvidence, _ *TopologyPolicy) { e.Inventory.Devices[0].WholeDevice = false },
		"incomplete inventory":  func(e *TopologyEvidence, _ *TopologyPolicy) { e.Inventory.Complete = false },
		"missing island":        func(e *TopologyEvidence, _ *TopologyPolicy) { e.Fabric.CoveredIslands = []string{"a"} },
		"revision drift":        func(e *TopologyEvidence, _ *TopologyPolicy) { e.Fabric.Source.TopologyRevision++ },
		"stale inventory":       func(e *TopologyEvidence, _ *TopologyPolicy) { e.Inventory.Source.ObservedAtMillis -= 61000 },
		"overlap link windows":  func(e *TopologyEvidence, _ *TopologyPolicy) { e.Fabric.Windows[1].StartMS-- },
		"missing safety margin": func(e *TopologyEvidence, _ *TopologyPolicy) { e.Fabric.Windows[0].HeadroomBytesPerSecond = 0 },
		"shared signer":         func(_ *TopologyEvidence, p *TopologyPolicy) { p.Fabric.PublicKey = p.Inventory.PublicKey },
	} {
		t.Run(name, func(t *testing.T) {
			e, policy, iPriv, fPriv := topologyFixture(t, now, d.Scope)
			change(&e, &policy)
			signTopology(t, &e, iPriv, fPriv)
			if v, err := VerifyTopology(now, e, policy); err == nil || v.digest != "" {
				t.Fatalf("accepted %+v %v", v, err)
			}
		})
	}
	e, policy, _, fPriv := topologyFixture(t, now, d.Scope)
	e.Fabric.InventorySHA256 = strings.Repeat("b", 64)
	b, err := FabricSigningBytes(e.Fabric)
	if err != nil {
		t.Fatal(err)
	}
	e.FabricSignature = ed25519.Sign(fPriv, b)
	if _, err := VerifyTopology(now, e, policy); err == nil {
		t.Fatal("accepted unrelated inventory digest")
	}
}

func TestProposeFromSourcesBindsPhysicalIdentity(t *testing.T) {
	now, d, p, policy, _, priv, raw, r := rawFixture(t)
	e, topologyPolicy, iPriv, fPriv := topologyFixture(t, now, d.Scope)
	a, err := ProposeFromSources(now, d, p, sign(t, p, priv), policy, time.Minute, raw, r, e, topologyPolicy)
	if err != nil || a.Candidate().GPUsNeeded != 3 || a.Candidate().IntentID == "" || !a.ValidUntil().After(now) {
		t.Fatalf("%+v %v", a, err)
	}
	e.Inventory.Devices[0].PhysicalUID = "gpu-0"
	signTopology(t, &e, iPriv, fPriv)
	b, err := ProposeFromSources(now, d, p, sign(t, p, priv), policy, time.Minute, raw, r, e, topologyPolicy)
	if err != nil || b.Candidate().IntentID == a.Candidate().IntentID || b.Candidate().GenerationKey != a.Candidate().GenerationKey {
		t.Fatalf("identity change not fenced: %+v %v", b, err)
	}
}
