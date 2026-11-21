package pd

import (
	"bytes"
	"crypto/ed25519"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"math/big"
	"slices"
	"sort"
	"time"
)

// SourceMeta binds an observation to a configured physical cluster/provider,
// authenticated adapter and exact inventory revisions. Revisions are control
// plane source watermarks, not parsed Kubernetes resourceVersion integers.
type SourceMeta struct {
	Scope                                    Scope
	ClusterUID, ProviderID, KeyID, AdapterID string
	PoolRevision, TopologyRevision           uint64
	ObservedAtMillis                         int64
}

type SourcePolicy struct {
	Scope                                    Scope
	ClusterUID, ProviderID, KeyID, AdapterID string
	PublicKey                                ed25519.PublicKey
	ValidFrom, ValidUntil                    time.Time
	MaxAge                                   time.Duration
}

// PhysicalGPU is a whole-device mapping certified by the inventory adapter.
// Hashes reference corroborating occupancy, health and connectivity evidence;
// the verifier does not retrieve those artifacts or query hardware itself.
type PhysicalGPU struct {
	PhysicalUID, Driver, Pool, Device                 string
	SliceUID, SliceResourceVersion, NodeUID, IslandID string
	State                                             string
	WholeDevice, Healthy                              bool
	OccupancySHA256, HealthSHA256, ConnectivitySHA256 string
}

type InventoryClaim struct {
	Source   SourceMeta
	Complete bool
	Devices  []PhysicalGPU
}

// LinkWindow measures a shared fabric bottleneck under the pinned coverage,
// not the sum of NIC speeds. Reserved and headroom are conservative deductions
// from the measured aggregate service rate, not deductions from residual rate.
type LinkWindow struct {
	StartMS, EndMS                                                int64
	MeasuredBytes, ReservedBytesPerSecond, HeadroomBytesPerSecond uint64
}

type FabricClaim struct {
	Source                        SourceMeta
	InventorySHA256, BottleneckID string
	CoveredIslands                []string
	Windows                       []LinkWindow
}

type TopologyEvidence struct {
	Inventory          InventoryClaim
	InventorySignature []byte
	Fabric             FabricClaim
	FabricSignature    []byte
}

type TopologyPolicy struct {
	Inventory, Fabric             SourcePolicy
	MinimumWindows                int
	MinimumWindowMS               int64
	MinimumHeadroomBytesPerSecond uint64
}

// verifiedTopology has no exported attestation fields. Only validated signed
// sources can supply the derived summary to ProposeFromSources.
type verifiedTopology struct {
	observation Topology
	digest      string
	validUntil  time.Time
}

func hashBytes(b []byte) string {
	s := sha256.Sum256(b)
	return hex.EncodeToString(s[:])
}

func signedPayload(domain string, value any) ([]byte, error) {
	b, err := json.Marshal(value)
	if err != nil {
		return nil, err
	}
	return append([]byte(domain), b...), nil
}

func InventorySigningBytes(v InventoryClaim) ([]byte, error) {
	return signedPayload("ccp/pd-physical-inventory/v1\x00", v)
}

func FabricSigningBytes(v FabricClaim) ([]byte, error) {
	return signedPayload("ccp/pd-fabric-measurement/v1\x00", v)
}

func validSource(now time.Time, s SourceMeta, p SourcePolicy, payload, signature []byte) bool {
	return validScope(s.Scope) && s.Scope == p.Scope && validID(s.ClusterUID) && s.ClusterUID == p.ClusterUID && validID(s.ProviderID) && s.ProviderID == p.ProviderID && validID(s.KeyID) && s.KeyID == p.KeyID && validID(s.AdapterID) && s.AdapterID == p.AdapterID && s.PoolRevision > 0 && s.TopologyRevision > 0 && !p.ValidFrom.IsZero() && p.ValidFrom.Before(p.ValidUntil) && !now.Before(p.ValidFrom) && now.Before(p.ValidUntil) && !time.UnixMilli(s.ObservedAtMillis).Before(p.ValidFrom) && fresh(now, s.ObservedAtMillis, p.MaxAge) && len(p.PublicKey) == ed25519.PublicKeySize && len(signature) == ed25519.SignatureSize && ed25519.Verify(p.PublicKey, payload, signature)
}

func earlier(a, b time.Time) time.Time {
	if b.Before(a) {
		return b
	}
	return a
}

// VerifyTopology independently authenticates device and fabric claims, joins
// them by exact inventory digest and revision, and derives a conservative
// free-device/topology/link budget. It is not physical allocation proof.
func VerifyTopology(now time.Time, e TopologyEvidence, p TopologyPolicy) (verifiedTopology, error) {
	fail := func() (verifiedTopology, error) { return verifiedTopology{}, ErrUnproven }
	i, f := e.Inventory, e.Fabric
	if now.IsZero() || !i.Complete || len(i.Devices) == 0 || len(i.Devices) > 65536 || len(f.CoveredIslands) > 1024 || len(f.Windows) == 0 || len(f.Windows) > 1024 || p.MinimumWindows <= 0 || len(f.Windows) < p.MinimumWindows || p.MinimumWindowMS <= 0 || p.MinimumHeadroomBytesPerSecond == 0 || bytes.Equal(p.Inventory.PublicKey, p.Fabric.PublicKey) || p.Inventory.KeyID == p.Fabric.KeyID || p.Inventory.AdapterID == p.Fabric.AdapterID {
		return fail()
	}
	ib, err := InventorySigningBytes(i)
	if err != nil || !validSource(now, i.Source, p.Inventory, ib, e.InventorySignature) {
		return fail()
	}
	fb, err := FabricSigningBytes(f)
	if err != nil || !validSource(now, f.Source, p.Fabric, fb, e.FabricSignature) || f.InventorySHA256 != hashBytes(ib) || !validID(f.BottleneckID) || i.Source.Scope != f.Source.Scope || i.Source.ClusterUID != f.Source.ClusterUID || i.Source.ProviderID != f.Source.ProviderID || i.Source.PoolRevision != f.Source.PoolRevision || i.Source.TopologyRevision != f.Source.TopologyRevision {
		return fail()
	}
	t := Topology{Scope: i.Source.Scope, PoolRevision: i.Source.PoolRevision, TopologyRevision: i.Source.TopologyRevision, ObservedAtMillis: i.Source.ObservedAtMillis, Attested: true}
	physical := map[string]bool{}
	addresses := map[[3]string]bool{}
	islands := map[string]int{}
	nodes := map[string]string{}
	slicesSeen := map[string][2]string{}
	last := ""
	for _, g := range i.Devices {
		for _, id := range []string{g.PhysicalUID, g.Driver, g.Pool, g.Device, g.SliceUID, g.SliceResourceVersion, g.NodeUID, g.IslandID} {
			if !validID(id) {
				return fail()
			}
		}
		address := [3]string{g.Driver, g.Pool, g.Device}
		sliceIdentity := [2]string{g.SliceResourceVersion, g.NodeUID}
		if g.PhysicalUID <= last || physical[g.PhysicalUID] || addresses[address] || !g.WholeDevice || !digest(g.OccupancySHA256) || !digest(g.HealthSHA256) || !digest(g.ConnectivitySHA256) {
			return fail()
		}
		last = g.PhysicalUID
		physical[g.PhysicalUID], addresses[address] = true, true
		if old, ok := nodes[g.IslandID]; ok && old != g.NodeUID {
			return fail()
		}
		if old, ok := slicesSeen[g.SliceUID]; ok && old != sliceIdentity {
			return fail()
		}
		nodes[g.IslandID], slicesSeen[g.SliceUID] = g.NodeUID, sliceIdentity
		if _, ok := islands[g.IslandID]; !ok {
			islands[g.IslandID] = 0
		}
		switch g.State {
		case "FREE":
			if !g.Healthy {
				return fail()
			}
			islands[g.IslandID]++
			t.FreeGPUs++
		case "RESERVED", "ACTIVE", "DRAINING", "UNKNOWN", "UNHEALTHY":
			// No capacity from an uncertain or unavailable device.
		default:
			return fail()
		}
	}
	if len(islands) > 1024 {
		return fail()
	}
	var ids []string
	for id := range islands {
		ids = append(ids, id)
	}
	sort.Strings(ids)
	if !slices.Equal(ids, f.CoveredIslands) {
		return fail()
	}
	for _, id := range ids {
		t.Islands = append(t.Islands, Island{ID: id, FreeGPUs: islands[id]})
		t.MaxGPUsPerReplica = max(t.MaxGPUsPerReplica, islands[id])
	}
	// A minimum across contiguous, non-overlapping observation windows is a
	// conservative measured budget under this exact probe coverage, not a QoS guarantee.
	lastEnd := int64(0)
	budget := ^uint64(0)
	for _, w := range f.Windows {
		if w.StartMS < i.Source.ObservedAtMillis || w.EndMS <= w.StartMS || (lastEnd != 0 && w.StartMS != lastEnd) || w.EndMS-w.StartMS < p.MinimumWindowMS || !fresh(now, w.StartMS, p.Fabric.MaxAge) || time.UnixMilli(w.StartMS).Before(p.Fabric.ValidFrom) || w.HeadroomBytesPerSecond < p.MinimumHeadroomBytesPerSecond {
			return fail()
		}
		lastEnd = w.EndMS
		rate := new(big.Int).Mul(new(big.Int).SetUint64(w.MeasuredBytes), big.NewInt(1000))
		rate.Quo(rate, big.NewInt(w.EndMS-w.StartMS))
		if !rate.IsUint64() {
			return fail()
		}
		free := rate.Uint64()
		free -= min(free, w.ReservedBytesPerSecond)
		free -= min(free, w.HeadroomBytesPerSecond)
		budget = min(budget, free)
	}
	if lastEnd != f.Source.ObservedAtMillis {
		return fail()
	}
	t.FreeKVBytesPerSecond = budget
	payload, err := signedPayload("ccp/pd-topology-provenance/v1\x00", struct {
		Evidence TopologyEvidence
		Policy   TopologyPolicy
	}{e, p})
	if err != nil {
		return fail()
	}
	expires := earlier(p.Inventory.ValidUntil, p.Fabric.ValidUntil)
	expires = earlier(expires, time.UnixMilli(i.Source.ObservedAtMillis).Add(p.Inventory.MaxAge))
	expires = earlier(expires, time.UnixMilli(f.Windows[0].StartMS).Add(p.Fabric.MaxAge))
	if !now.Before(expires) {
		return fail()
	}
	return verifiedTopology{observation: t, digest: hashBytes(payload), validUntil: expires}, nil
}

// VerifiedProposal is an offline immutable verification receipt. Its zero
// value is invalid; it conveys no authority to reserve or create a resource.
// A durable consumer must still bind current trusted revisions and Workload
// generation in one transaction and enforce authorization independently.
type VerifiedProposal struct {
	candidate  Proposal
	validUntil time.Time
	providerID string
}

func (v VerifiedProposal) Candidate() Proposal   { return v.candidate }
func (v VerifiedProposal) ValidUntil() time.Time { return v.validUntil }
func (v VerifiedProposal) ProviderID() string    { return v.providerID }

// ProposeFromSources requires independently signed inventory/fabric sources
// plus the raw-verified benchmark. It never accepts a caller's Attested flag.
func ProposeFromSources(now time.Time, d Demand, profile Profile, signature []byte, policy Policy, maxObservationAge time.Duration, raw []byte, re RawEvidence, e TopologyEvidence, tp TopologyPolicy) (VerifiedProposal, error) {
	fail := func() (VerifiedProposal, error) { return VerifiedProposal{}, ErrUnproven }
	if bytes.Equal(policy.PublicKey, tp.Inventory.PublicKey) || bytes.Equal(policy.PublicKey, tp.Fabric.PublicKey) || policy.KeyID == tp.Inventory.KeyID || policy.KeyID == tp.Fabric.KeyID {
		return fail()
	}
	v, err := VerifyTopology(now, e, tp)
	if err != nil {
		return fail()
	}
	candidate, err := ProposeRaw(now, d, profile, signature, policy, v.observation, maxObservationAge, raw, re)
	if err != nil {
		return fail()
	}
	payload, err := signedPayload("ccp/pd-source-intent/v1\x00", struct{ BaseIntent, TopologyDigest string }{candidate.IntentID, v.digest})
	if err != nil {
		return fail()
	}
	candidate.IntentID = hashBytes(payload)
	expires := earlier(v.validUntil, policy.ValidUntil)
	expires = earlier(expires, time.UnixMilli(profile.ObservedAtMillis).Add(policy.MaxAge))
	expires = earlier(expires, time.UnixMilli(d.ObservedAtMillis).Add(maxObservationAge))
	expires = earlier(expires, time.UnixMilli(v.observation.ObservedAtMillis).Add(maxObservationAge))
	if !now.Before(expires) {
		return fail()
	}
	return VerifiedProposal{candidate: candidate, validUntil: expires, providerID: e.Inventory.Source.ProviderID}, nil
}
