package pd

import (
	"crypto/ed25519"
	"math"
	"testing"
	"time"
)

func fixture(t *testing.T) (time.Time, Demand, Profile, Policy, Topology, ed25519.PrivateKey) {
	t.Helper()
	now := time.Date(2026, 11, 18, 0, 0, 0, 0, time.UTC)
	pub, priv, e := ed25519.GenerateKey(nil)
	if e != nil {
		t.Fatal(e)
	}
	scope := Scope{Tenant: "t", Project: "p", Pool: "pool", Model: "m", GPU: "g", Shape: "s", Fabric: "rdma-a"}
	d := Demand{Scope: scope, Workload: "w", Generation: 2, LoadRevision: 3, ObservedAtMillis: now.UnixMilli(), PrefillTokensPerSecond: 200, DecodeTokensPerSecond: 150, RequestsPerSecond: 5, MaxKVBytesPerRequest: 100, MaxTTFTms: 100, MaxTPOTms: 20, MaxTransferP95ms: 10}
	p := Profile{Scope: scope, KeyID: "key", RunID: "pd-run", ArtifactSHA256: "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", ObservedAtMillis: now.UnixMilli(), PrefillGPUs: 2, DecodeGPUs: 1, PrefillTokensPerSecond: 100, DecodeTokensPerSecond: 50, MaxKVBytesPerRequest: 100, P95TTFTms: 90, P95TPOTms: 15, P95TransferMs: 5}
	policy := Policy{Scope: scope, KeyID: "key", PublicKey: pub, ValidFrom: now.Add(-time.Hour), ValidUntil: now.Add(time.Hour), MaxAge: time.Minute}
	topology := Topology{Scope: scope, PoolRevision: 7, TopologyRevision: 9, ObservedAtMillis: now.UnixMilli(), FreeGPUs: 7, MaxGPUsPerReplica: 2, FreeKVBytesPerSecond: 500, Attested: true, Islands: []Island{{ID: "a", FreeGPUs: 7}}}
	return now, d, p, policy, topology, priv
}
func sign(t *testing.T, p Profile, priv ed25519.PrivateKey) []byte {
	t.Helper()
	msg, e := SigningBytes(p)
	if e != nil {
		t.Fatal(e)
	}
	return ed25519.Sign(priv, msg)
}
func TestPlanPDIndependentPhasesAndSharedCapacity(t *testing.T) {
	now, d, p, policy, top, priv := fixture(t)
	sig := sign(t, p, priv)
	a, e := Plan(now, d, p, sig, policy, top, time.Minute)
	if e != nil || a != (Advice{Outcome: Fit, PrefillReplicas: 2, DecodeReplicas: 3, GPUsNeeded: 7}) {
		t.Fatalf("%+v %v", a, e)
	}
	top.FreeGPUs = 6
	top.Islands[0].FreeGPUs = 6
	a, e = Plan(now, d, p, sig, policy, top, time.Minute)
	if e != nil || a.Outcome != InsufficientGPU {
		t.Fatalf("%+v %v", a, e)
	}
	top.FreeGPUs = 7
	top.Islands[0].FreeGPUs = 7
	top.FreeKVBytesPerSecond = 499
	a, e = Plan(now, d, p, sig, policy, top, time.Minute)
	if e != nil || a.Outcome != InsufficientLink {
		t.Fatalf("%+v %v", a, e)
	}
	top.FreeKVBytesPerSecond = 500
	top.MaxGPUsPerReplica = 1
	a, e = Plan(now, d, p, sig, policy, top, time.Minute)
	if e != nil || a.Outcome != InsufficientTopology {
		t.Fatalf("%+v %v", a, e)
	}
}
func TestPlanRejectsTamperScopeStaleAndSLO(t *testing.T) {
	now, d, p, policy, top, priv := fixture(t)
	sig := sign(t, p, priv)
	p.DecodeTokensPerSecond++
	if a, e := Plan(now, d, p, sig, policy, top, time.Minute); e == nil || a.Outcome != Unproven {
		t.Fatalf("%+v %v", a, e)
	}
	p.DecodeTokensPerSecond--
	for name, change := range map[string]func(*Demand, *Profile, *Topology){"scope": func(d *Demand, p *Profile, t *Topology) { t.Scope.Fabric = "other" }, "stale": func(d *Demand, p *Profile, t *Topology) { t.ObservedAtMillis -= 61000 }, "stale load": func(d *Demand, p *Profile, t *Topology) { d.ObservedAtMillis -= 61000 }, "no revision": func(d *Demand, p *Profile, t *Topology) { t.PoolRevision = 0 }, "unattested": func(d *Demand, p *Profile, t *Topology) { t.Attested = false }, "kv exceeds profile": func(d *Demand, p *Profile, t *Topology) { d.MaxKVBytesPerRequest++ }, "overflow": func(d *Demand, p *Profile, t *Topology) { d.PrefillTokensPerSecond = math.MaxFloat64 }} {
		t.Run(name, func(t *testing.T) {
			dd, pp, tt := d, p, top
			change(&dd, &pp, &tt)
			if a, e := Plan(now, dd, pp, sign(t, pp, priv), policy, tt, time.Minute); e == nil || a.Outcome != Unproven {
				t.Fatalf("%+v %v", a, e)
			}
		})
	}
	d.MaxTransferP95ms = 4
	if a, e := Plan(now, d, p, sig, policy, top, time.Minute); e != nil || a.Outcome != UnmetSLO {
		t.Fatalf("%+v %v", a, e)
	}
}
func TestProposalFencesReplayAndNeverActs(t *testing.T) {
	now, d, p, policy, top, priv := fixture(t)
	sig := sign(t, p, priv)
	a, e := Propose(now, d, p, sig, policy, top, time.Minute)
	if e != nil || a.IntentID == "" || a.Generation != 2 || a.PoolRevision != 7 || a.TopologyRevision != 9 || a.LoadRevision != 3 || a.PrefillReplicas != 2 || a.DecodeReplicas != 3 {
		t.Fatalf("%+v %v", a, e)
	}
	b, e := Propose(now, d, p, sig, policy, top, time.Minute)
	if e != nil || a != b {
		t.Fatalf("non deterministic: %+v %v", b, e)
	}
	top.PoolRevision++
	b, e = Propose(now, d, p, sig, policy, top, time.Minute)
	if e != nil || a.IntentID == b.IntentID {
		t.Fatalf("revision not fenced: %+v %v", b, e)
	}
	top.FreeGPUs = 0
	top.Islands[0].FreeGPUs = 0
	if b, e = Propose(now, d, p, sig, policy, top, time.Minute); e == nil || b != (Proposal{}) {
		t.Fatalf("nonfit proposed: %+v %v", b, e)
	}
}

func TestProposalIdentityBindsVerifierKeyAndTrustPolicy(t *testing.T) {
	now, d, p, policy, top, priv := fixture(t)
	original, err := Propose(now, d, p, sign(t, p, priv), policy, top, time.Minute)
	if err != nil {
		t.Fatal(err)
	}

	// The same signed benchmark content is not the same evidence after
	// its independently authorized signer changes.
	newPublic, newPrivate, err := ed25519.GenerateKey(nil)
	if err != nil {
		t.Fatal(err)
	}
	rotatedPolicy := policy
	rotatedPolicy.PublicKey = newPublic
	rotated, err := Propose(now, d, p, sign(t, p, newPrivate), rotatedPolicy, top, time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	if rotated.IntentID == original.IntentID || rotated.GenerationKey != original.GenerationKey {
		t.Fatalf("signer rotation must conflict within generation: original %+v, rotated %+v", original, rotated)
	}

	changedPolicy := policy
	changedPolicy.ValidUntil = changedPolicy.ValidUntil.Add(time.Minute)
	reconfigured, err := Propose(now, d, p, sign(t, p, priv), changedPolicy, top, time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	if reconfigured.IntentID == original.IntentID || reconfigured.GenerationKey != original.GenerationKey {
		t.Fatalf("policy change must conflict within generation: original %+v, changed %+v", original, reconfigured)
	}
}
