package pd

import (
	"crypto/ed25519"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/avaargsh/compute-control-panel/internal/capacity/pd/rawphase"
)

func rawFixture(t *testing.T) (time.Time, Demand, Profile, Policy, Topology, ed25519.PrivateKey, []byte, RawEvidence) {
	t.Helper()
	now, d, p, policy, top, priv := fixture(t)
	end := now.UnixMilli()
	start := end - 2000
	m := rawphase.Manifest{Version: "pd-raw/v1", RunID: p.RunID, Tenant: d.Scope.Tenant, Project: d.Scope.Project, Pool: d.Scope.Pool, Model: d.Scope.Model, GPU: d.Scope.GPU, Shape: d.Scope.Shape, Fabric: d.Scope.Fabric, EvaluatorCommit: strings.Repeat("a", 40), DeploymentSHA256: strings.Repeat("b", 64), PrefillWorker: "pf", DecodeWorker: "dc", PrefillGPUs: p.PrefillGPUs, DecodeGPUs: p.DecodeGPUs, StartMS: start, EndMS: end, Attempts: 2, KVBytesLimit: 100}
	raw, err := json.Marshal(m)
	if err != nil {
		t.Fatal(err)
	}
	for i, id := range []string{"a", "b"} {
		s := start + int64(i)*500
		a := rawphase.Attempt{AttemptID: id, TransferID: id + "-kv", PrefillWorker: "pf", DecodeWorker: "dc", Success: true, PrefillTokens: 100, DecodeTokens: 3, KVBytes: 100, RequestStartMS: s, PrefillStartMS: s, PrefillEndMS: s + 100, TransferStartMS: s + 100, TransferEndMS: s + 200, DecodeStartMS: s + 200, FirstTokenMS: s + 400, LastTokenMS: s + 800}
		b, err := json.Marshal(a)
		if err != nil {
			t.Fatal(err)
		}
		raw = append(append(raw, '\n'), b...)
	}
	sum := sha256.Sum256(raw)
	p.ArtifactSHA256 = hex.EncodeToString(sum[:])
	p.PrefillTokensPerSecond, p.DecodeTokensPerSecond = 100, 3
	p.P95TTFTms, p.P95TPOTms, p.P95TransferMs = 400, 200, 100
	d.PrefillTokensPerSecond, d.DecodeTokensPerSecond, d.RequestsPerSecond = 100, 3, 1
	d.MaxTTFTms, d.MaxTPOTms, d.MaxTransferP95ms = 400, 200, 100
	return now, d, p, policy, top, priv, raw, RawEvidence{Manifest: m, ArtifactSHA256: p.ArtifactSHA256, MinimumAttempts: 2, MinimumWindowMS: 2000}
}

func TestPlanRawRequiresIndependentMetricsAndManifest(t *testing.T) {
	now, d, p, policy, top, priv, raw, e := rawFixture(t)
	a, err := PlanRaw(now, d, p, sign(t, p, priv), policy, top, time.Minute, raw, e)
	if err != nil || a != (Advice{Outcome: Fit, PrefillReplicas: 1, DecodeReplicas: 1, GPUsNeeded: 3}) {
		t.Fatalf("%+v %v", a, err)
	}
	bad := p
	bad.DecodeTokensPerSecond++
	if a, err := PlanRaw(now, d, bad, sign(t, bad, priv), policy, top, time.Minute, raw, e); err == nil || a.Outcome != Unproven {
		t.Fatalf("signed false metric accepted: %+v %v", a, err)
	}
	for name, change := range map[string]func(*RawEvidence){
		"wrong digest":    func(v *RawEvidence) { v.ArtifactSHA256 = strings.Repeat("c", 64) },
		"wrong config":    func(v *RawEvidence) { v.Manifest.DeploymentSHA256 = strings.Repeat("c", 64) },
		"wrong scope":     func(v *RawEvidence) { v.Manifest.Tenant = "other" },
		"wrong GPU count": func(v *RawEvidence) { v.Manifest.PrefillGPUs++ },
		"sample floor":    func(v *RawEvidence) { v.MinimumAttempts++ },
	} {
		t.Run(name, func(t *testing.T) {
			bad := e
			change(&bad)
			if a, err := PlanRaw(now, d, p, sign(t, p, priv), policy, top, time.Minute, raw, bad); err == nil || a.Outcome != Unproven {
				t.Fatalf("accepted %+v %v", a, err)
			}
		})
	}
}

func TestRawProposalBindsVerificationPolicy(t *testing.T) {
	now, d, p, policy, top, priv, raw, e := rawFixture(t)
	sig := sign(t, p, priv)
	a, err := ProposeRaw(now, d, p, sig, policy, top, time.Minute, raw, e)
	if err != nil || a.IntentID == "" {
		t.Fatalf("%+v %v", a, err)
	}
	if err := RevalidateRawProposal(now, a, d.Generation, d, p, sig, policy, top, time.Minute, raw, e); err != nil {
		t.Fatal(err)
	}
	// A weaker independently configured sample floor changes the evidence contract.
	e.MinimumAttempts--
	b, err := ProposeRaw(now, d, p, sig, policy, top, time.Minute, raw, e)
	if err != nil || a.GenerationKey != b.GenerationKey || a.IntentID == b.IntentID {
		t.Fatalf("policy not fenced: %+v %v", b, err)
	}
	if err := RevalidateRawProposal(now, a, d.Generation, d, p, sig, policy, top, time.Minute, raw, e); err == nil {
		t.Fatal("stale raw policy accepted")
	}
	if err := RevalidateRawProposal(now, b, d.Generation+1, d, p, sig, policy, top, time.Minute, raw, e); err == nil {
		t.Fatal("stale generation accepted")
	}
}
