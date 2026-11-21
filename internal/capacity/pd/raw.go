package pd

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"time"

	"github.com/avaargsh/compute-control-panel/internal/capacity/pd/rawphase"
)

// RawEvidence is independently resolved from authenticated run provenance and
// the evaluator policy. Never construct it from the report being verified.
type RawEvidence struct {
	Manifest        rawphase.Manifest
	ArtifactSHA256  string
	MinimumAttempts int
	MinimumWindowMS int64
}

// PlanRaw requires the signed benchmark AND recomputation from pinned paired
// worker records. The legacy Plan remains an offline calculator only. Neither
// entry point is a workload acceptance or physical capacity authority.
func PlanRaw(now time.Time, d Demand, p Profile, signature []byte, policy Policy, t Topology, maxObservationAge time.Duration, raw []byte, e RawEvidence) (Advice, error) {
	fail := func() (Advice, error) { return Advice{Outcome: Unproven}, ErrUnproven }
	advice, err := Plan(now, d, p, signature, policy, t, maxObservationAge)
	if err != nil {
		return fail()
	}
	m := e.Manifest
	scope := Scope{Tenant: m.Tenant, Project: m.Project, Pool: m.Pool, Model: m.Model, GPU: m.GPU, Shape: m.Shape, Fabric: m.Fabric}
	if scope != p.Scope || e.ArtifactSHA256 != p.ArtifactSHA256 || m.RunID != p.RunID || m.EndMS != p.ObservedAtMillis || time.UnixMilli(m.StartMS).Before(policy.ValidFrom) || m.PrefillGPUs != p.PrefillGPUs || m.DecodeGPUs != p.DecodeGPUs || m.KVBytesLimit != p.MaxKVBytesPerRequest {
		return fail()
	}
	s, err := rawphase.Verify(raw, e.ArtifactSHA256, m, e.MinimumAttempts, e.MinimumWindowMS)
	if err != nil || s.PrefillTokensPerSecond != p.PrefillTokensPerSecond || s.DecodeTokensPerSecond != p.DecodeTokensPerSecond || s.P95TTFTms != p.P95TTFTms || s.P95TPOTms != p.P95TPOTms || s.P95TransferMs != p.P95TransferMs {
		return fail()
	}
	return advice, nil
}

// ProposeRaw binds the independent raw verification requirements into the
// content identity, retaining the existing generation conflict key. Identical
// replica counts do not permit reusing an intent after a trust-policy change.
func ProposeRaw(now time.Time, d Demand, p Profile, signature []byte, policy Policy, t Topology, maxObservationAge time.Duration, raw []byte, e RawEvidence) (Proposal, error) {
	advice, err := PlanRaw(now, d, p, signature, policy, t, maxObservationAge, raw, e)
	if err != nil || advice.Outcome != Fit {
		return Proposal{}, ErrUnproven
	}
	candidate, err := Propose(now, d, p, signature, policy, t, maxObservationAge)
	if err != nil {
		return Proposal{}, err
	}
	payload, err := json.Marshal(struct {
		BaseIntent string
		Evidence   RawEvidence
	}{candidate.IntentID, e})
	if err != nil {
		return Proposal{}, ErrUnproven
	}
	sum := sha256.Sum256(append([]byte("ccp/pd-raw-intent/v1\x00"), payload...))
	candidate.IntentID = hex.EncodeToString(sum[:])
	return candidate, nil
}

// RevalidateRawProposal is non-actuating. The future durable consumer must
// re-read authoritative inputs and perform revision/generation CAS atomically.
func RevalidateRawProposal(now time.Time, candidate Proposal, expectedNextGeneration uint64, d Demand, p Profile, signature []byte, policy Policy, t Topology, maxObservationAge time.Duration, raw []byte, e RawEvidence) error {
	if expectedNextGeneration == 0 || d.Generation != expectedNextGeneration || candidate.Generation != expectedNextGeneration {
		return ErrUnproven
	}
	actual, err := ProposeRaw(now, d, p, signature, policy, t, maxObservationAge, raw, e)
	if err != nil || actual != candidate {
		return ErrUnproven
	}
	return nil
}
