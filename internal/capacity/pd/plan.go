// Package pd provides offline, non-actuating split-inference sizing.
package pd

import (
	"crypto/ed25519"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"math"
	"math/big"
	"strings"
	"time"
	"unicode/utf8"
)

var ErrUnproven = errors.New("PD evidence or proposal is unproven")

type Outcome string

const (
	Unproven             Outcome = "UNPROVEN"
	Fit                  Outcome = "ESTIMATED_FIT"
	UnmetSLO             Outcome = "UNMET_BENCHMARK_SLO"
	InsufficientGPU      Outcome = "INSUFFICIENT_OBSERVED_GPU"
	InsufficientLink     Outcome = "INSUFFICIENT_OBSERVED_LINK"
	InsufficientTopology Outcome = "INSUFFICIENT_OBSERVED_TOPOLOGY"
)

type Scope struct{ Tenant, Project, Pool, Model, GPU, Shape, Fabric string }

// Demand must come from an authenticated load reader; generation is the
// desired workload generation. Rates must be measured over comparable windows.
type Demand struct {
	Scope                                                            Scope
	Workload                                                         string
	Generation, LoadRevision                                         uint64
	ObservedAtMillis                                                 int64
	PrefillTokensPerSecond, DecodeTokensPerSecond, RequestsPerSecond float64
	MaxKVBytesPerRequest                                             uint64
	MaxTTFTms, MaxTPOTms, MaxTransferP95ms                           float64
}

// Profile describes a jointly measured PD deployment with one replica per
// phase. Rates are phase-specific, NOT unified request wall throughput.
// Transfer and end-to-end p95s are measured, not summed marginal percentiles.
type Profile struct {
	Scope                                         Scope
	KeyID, RunID, ArtifactSHA256                  string
	ObservedAtMillis                              int64
	PrefillGPUs, DecodeGPUs                       int
	PrefillTokensPerSecond, DecodeTokensPerSecond float64
	MaxKVBytesPerRequest                          uint64
	P95TTFTms, P95TPOTms, P95TransferMs           float64
}

// Policy is independently configured, not received with the report.
type Policy struct {
	Scope                 Scope
	KeyID                 string
	PublicKey             ed25519.PublicKey
	ValidFrom, ValidUntil time.Time
	MaxAge                time.Duration
}

// Topology is supplied by an independent trusted reader. FreeGPU and link
// capacity use a single shared pool/fabric budget, never two phase snapshots.
// MaxGPUsPerReplica is only a necessary placement constraint, not a guarantee.
type Topology struct {
	Scope                          Scope
	PoolRevision, TopologyRevision uint64
	ObservedAtMillis               int64
	FreeGPUs, MaxGPUsPerReplica    int
	FreeKVBytesPerSecond           uint64
	Attested                       bool
	Islands                        []Island
}
type Advice struct {
	Outcome                                     Outcome
	PrefillReplicas, DecodeReplicas, GPUsNeeded int
}

func SigningBytes(p Profile) ([]byte, error) {
	b, e := json.Marshal(p)
	if e != nil {
		return nil, e
	}
	return append([]byte("ccp/pd-benchmark/v1\x00"), b...), nil
}
func validID(s string) bool {
	return s != "" && len(s) <= 256 && strings.TrimSpace(s) == s && utf8.ValidString(s) && !strings.ContainsRune(s, utf8.RuneError)
}
func validScope(s Scope) bool {
	return validID(s.Tenant) && validID(s.Project) && validID(s.Pool) && validID(s.Model) && validID(s.GPU) && validID(s.Shape) && validID(s.Fabric)
}
func positive(f float64) bool { return f > 0 && !math.IsInf(f, 0) && !math.IsNaN(f) }
func fresh(now time.Time, ms int64, age time.Duration) bool {
	t := time.UnixMilli(ms)
	return ms > 0 && age > 0 && !t.After(now) && now.Sub(t) <= age
}
func digest(s string) bool {
	b, e := hex.DecodeString(s)
	return e == nil && len(b) == 32 && strings.ToLower(s) == s
}
func ceilRatio(a, b float64) *big.Int {
	r := new(big.Rat).Quo(new(big.Rat).SetFloat64(a), new(big.Rat).SetFloat64(b))
	var q, rem big.Int
	q.QuoRem(r.Num(), r.Denom(), &rem)
	if rem.Sign() > 0 {
		q.Add(&q, big.NewInt(1))
	}
	return &q
}

// Plan returns estimates only. Even FIT does not establish gang placement,
// admission, allocation or that independent replicas scale linearly.
func Plan(now time.Time, d Demand, p Profile, signature []byte, policy Policy, t Topology, maxObservationAge time.Duration) (Advice, error) {
	fail := func() (Advice, error) { return Advice{Outcome: Unproven}, ErrUnproven }
	if now.IsZero() || !validScope(d.Scope) || d.Scope != p.Scope || d.Scope != policy.Scope || d.Scope != t.Scope || !validID(d.Workload) || d.Generation == 0 || d.LoadRevision == 0 || !fresh(now, d.ObservedAtMillis, maxObservationAge) || !fresh(now, t.ObservedAtMillis, maxObservationAge) || !t.Attested || t.PoolRevision == 0 || t.TopologyRevision == 0 || t.FreeGPUs < 0 || t.MaxGPUsPerReplica < 0 || !validIslands(t) {
		return fail()
	}
	if !validID(p.KeyID) || p.KeyID != policy.KeyID || !validID(p.RunID) || !digest(p.ArtifactSHA256) || len(policy.PublicKey) != ed25519.PublicKeySize || len(signature) != ed25519.SignatureSize || policy.ValidFrom.IsZero() || !policy.ValidFrom.Before(policy.ValidUntil) || now.Before(policy.ValidFrom) || !now.Before(policy.ValidUntil) || time.UnixMilli(p.ObservedAtMillis).Before(policy.ValidFrom) || !fresh(now, p.ObservedAtMillis, policy.MaxAge) {
		return fail()
	}
	for _, v := range []float64{d.PrefillTokensPerSecond, d.DecodeTokensPerSecond, d.RequestsPerSecond, d.MaxTTFTms, d.MaxTPOTms, d.MaxTransferP95ms, p.PrefillTokensPerSecond, p.DecodeTokensPerSecond, p.P95TTFTms, p.P95TPOTms, p.P95TransferMs} {
		if !positive(v) {
			return fail()
		}
	}
	if p.PrefillGPUs <= 0 || p.DecodeGPUs <= 0 || d.MaxKVBytesPerRequest == 0 || d.MaxKVBytesPerRequest > p.MaxKVBytesPerRequest {
		return fail()
	}
	msg, e := SigningBytes(p)
	if e != nil || !ed25519.Verify(policy.PublicKey, msg, signature) {
		return fail()
	}
	if p.P95TTFTms > d.MaxTTFTms || p.P95TPOTms > d.MaxTPOTms || p.P95TransferMs > d.MaxTransferP95ms {
		return Advice{Outcome: UnmetSLO}, nil
	}
	pre, dec := ceilRatio(d.PrefillTokensPerSecond, p.PrefillTokensPerSecond), ceilRatio(d.DecodeTokensPerSecond, p.DecodeTokensPerSecond)
	need := new(big.Int).Add(new(big.Int).Mul(pre, big.NewInt(int64(p.PrefillGPUs))), new(big.Int).Mul(dec, big.NewInt(int64(p.DecodeGPUs))))
	if need.Cmp(big.NewInt(int64(int(^uint(0)>>1)))) > 0 {
		return fail()
	}
	a := Advice{Outcome: Fit, PrefillReplicas: int(pre.Int64()), DecodeReplicas: int(dec.Int64()), GPUsNeeded: int(need.Int64())}
	// Use the verified profile's full upper bound; do not size on a mean KV size.
	kv := new(big.Rat).Mul(new(big.Rat).SetFloat64(d.RequestsPerSecond), new(big.Rat).SetInt(new(big.Int).SetUint64(p.MaxKVBytesPerRequest)))
	switch {
	case a.GPUsNeeded > t.FreeGPUs:
		a.Outcome = InsufficientGPU
	case p.PrefillGPUs > t.MaxGPUsPerReplica || p.DecodeGPUs > t.MaxGPUsPerReplica || !packIslands(t.Islands, p.PrefillGPUs, a.PrefillReplicas, p.DecodeGPUs, a.DecodeReplicas):
		a.Outcome = InsufficientTopology
	case kv.Cmp(new(big.Rat).SetInt(new(big.Int).SetUint64(t.FreeKVBytesPerSecond))) > 0:
		a.Outcome = InsufficientLink
	}
	return a, nil
}

// Proposal is a content-addressed, non-authoritative desired target. It does
// not create a workload, hold quota or claim an allocation. A durable consumer
// must reauthenticate, re-read revisions, enforce CAS and obtain Kueue admission.
type Proposal struct {
	IntentID, GenerationKey                                  string
	Scope                                                    Scope
	Workload                                                 string
	Generation, PoolRevision, TopologyRevision, LoadRevision uint64
	PrefillReplicas, DecodeReplicas, GPUsNeeded              int
}

func Propose(now time.Time, d Demand, p Profile, signature []byte, policy Policy, t Topology, maxObservationAge time.Duration) (Proposal, error) {
	a, e := Plan(now, d, p, signature, policy, t, maxObservationAge)
	if e != nil || a.Outcome != Fit {
		return Proposal{}, ErrUnproven
	}
	// Bind all immutable inputs (including evidence and observations), not just
	// replica counts. Identical evidence and generation replay the same proposal.
	payload, e := json.Marshal(struct {
		Demand   Demand
		Profile  Profile
		Topology Topology
		Advice   Advice
	}{d, p, t, a})
	if e != nil {
		return Proposal{}, ErrUnproven
	}
	hash := sha256.Sum256(append([]byte("ccp/pd-intent/v1\x00"), payload...))
	keyPayload, e := json.Marshal(struct {
		Tenant, Project, Workload string
		Generation                uint64
	}{d.Scope.Tenant, d.Scope.Project, d.Workload, d.Generation})
	if e != nil {
		return Proposal{}, ErrUnproven
	}
	keyHash := sha256.Sum256(append([]byte("ccp/pd-generation/v1\x00"), keyPayload...))
	return Proposal{IntentID: hex.EncodeToString(hash[:]), GenerationKey: hex.EncodeToString(keyHash[:]), Scope: d.Scope, Workload: d.Workload, Generation: d.Generation, PoolRevision: t.PoolRevision, TopologyRevision: t.TopologyRevision, LoadRevision: d.LoadRevision, PrefillReplicas: a.PrefillReplicas, DecodeReplicas: a.DecodeReplicas, GPUsNeeded: a.GPUsNeeded}, nil
}

// RevalidateProposal checks an exact candidate against independently refreshed
// observations and the next generation read from durable authority. Consumers
// must perform the generation/revision CAS in the same transaction as acceptance;
// this pure check cannot prevent a concurrent update after it returns.
func RevalidateProposal(now time.Time, candidate Proposal, expectedNextGeneration uint64, d Demand, p Profile, signature []byte, policy Policy, t Topology, maxObservationAge time.Duration) error {
	if expectedNextGeneration == 0 || d.Generation != expectedNextGeneration || candidate.Generation != expectedNextGeneration {
		return ErrUnproven
	}
	actual, err := Propose(now, d, p, signature, policy, t, maxObservationAge)
	if err != nil || actual != candidate {
		return ErrUnproven
	}
	return nil
}
