package capacity

import (
	"crypto/ed25519"
	"errors"
	"strings"
	"testing"
	"time"
)

func signedFixture(t *testing.T) (time.Time, Demand, BenchmarkClaim, BenchmarkTrustPolicy, PoolObservation, ed25519.PrivateKey) {
	t.Helper()
	now := time.Date(2026, 11, 18, 0, 0, 0, 0, time.UTC)
	pub, priv, err := ed25519.GenerateKey(nil)
	if err != nil {
		t.Fatal(err)
	}
	d := Demand{PoolID: "pool-1", ModelRevision: "rev-1", GPUProfile: "c500-64g", WorkloadShape: "170k-c8", PrefillTokensPerSec: 34000, DecodeTokensPerSec: 2500, MaxTTFTms: 3000, MaxTPOTms: 100}
	c := BenchmarkClaim{KeyID: "verifier-1", RunID: "run-1", RawSHA256: strings.Repeat("a", 64), EvaluatorRevision: strings.Repeat("b", 40), PoolID: d.PoolID, ModelRevision: d.ModelRevision, GPUProfile: d.GPUProfile, WorkloadShape: d.WorkloadShape, ReplicaCount: 1, GPUsPerReplica: 2, Attempts: 100, Successes: 99, TPOTSamples: 99, ObservedWallSeconds: 60, ObservedAtUnixMillis: now.UnixMilli(), PrefillTokensPerSec: 17000, DecodeTokensPerSec: 1000, P95TTFTms: 2500, P95TPOTms: 80}
	policy := BenchmarkTrustPolicy{KeyID: c.KeyID, PublicKey: pub, EvaluatorRevision: c.EvaluatorRevision, PoolID: d.PoolID, ModelRevision: d.ModelRevision, GPUProfile: d.GPUProfile, WorkloadShape: d.WorkloadShape, ValidFrom: now.Add(-time.Hour), ValidUntil: now.Add(time.Hour), MaxAge: time.Minute, MinAttempts: 100, MinSuccessRate: 0.95, MinWallSeconds: 30}
	pool := PoolObservation{PoolID: d.PoolID, GPUProfile: d.GPUProfile, AllocatableGPUs: 16, ReservedGPUs: 4, ObservedAt: now, Revision: 7, Attested: true}
	return now, d, c, policy, pool, priv
}

func signClaim(t *testing.T, c BenchmarkClaim, priv ed25519.PrivateKey) []byte {
	t.Helper()
	msg, err := BenchmarkSigningBytes(c)
	if err != nil {
		t.Fatal(err)
	}
	return ed25519.Sign(priv, msg)
}

func TestAdviseSignedVerifiedBenchmarkRemainsAdvisory(t *testing.T) {
	now, d, c, policy, pool, priv := signedFixture(t)
	a, err := AdviseSigned(now, d, c, signClaim(t, c, priv), policy, pool, time.Minute)
	if err != nil || a.Outcome != EstimatedFit || a.Replicas != 3 || a.GPUsNeeded != 6 || a.Admitted {
		t.Fatalf("unexpected advice: %+v, %v", a, err)
	}
	pool.Attested = false
	a, err = AdviseSigned(now, d, c, signClaim(t, c, priv), policy, pool, time.Minute)
	if !errors.Is(err, ErrUnprovenCapacity) || a != (Advice{Outcome: Unproven}) {
		t.Fatalf("benchmark must not attest pool: %+v %v", a, err)
	}
}

func TestAdviseSignedRejectsTamperingAndUntrustedKey(t *testing.T) {
	now, d, c, policy, pool, priv := signedFixture(t)
	sig := signClaim(t, c, priv)
	c.PrefillTokensPerSec *= 100
	a, err := AdviseSigned(now, d, c, sig, policy, pool, time.Minute)
	if !errors.Is(err, ErrUnprovenCapacity) || a != (Advice{Outcome: Unproven}) {
		t.Fatalf("tampering accepted: %+v %v", a, err)
	}
	_, other, err := ed25519.GenerateKey(nil)
	if err != nil {
		t.Fatal(err)
	}
	a, err = AdviseSigned(now, d, c, signClaim(t, c, other), policy, pool, time.Minute)
	if !errors.Is(err, ErrUnprovenCapacity) || a != (Advice{Outcome: Unproven}) {
		t.Fatalf("untrusted key accepted: %+v %v", a, err)
	}
}

func TestAdviseSignedRejectsInvalidEvenIfSigned(t *testing.T) {
	for _, tc := range []struct {
		name   string
		change func(*Demand, *BenchmarkClaim, *BenchmarkTrustPolicy)
	}{
		{"wrong pool", func(d *Demand, c *BenchmarkClaim, p *BenchmarkTrustPolicy) { c.PoolID = "other" }},
		{"wrong shape", func(d *Demand, c *BenchmarkClaim, p *BenchmarkTrustPolicy) { c.WorkloadShape = "other" }},
		{"wrong model", func(d *Demand, c *BenchmarkClaim, p *BenchmarkTrustPolicy) { c.ModelRevision = "other" }},
		{"wrong profile", func(d *Demand, c *BenchmarkClaim, p *BenchmarkTrustPolicy) { c.GPUProfile = "other" }},
		{"demand crosses scope", func(d *Demand, c *BenchmarkClaim, p *BenchmarkTrustPolicy) { d.PoolID = "other" }},
		{"wrong evaluator", func(d *Demand, c *BenchmarkClaim, p *BenchmarkTrustPolicy) {
			c.EvaluatorRevision = strings.Repeat("c", 40)
		}},
		{"wrong key ID", func(d *Demand, c *BenchmarkClaim, p *BenchmarkTrustPolicy) { c.KeyID = "other" }},
		{"bad digest", func(d *Demand, c *BenchmarkClaim, p *BenchmarkTrustPolicy) { c.RawSHA256 = "garbage" }},
		{"blank run", func(d *Demand, c *BenchmarkClaim, p *BenchmarkTrustPolicy) { c.RunID = " " }},
		{"invalid UTF8 run", func(d *Demand, c *BenchmarkClaim, p *BenchmarkTrustPolicy) { c.RunID = "\xff" }},
		{"multiple replicas", func(d *Demand, c *BenchmarkClaim, p *BenchmarkTrustPolicy) { c.ReplicaCount = 2 }},
		{"too few attempts", func(d *Demand, c *BenchmarkClaim, p *BenchmarkTrustPolicy) { p.MinAttempts = 101 }},
		{"too many successes", func(d *Demand, c *BenchmarkClaim, p *BenchmarkTrustPolicy) { c.Successes = 101 }},
		{"low success", func(d *Demand, c *BenchmarkClaim, p *BenchmarkTrustPolicy) { c.Successes, c.TPOTSamples = 90, 90 }},
		{"missing TPOT coverage", func(d *Demand, c *BenchmarkClaim, p *BenchmarkTrustPolicy) { c.TPOTSamples = 98 }},
		{"short observation", func(d *Demand, c *BenchmarkClaim, p *BenchmarkTrustPolicy) { c.ObservedWallSeconds = 1 }},
		{"stale", func(d *Demand, c *BenchmarkClaim, p *BenchmarkTrustPolicy) { c.ObservedAtUnixMillis -= 61000 }},
		{"future", func(d *Demand, c *BenchmarkClaim, p *BenchmarkTrustPolicy) { c.ObservedAtUnixMillis++ }},
		{"expired policy", func(d *Demand, c *BenchmarkClaim, p *BenchmarkTrustPolicy) {
			p.ValidUntil = time.UnixMilli(c.ObservedAtUnixMillis)
		}},
		{"pre-policy observation", func(d *Demand, c *BenchmarkClaim, p *BenchmarkTrustPolicy) {
			c.ObservedAtUnixMillis -= 1000
			p.ValidFrom = time.UnixMilli(c.ObservedAtUnixMillis + 1)
		}},
		{"missing public key", func(d *Demand, c *BenchmarkClaim, p *BenchmarkTrustPolicy) { p.PublicKey = nil }},
		{"disabled quality floor", func(d *Demand, c *BenchmarkClaim, p *BenchmarkTrustPolicy) { p.MinSuccessRate = 0 }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			now, d, c, policy, pool, priv := signedFixture(t)
			tc.change(&d, &c, &policy)
			a, err := AdviseSigned(now, d, c, signClaim(t, c, priv), policy, pool, time.Minute)
			if !errors.Is(err, ErrUnprovenCapacity) || a != (Advice{Outcome: Unproven}) {
				t.Fatalf("invalid signed claim accepted: %+v %v", a, err)
			}
		})
	}
}

func TestAdviseSignedIndependentFreshnessAndDomain(t *testing.T) {
	now, d, c, policy, pool, priv := signedFixture(t)
	c.ObservedAtUnixMillis = now.Add(-50 * time.Second).UnixMilli()
	sig := signClaim(t, c, priv)
	a, err := AdviseSigned(now, d, c, sig, policy, pool, time.Second)
	if err != nil || a.Outcome != EstimatedFit {
		t.Fatalf("independent benchmark age rejected: %+v %v", a, err)
	}
	pool.ObservedAt = now.Add(-2 * time.Second)
	a, err = AdviseSigned(now, d, c, sig, policy, pool, time.Second)
	if !errors.Is(err, ErrUnprovenCapacity) || a != (Advice{Outcome: Unproven}) {
		t.Fatalf("stale pool accepted: %+v %v", a, err)
	}
	pool.ObservedAt = now
	for _, invalid := range [][]byte{nil, ed25519.Sign(priv, []byte("other-domain"))} {
		a, err = AdviseSigned(now, d, c, invalid, policy, pool, time.Second)
		if !errors.Is(err, ErrUnprovenCapacity) || a != (Advice{Outcome: Unproven}) {
			t.Fatalf("wrong domain accepted: %+v %v", a, err)
		}
	}
}
