package capacity

import (
	"errors"
	"testing"
	"time"
)

func fixtures(now time.Time) (Demand, Benchmark, PoolObservation) {
	return Demand{PoolID: "pool-1", ModelRevision: "rev-1", GPUProfile: "c500-64g", WorkloadShape: "170k-context-c8", PrefillTokensPerSec: 34000, DecodeTokensPerSec: 2500, MaxTTFTms: 3000, MaxTPOTms: 100},
		Benchmark{ModelRevision: "rev-1", GPUProfile: "c500-64g", WorkloadShape: "170k-context-c8", GPUsPerReplica: 2, PrefillTokensPerSec: 17000, DecodeTokensPerSec: 1000, P95TTFTms: 2500, P95TPOTms: 80, ObservedAt: now, Attested: true},
		PoolObservation{PoolID: "pool-1", GPUProfile: "c500-64g", AllocatableGPUs: 16, ReservedGPUs: 4, ObservedAt: now, Revision: 7, Attested: true}
}

func TestAdviseTracksBottleneckAndFreeCapacity(t *testing.T) {
	now := time.Now()
	d, b, p := fixtures(now)
	a, err := Advise(now, d, b, p, 30*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	if a.Replicas != 3 || a.GPUsNeeded != 6 || a.ObservedFreeGPUs != 12 || a.Outcome != EstimatedFit {
		t.Fatalf("incorrect sizing: %+v", a)
	}
	p.ReservedGPUs = 11
	a, err = Advise(now, d, b, p, 30*time.Second)
	if err != nil || a.Outcome != InsufficientObservedCapacity {
		t.Fatalf("capacity overcommit: %+v %v", a, err)
	}
}

func TestAdviseFailsClosedOnStaleUntrustedAndMismatchedEvidence(t *testing.T) {
	now := time.Now()
	d, b, p := fixtures(now)
	for name, change := range map[string]func(){
		"stale benchmark":         func() { b.ObservedAt = now.Add(-40 * time.Second) },
		"unknown pool":            func() { p.PoolID = "different" },
		"untrusted capacity":      func() { p.Attested = false },
		"stale capacity revision": func() { p.Revision = 0 },
		"untrusted benchmark":     func() { b.Attested = false },
        "different workload shape": func() { b.WorkloadShape = "short-context-c1" },
	} {
		d, b, p = fixtures(now)
		change()
		a, err := Advise(now, d, b, p, 30*time.Second)
		if !errors.Is(err, ErrUnprovenCapacity) || a.Outcome != Unproven {
			t.Fatalf("%s: %+v %v", name, a, err)
		}
	}
}

func TestAdviseRejectsUnmetSLOAndInvalidRates(t *testing.T) {
	now := time.Now()
	d, b, p := fixtures(now)
	b.P95TTFTms = 3100
	a, err := Advise(now, d, b, p, 30*time.Second)
	if err != nil || a.Outcome != UnmetBenchmarkSLO {
		t.Fatalf("bad SLO should block sizing: %+v %v", a, err)
	}
	d, b, p = fixtures(now)
	b.DecodeTokensPerSec = 0
	a, err = Advise(now, d, b, p, 30*time.Second)
	if !errors.Is(err, ErrUnprovenCapacity) || a.Outcome != Unproven {
		t.Fatalf("missing rate must not produce fit: %+v %v", a, err)
	}
}

func TestAdviceNeverClaimsAdmission(t *testing.T) {
	now := time.Now()
	d, b, p := fixtures(now)
	a, _ := Advise(now, d, b, p, 30*time.Second)
	if a.Admitted {
		t.Fatal("advice must never admit or allocate Kueue resources")
	}
}
