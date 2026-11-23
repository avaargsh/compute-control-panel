package capacity

import (
	"math"
	"testing"
	"time"
)

func TestAdviseRoundsRepresentedDemandUpWithoutFalseFit(t *testing.T) {
	now := time.Date(2026, 11, 18, 0, 0, 0, 0, time.UTC)
	for _, tc := range []struct {
		name         string
		demand, rate float64
		free, want   int
		outcome      Outcome
	}{
		{"quotient rounds down to integer", 0.9, 0.3, 3, 4, InsufficientObservedCapacity},
		{"exact represented quotient", 12, 4, 3, 3, EstimatedFit},
		{"positive quotient underflows", math.SmallestNonzeroFloat64, math.MaxFloat64, 1, 1, EstimatedFit},
	} {
		t.Run(tc.name, func(t *testing.T) {
			d := Demand{PoolID: "p", ModelRevision: "m", GPUProfile: "g", WorkloadShape: "s", PrefillTokensPerSec: tc.demand}
			b := Benchmark{ModelRevision: "m", GPUProfile: "g", WorkloadShape: "s", GPUsPerReplica: 1, PrefillTokensPerSec: tc.rate, DecodeTokensPerSec: 1, P95TTFTms: 1, P95TPOTms: 1, ObservedAt: now, Attested: true}
			p := PoolObservation{PoolID: "p", GPUProfile: "g", AllocatableGPUs: tc.free, ObservedAt: now, Revision: 1, Attested: true}
			a, err := Advise(now, d, b, p, time.Minute)
			if err != nil || a.Replicas != tc.want || a.GPUsNeeded != tc.want || a.Outcome != tc.outcome || a.Admitted {
				t.Fatalf("want %d replicas, outcome %s, not admitted; got %+v err=%v", tc.want, tc.outcome, a, err)
			}
		})
	}
}
