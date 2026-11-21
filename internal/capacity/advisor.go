// Package capacity evaluates workload demand against attested, timestamped
// benchmark and pool observations. It never reserves, admits or places GPUs.
package capacity

import (
	"errors"
	"math"
	"math/big"
	"time"
)

var ErrUnprovenCapacity = errors.New("capacity advice requires current, consistent attested evidence")

type Outcome string

const (
	Unproven                     Outcome = "UNPROVEN"
	UnmetBenchmarkSLO            Outcome = "UNMET_BENCHMARK_SLO"
	InsufficientObservedCapacity Outcome = "INSUFFICIENT_OBSERVED_CAPACITY"
	EstimatedFit                 Outcome = "ESTIMATED_FIT"
)

type Demand struct {
	PoolID, ModelRevision, GPUProfile, WorkloadShape string
	PrefillTokensPerSec, DecodeTokensPerSec          float64
	MaxTTFTms, MaxTPOTms                             float64
}

type Benchmark struct {
	ModelRevision, GPUProfile, WorkloadShape string
	GPUsPerReplica                           int
	PrefillTokensPerSec, DecodeTokensPerSec  float64
	P95TTFTms, P95TPOTms                     float64
	ObservedAt                               time.Time
	// Attested is set by the trusted evidence adapter, not by external callers.
	Attested bool
}

type PoolObservation struct {
	PoolID, GPUProfile            string
	AllocatableGPUs, ReservedGPUs int
	ObservedAt                    time.Time
	Revision                      uint64
	// Attested is set by an authenticated provider / Kubernetes evidence adapter.
	Attested bool
}

type Advice struct {
	Outcome                                Outcome
	Replicas, GPUsNeeded, ObservedFreeGPUs int
	// Always false: only Kueue/Provider evidence may establish admission.
	Admitted bool
}

func positiveFinite(f float64) bool    { return f > 0 && !math.IsNaN(f) && !math.IsInf(f, 0) }
func nonnegativeFinite(f float64) bool { return f >= 0 && !math.IsNaN(f) && !math.IsInf(f, 0) }
func fresh(now, sampled time.Time, maxAge time.Duration) bool {
	return !sampled.IsZero() && !sampled.After(now) && now.Sub(sampled) <= maxAge
}

// Advise returns capacity *estimates* from a comparable, trustworthy benchmark.
// Observed availability is not quota admission, DRA assignment or a resource
// lease. Its only allowed operational use is as input to a separately fenced
// Workload request that is still subject to Kueue and Provider checks.
func Advise(now time.Time, d Demand, b Benchmark, pool PoolObservation, maxAge time.Duration) (Advice, error) {
	unproven := Advice{Outcome: Unproven}
	if now.IsZero() || maxAge <= 0 || d.PoolID == "" || d.ModelRevision == "" || d.GPUProfile == "" || d.WorkloadShape == "" ||
		!nonnegativeFinite(d.PrefillTokensPerSec) || !nonnegativeFinite(d.DecodeTokensPerSec) || (d.PrefillTokensPerSec == 0 && d.DecodeTokensPerSec == 0) ||
		!nonnegativeFinite(d.MaxTTFTms) || !nonnegativeFinite(d.MaxTPOTms) ||
		b.ModelRevision != d.ModelRevision || b.GPUProfile != d.GPUProfile || b.WorkloadShape != d.WorkloadShape || pool.PoolID != d.PoolID || pool.GPUProfile != d.GPUProfile ||
		!b.Attested || !pool.Attested || pool.Revision == 0 || !fresh(now, b.ObservedAt, maxAge) || !fresh(now, pool.ObservedAt, maxAge) ||
		b.GPUsPerReplica <= 0 || !positiveFinite(b.PrefillTokensPerSec) || !positiveFinite(b.DecodeTokensPerSec) ||
		!positiveFinite(b.P95TTFTms) || !positiveFinite(b.P95TPOTms) ||
		pool.AllocatableGPUs < 0 || pool.ReservedGPUs < 0 || pool.ReservedGPUs > pool.AllocatableGPUs {
		return unproven, ErrUnprovenCapacity
	}
	if (d.MaxTTFTms > 0 && b.P95TTFTms > d.MaxTTFTms) || (d.MaxTPOTms > 0 && b.P95TPOTms > d.MaxTPOTms) {
		return Advice{Outcome: UnmetBenchmarkSLO}, nil
	}
	prefill := ceilRepresentedRatio(d.PrefillTokensPerSec, b.PrefillTokensPerSec)
	decode := ceilRepresentedRatio(d.DecodeTokensPerSec, b.DecodeTokensPerSec)
	if decode.Cmp(prefill) > 0 {
		prefill = decode
	}
	// Check the GPU product against the native int limit before conversion.
	maxReplicas := int(^uint(0)>>1) / b.GPUsPerReplica
	if prefill.Sign() <= 0 || prefill.Cmp(big.NewInt(int64(maxReplicas))) > 0 {
		return unproven, ErrUnprovenCapacity
	}
	replicas := int(prefill.Int64())
	needed := replicas * b.GPUsPerReplica
	free := pool.AllocatableGPUs - pool.ReservedGPUs
	result := Advice{Outcome: EstimatedFit, Replicas: replicas, GPUsNeeded: needed, ObservedFreeGPUs: free}
	if needed > free {
		result.Outcome = InsufficientObservedCapacity
	}
	return result, nil
}

// ceilRepresentedRatio rounds the exact ratio of validated binary64 inputs up.
// Floating-point division can round a positive fraction down to an integer
// before math.Ceil sees it, underestimating required replicas. This is sizing
// arithmetic over measured inputs, not a guarantee of benchmark precision.
func ceilRepresentedRatio(demand, rate float64) *big.Int {
	ratio := new(big.Rat).Quo(new(big.Rat).SetFloat64(demand), new(big.Rat).SetFloat64(rate))
	var quotient, remainder big.Int
	quotient.QuoRem(ratio.Num(), ratio.Denom(), &remainder)
	if remainder.Sign() > 0 {
		quotient.Add(&quotient, big.NewInt(1))
	}
	return &quotient
}
