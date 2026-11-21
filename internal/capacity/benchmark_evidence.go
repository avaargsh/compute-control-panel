package capacity

import (
	"crypto/ed25519"
	"encoding/hex"
	"encoding/json"
	"math/big"
	"strings"
	"time"
	"unicode/utf8"
)

// BenchmarkClaim is a verifier-signed single-replica unified-service report.
// It contains no caller-controlled attestation flag. The verifier must check
// the raw artifact, execution configuration and sample coverage before signing.
type BenchmarkClaim struct {
	KeyID, RunID, RawSHA256, EvaluatorRevision       string
	PoolID, ModelRevision, GPUProfile, WorkloadShape string
	ReplicaCount, GPUsPerReplica                     int
	Attempts, Successes, TPOTSamples                 int64
	ObservedWallSeconds                              float64
	ObservedAtUnixMillis                             int64
	PrefillTokensPerSec, DecodeTokensPerSec          float64
	P95TTFTms, P95TPOTms                             float64
}

// BenchmarkTrustPolicy must originate from independent trusted configuration,
// never the signed report or its caller. The key is scoped to one benchmark
// identity and evaluator revision, with a half-open validity interval.
type BenchmarkTrustPolicy struct {
	KeyID                                            string
	PublicKey                                        ed25519.PublicKey
	EvaluatorRevision                                string
	PoolID, ModelRevision, GPUProfile, WorkloadShape string
	ValidFrom, ValidUntil                            time.Time
	MaxAge                                           time.Duration
	MinAttempts                                      int64
	MinSuccessRate, MinWallSeconds                   float64
}

// BenchmarkSigningBytes defines the v1 Go encoding. A signature over a
// different message domain, claim or encoding must not verify here.
func BenchmarkSigningBytes(c BenchmarkClaim) ([]byte, error) {
	payload, err := json.Marshal(c)
	if err != nil {
		return nil, err
	}
	return append([]byte("ccp/unified-benchmark/v1\x00"), payload...), nil
}

func lowercaseHex(s string, size int) bool {
	if len(s) != 2*size || s != strings.ToLower(s) {
		return false
	}
	_, err := hex.DecodeString(s)
	return err == nil
}

func validBenchmarkID(s string) bool {
	return len(s) > 0 && len(s) <= 256 && utf8.ValidString(s) && strings.TrimSpace(s) == s
}

// AdviseSigned verifies the benchmark evidence at evaluation time. It delegates
// pure sizing to Advise only after verification. The pool remains an independent
// trusted-adapter input; this function never admits, reserves or releases GPUs.
func AdviseSigned(now time.Time, d Demand, c BenchmarkClaim, signature []byte, policy BenchmarkTrustPolicy, pool PoolObservation, maxPoolAge time.Duration) (Advice, error) {
	unproven := Advice{Outcome: Unproven}
	observed := time.UnixMilli(c.ObservedAtUnixMillis)
	if now.IsZero() || policy.ValidFrom.IsZero() || !policy.ValidFrom.Before(policy.ValidUntil) ||
		now.Before(policy.ValidFrom) || !now.Before(policy.ValidUntil) ||
		policy.MaxAge <= 0 || c.ObservedAtUnixMillis <= 0 || observed.Before(policy.ValidFrom) || !fresh(now, observed, policy.MaxAge) ||
		!validBenchmarkID(c.KeyID) || c.KeyID != policy.KeyID || len(policy.PublicKey) != ed25519.PublicKeySize || len(signature) != ed25519.SignatureSize ||
		!validBenchmarkID(c.RunID) || !lowercaseHex(c.RawSHA256, 32) || !lowercaseHex(c.EvaluatorRevision, 20) || c.EvaluatorRevision != policy.EvaluatorRevision ||
		!validBenchmarkID(c.PoolID) || c.PoolID != policy.PoolID || c.PoolID != d.PoolID ||
		!validBenchmarkID(c.ModelRevision) || c.ModelRevision != policy.ModelRevision || c.ModelRevision != d.ModelRevision ||
		!validBenchmarkID(c.GPUProfile) || c.GPUProfile != policy.GPUProfile || c.GPUProfile != d.GPUProfile ||
		!validBenchmarkID(c.WorkloadShape) || c.WorkloadShape != policy.WorkloadShape || c.WorkloadShape != d.WorkloadShape ||
		c.ReplicaCount != 1 || c.GPUsPerReplica <= 0 || policy.MinAttempts <= 0 || c.Attempts < policy.MinAttempts ||
		c.Successes <= 0 || c.Successes > c.Attempts || c.TPOTSamples != c.Successes ||
		!positiveFinite(policy.MinSuccessRate) || policy.MinSuccessRate > 1 || !positiveFinite(policy.MinWallSeconds) ||
		!positiveFinite(c.ObservedWallSeconds) || c.ObservedWallSeconds < policy.MinWallSeconds ||
		!positiveFinite(c.PrefillTokensPerSec) || !positiveFinite(c.DecodeTokensPerSec) || !positiveFinite(c.P95TTFTms) || !positiveFinite(c.P95TPOTms) {
		return unproven, ErrUnprovenCapacity
	}
	if new(big.Rat).SetFrac64(c.Successes, c.Attempts).Cmp(new(big.Rat).SetFloat64(policy.MinSuccessRate)) < 0 {
		return unproven, ErrUnprovenCapacity
	}
	payload, err := BenchmarkSigningBytes(c)
	if err != nil || !ed25519.Verify(policy.PublicKey, payload, signature) || maxPoolAge <= 0 || !fresh(now, pool.ObservedAt, maxPoolAge) {
		return unproven, ErrUnprovenCapacity
	}
	b := Benchmark{ModelRevision: c.ModelRevision, GPUProfile: c.GPUProfile, WorkloadShape: c.WorkloadShape, GPUsPerReplica: c.GPUsPerReplica, PrefillTokensPerSec: c.PrefillTokensPerSec, DecodeTokensPerSec: c.DecodeTokensPerSec, P95TTFTms: c.P95TTFTms, P95TPOTms: c.P95TPOTms, ObservedAt: observed, Attested: true}
	// Each source was independently checked against its own age limit above.
	maxAge := policy.MaxAge
	if maxPoolAge > maxAge {
		maxAge = maxPoolAge
	}
	return Advise(now, d, b, pool, maxAge)
}
