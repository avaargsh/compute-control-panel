package capacity

import (
	"time"

	"github.com/avaargsh/compute-control-panel/internal/capacity/rawbenchmark"
)

// AdviseRawSigned authenticates the claim, independently recomputes its pinned
// unified-run metrics, and rejects any mismatch before returning advice.
// WorkloadShape must be the exact raw scenario identifier in this v1 contract.
// Run provenance and actual deployment configuration still require independent
// authentication; matching metrics alone do not prove those properties.
func AdviseRawSigned(now time.Time, d Demand, raw []byte, c BenchmarkClaim, signature []byte, policy BenchmarkTrustPolicy, pool PoolObservation, maxPoolAge time.Duration) (Advice, error) {
	a, err := AdviseSigned(now, d, c, signature, policy, pool, maxPoolAge)
	if err != nil {
		return a, err
	}
	s, err := rawbenchmark.Verify(raw, rawbenchmark.Expectation{SHA256: c.RawSHA256, RunID: c.RunID, Scenario: c.WorkloadShape, ModelRevision: c.ModelRevision})
	expected := rawbenchmark.Summary{Attempts: c.Attempts, Successes: c.Successes, TPOTSamples: c.TPOTSamples, WallSeconds: c.ObservedWallSeconds, InputTokensPerSecond: c.PrefillTokensPerSec, OutputTokensPerSecond: c.DecodeTokensPerSec, P95TTFTms: c.P95TTFTms, P95TPOTms: c.P95TPOTms}
	if err != nil || s != expected {
		return Advice{Outcome: Unproven}, ErrUnprovenCapacity
	}
	return a, nil
}
