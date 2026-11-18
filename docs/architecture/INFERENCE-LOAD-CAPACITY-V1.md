# Inference Workload Capacity Advice v0.1

Scope: `internal/capacity` is a **pure advisory calculation**, not a scheduler, scaling actuator, quota admission, provider command, verified Allocation or finance meter. No new API route is mounted in this slice.

`Advise` compares a workload's pool ID, model revision, GPU profile and **workload shape** (e.g. `170k-context-c8`) against current **attested** benchmark evidence, and separately checks a current attested pool observation. Shape must bind input/output token length distribution, batch/concurrency, runtime version, quantization, TP/PP layout and cache warm/cold mode before an adapter marks a benchmark trusted. All evidence fields, including the `Attested` booleans, must be constructed from authenticated internal readers, never copied from user HTTP payloads.

For a homogeneous *unified* service replica, initial sizing uses:

```
replicas = max(ceil(demand_prefill_tokens_per_s / benchmark_prefill_tokens_per_s),
               ceil(demand_decode_tokens_per_s / benchmark_decode_tokens_per_s))
gpus_needed = replicas * benchmark_gpus_per_replica
observed_free_gpus = allocatable_gpus - already_reserved_gpus
```

The ceiling is computed over the exact values represented by the validated binary64 inputs, before converting to native integers. This avoids underestimating replicas when floating-point division rounds a fractional quotient down to an integer, and checks the GPU product against the native integer limit. It can conservatively round up near a decimal boundary; it does not improve the precision of measured rates or replace an uncertainty margin.

This arithmetic is deliberately conservative about evidence (rejects stale, mismatched, future, missing, overcommitted, nonfinite or implausible samples), but is **not a statistical capacity guarantee**. Single-replica performance generally changes with concurrency and batch scheduling; a validated benchmark shape is essential. Unmet p95 TTFT / TPOT gates report `UNMET_BENCHMARK_SLO`. A positive observed capacity comparison reports only `ESTIMATED_FIT`, never Kueue admission.

PD split services require separate prefill/decode benchmark and topology/transfer costs. This v0.1 interface does not treat unified-replica sizing as a complete PD split plan. Next slice: versioned per-phase throughput curves, sustained TTFT/TPOT SLO envelope, NVLink/RDMA topology, queue pressure and worker recovery/scale decisions.

Integration sequence:

1. XPUlink performance evaluator emits measured, deduplicated request-level evidence with exact run/scenario/revision and source provenance.
2. A trusted verifier certifies comparable benchmark profiles; the verified source adapter sets `Benchmark.Attested`. No unverified JSONL goes straight to resource policy.
3. Kueue/Provider/physical inventory adapters provide current pool observations and fencing revision; old or conflicting facts become `UNPROVEN`.
4. CCP creates an ordinary authenticated, idempotent, generation-scoped ComputeWorkload intent. Real admission remains Kueue's, placement Kubernetes', assignment DRA/certified device plugin's; Allocation BOUND and release depend on their separate proofs.
5. Compare advised vs observed GPU-hours, TTFT/TPOT and tokens/s through repeated baseline/candidate runs before applying any automated scaling.

Run focused tests:

```sh
go test -race ./internal/capacity
```

Unverified: production capacity, live GPU benchmark, source attestation, persisted recommendation, Kueue quota, PD topology and Provider release finality.


## Signed benchmark evidence candidate

`AdviseSigned` is a separate offline, non-actuating entry point. It verifies an Ed25519 signature over the domain prefix `ccp/unified-benchmark/v1\\x00` (one NUL byte) followed by Go `json.Marshal(BenchmarkClaim)` bytes. The signed typed claim covers verifier key ID, run ID, raw artifact SHA-256, evaluator commit, pool/model/GPU/shape identity, single-replica GPU count, attempt/success/TPOT coverage, observed wall time, observation end time (Unix milliseconds), rates and p95 metrics. No external caller can set an attestation flag through this entry point. Invalid UTF-8 identities, malformed digests, invalid numeric values, stale/future observations, wrong signatures and mismatched policy scope fail closed.

The trusted policy is supplied independently by infrastructure code, never from the report or HTTP request. It pins the authorized verifier key, evaluator commit and exact benchmark scope, sets a half-open validity window and maximum evidence age, and requires positive minimum attempts, success-rate and observed-wall-time thresholds. The v1 acceptance rule requires every successful request to contribute a TPOT sample. Pool evidence has a separate maximum age and still requires the existing independent attested observation; benchmark signing cannot certify pool capacity. This is in-process Go encoding, not yet a cross-language canonical wire specification. The old `Advise` remains a pure internal calculator with trusted-adapter input and must not be exposed directly to untrusted callers.

The XPUlink offline evaluator currently produces input/output tokens per **whole run wall time**, not isolated PD phase throughput. Its current JSON summary does not identify verified single-replica deployment configuration or TPOT sample count. An independently trusted verifier must validate raw artifact bytes, run grouping, execution configuration, full successful-request TPOT coverage and exact measured values before signing; a summary alone is insufficient. Never reinterpret those wall rates as separate Prefill/Decode device rates. The claim digest is a signed commitment, not proof that this code fetched or checked the raw artifact.

Before live integration: provision independently administered keys/policies and revocation, establish artifact retrieval and digest recomputation and source authentication, prove deployment/configuration binding and cross-language encoding, implement independently authenticated pool observation, add real GPU matched-run and PD topology evidence. No new ingress, storage schema, signer service, admission, scaling, release or finance path is enabled.
