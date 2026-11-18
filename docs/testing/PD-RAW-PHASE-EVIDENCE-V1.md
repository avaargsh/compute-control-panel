# M5.4 paired PD raw phase evidence v1

Status: offline implementation candidate, non-actuating. This is a new narrow trace contract, not a claim that XPUlink/SGLang/vLLM emits this format today. Production collectors, authenticated manifest/artifact retrieval and benchmark acceptance are still open.

## Input contract

- First physical JSONL record is the complete Manifest; subsequent records are complete Attempt objects. Schema version pd-raw/v1. Exact field names are specified by rawphase.Manifest / rawphase.Attempt JSON tags; all fields required; unknown fields, duplicate keys, null, trailing objects, malformed UTF-8, missing attempts and oversized lines/artifacts fail closed.
- Independently authenticated manifest pins tenant/project/pool/model/GPU/shape/fabric, exact evaluator commit, deployment SHA-256, run/worker identities, GPU counts, measurement start/end Unix milliseconds, expected attempt count and a conservative tested wire-byte envelope per request. The deployment digest must identify actual model/runtime/image, TP, cache regime, worker and clock configuration rather than a user-entered label. Clock domain/accuracy must be externally established.
- RawEvidence separately pins exact artifact SHA-256 and minimum attempts/window policy. Neither the raw file nor the signed claim may provide its own trusted expectation. Only an independently authenticated source should construct this input.
- Each attempt binds unique request and transfer identities to the exact two distinct workers, phase counters and causal timestamps. The v1 contract requires successful complete attempts, positive Prefill tokens, at least two Decode tokens, a positive complete KV transfer and all output tokens from Decode. Failed attempts, partial traces and retries with ambiguous transfer attribution reject the entire run. Never omit failed requests or fabricate timestamps to fit this narrow contract.
- Prefill tokens are the actual defined phase work units, not blindly copied input tokens; Decode tokens are measured generated tokens. Workload shape must define the matching cache treatment and rate semantics. KVBytes includes all attributed wire bytes and must not exceed the independently pinned conservative protocol/retry envelope; the observed maximum does not establish an upper bound by itself.

## Verification and estimates

Verify artifact size (16 MiB), physical lines (16 KiB), exact digest, manifest equality, at most 100,000 attempts, bounded positive measurement window (up to 24h), complete attempt count and independent minimum floors. Reject duplicate request/transfer IDs, worker drift, timestamp order drift, KV before Prefill completion, Decode before KV completion and out-of-window counters. Count each phase's measured tokens over the same pinned whole paired-run wall window including idle time; never sum overlapping request durations or reinterpret unified gateway throughput as phase-specific telemetry. Derive nearest-rank p95 TTFT (first output minus request start), TPOT ((last output minus first output)/(Decode tokens minus 1)), and transfer duration from raw traces.

PlanRaw requires both the existing independently signed Profile/Policy and exact recomputation of all those metrics, run end, scope, GPU counts and KV limit before returning any estimate. ProposeRaw uses a distinct domain-separated content digest that binds the independent raw verification requirements and retains the prior generation conflict key. RevalidateRawProposal verifies the entire candidate again; it is not concurrent database CAS.

## Integration gates

- The legacy Plan/Propose/Advise remain internal offline calculations only. Future durable intent acceptance must use raw-verified and independently source-authenticated evidence, not just caller-supplied attestation flags or a signed summary.
- Real PD collector must correlate runtime phase events and independent request population; clock skew, transfer retries and cross-worker clock calibration are externally verified before v1 acceptance.
- Add independently authenticated immutable artifact retrieval, manifest registry and policy/revocation, exact source adapter and real GPU evidence. This contract doesn't prove hardware allocation, topology, paired admission, scaling, release or finance finality.
- Joint measurement counters over one paired window are conservative measured throughput, not a guarantee of each phase's maximum independent throughput or linear performance scaling. Require matched repeated runs, multi-replica SLO and saturation evidence before automation.
