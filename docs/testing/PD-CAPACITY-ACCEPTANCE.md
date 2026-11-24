# M5.3–M5.7 inference capacity acceptance contract

Status: offline implementation candidate; integration gates OPEN. This contract is not a test result or deployment authorization. Preserve the existing single Compute CP authority and the Kubernetes/Kueue boundary.

## Evidence paths

- Unified: independently collected request JSONL -> independently pinned artifact -> rawbenchmark.Verify -> AdviseRawSigned -> advisory result.
- PD: independently verified jointly executed PD profile -> domain-separated Ed25519 signature -> pd.Plan -> pd.Propose -> pd.RevalidateProposal. The PD raw-phase verifier and authorized policy/observation adapters are not implemented here.
- Existing Advise / AdviseSigned remain internal, non-actuating primitives. No public route or automated acceptance is mounted.

M5.3 recomputes raw SHA-256, one run/scenario/model, integer token counts, unique request attempts, full positive TPOT coverage, whole-window goodput and nearest-rank p95. The scenario identifier must equal WorkloadShape in the current wire contract. Unknown fields, duplicate JSON fields, fractional counts, malformed records, or inconsistent metrics fail closed. Input limits: 16 MiB artifact, 16 KiB row, 100,000 rows. Successful attempts require at least two output tokens. Full deployment configuration, immutable storage retrieval, independent signer authorization, evidence timestamp and source-clock proof remain external gates. Whole-window input/output rates are never independent PD phase rates.

M5.4 is a homogeneous same-pool/fabric candidate for a paired one-replica-per-phase profile. All profile values must be verified by an independently authorized signer. Pin a trusted evaluator revision, artifact digest, phase counters/sample windows, deployment shape, GPU/TP layout, cache regime, success coverage and measured end-to-end/transfer p95 before real use. No linear scaling or SLO guarantee.

M5.5 requires reconciled, sorted, unique and disjoint compatible free GPU islands; an authenticated reader must prove actual device UID membership, connectivity and no double counting. The bounded packing check gives a conservative witness for whole replicas, not a scheduler or exhaustive search. Shared available fabric bandwidth must be a conservative measured bottleneck budget, not the sum of NIC link speeds. Use the profile's maximum transferred bytes per request, including protocol/retry overhead under the documented test envelope. The model does not yet prove per-path contention, multi-hop routing, heterogeneous pools or burst latency. Its result is an estimate only.

M5.6 Proposal is a full target from demand compared against observed free capacity (greenfield/coexistence sizing, not an incremental in-place resize). GenerationKey is stable for the same tenant/project/workload/generation; IntentID is a content digest. RevalidateProposal is pure; it cannot supply concurrent CAS. Durable acceptance must authenticate the service and owner, bind current authorization/policy and trusted load/pool/topology revisions, persist the generation key with the normalized digest, and perform generation/revision CAS atomically. Exact replay returns the existing accepted identity; conflicting digest under the same generation returns conflict. Never turn a stale proposal into a fresh automatic action. Keep budget, scale hysteresis, cooldown, queue pressure and rollback policy external until verified.

## M5.7 gates

The current v0.1 provider path owns one top-level suspended Kubernetes Job and observes Kueue's controller-owned Workload. It is not yet a heterogeneous PD workload adapter. Before implementation, agree on one versioned controller integration supporting grouped Prefill/Decode pod sets and atomically admitting the pair (evaluate a supported JobSet or equivalent integration in a separately reviewed ADR). Do not create two unrelated Jobs and label partial admission a successful PD deployment; do not manually create Kueue-native Workloads alongside the integration. Route acceptance through the existing Repository.AcceptWorkload transaction only after identity/authorization, quota mapping, durable intent journaling, provider capability checks and the new execution contract pass. Advised capacity never bypasses Kueue or creates a second resource authority.

| Gate | Exercise | Required evidence |
|---|---|---|
| M5.7-A | Disposable cluster, two phases, excess and insufficient quota | Exact source SHA, cluster/controller versions, admitted/rejected native Workload UID/RV, per-phase pod-set counts, resourceUsage and quota reservation. No partial-admission READY. |
| M5.7-B | Two tenants contend; stale revision, duplicate request, conflicting generation digest | One authoritative accepted generation and operation, no second allocation, explicit requeue/rejection, durable DB replay or conflict, no cross-tenant ownership. |
| M5.7-C | Lost create ACK; crash after accept; restart and inspect | Durable operation journal, UNKNOWN resolution through exact resource UID and generation; no blind retry or duplicate resource. |
| M5.7-D | Physical GPU allocation across topology domains | Authorized dedicated GPU environment, discovered provider/driver/firmware and device UIDs, UID/RV-pinned claims, unique device assignments, node binding, authenticated NodePrepare evidence, PD runtime readiness. Simulated DRA is not accepted as physical proof. |
| M5.7-E | Paired PD load test, link saturation, topology fragmentation | Raw phase/transfer records and end-to-end TTFT/TPOT, measured goodput, success coverage, exact image/model/config digest, actual GPU-hours, baseline and repeated candidate runs; validate predicted versus observed capacity. |
| M5.7-F | Kill one worker, lose stop ACK, requeue, teardown and reclaim | Exact parent and child UIDs, attempt/transfer fences, no decode without confirmed KV, independent Pod/Claim absence or unallocated evidence, authenticated driver NodeUnprepare and provider absence/release evidence before Allocation RELEASED; no reuse on UNKNOWN. |
| M5.7-G | Multi-replica burst, recovery, and replay | Independent verifier assessment, configured SLO/quality floor, stable recovery, no double claim or financial FINAL promotion. |

For disposable tests use the repository's existing guarded smoke path as a baseline, not as PD-specific evidence:

    CCP_DISPOSABLE_KIND_SMOKE=1 bash scripts/kueue_kind_smoke.sh

Run only in a dedicated environment with explicit authorization. The existing single-Job simulated DRA script is not a substitute for implementing the paired workload adapter or passing A–G. Production GPU actions require approved non-production allocation and safe cleanup scope; never run against an arbitrary kubeconfig. Capture the exact tested commit, external resource IDs, versions, timestamps, exit codes and externally corroborated evidence. Unknown evidence remains UNKNOWN, not PASS. Finance FINAL is a separate gate.

## Current candidate verification

    go test -race -count=1 ./internal/capacity/...
    go vet ./internal/capacity/...

Full repository Go/PG/CI and relevant disposable/real GPU checks remain independently required. No production rollout, automatic scaling, admission, physical allocation, release or accounting is authorized by this document.
