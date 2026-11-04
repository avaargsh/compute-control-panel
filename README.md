# Compute Control Panel

Go-based Compute Control Plane for self-hosted, Kubernetes-first heterogeneous compute.

## Product boundary

Owns compute provider registrations, pools, workload intent, allocation references, quota bindings, reconciliation, normalized compute evidence and workload-attributed economics. Kubernetes, Kueue, device plugins/DRA and provider APIs remain authoritative for actual scheduling, admission, placement and resource realization.

## Product intent

Evolve the control plane from resource lifecycle management into an evidence-backed **Compute Economics Control Plane**:

**Compute Control Plane = Resource Control + Workload Control + Observability + Compute Economics.**

The product should answer:

- What capacity is registered, allocatable, admitted, placed, active, idle or unavailable?
- Which Workload, Project and Tenant consumed each accepted allocation, and what evidence supports that attribution?
- What is the attributable direct compute cost for a Workload over a proven usage interval?
- How do comparable Provider / Pool / GPU-profile cost and effective throughput differ?
- Which capacity is reserved but unallocated, underused, constrained or economically unattractive?
- When commercial evidence exists, what revenue and gross margin does a capacity product generate?

This is an additive domain capability. It must not introduce a custom scheduler or weaken Kubernetes, Kueue, DRA or Provider authority boundaries.

## Economic metric vocabulary

Keep these facts separate; do not collapse them into one generic GPU-utilization number:

| Measure | Meaning | Evidence boundary |
|---|---|---|
| Allocated GPU time | Capacity assigned to an accepted Allocation | Durable Allocation lifecycle |
| Active GPU time | Allocated time weighted by a named device/workload activity signal | Timestamped telemetry with source and adapter provenance |
| Effective workload output | Useful workload-specific output, such as tokens/s or samples/s | Workload-linked benchmark or execution evidence |
| Attributable direct cost | Usage inside the intersection of Provider cost-liability and proven Workload-attribution windows, priced by an effective-dated Provider rate | Trusted Provider, Kueue, Kubernetes and device-assignment evidence |
| Reserved / unallocated cost | Provider liability outside proven Workload attribution | Provider lease / reservation evidence |
| Revenue and gross margin | Customer charge less attributable cost under an explicit commercial contract | Billable usage, price book, invoice/settlement and correction evidence |

The initial economics scope is intentionally narrow: whole-GPU quantities and fixed allocated-GPU-hour Provider procurement rates. Fractional GPU/MIG pricing, tiered pricing, minimum commitments, customer billing and revenue recognition require separate versioned contracts and acceptance tests.

Financial values must use exact rational/decimal arithmetic and an explicit versioned currency metadata snapshot. Incomplete or contradictory evidence must fail closed; no partial ledger posting is allowed. Corrections and reversals are append-only and linked to the original entry.

## Authority boundaries

- **Compute Control Plane:** ComputeProvider, Pool, Profile, Workload, Allocation, ComputeOperation, provider-neutral reconciliation, trusted-evidence references, UsageInterval and WorkloadCostEntry.
- **Provider:** provider-native resource identity, lease/cost-liability facts, operation outcome and provider-specific capabilities.
- **Kueue:** queue admission and quota; admission is not Pod placement or device ownership.
- **Kubernetes scheduler:** Pod placement and binding.
- **DRA / device plugin / certified device adapter:** device claim and assignment evidence.
- **Telemetry source:** timestamped device/workload observations; source timestamps are trusted only for explicitly allowlisted source/adapter versions.
- **Agent Control Plane / Agent Runtime:** Agent, Session and execution identity. Compute stores only an authenticated external execution reference when a cross-plane attribution contract is ratified; it does not mint or redefine Agent Run IDs.
- **EngageGuard:** protected-action Permit and action-level operation evidence.

A provider ACK, a PENDING Allocation, a Pod resource request or a queue admission alone is not proof of GPU allocation, workload attribution, readiness or release.

## Architecture principles

- Go API and controller/reconciler processes.
- PostgreSQL as the control-plane domain store.
- Kubernetes + Kueue as the initial workload admission and execution substrate.
- GPU support is an adapter capability, not a custom scheduler.
- Provider operations are idempotent, durable and reconciled; ambiguous outcomes remain UNKNOWN.
- A provider acknowledgement is not proof of allocation, readiness or release.
- Tenant and Project identity come from authenticated service identity and authorization; client payloads cannot select trusted Provider or tenant identity.
- Cost is derived from immutable evidence and effective-dated rates, not directly from desired state or unverified metrics.

## Current repository status — 2026-11-04

The `main` branch contains the project charter and governance baseline. Implementation work remains in Draft PR branches and is not promoted to `main` until its dependency, exact-head CI and integration gates are independently satisfied.

### P0 — durable Workload control and trust boundary

- [PR #1 — Compute architecture and domain boundary](https://github.com/avaargsh/compute-control-panel/pull/1) (Draft).
- [PR #2 — Go/PostgreSQL foundation and Provider operation invariants](https://github.com/avaargsh/compute-control-panel/pull/2) (Draft).
- [PR #11 — atomic Workload + initial Allocation + CREATE Operation acceptance](https://github.com/avaargsh/compute-control-panel/pull/11) (Draft; one PostgreSQL transaction, durable intent only).
- [PR #15 — authenticated Workload acceptance API](https://github.com/avaargsh/compute-control-panel/pull/15) (Draft; production CA, identity provisioning, Guard/Project policy, tenant RLS and Provider selector wiring remain gates).
- [Issue #3 — P0 integration and release gate](https://github.com/avaargsh/compute-control-panel/issues/3) is the authoritative acceptance tracker.

### Economics — contract, evidence and calculation slices

- [Issue #8 — Compute Economics / GPU FinOps product direction](https://github.com/avaargsh/compute-control-panel/issues/8).
- [PR #9 — E0 economics domain, schemas and golden fixtures](https://github.com/avaargsh/compute-control-panel/pull/9) (Draft).
- [PR #16 — E1 trusted Allocation usage evidence contract](https://github.com/avaargsh/compute-control-panel/pull/16) (Draft; design contract).
- [PR #17 — provider-independent derivation of costable GPU UsageInterval](https://github.com/avaargsh/compute-control-panel/pull/17) (Draft; deterministic evidence validation, no live collectors or durable evidence ledger).
- [PR #19 — clean-main-based deterministic Workload cost calculator](https://github.com/avaargsh/compute-control-panel/pull/19) (Draft; calculation only, no production metering or posting).

These are independent proof layers, not a claim that end-to-end billing or FinOps is ready. The calculator can price only a complete, validated UsageInterval. Production cost posting remains gated on trusted evidence collection, durable evidence-set persistence, append-only WorkloadCostEntry storage, replay/reversal semantics and exact-head integration tests.

## Delivery sequence

1. **P0 — durable resource control:** ratify domain ownership; complete authenticated Workload acceptance, idempotency, Outbox/Inbox and UNKNOWN reconciliation.
2. **P0 — trusted allocation evidence:** implement a real Provider capability profile and authenticated Provider inspect; collect exact Kueue admission/release, Kubernetes Job/Pod identity and DRA or certified device assignment.
3. **P1 — evidence persistence:** persist immutable evidence sets with provenance, digest, source-effective/observed timestamps and policy version; support duplicate delivery, conflict quarantine and replay.
4. **P1 — cost ledger:** persist WorkloadCostEntry from complete UsageIntervals and rate cards; make correction/reversal append-only and replay-safe; reconcile ledger totals against source evidence.
5. **P1 — utilization and effectiveness:** add normalized telemetry with explicit source/adapter trust, distinguish allocated from active time, and attach workload-specific throughput.
6. **P2 — commercial economics:** add sell rates, billable usage, revenue, margin and reserved-capacity economics only after commercial evidence and product contracts exist.
7. **P2 — cross-plane attribution:** correlate authenticated Agent execution references with Compute Workloads without transferring Run identity or workflow authority into this repository.

## Non-negotiable invariants

1. No custom scheduler; preserve Kubernetes and Kueue authority boundaries.
2. UNKNOWN must be inspected/reconciled before retry; never blindly retry an ambiguous Provider side effect.
3. Stale generations cannot overwrite current Workload state.
4. DELETE finality requires absence of the exact Provider resource UID and evidence for applicable reservation/quota release.
5. FakeProvider is test-only until an authenticated production Provider adapter exists.
6. A single PostgreSQL transaction does not provide distributed exactly-once delivery.
7. PENDING, UNKNOWN, RELEASING, queue admission, Pod placement and device allocation remain separate facts.
8. No Workload cost posting without complete, tenant-scoped, digest-bound evidence and full rate coverage.
9. Provider liability after Workload attribution ends belongs to reserved/unallocated capacity accounting; do not silently discard or charge it to an unrelated Workload.
10. Revenue and gross margin require explicit commercial source evidence; procurement cost alone is not revenue evidence.

## Retired repository

`gpu-compute-platform` is retired as an active delivery repository. It is historical migration/reference input only; all new Compute implementation, provider adapters, economics and release evidence belong here. The consolidation record is maintained in the [GPU Compute Platform migration matrix](https://github.com/avaargsh/compute-control-panel/blob/feat/p0-contract-artifacts/docs/migration/GPU-COMPUTE-PLATFORM-CONSOLIDATION.md).
