# Compute Control Panel

Go-based Compute Control Plane for self-hosted, Kubernetes-first heterogeneous compute.

## Interview highlights

- **Evidence-first Compute Economics**: Separates allocated GPU time, active time, attributable direct cost, reserved/unallocated capacity, and revenue/margin. Costing is derived from immutable, provenance-tracked evidence with exact decimal arithmetic; incomplete or contradictory evidence fails closed.
- **Strict authority boundaries**: Does not implement a custom scheduler. The control plane owns intent, reconciliation, allocation references, and economics; Kubernetes, Kueue, DRA/device plugins, and Providers remain authoritative for admission, placement, and resource realization.
- **Production-oriented foundations in main**: Atomic Workload acceptance with Outbox, SPIFFE identity, independent Provider audit writer role, generation-fenced state machines, UNKNOWN reconciliation invariants, and migration lineage guards.
- **Current honest status**: Go/PostgreSQL core, authenticated handlers, E1 policy/approval persistence, and deterministic cost calculator are implemented and CI-passing. Live Provider/Kueue/DRA evidence and production metering remain open (issues #3, #32, #33, #39).

## Product boundary

Owns compute provider registrations, pools, workload intent, allocation references, quota bindings, reconciliation, normalized compute evidence and workload-attributed economics. Kubernetes, Kueue, device plugins/DRA and provider APIs remain authoritative for actual scheduling, admission, placement and resource realization.

## Product intent

Evolve into an evidence-backed **Compute Economics Control Plane**:

**Compute Control Plane = Resource Control + Workload Control + Observability + Compute Economics.**

It answers:

- What capacity is registered, allocatable, admitted, placed, active, idle or unavailable?
- Which Workload/Project/Tenant consumed each accepted allocation, and what evidence supports that?
- What is the attributable direct compute cost for a Workload over a proven usage interval?
- How do comparable Provider/Pool/GPU-profile cost and effective throughput differ?
- Which capacity is reserved but unallocated, underused or economically unattractive?

This is additive. It must not introduce a custom scheduler or weaken Kubernetes, Kueue, DRA or Provider authority boundaries.

## Economic metric vocabulary

Keep these facts separate (do not collapse into one generic utilization number):

| Measure | Meaning | Evidence boundary |
|---|---|---|
| Allocated GPU time | Capacity assigned to an accepted Allocation | Durable Allocation lifecycle |
| Active GPU time | Allocated time weighted by a named activity signal | Timestamped telemetry with source/adapter provenance |
| Effective workload output | Useful output (e.g. tokens/s) | Workload-linked benchmark or execution evidence |
| Attributable direct cost | Usage inside Provider liability ∩ proven Workload attribution, priced by effective-dated rate | Trusted Provider + Kueue + K8s + device evidence |
| Reserved / unallocated cost | Provider liability outside proven Workload attribution | Provider lease / reservation evidence |
| Revenue and gross margin | Customer charge less attributable cost | Explicit commercial contract evidence |

Initial scope is intentionally narrow: whole-GPU quantities and fixed allocated-GPU-hour Provider rates. Fractional/MIG, tiered pricing, billing and revenue recognition require separate versioned contracts. Financial values use exact decimal arithmetic; incomplete evidence fails closed; corrections are append-only.

## Authority boundaries

- **Compute Control Plane**: ComputeProvider, Pool, Profile, Workload, Allocation, ComputeOperation, reconciliation, trusted-evidence references, UsageInterval, WorkloadCostEntry.
- **Provider**: native resource identity, lease/cost-liability, operation outcome.
- **Kueue**: queue admission and quota (not Pod placement or device ownership).
- **Kubernetes scheduler**: Pod placement and binding.
- **DRA / device plugin**: device claim and assignment evidence.
- **Agent Control Plane / Runtime**: Agent/Session/Run identity (Compute only stores authenticated external references).
- **EngageGuard**: protected-action Permit and operation evidence.

A provider ACK, PENDING Allocation, Pod request or queue admission alone is not proof of GPU allocation, attribution, readiness or release.

## Architecture principles

- Go API + controller/reconciler; PostgreSQL as domain store.
- Kubernetes + Kueue as initial admission/execution substrate.
- GPU support is an adapter, not a custom scheduler.
- Provider operations are idempotent, durable and reconciled; ambiguous outcomes stay UNKNOWN.
- Tenant/Project identity comes from authenticated service identity, never from client payload.
- Cost is derived from immutable evidence and effective-dated rates.

## Current status (2026-11-10)

Implemented and CI-passing on main: Go/PostgreSQL foundation, atomic Workload acceptance with Outbox, authenticated Workload/Provider handlers, E1 policy/independent-approval persistence, deterministic cost calculator, migration lineage guards.

Not yet: live Provider/Kueue/DRA admission and release evidence, production tenant isolation, authenticated evidence-source acceptance, or production metering. Default service mounts only health/metadata routes; Workload and Provider management handlers remain unmounted.

Key open gates:

- [#3 P0 integration gate](https://github.com/avaargsh/compute-control-panel/issues/3)
- [#32 Provider readiness / reclamation](https://github.com/avaargsh/compute-control-panel/issues/32)
- [#33 Kubernetes/Kueue/DRA evidence](https://github.com/avaargsh/compute-control-panel/issues/33)
- [#39 E1 trusted evidence policy and source authentication](https://github.com/avaargsh/compute-control-panel/issues/39)

Detailed historical PR lists live in the commit history and linked issues. Older issue/PR descriptions are snapshots, not live status.

## Delivery sequence

1. **P0** — durable resource control (authenticated acceptance, idempotency, Outbox/Inbox, UNKNOWN reconciliation).
2. **P0** — trusted allocation evidence (real Provider inspect + exact Kueue/K8s/DRA evidence).
3. **P1** — immutable evidence persistence with provenance and replay safety.
4. **P1** — cost ledger (WorkloadCostEntry from complete UsageIntervals; append-only corrections).
5. **P1** — utilization and effectiveness (normalized telemetry with explicit trust).
6. **P2** — commercial economics (sell rates, revenue, margin) only after commercial evidence exists.
7. **P2** — cross-plane attribution with Agent Runtime (without transferring Run identity).

## Non-negotiable invariants

1. No custom scheduler; preserve Kubernetes and Kueue authority boundaries.
2. UNKNOWN must be inspected/reconciled before retry; never blindly retry an ambiguous side effect.
3. Stale generations cannot overwrite current Workload state.
4. DELETE finality requires absence of the exact Provider resource UID plus applicable release evidence.
5. FakeProvider is test-only until a real authenticated Provider adapter exists.
6. A single PostgreSQL transaction does not provide distributed exactly-once delivery.
7. PENDING, UNKNOWN, RELEASING, queue admission, Pod placement and device allocation remain separate facts.
8. No Workload cost posting without complete, tenant-scoped, digest-bound evidence and full rate coverage.
9. Provider liability after Workload attribution ends belongs to reserved/unallocated accounting.
10. Revenue and gross margin require explicit commercial source evidence.

## Retired repository

`gpu-compute-platform` is retired. All new Compute implementation, adapters, economics and release evidence belong here. See [migration matrix](docs/migration/GPU-COMPUTE-PLATFORM-CONSOLIDATION.md).

## Documentation

Start with the [documentation index](docs/README.md) for a map of architecture, contracts, operations and testing docs.

Key entry points:

- [Architecture baseline](docs/architecture/BASELINE.md)
- [Domain ownership](docs/architecture/DOMAIN-OWNERSHIP.md)
- [Cross-repository ownership ADR](docs/adr/ADR-001-CROSS-REPO-OWNERSHIP.md)
- [OpenAPI + schemas](docs/contracts/openapi.json)
- [Reliability acceptance](docs/testing/RELIABILITY-ACCEPTANCE.md)

These are design contracts; external acceptance (live Provider/Kueue/DRA, production tenant isolation) remains open.
