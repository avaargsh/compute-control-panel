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

## Current repository status — 2026-11-07

Follow the [main Go/PG16 checks](https://github.com/avaargsh/compute-control-panel/actions/workflows/go.yml?query=branch%3Amain) and [Goal #53](https://github.com/avaargsh/compute-control-panel/issues/53) for exact integration commits and remaining acceptance gates. Repository tests do not establish live Provider/Kueue/DRA admission, resource release, production tenant isolation or financial acceptance.

The source includes the Go/PostgreSQL foundation, atomic Workload acceptance, authenticated Workload and Provider management handlers, E1 policy/independent-approval persistence gates, and a separately owned Provider audit insertion boundary. Migration 0013 requires an independently preprovisioned `ccp_provider_audit_owner`; see the [Provider audit provisioning requirements](docs/architecture/PROVIDER-MANAGEMENT-API-V1.md#provisioning-before-migration). The default service mounts only health and metadata routes; Workload and Provider management handlers remain unmounted. These are implemented foundations, not verified compute capacity or production-enabled metering. Real Provider, Kubernetes/Kueue/DRA, deployed roles and authenticated evidence-source acceptance remain open.

### P0 — durable Workload control and trust boundary

- [PR #2 — Go/PostgreSQL foundation and Provider operation invariants](https://github.com/avaargsh/compute-control-panel/pull/2) **merged** into main on 2026-11-04.
- [PR #18 — atomic Workload, initial PENDING Allocation and bound CREATE Operation acceptance](https://github.com/avaargsh/compute-control-panel/pull/18) **merged** on 2026-11-04. The PostgreSQL transaction includes both dispatchable Outbox events; it is not scheduler admission or Provider realization.
- [PR #20 — authenticated Workload create/read API and SPIFFE identity directory](https://github.com/avaargsh/compute-control-panel/pull/20) **merged** on 2026-11-04. Deployment trust bundle, tenant RLS/DB roles, concrete Guard/Project policy, Provider selector/Inspect and production endpoint mounting remain gated.
- [PR #31 — UID/Generation-fenced FakeProvider CREATE/UPDATE/DELETE lifecycle](https://github.com/avaargsh/compute-control-panel/pull/31) **merged**; it is a test double, not a real Provider adapter.
- [PR #34 — Provider registration, durable one-way Drain CAS and append-only audit](https://github.com/avaargsh/compute-control-panel/pull/34) **merged**; REGISTERED is not READY. [PR #42](https://github.com/avaargsh/compute-control-panel/pull/42) added the transactional Drain gate for new Workload acceptance.
- [PR #45 — tenant-scoped FakeProvider idempotency](https://github.com/avaargsh/compute-control-panel/pull/45) **merged**; fake conformance remains distinct from real Provider acceptance.
- [PR #52 — Provider management API](https://github.com/avaargsh/compute-control-panel/pull/52) supplies dependency-injected registration/read/Drain handlers and PostgreSQL/TLS tests. Production mounting still requires independent writer-role/audit provisioning and real identity/source acceptance in [#32](https://github.com/avaargsh/compute-control-panel/issues/32).
- [PR #36 — read-only cross-branch migration identity guard](https://github.com/avaargsh/compute-control-panel/pull/36) **merged**; it prevented a real stacked PR 0005 collision (Issue #35).
- [Issues #32](https://github.com/avaargsh/compute-control-panel/issues/32) and [#33](https://github.com/avaargsh/compute-control-panel/issues/33) remain open for trusted Provider readiness/cleanup and Kubernetes/Kueue/DRA admission/release evidence.
- [PR #12 — architecture baseline and proposed OpenAPI/JSON Schema contracts](https://github.com/avaargsh/compute-control-panel/pull/12) remains **Draft**; its base snapshot predates the main implementation merges and requires reconciliation and cross-repository consumer validation.
- [Issue #3 — P0 integration and release gate](https://github.com/avaargsh/compute-control-panel/issues/3) remains the acceptance tracker; historical status snapshots in its body must not override current PR and main-branch evidence.
- Original PRs #1, #11 and #15 are closed without merging; their relevant slices were superseded or integrated via newer PRs above.

### Economics — contract, evidence and calculation slices

- [Issue #8 — Compute Economics / GPU FinOps product direction](https://github.com/avaargsh/compute-control-panel/issues/8).
- [PR #9 — E0 economics domain, schemas and golden fixtures](https://github.com/avaargsh/compute-control-panel/pull/9) remains **Draft**.
- [PR #14 — currency metadata contract alignment](https://github.com/avaargsh/compute-control-panel/pull/14) remains **Draft**.
- [PR #16 — E1 trusted Allocation usage evidence contract](https://github.com/avaargsh/compute-control-panel/pull/16) remains **Draft**.
- [PR #37 — deterministic GPU cost calculator and typed E1 evidence derivation](https://github.com/avaargsh/compute-control-panel/pull/37) **merged** on current main; pure computation and synthetic tests, no live collectors.
- [PR #38 — immutable FINAL Allocation evidence persistence, migration 0006](https://github.com/avaargsh/compute-control-panel/pull/38) **merged**; repository tests, migration lineage and main CI passed. This is an internal storage primitive, not authoritative source authentication or production billing.
- [PR #41 — scoped policy and independent FINAL authorization](https://github.com/avaargsh/compute-control-panel/pull/41) **merged**. Forward-only migrations 0007–0012 add policy/revision fencing, exact-row approvals, hardened triggers and database-owned recording time. Post-lock deadline and temporary-catalog regressions protect the internal persistence gate; historical migration bytes remain unchanged.
- Old stacked [PR #23](https://github.com/avaargsh/compute-control-panel/pull/23) and [PR #26](https://github.com/avaargsh/compute-control-panel/pull/26) were **closed unmerged** after their source content was safely rebuilt as #37/#38. Earlier #17/#19 were likewise superseded.
- [Issue #35 — migration version conflict](https://github.com/avaargsh/compute-control-panel/issues/35) **closed** after renumbering Economics to 0006 and preserving Provider 0005; the new lineage guard passed.
- [Issue #39 — authoritative evidence policy and source authentication](https://github.com/avaargsh/compute-control-panel/issues/39) remains **OPEN** and blocks use of persisted FINAL records as trusted production metering, cost or resource-finality evidence.

The internal evidence repository requires an independently resolved policy matching the requested snapshot, a current database policy fence, a separately issued exact-row approval and a restricted writer role with verified catalogs. These controls do not authenticate the external observations or certify a stored FINAL as production metering. The calculator can price only a complete, validated UsageInterval. Production cost posting remains gated on authenticated evidence collection, reviewed issuer/policy provisioning, deployed role isolation, append-only WorkloadCostEntry/reversal storage and cross-service validation.

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

## Architecture and contract references (PR #12)

These documents and schemas are proposed cross-repository contracts, not proof that every API endpoint or integration exists in production:

- [Architecture baseline](docs/architecture/BASELINE.md)
- [Domain ownership and Kubernetes/Kueue boundary](docs/architecture/DOMAIN-OWNERSHIP.md)
- [Workload and Provider operation contract v1](docs/architecture/WORKLOAD-OPERATION-V1.md)
- [Compute Workload API and event contract v1](docs/contracts/COMPUTE-WORKLOAD-V1.md)
- [OpenAPI 3.1 contract and JSON Schemas](docs/contracts/openapi.json)
- [Cross-repository ownership ADR](docs/adr/ADR-001-CROSS-REPO-OWNERSHIP.md)
- [Reliability acceptance matrix](docs/testing/RELIABILITY-ACCEPTANCE.md)
- [Implementation plan](tasks/plan.md)
- [Tracked tasks and cross-repository gates](tasks/todo.md)

The OpenAPI implementation-status markers distinguish the present, unmounted POST/GET Workload handlers from proposed endpoints. Consumer-driven contract tests, production tenant isolation and independent Provider/Kueue/DRA acceptance remain open gates.
