# Compute Control Plane

Go-based control plane for heterogeneous compute capacity and workload allocation in a self-hosted Kubernetes-first platform.

## Product boundary

Compute Control Plane owns Provider registrations, Pools, Profiles, Workload intent, Allocation identities, quota bindings, provider-operation journals, reconciliation and compute evidence. Kubernetes, Kueue, DRA/device plugins and provider APIs remain authoritative for admission, Pod placement, device realization and external resource state. This service is not a custom scheduler.

## Architecture authority

| Concern | Authority |
|---|---|
| Agent / Session / RunIntent | Agent Control Plane |
| Run / Task / RunAttempt / workflow execution | Agent Runtime |
| Workload / Allocation / ComputeOperation | Compute Control Plane |
| Protected-action Permit and action receipt | EngageGuard |
| Queue admission and quota | Kueue |
| Pod placement | Kubernetes scheduler |
| Device/resource realization | DRA / device plugin / provider |
| External resource existence and UID | Kubernetes / provider API |
| Sandbox identity, isolation and lifecycle | SandboxProvider |

For direct Run capacity, Runtime references `agent-runtime/run/<run_id>`. For sandbox capacity, SandboxProvider is the sole Compute requester for `sandbox-provider/sandbox-claim/<claim_id>`. Guard and Compute keep distinct operation journals.

## Reliability invariants

1. No custom scheduler; preserve Kubernetes and Kueue authority boundaries.
2. Persist Workload + initial PENDING Allocation + bound CREATE Operation + dispatchable Outbox events atomically through `AcceptWorkload`.
3. Same idempotency key and canonical digest replay stable identities; changed digest is rejected before provider dispatch.
4. UNKNOWN enters authoritative inspection/reconciliation before retry; never blindly retry an ambiguous provider side effect.
5. Stale generations cannot overwrite current Workload state.
6. DELETE finality requires absence of the exact provider resource UID and evidence that applicable queue reservations are released.
7. FakeProvider is test-only until an authenticated production provider adapter exists.
8. PostgreSQL transaction atomicity does not imply distributed exactly-once delivery.

## Repository status

The `main` branch contains the project charter and governance entry point. Architecture, implementation and economics work remains staged in Draft pull requests and must pass their exact-head evidence and the P0 gates before promotion:

- [Architecture baseline replacement — PR #12](https://github.com/avaargsh/compute-control-panel/pull/12) (Draft review branch)
- [Go/PostgreSQL foundation — PR #2](https://github.com/avaargsh/compute-control-panel/pull/2) (Draft)
- [Atomic Workload acceptance — PR #11](https://github.com/avaargsh/compute-control-panel/pull/11), stacked on PR #2 (Draft)
- [Compute Economics E0 contracts — PR #9](https://github.com/avaargsh/compute-control-panel/pull/9) (Draft)
- [Compute Economics calculator — PR #10](https://github.com/avaargsh/compute-control-panel/pull/10) (Draft)
- [Currency registry contract alignment — PR #14](https://github.com/avaargsh/compute-control-panel/pull/14) (Draft; replaces closed PR #13)
- [P0 integration and release gate — Issue #3](https://github.com/avaargsh/compute-control-panel/issues/3) (authoritative acceptance tracker)

`gpu-compute-platform` is retired as an active delivery repository and remains historical migration/reference input only. All new compute implementation and release evidence belongs here.

## Architecture and implementation references

- [Architecture baseline](docs/architecture/BASELINE.md)
- [Workload and provider operation contract v1](docs/architecture/WORKLOAD-OPERATION-V1.md)
- [Compute Workload API and event contract v1](docs/contracts/COMPUTE-WORKLOAD-V1.md)
- [Cross-repository ownership ADR](docs/adr/ADR-001-CROSS-REPO-OWNERSHIP.md)
- [Reliability acceptance matrix](docs/testing/RELIABILITY-ACCEPTANCE.md)
- [Implementation plan](tasks/plan.md)
- [Tracked tasks and cross-repository gates](tasks/todo.md)
