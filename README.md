# Compute Control Panel

Go-based Compute Control Plane for self-hosted, Kubernetes-first heterogeneous compute.

## Product boundary

Owns compute provider registrations, pools, workload intent, allocation references, quota bindings, reconciliation and compute evidence. Kubernetes, Kueue, device plugins/DRA and provider APIs remain authoritative for actual scheduling, admission and execution.

## Architecture principles

- Go API and controller/reconciler processes.
- PostgreSQL as the control-plane domain store.
- Kubernetes + Kueue as the initial workload admission and execution substrate.
- GPU support is an adapter capability, not a custom scheduler.
- Provider operations are idempotent, durable and reconciled; ambiguous outcomes remain UNKNOWN.
- A provider acknowledgement is not proof of allocation, readiness or release.

## Repository status

The `main` branch currently contains the project charter. Architecture and implementation work remains in the following open pull requests and has **not yet been promoted to main**:

- [PR #1 — Compute architecture and domain boundary](https://github.com/avaargsh/compute-control-panel/pull/1) (Draft)
- [PR #2 — Go/PostgreSQL foundation and provider operation invariants](https://github.com/avaargsh/compute-control-panel/pull/2) (Draft)
- [Issue #3 — P0 integration and release gate](https://github.com/avaargsh/compute-control-panel/issues/3) (authoritative acceptance tracker)

The PR #2 branch contains the current persistence, operation recovery, Outbox and Inbox implementation slices. Consult its exact-head CI and the P0 tracker before treating any capability as accepted.

## Cross-repository ownership

- Agent Control Plane owns RunIntent and upstream desired state.
- Agent Runtime owns Run/Task/RunAttempt execution identity and workflow semantics.
- Compute Control Plane owns Compute Workload, Allocation, Provider operation and compute evidence.
- EngageGuard owns protected-action Permit and action-level operation evidence.
- Kueue owns admission and quota; Kubernetes scheduler owns Pod placement; DRA/provider owns device/resource realization.

`gpu-compute-platform` is retired as an active delivery repository. It is historical migration/reference input only; all new Compute implementation and release evidence belong here. The consolidation record is maintained in the [GPU Compute Platform migration matrix](https://github.com/avaargsh/compute-control-panel/blob/feat/p0-contract-artifacts/docs/migration/GPU-COMPUTE-PLATFORM-CONSOLIDATION.md).

## Non-negotiable invariants

1. No custom scheduler; preserve Kubernetes and Kueue authority boundaries.
2. UNKNOWN must be inspected/reconciled before retry; never blindly retry an ambiguous provider side effect.
3. Stale generations cannot overwrite current Workload state.
4. DELETE finality requires absence of the exact provider resource UID and evidence for applicable reservation/quota release.
5. FakeProvider is test-only until an authenticated production provider adapter exists.
6. A single PostgreSQL transaction does not provide distributed exactly-once delivery.
