# ADR-001: Cross-Repository Compute Ownership and References

- Status: Accepted for the Compute CP boundary; upstream identity alignment remains BLOCKED
- Date: 2026-11-02
- Owners: compute-control-panel maintainers
- Scope: agent-control-panel, agent-runtime, EngageGuard, SandboxProvider and Compute CP integration

## Context

The platform is split across independent repositories. Each service must own its own database and state transitions. A resource can be referenced by several services, but it must have one authoritative writer.

Current source documents are not fully aligned:
- agent-control-panel/docs/architecture/BASELINE.md says the Control Plane owns Session/Run identity and Run lifecycle coordination.
- agent-runtime/README.md says Runtime owns Run identity and Run/Task lifecycle projections.
- Compute CP must not choose between these claims by assumption.

The Agent Control Panel P0 design also refers to SandboxProvider allocation/claim while Compute CP owns ComputeWorkload and Allocation. Without a boundary, both paths could create the same underlying CPU/GPU resource.

EngageGuard owns authorization/Permit evidence. Compute CP needs its own durable journal for provider-side compute mutations. These journals are related but not interchangeable.

## Decision

### 1. Compute ownership

Compute CP is the sole authority for:
- ComputeProvider registration identity and adapter configuration;
- ComputePool and ComputeProfile;
- ComputeWorkload desired state and lifecycle projection;
- Allocation identity and provider binding;
- ComputeOperation, dispatch attempts and UNKNOWN reconciliation;
- QueueBinding and provider observation projections.

Compute CP is not a scheduler or executor. Kueue owns admission/quota/fair sharing; Kubernetes Scheduler owns Pod placement; native controllers and providers own actual external state.

### 2. Upstream Agent reference

Compute CP accepts an opaque, tenant-scoped requester_ref. It stores the caller's stable resource type and ID, but does not create, mutate or resolve Agent Run/Session state.

The target contract is:
- Agent Control Plane owns a canonical business RunIntent reference.
- Agent Runtime owns a distinct execution_id and Temporal workflow history.
- Runtime consumes allocation_id and never creates or releases the Allocation directly.

This target is a proposed cross-repository contract, not a verified current fact. The current ACP baseline and Runtime README conflict on Run identity ownership. Before enabling the golden path, those repositories must publish a coordinated ADR and compatible schema. Until then, Compute CP must not hard-code run_id or execution_id as its domain identity.

### 3. Sandbox boundary

SandboxProvider owns sandbox isolation and sandbox-session semantics. It may create runtime-specific sandbox objects only within an already authorized compute allocation or within an explicitly declared non-compute sandbox class.

For CPU/GPU capacity, SandboxProvider must reference an Allocation from Compute CP. It must not independently create a second Job/Pod/VM or reserve GPU capacity behind Compute CP for the same workload.

For the v0.1 Kubernetes Job POC, the Compute Kubernetes adapter is the only Job creator. Do not route the same request through a second SandboxProvider provisioner.

### 4. Provider operation and governance journals

For a protected compute mutation:
- EngageGuard owns authorization decision, approval and Permit evidence.
- The caller supplies permit_ref and action_hash.
- Compute CP validates the Permit for the exact action and owns ComputeOperation intent, attempt, provider receipt and reconciliation evidence.
- The ComputeOperation links back to the governance evidence.

The operation_id is stable for one immutable side-effect intent. attempt_id changes per dispatch attempt. A changed target or action hash creates a new operation. UNKNOWN is reconciled by Compute CP; Runtime/Temporal may retry the API command with the same idempotency key, but may not independently redispatch the provider mutation.

The action_hash canonicalization, Permit audience, expiry, one-time-use and replay rules must be shared with EngageGuard. No private Compute-only interpretation is allowed.

### 5. Retry ownership

| Layer | Retry responsibility |
|---|---|
| API caller / Agent Runtime | Retry the same Compute CP command with the same idempotency key; read operation status |
| Compute CP | Claim durable operations, dispatch provider actions, inspect UNKNOWN outcomes and decide whether a provider retry is safe |
| Provider adapter | Apply provider-specific idempotency token/conditional request and normalize external responses |
| Temporal | Retry workflow/activity orchestration according to Runtime policy; it does not bypass Compute CP to repeat provider calls |
| EngageGuard | Re-evaluate/revoke authorization according to Permit policy; it does not dispatch provider calls |

A retry of an API command is not permission to create a new provider operation. An ambiguous provider side effect is never converted into a new attempt with a new operation_id.

## Alternatives considered

### Runtime owns Compute Allocation and provider mutation

Rejected. It couples execution lifecycle to infrastructure lifecycle and creates a second provider operation journal.

### SandboxProvider provisions compute independently

Rejected for CPU/GPU resources. It creates competing resource creators and bypasses provider identity, quota binding, idempotency and release finality.

### Compute CP owns Agent Run identity

Rejected. Compute CP must not become an Agent lifecycle authority.

### Compute CP builds a scheduler

Rejected. Kueue and Kubernetes already own queue admission and node placement.

## Consequences

Positive:
- one writer per resource and side effect;
- stable references across independent service databases;
- Temporal retry does not duplicate provider mutations;
- provider UNKNOWN can be reconciled without changing Agent Run identity;
- Kueue and Kubernetes remain replaceable external scheduling/execution systems.

Costs:
- cross-repository schemas and compatibility tests are required;
- release may remain pending while provider/Kueue evidence is unavailable;
- a coordinated identity ADR is required before the end-to-end golden path.

## Required cross-repository follow-up

1. Resolve Run identity ownership between agent-control-panel and agent-runtime; publish one canonical business reference and a distinct Runtime execution reference.
2. Update Runtime contract so Compute interactions use allocation_id and Compute CP APIs, never direct provider_resource_id mutation.
3. Align SandboxProvider claim/provision semantics with Compute Allocation ownership.
4. Align EngageGuard Permit/action_hash canonicalization and replay rules with ComputeOperation.
5. Add consumer-driven contract tests for create, read, release, duplicate delivery, UNKNOWN and stale generation.

This ADR does not authorize implementation of a custom scheduler, direct Runtime-to-Provider calls or duplicate Sandbox/Compute provisioning.
