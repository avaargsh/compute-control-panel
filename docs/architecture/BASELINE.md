# Compute Control Plane Baseline

Status: Proposed v0.2  
Date: 2026-11-02

## Mission

A provider-neutral Compute Control Plane that owns compute-domain intent, resource bindings, provider operation records and reconciliation. It is not a scheduler, executor, Agent lifecycle service or GPU-only management layer.

## Authority

Compute CP owns:
- ComputeProvider registration identity and adapter configuration;
- ComputePool, ComputeProfile and QueueBinding;
- ComputeWorkload intent and lifecycle projection;
- Allocation identity and provider-resource mapping;
- ComputeOperation journal and UNKNOWN reconciliation;
- timestamped inventory/resource observations as projections.

Kubernetes, Kueue and provider APIs remain authoritative for actual external admission, scheduling, execution and resource state. Agent Control Panel owns the upstream business reference; Agent Runtime owns execution coordination and Temporal workflow history, subject to the identity-alignment gate in ADR-001.

## Domain model

ComputeProvider, ComputePool, ComputeProfile, ComputeWorkload, Allocation, ComputeOperation, ResourceObservation and QuotaBinding are defined in [DOMAIN-OWNERSHIP.md](DOMAIN-OWNERSHIP.md).

ComputeWorkload is the platform domain object. It is not the Kueue Workload CRD. Allocation is a platform binding, not a Kubernetes Job or Agent Run.

## v0.1 Kubernetes/Kueue golden path

1. Caller submits a versioned ComputeWorkload request with tenant/project scope, opaque requester_ref and a stable Idempotency-Key.
2. Compute CP validates policy, persists desired state and outbox atomically, and records an Allocation/ComputeOperation before external I/O.
3. The Compute Kubernetes adapter creates one suspended Kubernetes Job for the workload generation.
4. Kueue's built-in Job integration creates and manages the native Kueue Workload for that Job. Compute CP does not create or patch that object.
5. Kueue owns quota reservation/admission; Kubernetes Scheduler owns Pod-to-Node placement; native controllers own Job/Pod lifecycle.
6. Compute CP observes authoritative status and binds provider_id/provider_resource_id to Allocation only after verification.
7. Agent Runtime consumes allocation_id. It does not create/delete provider resources or own Compute Allocation state.
8. Release persists desired absence, deletes through the same operation journal, and verifies provider absence plus applicable Kueue reservation release before marking RELEASED.

## Invariants

- One authoritative writer per domain fact and external object.
- Stable operation_id per immutable side effect; attempt_id is per dispatch attempt.
- Every provider mutation is fenced by desired_generation and action_hash.
- Lost acknowledgement is UNKNOWN; inspect/reconcile before retry.
- Delete ACK is not release proof.
- RELEASED requires authoritative absence and applicable quota-release evidence.
- Stale workers cannot overwrite newer generations.
- Idempotency keys are atomically claimed and bound to canonical request hashes.
- No cross-tenant references or provider identity ambiguity.
- No custom scheduler, duplicate Kueue Workload writer or direct Runtime-to-Provider mutation path.
- DRA/HAMi/vendor-specific support is capability-gated, not assumed.
- EngageGuard owns authorization/Permit evidence; Compute CP owns compute-side operation journal.

## Implementation constraints

Go API and separate reconciler process; PostgreSQL per service; transactional outbox/inbox; provider-neutral SPI; Kubernetes client adapter; Kueue integration through one declared Job path; OpenTelemetry and versioned OpenAPI/JSON Schema.

Use one controller/reconciliation framework per resource class after POC. Do not introduce both controller-runtime and custom pollers for the same resource. Start with FakeProvider before real provider integration.

## Architecture documents

- [Domain ownership](DOMAIN-OWNERSHIP.md)
- [Compute Workload API/Event Contract v1](../contracts/COMPUTE-WORKLOAD-V1.md)
- [State machines and recovery](STATE-MACHINES.md)
- [Kubernetes/Kueue boundary](KUBERNETES-KUEUE-BOUNDARY.md)
- [ADR-001: cross-repository ownership](../adr/ADR-001-CROSS-REPO-OWNERSHIP.md)
- [Reliability acceptance matrix](../testing/RELIABILITY-ACCEPTANCE.md)

## Review blockers

- Agent Control Panel and Agent Runtime currently disagree on canonical Run identity. See ADR-001.
- SandboxProvider versus ComputeProvider resource creation must remain single-writer.
- EngageGuard Permit/action_hash canonicalization must be versioned and contract-tested.
- No implementation, cluster integration or production readiness is claimed by this baseline.
