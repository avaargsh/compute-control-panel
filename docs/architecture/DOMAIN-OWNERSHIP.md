# Compute Domain Ownership

Status: Proposed v0.2  
Date: 2026-11-02  
Scope: compute-control-panel

## 1. Decision rule

One business fact has one authoritative writer. Compute Control Plane (Compute CP) owns the platform's compute-domain records and reconciliation decisions. Kubernetes, Kueue and infrastructure providers remain authoritative for the external facts they create and operate.

Compute CP is a resource control plane, not a scheduler, workload executor, Agent Runtime or second source of truth for Kubernetes/Kueue state.

## 2. Ownership matrix

| Concept / fact | Authoritative owner | Compute CP responsibility |
|---|---|---|
| Provider registration identity and adapter configuration | Compute CP | Create, version, disable and resolve stable Provider IDs |
| Provider API health and capabilities actually observed | Provider; observed by adapter | Store timestamped observations and capability snapshots |
| ComputePool logical grouping and policy binding | Compute CP | Own pool identity, eligibility constraints and provider/cluster references |
| Physical inventory and device health | Provider / Kubernetes device and node APIs | Maintain a freshness-bounded read projection; never claim the projection is the source of truth |
| ComputeProfile and normalized resource requirements | Compute CP | Validate and version platform-level resource profiles |
| ComputeWorkload intent and lifecycle projection | Compute CP | Persist desired state, tenant scope, source reference and generation |
| Allocation identity and binding record | Compute CP | Own allocation ID, selected provider/pool and resource-reference mapping |
| Provider operation intent, attempts and reconciliation evidence | Compute CP | Own the compute-side operation journal and dispatch/reconciliation policy |
| Queue binding configuration | Compute CP, subject to cluster-admin policy | Map a tenant/project/pool to a Kueue LocalQueue and permitted ClusterQueue/ResourceFlavor set |
| Quota reservation, admission, fair sharing and preemption | Kueue | Read and project native status; do not calculate or overwrite Kueue reservations |
| Pod-to-Node placement | Kubernetes Scheduler | Observe placement; do not choose or bind Nodes |
| Job/Pod object lifecycle | Kubernetes API and native controllers; Job creation is requested by the Compute adapter | Create/delete only the declared top-level execution object; observe controller-owned descendants |
| Cloud/GPU VM, device or remote resource facts | Provider API / provider controller | Invoke through an adapter and verify through authoritative observation |
| Agent, Session and business Run identity | Agent Control Panel / Agent Control Plane contract | Store only an opaque consumer reference; never create or mutate these identities |
| Run execution, Task state and workflow history | Agent Runtime / Temporal | Consume allocation references; do not own execution state |
| Authorization decision and Permit | EngageGuard | Validate and record authorization references; do not replace the governance ledger |

## 3. Domain objects

### ComputeProvider

A stable registration for one provider endpoint/account/cluster boundary. Its ID is immutable and independent of display name, endpoint URL, credentials or deployment instance. Configuration revisions may change without changing the identity of resources already created through it.

A Provider capability document has two parts:

- configured capabilities: what the operator enables;
- observed capabilities: what the adapter has successfully discovered, with observation time and source revision.

A capability is usable only when configured and currently observed. Missing, stale or unknown capability data fails closed for operations that require it.

### ComputePool

A platform-owned logical resource pool. It may represent a cluster, cloud account/region, GPU fleet or a deliberately bounded subset of a provider. It is not a mirror of a Kueue ClusterQueue and does not contain an independently authoritative copy of physical capacity or quota reservation.

A pool may reference a Kubernetes cluster, one or more permitted queue bindings and a provider identity. Pool selection is high-level policy routing over explicitly configured candidates; it must not become node-level placement or an independent quota scheduler.

### ComputeProfile

A versioned, provider-neutral resource shape: CPU, memory, accelerator vendor/model/count, storage and other declared capabilities. Provider-specific resource names and Kueue ResourceFlavor mappings belong in adapters/configuration, not in Agent-facing contracts.

### ComputeWorkload

The durable compute request owned by Compute CP. It captures tenant/project scope, an opaque requester reference, normalized resource requirements, constraints, desired presence, generation and lifecycle projection.

A ComputeWorkload is not:
- a Kueue Workload CRD;
- a Kubernetes Job or Pod;
- an Agent Run or Runtime Task;
- a scheduler queue entry owned by Compute CP.

One ComputeWorkload has at most one active Allocation at a time. Historical allocations remain immutable records. Reallocation creates a new Allocation identity and advances the workload generation; it does not rewrite the identity or history of an earlier allocation.

### Allocation

The platform's durable binding between one ComputeWorkload generation and a selected provider/pool resource set. Compute CP owns the Allocation ID and binding record. The Provider owns the external resource identity and actual existence/state.

An Allocation may refer to multiple provider-native objects (for example, a Job plus associated reservations or devices). The primary provider resource is exposed as provider_resource_id; additional objects are represented as typed provider_resource_refs. Every provider-native ID is interpreted in the context of provider_id, resource kind and provider scope.

Allocation means that the resource binding is verified and usable. It does not mean that an Agent Run has started or completed.

### ComputeOperation

A durable record for one intended external side effect, such as create, inspect, update or delete. It is the compute-side operation journal. It records a stable compute_operation_id, workload/allocation references, desired generation, provider identity, provider_request_digest, compute_attempt_id records, optional Guard operation reference, provider receipts and reconciliation evidence.

compute_operation_id is stable across retries of the same immutable provider intent. compute_attempt_id is unique for each provider dispatch attempt. A changed target, payload or desired generation is a new operation, not a retry.

### ResourceObservation

A timestamped projection of state read from an authoritative external system. It includes provider_id, provider_resource_id, resource kind, observed state, observed_at, source version/resourceVersion and freshness. An observation is not a command and cannot independently prove a transition if its source is stale or non-authoritative.

### QuotaBinding

A platform mapping from tenant/project/pool policy to a namespaced Kueue LocalQueue and the approved cluster queue/flavor domain. It is configuration, not a second quota ledger. Kueue owns actual quota reservation, admission, borrowing, preemption and release.

## 4. Aggregate and cardinality rules

- A ComputeWorkload belongs to exactly one tenant and project scope.
- A ComputeWorkload may have zero or one active Allocation; historical Allocations are retained.
- An Allocation belongs to exactly one ComputeWorkload generation and one ComputeProvider.
- An Allocation may contain multiple typed provider-resource references, all scoped by provider_id.
- A ComputeOperation mutates one declared target and is fenced to one desired generation and provider_request_digest.
- A ResourceObservation is append-only evidence; a newer observation supersedes the current projection but does not erase history.
- Cross-tenant references are rejected on every API read, write, event consumption and provider binding.

## 5. Explicit non-ownership

Compute CP must not own or implement:

1. Agent, Session, RunIntent/Run identity, Agent Release or business lifecycle.
2. Runtime Execution, Task/Step lifecycle, Agent framework loops or Temporal workflow history.
3. Kueue's admission algorithm, quota reservation, fair sharing, flavor selection or preemption.
4. Kubernetes Pod-to-Node scheduling or Kubernetes controller semantics.
5. Provider-internal inventory truth, cloud capacity truth or GPU device health.
6. A generic sandbox lifecycle that duplicates Compute Allocation.

A SandboxProvider may own isolation and sandbox-session semantics. It must consume or reference a Compute Allocation for capacity and must not create a second GPU/CPU allocation path behind Compute CP. The v0.1 Kubernetes Job path has one resource creator: the Compute Kubernetes adapter.

## 6. Operation and governance boundary

ComputeOperation is not a replacement for EngageGuard's authorization/approval evidence. For a protected mutation:

1. For a protected Agent-originated compute action, EngageGuard authorizes the exact ActionRequest and issues a scoped Permit for the Compute CP audience.
2. The request carries guard_operation_id, permit_ref and opaque action_hash; caller-supplied fields alone are not proof of authorization.
3. Compute CP validates the Permit through the Guard verification interface and binds it to the exact normalized Compute request digest before dispatch. Until that attestation contract exists, fail closed.
4. Compute CP persists and dispatches its own ComputeOperation using compute_operation_id and provider_request_digest.
5. Provider receipt and reconciliation evidence are recorded by Compute CP. EngageGuard retains the Action Operation and links the downstream compute_operation_id.
6. Guard action_hash is not recomputed by Compute CP and is not reused as provider_request_digest.

EngageGuard owns protected-action authorization and the Action Operation Journal. Compute CP is the trusted enforcement point for provider mutations delegated to it and owns the ComputeOperation journal. Internal Run capacity requests use authenticated service identity and Compute policy. The Guard-to-Compute Permit attestation schema and action_hash binding remain versioned cross-repository dependencies.

## 7. Alternatives considered

### Build a Compute-owned scheduler

Rejected. It would duplicate Kueue quota/admission and Kubernetes placement semantics, create a second scheduler state, and make GPU/DRA integrations harder to reuse.

### Make native Kueue Workload the platform's domain object

Rejected. Kueue Workload is an external admission object with Kueue-specific schema and lifecycle. The platform needs a provider-neutral ComputeWorkload that can map to Kubernetes/Kueue today and other providers later.

### Let Runtime or SandboxProvider create compute resources directly

Rejected. It creates competing writers for provider resources, bypasses ComputeOperation idempotency/reconciliation and makes Allocation ownership ambiguous.

## 8. Implementation consequence

The first implementation should be contract-first and use a FakeProvider. The first real Kubernetes proof should use one Job + Kueue Job integration path. Do not introduce a second Kueue Workload writer, custom scheduler, GPU-specific assumption or direct Runtime-to-Provider path.
