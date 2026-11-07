# Compute Control Plane Baseline

Status: Proposed v0.2 — implementation gate

## 1. Authority

Compute Control Plane is the sole writer for ComputeProvider registration, ComputePool, ComputeProfile, ComputeWorkload intent, Allocation, quota binding, compute_operation_id/provider journal and ResourceObservation records/projections.

External authorities remain:
- Kueue: queue admission and quota admission.
- Kubernetes scheduler: Pod placement onto nodes.
- Kubernetes API / device plugins / DRA: Pod, Job and device allocation state.
- Provider APIs: provider-specific resource existence, UID and operation state.
- SandboxProvider: sandbox identity, isolation and sandbox lifecycle.

No custom scheduler is built in v0.1. No two components may create/delete the same provider resource as independent authorities.

## 2. Domain records (separate state machines)

### Workload

Workload is a desired compute request, not a Pod or a Run.

Fields include: workload_id, tenant_id, project_id, owner_ref (opaque source service/resource), compute_profile_ref, canonical request digest, desired_generation, desired_state, observed_generation, observed_phase, allocation_id, provider reference, timestamps and version.

- Desired state: ACTIVE or DELETE_REQUESTED.
- Observed Workload phase is a platform projection: REQUESTED, ACCEPTED, PROVISIONING, ALLOCATED, RELEASING, RELEASED, REJECTED or FAILED_CONFIRMED (see STATE-MACHINES.md). Provider/Kueue statuses and UNKNOWN observations belong to ResourceObservation/ComputeOperation, not the Workload phase enum.
- An observation never changes desired state.
- A stale-generation observation may be retained as evidence but cannot overwrite the current projection.
- Workload references Runtime Run via owner_ref=agent-runtime/run/<run_id>, or a SandboxProvider claim via owner_ref=sandbox-provider/sandbox-claim/<claim_id>; it does not own Run or Sandbox lifecycle state. RunIntent is not a Workload owner reference.

### Allocation

Allocation records the selected provider/pool and the capacity/resource reference for a Workload. It is not a scheduler decision history and not a Pod status. Release is terminal only after provider inspection proves the exact allocated resource UID absent.

### ComputeOperation

One immutable intent to mutate a provider resource. Fields include compute_operation_id, tenant/workload/provider IDs, kind, desired_generation, provider idempotency token, provider_request_digest, target provider resource ID/UID where applicable, status, version, lease owner/expiry and fencing token, plus optional guard_operation_ref.

Operation state:

```text
PENDING → LEASED → IN_FLIGHT → SUCCEEDED | FAILED
IN_FLIGHT → UNKNOWN → RECONCILING
RECONCILING → SUCCEEDED | PENDING | FAILED | MANUAL_REVIEW | UNKNOWN
LEASED / IN_FLIGHT / RECONCILING -- lease expiry --> UNKNOWN
```

- PENDING after RECONCILING is allowed only when provider operation lookup authoritatively proves no effect was applied; retry the same operation_id, idempotency key and digest.
- UNKNOWN or RECONCILING is never treated as FAILED or safe-to-retry.
- Provider conflict, wrong resource UID, wrong generation or contradictory evidence enters MANUAL_REVIEW.
- A stale fencing token cannot commit an operation result.
- A stale desired generation cannot overwrite current Workload state; a stale CREATE that did create a resource schedules compensating cleanup.

### ResourceObservation

An append-only observation is distinct from ComputeOperation status. It records provider_id, workload_id, operation_id when known, desired_generation, provider_resource_id/UID, provider resourceVersion or provider sequence, observed_at, result and provenance. Event notifications are wake-ups; reconcile against a fresh provider inspection before updating the Workload projection.

## 3. Provider operation semantics

- Every provider mutation carries stable compute_operation_id, provider idempotency token, provider_request_digest and desired_generation.
- Persist ComputeOperation before making the provider call.
- A lost acknowledgement transitions to UNKNOWN.
- Inspect by compute_operation_id and exact provider resource identity before retry.
- CREATE/UPDATE success requires observed resource identity plus operation/generation correlation.
- DELETE success requires absence of the exact target resource UID (not just a successful delete response or absence by a reused name).
- If the provider cannot expose an authoritative operation lookup or safe idempotency, ambiguous operations stay UNKNOWN / MANUAL_REVIEW; do not invent exactly-once semantics.

## 4. Kubernetes / Kueue boundary

`Workload API → Compute reconciler → Kubernetes Job/Workload object → Kueue queue/admission/quota → Kubernetes scheduler → node/device plugins/DRA/provider`

Kueue admits queued workloads and enforces configured quota/borrowing/preemption policy. Kubernetes scheduler performs Pod placement. Device plugins/DRA/provider realize accelerator resources. Compute Control Plane translates its provider-neutral intent to these APIs and observes results; it does not replace any of them.

## 5. API contract

- `POST /api/v1/workloads`: requires Idempotency-Key and canonical request digest; returns 202 with canonical Workload identity after DB + Outbox commit. It does not wait for provider allocation.
- `GET /api/v1/workloads/{id}`: returns desired state, observed phase, generation, allocation reference and evidence links.
- `GET /api/v1/workloads`: cursor pagination with bounded page size and tenant scope.
- `DELETE /api/v1/workloads/{id}`: records DELETE_REQUESTED and returns 202. It does not claim the provider resource is already released.
- Provider IDs, raw credentials and Kubernetes implementation objects are not accepted as authorization inputs from callers. For protected Agent-originated compute requests, Compute CP validates a Guard Permit bound to the exact normalized request; internal Runtime/SandboxProvider requests use authenticated service identity and Compute policy.

## 6. Persistence and reconciliation

Go API + reconciler process; PostgreSQL per service; transactional Outbox/Inbox; versioned API and events. Controller claims durable operations with lease owner, lease expiry and monotonically increasing fencing token. Use conditional updates on version and fencing token. Redis is cache only. Choose one reconciliation framework after a small Kubernetes/Kueue POC; do not run controller-runtime and custom pollers for the same resource.

## 7. Acceptance

- Idempotent Workload create and 409 for key/digest mismatch.
- Outbox atomicity, duplicate delivery and inbox version-gap handling.
- Operation lost ACK → UNKNOWN → RECONCILING → found / not-applied-final / conflict.
- Lease expiry, stale fencing token and stale generation cannot overwrite newer state.
- CREATE success correlates exact operation ID + generation + provider resource UID.
- DELETE release is proven by exact target UID absence.
- Process restart resumes from durable operation and observation state.
- Tenant/project isolation and credential hygiene.
- FakeProvider contract suite passes before Kubernetes/Kueue adapter.
