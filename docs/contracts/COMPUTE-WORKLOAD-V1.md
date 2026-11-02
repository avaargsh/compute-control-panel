# Compute Workload API and Event Contract v1

Status: Proposed v1; implementation gate
Date: 2026-11-02
Audience: agent-control-panel, agent-runtime, EngageGuard, compute-control-panel, SandboxProvider

## 1. Contract principles

- The domain aggregate is ComputeWorkload; the public resource path uses /api/v1/workloads and the wire identifier is workload_id.
- Compute CP is the sole writer for Workload, Allocation and ComputeOperation.
- owner_ref is an opaque, tenant-scoped reference to the lifecycle owner: agent-runtime/run/<run_id> or sandbox-provider/sandbox-claim/<claim_id>. RunIntent is not a Compute owner reference.
- All state-changing calls are asynchronous and idempotent.
- A lost response is an unknown transport outcome, not proof that the provider operation failed.
- Tenant scope comes from authenticated workload identity; body fields cannot grant tenant/project access.
- No provider credential, kubeconfig, raw token or provider secret appears in the request or event.

The shapes below are proposed v1 contract examples, not implemented endpoints.

## 2. Create Workload

```http
POST /api/v1/workloads
Idempotency-Key: compute-request-<stable-opaque-id>
Content-Type: application/json
```

```json
{
  "api_version": "compute.platform/v1",
  "project_id": "project-001",
  "owner_ref": {
    "service": "agent-runtime",
    "resource_type": "run",
    "resource_id": "run-001"
  },
  "compute_profile_ref": {
    "id": "gpu-inference",
    "revision": "3"
  },
  "resources": {
    "cpu": "16",
    "memory": "64Gi",
    "accelerators": [{ "vendor": "nvidia", "model": "H100", "count": 2 }]
  },
  "constraints": {
    "pool_selector": { "capability": "gpu.nvidia.h100" },
    "max_duration_seconds": 7200
  },
  "authorization": {
    "guard_operation_id": "action-op-001",
    "permit_ref": "permit-001",
    "action_hash": "sha256:...",
    "audience": "compute-control-panel"
  }
}
```

Authorization is omitted for platform-internal Runtime/SandboxProvider capacity requests authenticated by workload identity and Compute policy. It is required for an Agent-originated protected compute action. Compute CP validates the delegated Permit through EngageGuard and binds it to the exact normalized Compute request. A permit_ref/action_hash pair supplied by the caller is not sufficient by itself; the Guard verification/attestation contract must pass. Until that contract is implemented, protected Agent-originated Compute requests fail closed.

Request rules:
- Idempotency-Key is generated and durably stored by the caller once per logical capacity request. It is reused for every retry and does not contain an attempt number or timestamp.
- The same key + same canonical request digest returns the original Workload/operation reference. The same key + a different digest returns 409 IDEMPOTENCY_KEY_REUSED.
- Exactly one of compute_profile_ref or a complete resources object is supplied, unless an explicitly versioned override rule applies.
- The caller cannot choose provider_id, provider_resource_id, Job name, Kueue Workload or NodeName in the normal API.
- project_id must belong to the authenticated tenant.

Sandbox request example:

```json
{
  "api_version": "compute.platform/v1",
  "project_id": "project-001",
  "owner_ref": {
    "service": "sandbox-provider",
    "resource_type": "sandbox-claim",
    "resource_id": "claim-001"
  },
  "compute_profile_ref": { "id": "sandbox-cpu-gpu", "revision": "1" }
}
```

Runtime calls SandboxProvider Claim; SandboxProvider is the only Compute CP caller for this claim. Runtime must not submit a second Workload for the same sandbox capacity.

Response:

```http
202 Accepted
Location: /api/v1/workloads/workload-001
```

```json
{
  "workload_id": "workload-001",
  "desired_generation": 1,
  "desired_state": "ACTIVE",
  "observed_phase": "PENDING",
  "compute_operation_id": "compute-op-001",
  "status_url": "/api/v1/workloads/workload-001"
}
```

202 confirms Workload + Outbox + idempotency record are durable. It does not mean provider creation, Kueue admission, Pod scheduling or Allocation readiness succeeded.

## 3. Read and release

```http
GET /api/v1/workloads/{workload_id}
GET /api/v1/workloads/{workload_id}/allocations
GET /api/v1/compute-operations/{compute_operation_id}
DELETE /api/v1/workloads/{workload_id}
```

DELETE records desired_state=DELETE_REQUESTED and returns 202. It does not report RELEASED because the API or Kubernetes accepted a deletion request. The caller waits for authoritative provider absence and applicable Kueue reservation release.

## 4. Allocation reference

```json
{
  "allocation_id": "alloc-001",
  "workload_id": "workload-001",
  "provider_id": "provider-001",
  "provider_resource_id": "k8s-job-uid-opaque",
  "provider_resource_refs": [
    { "resource_kind": "kubernetes.job", "provider_resource_id": "job-uid", "namespace": "tenant-001", "name": "workload-001-g1" },
    { "resource_kind": "kueue.workload", "provider_resource_id": "kueue-uid", "namespace": "tenant-001", "name": "workload-001-g1" }
  ],
  "pool_id": "pool-gpu-east",
  "desired_generation": 1,
  "phase": "BOUND",
  "observed_at": "2026-11-02T10:00:00Z"
}
```

- allocation_id is the stable platform reference consumed by Runtime/SandboxProvider.
- provider_id scopes every provider-native resource ID. A provider_resource_id alone is never globally meaningful.
- Kueue Workload references are read-only observations; Compute CP does not patch or delete the Kueue-owned object.
- BOUND means the provider resource mapping is verified and usable. It does not mean the Agent Run has started or succeeded.

## 5. Error contract

Errors use the platform envelope with code, message, retryable, correlation_id and bounded details.

| Condition | HTTP | Required behavior |
|---|---:|---|
| Invalid schema or unsupported resource capability | 422 | No provider side effect |
| Unauthenticated | 401 | No resource disclosure |
| Tenant/project mismatch | 403 | Do not reveal cross-tenant resource existence |
| Resource absent in caller scope | 404 | No cross-tenant lookup leak |
| Same Idempotency-Key with different request digest | 409 | Reject; never replay a different request |
| Same key while request is in flight | 202 or 409 | Return same Workload/status reference; never dispatch a second logical operation |
| Stale desired generation/version | 409 | Caller re-reads and creates a new intent if needed |
| Provider outcome ambiguous | 202 | Preserve UNKNOWN and return the operation status URL |

## 6. Identifier and journal boundaries

- caller Idempotency-Key identifies one immutable Compute API request and is stable across retries.
- workload_id and allocation_id are created by Compute CP.
- compute_operation_id is created by Compute CP for one immutable provider mutation; compute_attempt_id is unique per provider dispatch attempt.
- provider_request_digest is computed by Compute CP from the normalized provider mutation and is distinct from Guard action_hash.
- guard_submission_key (HTTP Idempotency-Key) is created by Runtime and reused across retries. EngageGuard creates the server-side operation_id; guard_operation_id in the authorization block references that returned Action Operation.
- action_hash and Permit are Guard-owned. Compute CP treats action_hash as opaque and validates the delegated Permit at its enforcement point; it does not recompute Guard's hash.
- If a Guard executor delegates to Compute CP, it persists a stable downstream Compute Idempotency-Key before the call and records returned workload_id/compute_operation_id. The Guard operation_id is server-generated by EngageGuard and is not reused as the Compute Idempotency-Key. Guard reconciles the action-level operation; Compute CP reconciles the provider effect.
- Same Compute Idempotency-Key + same request digest returns the same Workload/operation. Same key + different digest returns 409.
- A provider retry reuses compute_operation_id, provider idempotency token and provider_request_digest. A changed target, payload or desired generation is a new operation only after prior ambiguity is resolved or safely fenced.

## 7. Event envelope

Events use the platform envelope and at-least-once delivery:

```json
{
  "event_id": "evt-001",
  "event_type": "compute.allocation.bound.v1",
  "schema_version": 1,
  "producer": "compute-control-panel",
  "tenant_id": "tenant-001",
  "aggregate_type": "compute_workload",
  "aggregate_id": "workload-001",
  "aggregate_version": 4,
  "occurred_at": "2026-11-02T10:00:00Z",
  "correlation_id": "corr-001",
  "causation_id": "cmd-001",
  "traceparent": "00-...",
  "data": {}
}
```

Consumers deduplicate by event_id and apply only contiguous aggregate_version. A version gap is parked/recovered; it is not silently skipped. Events are published through transactional Outbox and consumed through durable Inbox. Delivery is at least once, not exactly once.

Initial event types:

| Event | Meaning |
|---|---|
| compute.workload.accepted.v1 | Workload and desired generation durably accepted |
| compute.workload.rejected.v1 | Request rejected before any provider dispatch |
| compute.allocation.bound.v1 | Provider resource binding verified |
| compute.allocation.observation-updated.v1 | New timestamped provider/Kubernetes/Kueue observation |
| compute.allocation.releasing.v1 | Desired absence accepted; release not proven |
| compute.allocation.released.v1 | Exact provider resource absence and applicable queue release verified |
| compute.operation.unknown.v1 | Provider outcome ambiguous; reconciliation required |
| compute.operation.reconciled.v1 | Compute UNKNOWN resolved with evidence |
| compute.operation.manual-review.v1 | Automated provider reconciliation is unsafe |

An observation event is evidence/wakeup, not a terminal allocation result.

## 8. Cross-repository compatibility gates

1. ACP owns run_intent_id; Runtime owns run_id/workflow_id. Compute owner_ref uses agent-runtime/run/<run_id>, not RunIntent.
2. Runtime direct Run capacity and SandboxProvider Claim capacity are mutually exclusive request paths; SandboxProvider is sole requester for a sandbox claim.
3. EngageGuard owns action_operation_id record, action_hash and Permit. Compute CP owns compute_operation_id and provider reconciliation.
4. The Guard Permit must bind the exact normalized Compute request digest and audience compute-control-panel. The Guard verification/attestation schema is not implemented; protected Agent-originated Compute requests remain blocked until it is.
5. OpenAPI/JSON Schema, event schemas and consumer-driven golden tests must run across Runtime, Guard and Compute CP before the golden path is enabled.
6. No contract claims PostgreSQL, FakeProvider, Kubernetes/Kueue or production acceptance without evidence at the corresponding level.