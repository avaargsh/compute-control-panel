# Compute Workload API and Event Contract v1

Status: Proposed; implementation gate  
Date: 2026-11-02  
Audience: agent-control-panel, agent-runtime, compute-control-panel

## 1. Contract principles

- Compute CP is the sole writer for ComputeWorkload, Allocation and ComputeOperation.
- Consumers use stable platform IDs. Provider-native IDs are opaque and provider-scoped.
- All state-changing calls are idempotent and asynchronous.
- A timeout or lost response is an unknown outcome, not a failed operation.
- API and event schemas are versioned independently from provider-specific payloads.
- Tenant scope is derived from authenticated identity and validated against any requested project; a body field cannot grant tenant access.
- No secret bytes, kubeconfig, cloud credential or provider token may appear in request/event payloads.

The contract is resource-oriented under /api/v1. The examples below are illustrative v1 shapes, not implemented endpoints.

## 2. Create request

Request:

~~~http
POST /api/v1/compute-workloads
Idempotency-Key: cw-create:ri-001:profile-v3
Content-Type: application/json
~~~

~~~json
{
  "apiVersion": "compute.platform/v1",
  "projectId": "project-001",
  "requesterRef": {
    "system": "agent-control-panel",
    "resourceType": "RunIntent",
    "resourceId": "ri-001"
  },
  "profileRef": {
    "id": "gpu-inference",
    "revision": "3"
  },
  "resources": {
    "cpu": "16",
    "memory": "64Gi",
    "accelerators": [
      {
        "vendor": "nvidia",
        "model": "H100",
        "count": 2
      }
    ]
  },
  "constraints": {
    "poolSelector": {
      "capability": "gpu.nvidia.h100"
    },
    "maxDurationSeconds": 7200
  },
  "authorization": {
    "permitRef": "permit-001",
    "actionHash": "sha256:..."
  }
}
~~~

Contract notes:

- requesterRef is an opaque, tenant-scoped reference owned by the caller. Compute CP does not resolve it by querying Agent Runtime.
- RunIntent is the proposed Agent Control Plane resource name. The current Agent Control Panel baseline and Agent Runtime README disagree about Run identity ownership. Until the two repositories resolve that conflict, requesterRef.resourceType is an opaque versioned string and the cross-repository golden path is BLOCKED. Do not silently substitute execution_id.
- tenantId is taken from authenticated context. projectId must belong to that tenant.
- Exactly one of profileRef or a fully specified resources object may be supplied; if both are supplied, resources must be a documented override, not an implicit merge.
- The caller does not select provider_id or provider_resource_id in the normal request. Provider/pool selection is a Compute CP decision constrained by policy.
- Idempotency-Key is generated once per intent and reused for every retry. It must not include an attempt number or timestamp.

Response:

~~~http
202 Accepted
Location: /api/v1/compute-workloads/cw-001
~~~

~~~json
{
  "workloadId": "cw-001",
  "generation": 1,
  "phase": "ACCEPTED",
  "operationId": "op-001",
  "statusUrl": "/api/v1/compute-workloads/cw-001"
}
~~~

The response confirms durable acceptance of the request, not provider creation, Kueue admission, Pod scheduling or execution readiness.

## 3. Read and release operations

~~~http
GET /api/v1/compute-workloads/{workloadId}
GET /api/v1/compute-workloads/{workloadId}/allocations
GET /api/v1/compute-operations/{operationId}
DELETE /api/v1/compute-workloads/{workloadId}
~~~

DELETE records desired absence and returns 202 while release is pending. It is idempotent. It does not report Released merely because the delete command or Kubernetes API accepted a deletion request.

The caller polls or consumes events until the workload is RELEASED, REJECTED or FAILED_CONFIRMED. UNKNOWN remains non-terminal and must be reconciled.

## 4. Allocation reference

~~~json
{
  "allocationId": "alloc-001",
  "workloadId": "cw-001",
  "providerId": "provider-001",
  "providerResourceId": "k8s-job-uid-opaque",
  "providerResourceRefs": [
    {
      "resourceKind": "kubernetes.job",
      "providerResourceId": "k8s-job-uid-opaque",
      "namespace": "tenant-001",
      "name": "cw-001-g1"
    },
    {
      "resourceKind": "kueue.workload",
      "providerResourceId": "kueue-workload-uid-opaque",
      "namespace": "tenant-001",
      "name": "cw-001-g1"
    }
  ],
  "poolId": "pool-gpu-east",
  "generation": 1,
  "phase": "BOUND",
  "observedAt": "2026-11-02T10:00:00Z"
}
~~~

- allocationId is the stable platform reference consumed by Agent Control Plane and Runtime.
- providerId is the stable Compute CP registration identity.
- providerResourceId is an opaque external identifier and is only meaningful with providerId, resourceKind and provider scope. It may be absent before the provider object is discovered.
- providerResourceRefs records auxiliary provider objects. Kueue Workload references are observations created/owned by Kueue, not resources Compute CP may patch.
- generation binds the Allocation to the exact ComputeWorkload desired generation.

Runtime consumes allocationId and, only when needed, provider-neutral capability/endpoint references. Runtime must not dispatch provider Create/Delete or infer allocation state from a providerResourceId.

## 5. Error contract

All errors use one envelope:

~~~json
{
  "error": {
    "code": "IDEMPOTENCY_KEY_PAYLOAD_MISMATCH",
    "message": "The idempotency key was already used with a different request.",
    "retryable": false,
    "correlationId": "corr-001"
  }
}
~~~

Minimum semantics:

| Condition | HTTP | Required behavior |
|---|---:|---|
| Invalid schema/resource request | 422 | No provider side effect |
| Unauthenticated | 401 | No resource disclosure |
| Tenant/project mismatch | 403 | Do not reveal cross-tenant resource existence |
| Resource not found in caller scope | 404 | No cross-tenant lookup leak |
| Same key, different canonical request hash | 422 | Reject; never replay a different request |
| Same key, request still in flight | 202 or 409 | Return same resource/status reference or bounded retry guidance; never dispatch a second operation |
| Stale generation / resource version conflict | 409 | Caller must re-read and submit a new intent if needed |
| Provider outcome ambiguous | 202 | Preserve UNKNOWN and return operation status URL; do not report failure |

## 6. Idempotency and operation identity

- idempotency_key is stable across retries of one caller intent.
- request_hash is computed from canonical validated request content, excluding transport-only trace headers.
- operation_id identifies one immutable provider-side action and remains stable across retries/reconciliation.
- attempt_id identifies one dispatch attempt and changes per attempt.
- action_hash binds the protected action to the Permit. Its canonicalization algorithm is owned by the cross-repository authorization contract; Compute CP must not invent a private incompatible algorithm.
- desired_generation fences the operation to the ComputeWorkload version it was created for.
- Reusing an idempotency key with a different request_hash is a hard conflict.
- The idempotency record must outlive the maximum redelivery/retry/reconciliation window. No fixed TTL is assumed in v1.

## 7. Event envelope

All events use an at-least-once delivery envelope:

~~~json
{
  "eventId": "evt-001",
  "eventType": "compute.allocation.bound.v1",
  "schemaVersion": "1",
  "aggregateType": "ComputeWorkload",
  "aggregateId": "cw-001",
  "tenantId": "tenant-001",
  "generation": 1,
  "occurredAt": "2026-11-02T10:00:00Z",
  "correlationId": "corr-001",
  "causationId": "cmd-001",
  "traceparent": "00-...",
  "payload": {}
}
~~~

Consumers deduplicate by eventId and must also reject stale aggregate generations. Events are published through a transactional outbox; consumers use durable inbox/deduplication. Delivery is at least once, not exactly once.

Initial event types:

| Event | Meaning |
|---|---|
| compute.workload.accepted.v1 | Request and desired generation durably accepted |
| compute.workload.rejected.v1 | Request rejected before provider side effect |
| compute.allocation.bound.v1 | Provider resource binding verified |
| compute.allocation.observation-updated.v1 | New timestamped provider/Kubernetes/Kueue observation |
| compute.allocation.releasing.v1 | Desired absence accepted; release not yet proven |
| compute.allocation.released.v1 | Authoritative absence and applicable quota release verified |
| compute.operation.unknown.v1 | Dispatch outcome ambiguous; reconciliation required |
| compute.operation.reconciled.v1 | UNKNOWN resolved with evidence |
| compute.operation.manual-review.v1 | Automated reconciliation cannot safely decide |

An observation event is not a substitute for the terminal allocation event. Consumers must not interpret an event name or transport delivery as proof beyond its payload and source evidence.

## 8. Cross-repository dependency

Before the golden path is enabled:

1. Agent Control Panel and Agent Runtime must agree on one canonical upstream business Run reference and one Runtime execution reference.
2. Agent Control Panel sends the stable caller-owned reference in requesterRef; Compute CP treats it as opaque.
3. Runtime receives allocationId and does not create or mutate ComputeWorkload/Allocation.
4. EngageGuard and Compute CP must agree on Permit reference, action_hash canonicalization, audience, expiry and replay semantics.
5. OpenAPI/JSON Schema and event schema compatibility tests must run against all consumers.

Until those dependencies are resolved, this document defines the Compute CP boundary but does not claim cross-repository compatibility has passed.
