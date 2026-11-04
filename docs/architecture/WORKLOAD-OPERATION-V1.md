# Workload and Provider Operation Contract v1

Status: Proposed v0.2 — implementation gate

## 1. Resource model

### ComputeProvider
A registered adapter endpoint and trust boundary. Exposes capabilities, health and provider-specific configuration references. Secret material is resolved by the credential authority, not stored in provider manifests.

### ComputePool
A logical capacity pool owned by one provider/cluster/region. Contains capacity observations and policy bindings. It is not a scheduler.

### ComputeProfile
A versioned provider-neutral resource requirement (CPU, memory, accelerator type/count/memory, storage, placement constraints and capability requirements). It contains no provider resource ID or credential.

### Workload
A durable request for compute capacity, owned by Compute Control Plane. Required fields:
- workload_id, tenant_id, project_id
- owner_ref: source service + source resource type + source resource ID (e.g. agent-runtime/run/<run_id>)
- compute_profile_ref and immutable request digest
- idempotency key, desired_generation and version
- desired_state: ACTIVE | DELETE_REQUESTED
- observed_generation, observed_phase, allocation_id and evidence references

The caller's tenant/principal is taken from verified service identity; body fields are checked against that identity and cannot grant access.

### Allocation
The selected provider/pool and provider resource mapping for a Workload. It is owned by Compute Control Plane. It does not become the canonical Run or Sandbox identity.

### ComputeOperation
A single provider mutation. Its compute_operation_id is stable for the logical mutation and is passed as provider idempotency/correlation metadata when supported. It stores tenant/workload/provider, operation kind, desired_generation, provider_request_digest, target resource ID/UID, lease/fencing, outcome and evidence, plus an optional Guard operation reference.

### ResourceObservation
An append-only provider observation with observed_at, provider ID, workload/operation correlation, generation, provider resource ID/UID, provider resourceVersion/sequence, result and provenance. It is not an operation status.

## 2. Workload desired and observed state

Desired state is controlled by this service:
```text
ACTIVE → DELETE_REQUESTED
```

Observed phase is a projection from Kubernetes/Kueue/provider:
```text
PENDING | ADMISSION_PENDING | ADMITTED | RUNNING
SUCCEEDED | FAILED | TERMINATING | RELEASED | UNKNOWN
```

An observation does not mutate desired state. Provider watch events are wake-up hints; the reconciler performs a fresh inspect before updating the projection. An older desired_generation cannot overwrite a newer Workload generation.

## 3. ComputeOperation state

```text
PENDING → LEASED → IN_FLIGHT → SUCCEEDED | FAILED
IN_FLIGHT → UNKNOWN → RECONCILING
RECONCILING → SUCCEEDED | PENDING | FAILED | MANUAL_REVIEW | UNKNOWN
LEASED / IN_FLIGHT / RECONCILING -- lease expiry --> UNKNOWN
```

- Lease claim increments a fencing token and uses a conditional version update.
- A worker must present owner + fencing token + expected version to commit an outcome.
- A stale token or expired lease is rejected.
- UNKNOWN can only enter RECONCILING; it cannot be directly re-issued.
- RECONCILING → PENDING is allowed only after provider operation lookup authoritatively proves no effect was applied. The exact compute_operation_id, provider idempotency token and provider_request_digest are reused.
- A contradictory observation, identity mismatch, unsupported provider ambiguity or partial side effect enters MANUAL_REVIEW.
- A stale-generation CREATE that created a resource is marked SUPERSEDED and emits a compensating delete intent; it must not update the current Workload projection.

## 4. Provider observation requirements

Every inspection result is correlated to:
- exact compute_operation_id;
- exact provider_id;
- tenant_id and workload_id;
- desired_generation;
- target provider_resource_id and UID for DELETE;
- provider resourceVersion/sequence and observed_at.

CREATE/UPDATE is verified only when the provider resource identity is present and provider metadata correlates to compute_operation_id + desired_generation. A name match alone is insufficient.

DELETE is verified only when the exact target resource UID is absent. A successful delete response, a missing resource by reused name, or a generic empty list is not release evidence.

If a provider cannot supply an authoritative operation lookup, idempotency guarantee, or exact resource identity, an ambiguous operation remains UNKNOWN/MANUAL_REVIEW. The controller does not invent exactly-once behavior.

## 5. HTTP API v1

| Method | Path | Semantics |
|---|---|---|
| POST | /api/v1/workloads | Idempotent create through `AcceptWorkload`; atomically commit Workload, initial PENDING Allocation, bound CREATE ComputeOperation and both dispatchable Outbox events; return 202 |
| GET | /api/v1/workloads/{id} | Tenant-scoped desired/observed state and evidence refs |
| GET | /api/v1/workloads | Cursor pagination, bounded page size |
| DELETE | /api/v1/workloads/{id} | Set DELETE_REQUESTED and return 202; not a release acknowledgement |

All mutating requests require Idempotency-Key. The server computes a canonical request digest. Same key + same digest returns the original Workload, Allocation and initial ComputeOperation identities; same key + different digest returns 409 before provider dispatch. The public create path must call `Repository.AcceptWorkload`, which commits the tenant-scoped idempotency claim, Workload, initial PENDING Allocation, `latest_allocation_id` projection, Allocation-bound CREATE ComputeOperation, `compute.workload.accepted.v1` and `compute.operation.pending.v1` Outbox events in one PostgreSQL transaction. The returned operation identity is therefore already durable. No provider call runs inside the HTTP request transaction. Low-level `CreateWorkload` and `CreateOperation` remain storage primitives and emit non-dispatchable `recorded` events; they are not the public acceptance boundary.

## 6. Kueue and Kubernetes boundary

```text
Compute Workload intent
  → Compute reconciler creates/updates Kubernetes Job or Workload
  → Kueue queue admission and quota admission
  → Kubernetes scheduler places Pods
  → device plugin / DRA / provider realizes devices
  → Compute adapter inspects actual provider state
  → ResourceObservation + Workload projection
```

Kueue is not the Pod scheduler. Compute Control Plane selects provider/pool and translates intent; it does not replace Kueue, Kubernetes scheduler, device plugins, DRA or provider APIs.

## 7. Contract test matrix

- Same idempotency key and digest creates one Workload; different digest returns 409.
- Outbox is atomic with Workload; duplicate Inbox delivery is harmless.
- Aggregate version duplicate ignored; version gap parked/retried.
- Concurrent claims yield one lease owner/fencing token.
- Expired lease makes operation UNKNOWN; stale worker cannot commit.
- UNKNOWN always enters RECONCILING before retry.
- Provider inspect wrong operation/provider/workload/generation is rejected.
- CREATE requires operation/generation labels and resource UID.
- DELETE requires exact target UID absence; ACK alone cannot release Allocation.
- Stale generation cannot update current Workload; stale CREATE schedules cleanup.
- Restart after mutation-before-ACK recovers by inspect, not blind create.
- Tenant and project boundary tests cover every read/write.


## 8. Agent and Guard integration

- For non-sandbox Run capacity, Runtime is the sole Compute API caller and owner_ref references agent-runtime/run/<run_id>.
- For sandbox capacity, Runtime calls SandboxProvider Claim; SandboxProvider is the sole Compute API caller for that claim and owner_ref references sandbox-provider/sandbox-claim/<claim_id>.
- A protected Agent-originated compute action is authorized by EngageGuard. Compute CP validates a Permit for its audience and the exact normalized Compute request digest before accepting the provider mutation. Caller-supplied action_hash is opaque and is never recomputed by Compute CP.
- EngageGuard Action Operation ID and Compute compute_operation_id are distinct. Guard owns authorization/action receipt; Compute CP owns provider dispatch and UNKNOWN reconciliation.
- A platform-internal Runtime capacity request is authorized by workload identity and Compute policy; it does not create a duplicate Guard Operation.
