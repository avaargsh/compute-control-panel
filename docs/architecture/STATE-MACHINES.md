# Compute State Machines and Recovery Semantics

Status: Proposed v0.2  
Date: 2026-11-02

## 1. Keep three state axes separate

Compute CP persists three different kinds of facts. They must not be collapsed into one status enum.

1. ComputeWorkload/Allocation lifecycle: the platform's accepted desired resource lifecycle.
2. ComputeOperation lifecycle: whether one external side effect was recorded, dispatched and resolved.
3. ResourceObservation: what an authoritative external system reported at a specific time.

Kubernetes Job status, Kueue admission conditions and Agent Run/Execution status remain external projections. A ComputeWorkload does not become RUNNING merely because an Agent Runtime Run is running.

## 2. ComputeWorkload lifecycle

The workload phase describes the platform resource request:

~~~text
REQUESTED
   |
   v
ACCEPTED
   |
   v
PROVISIONING
   |
   v
ALLOCATED
   |
   v
RELEASING
   |
   v
RELEASED
~~~

Pre-side-effect rejection: REQUESTED -> REJECTED.  
Confirmed provisioning failure: PROVISIONING -> FAILED_CONFIRMED.  
REJECTED is reserved for validation/policy rejection before any external dispatch. If a Kubernetes Job or other control object was created and Kueue/provider later gives a definitive negative outcome, use FAILED_CONFIRMED only after all objects created for that attempt and any queue reservation are cleaned up or authoritatively absent. A provider timeout after dispatch does not justify FAILED_CONFIRMED.

Definitions:

| Phase | Meaning | Terminal |
|---|---|---|
| REQUESTED | Validated command is being persisted/claimed | No |
| ACCEPTED | Desired state and outbox record are durable | No |
| PROVISIONING | Provider/Kubernetes operation is in progress or being reconciled | No |
| ALLOCATED | A usable provider resource binding is verified | No |
| RELEASING | Desired absence is durable; deletion/cleanup is being reconciled | No |
| RELEASED | Provider resource absence and applicable queue/quota release are verified | Yes |
| REJECTED | Request failed validation/policy before any external dispatch | Yes |
| FAILED_CONFIRMED | Provider/Kueue evidence confirms the requested allocation did not materialize and all objects/reservations created for that attempt are cleaned up or authoritatively absent | Yes |

There is no workload-level UNKNOWN terminal state. UNKNOWN belongs to ComputeOperation and ResourceObservation. While a create operation is UNKNOWN, the workload remains PROVISIONING. While a delete operation is UNKNOWN, it remains RELEASING. This prevents an ambiguous external effect from being mislabeled as failure or success.

## 3. Allocation lifecycle

An Allocation is the durable binding for one workload generation:

~~~text
PENDING -> BOUND -> RELEASING -> RELEASED
    |          |
    |          +-> RELEASING while release is UNKNOWN
    |
    +-> REJECTED (no external dispatch)
    +-> FAILED_CONFIRMED (definitive negative outcome + cleanup verified)
~~~

- PENDING: Allocation identity exists, but no verified provider binding exists.
- BOUND: provider_id and the required provider resource reference set are verified.
- RELEASING: release intent is durable; the external resource may still exist.
- RELEASED: all required provider resources are authoritatively absent and relevant queue reservation is no longer held.
- REJECTED: validation/policy rejection before any external dispatch.
- FAILED_CONFIRMED: a provider/Kueue attempt definitively did not materialize the requested allocation and cleanup/absence has been verified.

A new placement/reallocation creates a new allocation_id. Do not mutate an old Allocation to point at a different provider resource.

## 4. ComputeOperation lifecycle

~~~text
INTENT_RECORDED
      |
      v
   CLAIMED
      |
      v
  DISPATCHED
      |
      +---------------------> ACKNOWLEDGED
      |                            |
      |                            v
      |                       RECONCILING
      |                            |
      |                            v
      |                         SUCCEEDED
      |
      +-> timeout / lost ACK / worker crash
                   |
                   v
                UNKNOWN
                   |
                   v
              RECONCILING
                /     \
               v       v
     SUCCEEDED     FAILED_CONFIRMED
               \
                v
          MANUAL_REVIEW
~~~

CANCELLED_BEFORE_DISPATCH is allowed only while no external request could have been sent. Once DISPATCHED, cancellation is a new compensating operation; it cannot erase the original operation.

| State | Meaning |
|---|---|
| INTENT_RECORDED | Immutable action and request digest durably stored before external I/O |
| CLAIMED | A worker holds a durable lease/fencing epoch |
| DISPATCHED | External call may have been sent |
| ACKNOWLEDGED | Provider returned a response; response is not automatically proof of final state |
| UNKNOWN | Side effect may have occurred but no trusted outcome is available |
| RECONCILING | Adapter is inspecting authoritative provider state or a provider idempotency token |
| SUCCEEDED | The intended effect is confirmed by provider receipt plus required observation |
| FAILED_CONFIRMED | Provider evidence confirms the effect did not occur and no target resource exists |
| MANUAL_REVIEW | Automated resolution is unsafe or provider evidence is insufficient |
| CANCELLED_BEFORE_DISPATCH | Operation was cancelled while it was provable that no external request had been sent |

## 5. Resource observations

Observations use a separate provider-facing vocabulary:

~~~text
NOT_FOUND | PENDING | ADMITTED | SCHEDULED | RUNNING |
READY | DEGRADED | DELETING | TERMINAL | UNKNOWN
~~~

Every observation records:

- provider_id and provider_resource_id;
- resource kind and provider scope;
- observed_at and source timestamp when available;
- Kubernetes resourceVersion/UID or provider revision/ETag when available;
- source system and adapter version;
- freshness/expiry policy;
- raw response digest or evidence reference, with secrets removed.

NOT_FOUND is a point-in-time observation, not automatically a release decision. For eventually consistent providers, the adapter must apply the provider-specific absence-verification policy. UNKNOWN means the source could not provide a trustworthy observation.

## 6. UNKNOWN recovery rules

### Create

1. Persist ComputeOperation and Allocation identity before dispatch.
2. Dispatch using the stable operation_id as the provider idempotency/client token when supported.
3. If the response is lost, set operation UNKNOWN and keep workload PROVISIONING / allocation PENDING.
4. Inspect by the provider token or deterministic external reference.
5. If the resource exists and matches the immutable action hash, bind its provider_resource_id and resolve SUCCEEDED.
6. If the provider proves no resource was created, resolve FAILED_CONFIRMED or safely re-dispatch the same operation.
7. If the provider cannot prove either outcome, keep UNKNOWN or move to MANUAL_REVIEW. Never create a new operation merely to escape uncertainty.

### Delete

1. Persist desired absence and a new delete operation before dispatch.
2. Set workload/allocation to RELEASING.
3. If ACK is lost, set operation UNKNOWN and inspect the same provider resource.
4. If it exists, continue deletion/reconciliation using the same operation identity and provider idempotency semantics.
5. If it is authoritatively absent, verify any Kueue reservation/associated resources have been released, then resolve SUCCEEDED and mark RELEASED.
6. If the provider is unreachable or absence is not authoritative, retain RELEASING/UNKNOWN and retain accounting/quota conservatively.

Delete ACK, HTTP 202, Kubernetes deletionTimestamp or a local timeout is not release proof.

## 7. Concurrency and generation fencing

Every desired-state mutation increments desired_generation. Every operation stores the generation and immutable action_hash it was created for.

- A worker claims an operation with a monotonically increasing lease_epoch/fencing token.
- A worker must re-check the current generation and claim before committing a state transition.
- A stale worker may append its provider receipt/observation as evidence, but cannot overwrite the current workload/allocation projection.
- A newer generation does not erase an older UNKNOWN operation. The old operation must be reconciled before a conflicting external mutation is dispatched.
- A payload change creates a new operation_id and action_hash. A retry of the same payload reuses them.
- Database uniqueness constraints, not read-then-write checks, enforce idempotency and one active Allocation per Workload.

Provider APIs that do not support idempotency or conditional writes require stricter serialization. If the result of a non-idempotent call is ambiguous, do not dispatch a second call until authoritative inspection or manual resolution.

## 8. Cancellation and release

Cancellation of an Agent Run is not proof that compute resources are released. Agent Runtime owns execution cancellation; Compute CP receives a release command/reference and reconciles the allocation independently.

A ComputeWorkload may be marked RELEASED only when:
- all required provider resources are authoritatively absent;
- any provider-specific reservation/lease is released;
- Kueue no longer holds the corresponding admission/quota reservation, where applicable;
- no in-flight operation can recreate the resource for the released generation;
- final evidence is persisted and linked to the Allocation.

If finalizers, provider cleanup or admission checks remain pending, keep RELEASING.

## 9. Required invariants

1. No provider mutation without a durable operation intent.
2. No blind retry after an ambiguous side effect.
3. No terminal success from an ACK alone.
4. No RELEASED state without authoritative absence evidence.
5. No stale generation may mutate current state.
6. No cross-tenant reference may be resolved.
7. No Agent Run/Execution state is inferred from ComputeWorkload state.
8. No Kueue quota reservation is maintained as a second Compute CP truth.
