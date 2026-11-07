# Compute Control Plane Reliability Acceptance Matrix

Status: Required test plan; tests not yet implemented or executed  
Date: 2026-11-02

## 1. Evidence levels

Every acceptance result must be labeled accurately:

1. Contract defined: schema, ownership and expected behavior are documented.
2. Unit/contract test passed: deterministic local test passed.
3. Adapter integration passed: fake or test provider contract passed.
4. Cluster integration passed: real Kubernetes/Kueue environment produced inspectable evidence.
5. Production behavior observed: deployed system metrics/logs/evidence confirm behavior over an operational window.

A passing FakeProvider test is not evidence of real provider behavior. A successful Kubernetes POC is not production acceptance.

## 2. Required test matrix

| ID | Layer | Scenario | Required assertion |
|---|---|---|---|
| C01 | Schema | Valid/invalid ComputeWorkload request | Versioned schema accepts valid inputs and rejects unknown/invalid capability combinations |
| C02 | Authorization | Missing, expired or mismatched Guard Permit/action_hash/request digest | Protected Agent-originated compute mutation fails closed before provider dispatch; internal Run capacity requires authenticated service identity and Compute policy |
| C03 | Tenant isolation | Cross-tenant workload/allocation/provider reference | Denied without disclosing resource existence |
| C04 | Idempotency | Concurrent duplicate create with same key and same body | One ComputeWorkload, one operation intent and at most one active Allocation |
| C05 | Idempotency | Same key with different canonical request digest | 409 IDEMPOTENCY_KEY_REUSED; no second side effect |
| C06 | Inbox/outbox | Duplicate event delivery and consumer restart | Event is applied once logically; stale generations are ignored |
| C07 | Create recovery | Provider creates resource but ACK is lost | Operation becomes UNKNOWN; Inspect finds and binds the existing resource; no duplicate create |
| C08 | Create recovery | Provider proves request did not take effect | Operation RECONCILING → PENDING before safely redispatching the same immutable intent; definitive failure resolves operation FAILED, and only after verified cleanup/absence may Workload/Allocation project FAILED_CONFIRMED |
| C09 | Ambiguity | Provider unavailable after dispatch | UNKNOWN remains non-terminal; no blind retry and no false failure |
| C10 | Delete recovery | Provider deletes resource but ACK is lost | Operation becomes UNKNOWN; Inspect verifies absence before RELEASED |
| C11 | Release finality | Delete ACK/HTTP 202/Kubernetes deletionTimestamp only | Allocation remains RELEASING until authoritative absence and applicable quota release |
| C12 | Generation fencing | Old worker returns after newer desired generation | Old receipt is retained as evidence but cannot overwrite current projection |
| C13 | Crash recovery | Process crashes after dispatch and before receipt persistence | Restart loads durable operation and reconciles before another side effect |
| C14 | Lease fencing | Two workers claim/execute the same operation | One valid lease epoch may commit; stale worker cannot commit state |
| C15 | Kueue queueing | Queue has insufficient quota | Job remains queued/suspended; Compute CP does not invent an admission result or create a duplicate Kueue Workload |
| C16 | Kueue admission | Quota becomes available | Kueue admits its native Workload; Job integration resumes Job; Job controller creates Pods |
| C17 | Scheduler boundary | Pod placement | Kubernetes Scheduler selects Node; Compute CP does not set NodeName or implement placement |
| C18 | Kueue release | Job deletion and queue cleanup | Associated native Workload/reservation is cleaned up by Kueue; release is not reported early |
| C19 | Provider identity | Same provider_resource_id under two providers/scopes | References remain distinct by provider_id, resource kind and scope |
| C23 | Journal boundary | Guard action delegates to Compute provider mutation | Guard server-generated operation_id and Compute compute_operation_id remain distinct; each journal reconciles only its own effect |
| C24 | Delegated Permit | Permit valid for action but mismatched Compute request digest/audience | Compute CP rejects before provider dispatch; action_hash is not recomputed by Compute CP |
| C20 | Cancellation race | Release arrives while create is UNKNOWN | No conflicting delete/create is dispatched until the create outcome is reconciled or safely fenced |
| C21 | Cross-repo contract | RunIntent -> Runtime Run -> ComputeWorkload -> AllocationRef | Runtime owner_ref uses run_id, not run_intent_id; stable mapping and allocation_id survive retries/restarts; Runtime never dispatches provider mutations |
| C22 | Sandbox boundary | Sandbox claim and Compute capacity request retried | SandboxProvider is the sole Compute caller for a claim; exactly one Workload/Allocation exists and SandboxProvider consumes the Allocation |

## 3. FakeProvider contract

The FakeProvider must support deterministic fault injection at these points:

- fail before side effect;
- apply side effect then drop response;
- delay response beyond caller deadline;
- return stale resource version;
- return inconsistent observation;
- become unavailable during create or delete;
- return resource already absent;
- return same external name with a different immutable UID;
- reject an idempotency token reused with a different request digest.

The conformance suite must assert provider call count, operation state, request/action digest, allocation mapping and persisted evidence after restart.

## 4. Kubernetes/Kueue integration evidence

The cluster POC must retain:
- Kubernetes and Kueue versions;
- Job manifest and immutable UID;
- corresponding Kueue Workload name/UID and admission conditions;
- LocalQueue/ClusterQueue/ResourceFlavor references;
- Pod UID, NodeName and terminal status;
- resourceVersion and observation timestamps;
- deletion/finalizer timeline;
- proof that Kueue reservation is no longer held before Allocation becomes RELEASED.

The test must also prove that Compute CP does not create or patch the Kueue-native Workload, admission conditions, Pods or Node bindings.

## 5. Release gate

The architecture may enter implementation review only when:
- RunIntent/Run identity mapping is contractually aligned and its Runtime idempotency/lookup tests are implemented;
- API/event schemas are committed and linted;
- operation state machine and idempotency rules have contract tests;
- FakeProvider create/inspect/delete conformance passes;
- UNKNOWN, lost ACK, crash recovery and release-finality tests pass;
- Kubernetes Job + Kueue integration ownership is unambiguous;
- no test claims real provider or production acceptance without corresponding evidence.

The end-to-end golden path remains blocked until Runtime's RunIntent mapping, SandboxProvider single-requester rule, and Guard Permit-to-Compute request digest attestation have executable contract evidence.
