# Compute Control Plane Plan

## Phase 0 — Contract freeze

1. Freeze ComputeProvider, Pool, Profile, ComputeWorkload, Allocation and ResourceObservation ownership.
2. Freeze Workload v1 OpenAPI contract and event envelope.
3. Freeze ComputeOperation journal states, lease/fencing and provider inspect semantics.
4. Freeze Kueue admission vs Kubernetes scheduling vs provider/device realization.
5. Define Run owner_ref and SandboxClaim owner_ref; enforce exactly one Compute requester per capacity claim.
6. Define the Guard Permit-to-Compute request digest attestation; fail closed until verified.
7. Define FakeProvider contract suite before any real provider adapter.

## Phase 1 — Go foundation and persistence

8. Go API/reconciler process modes, config validation, health, telemetry and shutdown.
9. PostgreSQL migrations and tenant-scoped resource ownership.
10. Idempotency key + canonical request digest; same key/different digest is 409.
11. Transactional Outbox and Inbox; aggregate-version duplicates/gaps.
12. Workload create/get/list/delete-intent API; no provider call in HTTP request path; POST must use the composite `AcceptWorkload` transaction.

## Phase 2 — Durable reconciliation

13. ComputeOperation journal with stable operation ID, request digest and desired generation.
14. Durable leases, lease expiry and fencing token; stale workers cannot commit.
15. UNKNOWN → RECONCILING → resolved / retry-same-operation / MANUAL_REVIEW.
16. ResourceObservation history and fresh-inspect projection updates.
17. Stale generation compensation and deletion finality by exact resource UID.

## Phase 3 — Provider proof

18. FakeProvider create/inspect/delete and contract tests.
19. Lost ACK, wrong operation ID, wrong generation, wrong resource UID, restart and concurrent claim tests.
20. Prove no duplicate provider resource on retry; no release until exact absence is verified.

## Phase 4 — Kubernetes integration

21. Kubernetes Job adapter using provider-neutral Workload contract.
22. Kueue queue/admission/quota integration.
23. Kubernetes scheduler and device plugin/DRA observations.
24. Real-cluster POC and operational runbook; capability-gate each GPU backend.

## Exit gate

No Workload is considered admitted/running/released based solely on an API acknowledgement. Restart resumes from durable intent. Ambiguous provider effects are inspected before retry. No custom scheduler is introduced.
