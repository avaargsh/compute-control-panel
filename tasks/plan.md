# Compute Control Plane Plan

## Phase 0 — Contract freeze

1. Freeze ComputeProvider, Pool, Profile, Workload, Allocation and ResourceObservation ownership.
2. Freeze Workload v1 OpenAPI contract and event envelope.
3. Freeze ComputeOperation journal states, lease/fencing and provider inspect semantics.
4. Freeze Kueue admission vs Kubernetes scheduling vs provider/device realization.
5. Define FakeProvider contract suite before any real provider adapter.

## Phase 1 — Go foundation and persistence

6. Go API/reconciler process modes, config validation, health, telemetry and shutdown.
7. PostgreSQL migrations and tenant-scoped resource ownership.
8. Idempotency key + canonical request digest; same key/different digest is 409.
9. Transactional Outbox and Inbox; aggregate-version duplicates/gaps.
10. Workload create/get/list/delete-intent API; no provider call in HTTP request path.

## Phase 2 — Durable reconciliation

11. ComputeOperation journal with stable operation ID, request digest and desired generation.
12. Durable leases, lease expiry and fencing token; stale workers cannot commit.
13. UNKNOWN → RECONCILING → resolved / retry-same-operation / MANUAL_REVIEW.
14. ResourceObservation history and fresh-inspect projection updates.
15. Stale generation compensation and deletion finality by exact resource UID.

## Phase 3 — Provider proof

16. FakeProvider create/inspect/delete and contract tests.
17. Lost ACK, wrong operation ID, wrong generation, wrong resource UID, restart and concurrent claim tests.
18. Prove no duplicate provider resource on retry; no release until exact absence is verified.

## Phase 4 — Kubernetes integration

19. Kubernetes Job adapter using provider-neutral Workload contract.
20. Kueue queue/admission/quota integration.
21. Kubernetes scheduler and device plugin/DRA observations.
22. Real-cluster POC and operational runbook; capability-gate each GPU backend.

## Exit gate

No Workload is considered admitted/running/released based solely on an API acknowledgement. Restart resumes from durable intent. Ambiguous provider effects are inspected before retry. No custom scheduler is introduced.
