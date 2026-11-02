# Compute Control Plane Plan

## Phase 0: architecture and contracts — current gate

1. Freeze domain ownership and one-writer boundaries for Provider, Pool, ComputeWorkload, Allocation and ComputeOperation.
2. Freeze the distinction between platform ComputeWorkload and Kueue-native Workload.
3. Define versioned ComputeWorkload API, AllocationRef, operation status and event envelope.
4. Define desired-state, operation and observation state machines separately.
5. Define Kubernetes Job + Kueue built-in Job integration as the v0.1 POC path; no second Kueue Workload writer.
6. Resolve or explicitly isolate the Agent Control Panel / Agent Runtime Run identity conflict.
7. Align SandboxProvider claim/provision behavior with Compute Allocation ownership.
8. Align EngageGuard Permit/action_hash canonicalization and replay semantics.
9. Define FakeProvider conformance and reliability acceptance matrix.

Phase 0 exit: all contracts are reviewable; cross-repository blockers are either resolved with coordinated ADRs or isolated behind opaque versioned references. No scheduler/provider implementation is part of this phase.

## Phase 1: Go and persistence foundation

10. Go API bootstrap, config validation, health/readiness, telemetry and graceful shutdown.
11. PostgreSQL schema and migrations for Provider, Pool, Profile, Workload, Allocation, Operation, observations and idempotency.
12. Atomic idempotency claim, request-hash mismatch rejection and transactional outbox/inbox.
13. Workload create/get/list/delete-intent APIs and operation status API.
14. Tenant/project authorization at every resource boundary.

## Phase 2: operation journal and reconciliation

15. Durable operation claims, leases, fencing epochs and desired-generation checks.
16. FakeProvider create/inspect/delete contract adapter.
17. UNKNOWN recovery, crash recovery and manual-review path.
18. Provider resource identity mapping and append-only observation evidence.

## Phase 3: Kubernetes/Kueue proof

19. Kubernetes Job adapter using the supported Kueue Job integration path.
20. Read-only Kueue Workload, LocalQueue, ClusterQueue and ResourceFlavor observation.
21. Admission, insufficient quota, requeue, Pod placement and completion tests.
22. Delete, finalizer, quota-release and release-finality tests.

## Phase 4: provider expansion

23. Add a second provider only after the first adapter passes the same conformance suite.
24. Add DRA, HAMi, vendor GPU and cloud-specific capabilities behind explicit capability/version gates.
25. Add metering and usage projections only after authoritative usage sources and billing semantics are defined.

## Exit gate

- No duplicate provider resource on retry.
- No blind retry after UNKNOWN.
- No RELEASED before authoritative absence and applicable quota release.
- Restart resumes from durable intent and operation evidence.
- No stale generation overwrites current state.
- No cross-tenant access.
- No second scheduler or competing Kueue Workload writer.
- Real-cluster and production evidence are labeled separately from FakeProvider/unit results.
