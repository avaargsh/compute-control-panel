# Compute Control Plane Tasks

## Architecture and contract gate

- [x] Expand compute domain ownership and non-ownership boundaries.
- [x] Separate ComputeWorkload from Kueue-native Workload.
- [x] Define Kubernetes Job + Kueue Job integration ownership for the v0.1 POC.
- [x] Define workload, allocation, operation and observation state semantics.
- [x] Define versioned API/event contract and provider-scoped resource references.
- [x] Add reliability acceptance matrix; mark tests as not yet implemented.
- [ ] Resolve canonical Run identity ownership between agent-control-panel and agent-runtime.
- [ ] Align SandboxProvider claim/provision with Compute Allocation ownership.
- [ ] Align EngageGuard Permit/action_hash schema and replay semantics.
- [x] Add OpenAPI 3.1 + JSON Schema Draft 2020-12 source contracts and CI-backed golden validation tests (PR #12); cross-version compatibility checks and cross-repository consumer tests remain open.
- [x] Obtain independent architecture review; keep PR Draft until blockers are resolved.

## Implementation

- [ ] Go API bootstrap, configuration, health/readiness, telemetry and graceful shutdown.
- [ ] PostgreSQL migrations and repositories for Provider, Pool, Profile, Workload, Allocation and Operation.
- [ ] Atomic idempotency claim and canonical request-hash validation.
- [ ] Transactional outbox/inbox and consumer deduplication.
- [ ] Durable operation leases, fencing epochs and desired-generation checks.
- [ ] FakeProvider create/inspect/delete conformance suite.
- [ ] UNKNOWN, lost-ACK, process-restart and manual-review recovery tests.
- [ ] Kubernetes Job adapter with Kueue built-in Job integration.
- [x] Read-only Kueue Job `Collector` and in-memory `FakeCollector` (#33 slice): return `KueueJobEvidence` plus provenance only when tenant, workload, generation, namespace, Job name, and Job UID match; identity mismatch returns `ErrInvalidKueueObservation` and `UNPROVEN` with no evidence. No Kubernetes client, Kubernetes write, Allocation READY mutation, or device-assignment claim. The existing HTTPS KubernetesObservationReader must implement Collector with explicitly unattested provenance; trusted live cluster, write adapter and release gates remain open.
- [ ] Kueue admission, queueing, requeue, Pod placement and quota-release integration tests.
- [ ] Provider expansion only after first adapter conformance passes.


## Cross-repository gates

- [ ] Runtime StartRun persists unique run_intent_id → run_id mapping and authoritative lookup.
- [ ] Compute owner_ref uses agent-runtime/run/<run_id> for direct Run capacity, never RunIntent.
- [ ] SandboxProvider is the sole Compute caller for a SandboxClaim; duplicate direct Runtime request is rejected.
- [ ] EngageGuard Permit attestation binds guard_operation_id, audience and exact normalized Compute request digest.
- [ ] Runtime guard_submission_key, Guard-generated operation_id and Compute compute_operation_id are distinct; Guard and Compute operation IDs are linked by references only.
- [ ] Add consumer-driven tests for both journals independently entering UNKNOWN and reconciling.
