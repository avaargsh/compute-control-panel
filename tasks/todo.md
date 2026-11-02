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
- [ ] Add OpenAPI and JSON Schema source files plus schema lint/compatibility checks.
- [ ] Obtain independent architecture review; keep PR Draft until blockers are resolved.

## Implementation

- [ ] Go API bootstrap, configuration, health/readiness, telemetry and graceful shutdown.
- [ ] PostgreSQL migrations and repositories for Provider, Pool, Profile, Workload, Allocation and Operation.
- [ ] Atomic idempotency claim and canonical request-hash validation.
- [ ] Transactional outbox/inbox and consumer deduplication.
- [ ] Durable operation leases, fencing epochs and desired-generation checks.
- [ ] FakeProvider create/inspect/delete conformance suite.
- [ ] UNKNOWN, lost-ACK, process-restart and manual-review recovery tests.
- [ ] Kubernetes Job adapter with Kueue built-in Job integration.
- [ ] Kueue admission, queueing, requeue, Pod placement and quota-release integration tests.
- [ ] Provider expansion only after first adapter conformance passes.
