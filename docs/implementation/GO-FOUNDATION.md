# Go foundation — first implementation slice

Status: **non-production foundation; Compute Golden Path not yet complete**.

## Available

- Go 1.27 module, bounded HTTP process, SIGTERM drain, health/readiness.
- `GET /livez`, `GET /readyz`, `GET /api/v1/meta`.
- Pure provider operation contract with fixed ID, tenant, workload, provider
  and desired generation; an optimistic version check.
- Lost acknowledgement becomes UNKNOWN. UNKNOWN cannot be blindly
  dispatched again. Verified CREATE requires observed resource identity;
  Verified DELETE requires provider-observed resource absence.
- Tests for stale version, wrong operation ID, stale generation,
  lost acknowledgement, exact target UID and deletion finality; pinned-action Go CI.
- Initial PostgreSQL schema migration for Workload, Allocation, ComputeOperation,
  append-only ResourceObservation, Outbox, Inbox and aggregate cursors; PostgreSQL 16 CI applies it and executes constraint smoke tests.
- `internal/store/postgres.Repository.CreateWorkload` atomically commits tenant-scoped idempotency claim, Workload and accepted Outbox event.
- Concurrent same-key/same-digest requests replay the stable Workload identity; same-key/different-digest returns `ErrIdempotencyConflict`.
- The idempotent replay path now also rejects changes in immutable `project_id`, owner service/type/resource or compute profile/revision **even if the caller reuses the same digest**; the normalized request's `project_id` must match the persisted Workload scope before insert. PostgreSQL regression tests cover the metadata-confusion path.
- The current `DigestRequest` hashes provided normalized bytes; it does not independently prove RFC 8785 canonicalization or bind an authenticated principal. Canonicalization conformance, identity-derived requester scope and API AuthZ remain production-blocking acceptance gates.
- PostgreSQL integration tests exercise concurrent callers, one Workload/Outbox row, changed-digest rejection, request digest binding and input validation.
- `Repository.CreateOperation` atomically commits an immutable operation and an Outbox event; operation row locks, expected-version CAS, PostgreSQL-clock lease expiry and monotonic fencing survive repository reconstruction.
- Concurrent claim, ACK-loss UNKNOWN recovery, stale worker rejection and historical idempotency replay across workload generation changes have PostgreSQL-backed integration tests.
- `internal/provider.Adapter` + non-production `FakeProvider` test the inspect-first lost-ACK, duplicate request, fingerprint conflict and authoritative no-effect finality protocol.
- Migration `0002_operation_inspection.sql` introduces typed `operation_result` evidence distinct from Kubernetes resource phase and a database trigger forbidding modification/deletion of stored observations.
- `Repository.ReconcileOperation` atomically writes the append-only inspected evidence, fenced operation outcome and a contiguous Outbox event sequence. Inconclusive inspections preserve UNKNOWN, authoritative no-effect can enable same-key retry, and stale CREATE produces a compensation requirement.
- The Outbox `aggregate_version` is a contiguous published-event sequence. The payload separately carries `operation_version`, which includes lease and other private CAS transitions.
- `internal/recovery.Worker` is a library-only inspect-first recovery coordinator: it accepts UNKNOWN or expired leases, loads the durable operation and trusted bootstrapped adapter registration, never calls `Provider.Apply`, and persists the resulting inspection through the fenced PostgreSQL journal. PostgreSQL integration tests exercise lost ACK, tenant isolation, unknown provider denial, provider failure, and lease-expiry reinspection.
- `TestSIGKILLRecoveryLeaseIsFencedBeforeInspection` starts a separate OS process that commits a PostgreSQL reconciliation lease, then the parent SIGKILLs that process. The parent confirms the lease remains durable, cannot be stolen while active, and after expiry a new in-process Worker recovers through FakeProvider Inspect with a higher fence and exactly one persisted inspection. This validates **crash of the lease holder**, not crash during a real provider API call or provider-state durability across processes.
- CI enforces formatting without editing checkout sources (`gofmt -l .`), then validates both PostgreSQL migrations, schema constraints, `go vet` and race-enabled integration tests.
- Consolidation scope and evidence limits: [GPU-COMPUTE-PLATFORM-CONSOLIDATION.md](../migration/GPU-COMPUTE-PLATFORM-CONSOLIDATION.md).

## Boundaries and caveats

Kubernetes/Kueue/provider APIs are authoritative for actual admission,
scheduling, running, and resource existence. `Operation.ApplyObservation` is only
a pure rule; an authenticated Provider adapter must supply observations
from its own inspect call, never untrusted user JSON.

The repository includes Workload/Operation create + transactional Outbox, PostgreSQL operation CAS/lease state, typed append-only inspection persistence, and a pure FakeProvider contract. The fake adapter is process-local only. The recovery worker's provider registry is configured internally rather than accepting caller-supplied inspection bodies, but its adapter source/version are **audit labels, not authenticated external Provider evidence**. No real worker process supervisor or durable dispatch loop is installed. The database reconciliation method accepts trusted adapter data from internal callers but **does not independently authenticate its source**. A production Provider Inspect adapter, Allocation projection under capacity/admission evidence, Outbox delivery workers, migration runner, read/list/delete API, tenant AuthN/AuthZ/RLS, quota and Kubernetes/Kueue/DRA adapters remain absent. PostgreSQL 16 integration tests do not establish production restore or multi-process crash recovery. A successful mutation acknowledgement does not prove release or execution completion. Readiness indicates process admission, not dependency health.

## Local

```sh
go test -race ./...
go run ./cmd/compute-control-panel
curl http://127.0.0.1:8080/api/v1/meta
```

`LISTEN_ADDR` defaults to loopback; bind `:8080` only behind protected
cluster ingress/network policy.

## Next P0 slice

1. Integrate the inspect-only recovery library with a real, authenticated Provider Inspect adapter and supervised worker lifecycle; verify genuine process termination, ambiguous commit acknowledgements and fencing recovery.
2. Complete Workload/Allocation API and admission-derived Allocation projection, tenant AuthN/AuthZ and quota policy; keep Kueue as the admission authority.
3. Add versioned/checksummed migration runner and restore testing, durable Outbox publisher, unique event delivery and operational metrics.
4. Implement Kubernetes Job + Kueue + DRA adapter with exact-UID deletion finality, then migrate GHCR/distroless, cosign, Trivy and release acceptance gates.

No custom GPU scheduler is part of this plan.


## P0 Outbox delivery slice

The durable Outbox is a **tenant-scoped at-least-once** protocol, not an
exactly-once broker abstraction:

- `Repository.ClaimOutbox` uses PostgreSQL `FOR UPDATE SKIP LOCKED`,
  database-clock leases, and monotonically increasing `attempts` as a
  delivery fencing token. It does not allow a later version of the same
  aggregate to pass an earlier unpublished event; other tenants/aggregates
  can make independent progress.
- `MarkOutboxPublished` and `RetryOutbox` use the exact tenant, event ID,
  owner, attempt and unexpired lease. A stale publisher cannot ACK or
  retry an event after another worker has reclaimed it.
- `internal/outbox.Dispatcher` is a bounded in-process adapter: it
  claims a committed event, calls a `Publisher` interface once and
  persists an ACK or a delayed retry. If a broker ACK is ambiguous or a
  PostgreSQL ACK fails after send, the event can be redelivered with the
  same stable `event_id`. Consumers MUST deduplicate; duplicate delivery
  is an expected failure mode.
- No broker connection/authentication, durable process supervisor, Inbox
  consumer, poison-event quarantine policy, operator tooling or production
  recovery deployment is included. A QUARANTINED predecessor blocks later
  versions of the same aggregate until an explicitly approved repair.
- Database tests verify concurrency, tenant isolation, strict same-aggregate
  sequence, expired-lease takeover, stale ACK refusal and retry. Dispatcher
  tests verify publisher ACK sequencing, ambiguous failure and persistence
  failure visibility. CI provides PostgreSQL integration and Go Race evidence
  only, not broker end-to-end acceptance.


## P0 Inbox deterministic apply slice

`Repository.ApplyInbox` is the PostgreSQL-only, bounded consumer transaction
for a **trusted, already authenticated** broker event. It neither polls a broker
nor accepts tenant-supplied HTTP payloads.

- The cursor primary key is `(consumer_name, tenant_id, aggregate_type,
  aggregate_id)`; `FOR UPDATE` serializes concurrent deliveries for that
  aggregate. The first accepted version is 1 and the next must be exactly
  `last_applied_version+1`. Gaps and conflicting aggregate versions fail
  closed without advancing the cursor.
- A replayed `(consumer_name,event_id)` is a no-op **only if** producer, event
  type, schema, tenant, aggregate identity, version and JSONB payload all match
  the previously `APPLIED` event. Changed fingerprints, reused cross-tenant
  event IDs and any other non-applied state are rejected.
- One PostgreSQL transaction inserts the Inbox record, calls a trusted
  `InboxHandler` with the same `*sql.Tx`, marks the event APPLIED and updates
  the aggregate cursor. Handler failure rolls back the record, business
  mutations and cursor together. The callback **must not** perform external
  side effects or independently commit another transaction.
- Integration tests cover 12 concurrent duplicates, strict version order,
  payload/identity conflict, tenant and consumer cursor isolation, and
  transactional business failure followed by a safe retry.

**Not covered:** authenticated broker consumer, dead-letter/quarantine policy,
out-of-order event buffering, published-message end-to-end verification,
operational backpressure, independent databases/cross-service atomicity or
production AuthZ. Version gaps require a later replay of the missing event,
not an unsafe cursor skip.
