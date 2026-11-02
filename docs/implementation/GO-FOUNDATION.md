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
- PostgreSQL integration tests exercise concurrent callers, one Workload/Outbox row, changed-digest rejection, request digest binding and input validation.
- `Repository.CreateOperation` atomically commits an immutable operation and an Outbox event; operation row locks, expected-version CAS, PostgreSQL-clock lease expiry and monotonic fencing survive repository reconstruction.
- Concurrent claim, ACK-loss UNKNOWN recovery, stale worker rejection and historical idempotency replay across workload generation changes have PostgreSQL-backed integration tests.
- `internal/provider.Adapter` + non-production `FakeProvider` test the inspect-first lost-ACK, duplicate request, fingerprint conflict and authoritative no-effect finality protocol.
- A strict `gofmt` validation gate runs before migration and Go tests.
- Consolidation scope and evidence limits: [GPU-COMPUTE-PLATFORM-CONSOLIDATION.md](../migration/GPU-COMPUTE-PLATFORM-CONSOLIDATION.md).

## Boundaries and caveats

Kubernetes/Kueue/provider APIs are authoritative for actual admission,
scheduling, running, and resource existence. `Operation.ApplyObservation` is only
a pure rule; an authenticated Provider adapter must supply observations
from its own inspect call, never untrusted user JSON.

The repository includes Workload/Operation create + transactional Outbox, PostgreSQL operation CAS/lease state, and a pure FakeProvider contract. The fake adapter is process-local only: production-authenticated Inspect, durable post-observation commits, outbox delivery workers, migration runner, read/list/delete API, tenant AuthN/AuthZ/RLS, quota and Kubernetes/Kueue/DRA adapters remain absent. PostgreSQL 16 integration tests do not establish production restore or multi-process crash recovery. A successful mutation acknowledgement does not prove release or execution completion. Readiness indicates process admission, not dependency health.

## Local

```sh
go test -race ./...
go run ./cmd/compute-control-panel
curl http://127.0.0.1:8080/api/v1/meta
```

`LISTEN_ADDR` defaults to loopback; bind `:8080` only behind protected
cluster ingress/network policy.

## Next P0 slice

1. Wire provider-authenticated Inspect into a durable reconcile + immutable observation + outbox transaction; verify UNKNOWN outcomes across real process termination.
2. Complete Workload/Allocation API, generation CAS, tenant AuthN/AuthZ and quota policy; keep Kueue as the admission authority.
3. Add schema migration/rollback testing, backup/restore, unique operation event delivery and operational metrics.
4. Implement Kubernetes Job + Kueue + DRA adapter with exact-UID deletion finality, then migrate GHCR/distroless, cosign, Trivy and release acceptance gates.

No custom GPU scheduler is part of this plan.
