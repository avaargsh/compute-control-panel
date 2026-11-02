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

## Boundaries and caveats

Kubernetes/Kueue/provider APIs are authoritative for actual admission,
scheduling, running, and resource existence. `Operation.Verify` is only
a pure rule; an authenticated Provider adapter must supply observations
from its own inspect call, never untrusted user JSON.

The repository currently implements only Workload create + idempotency + Outbox transaction. PostgreSQL operation lease/CAS repository, migration runner, read/list/delete API, authorization/quota, durable workers, FakeProvider and Kubernetes/Kueue integration remain absent. Integration tests pass against PostgreSQL 16 CI; no production deployment or restore evidence is claimed. A successful mutation acknowledgement does not prove
release or execution completion. The current readiness endpoint indicates
process admission only.

## Local

```sh
go test -race ./...
go run ./cmd/compute-control-panel
curl http://127.0.0.1:8080/api/v1/meta
```

`LISTEN_ADDR` defaults to loopback; bind `:8080` only behind protected
cluster ingress/network policy.

## Next P0 slice

1. Migration runner and PostgreSQL operation repository: expected-version CAS, durable claim/lease, monotonic fence token, expiry-to-UNKNOWN and authoritative reconcile transitions.
2. Workload/Allocation read/list/delete API, quota and tenant authorization.
3. FakeProvider SPI with stable operation IDs and inspect-first recovery.
4. Kubernetes Job + Kueue adapter and deletion-finality integration tests.

No custom GPU scheduler is part of this plan.
