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
  append-only ResourceObservation, Outbox, Inbox and aggregate cursors.
- CI applies the migration against PostgreSQL 16 before Go static analysis and race tests.

## Boundaries and caveats

Kubernetes/Kueue/provider APIs are authoritative for actual admission,
scheduling, running, and resource existence. `Operation.Verify` is only
a pure rule; an authenticated Provider adapter must supply observations
from its own inspect call, never untrusted user JSON.

The migration is schema groundwork only: the project does **not** yet have a
PostgreSQL repository/transaction boundary, durable operation workers, execute
Workloads, or integrate Kueue. The migration has not been validated as a
production deployment or exercised through repository-level integration tests. A successful mutation acknowledgement does not prove
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

1. PostgreSQL migration runner and tenant-scoped repository: atomic idempotency claim, Workload + Outbox transaction, version/fence CAS, and durable leases.
2. Workload/Allocation API, quota and tenant authorization.
3. FakeProvider SPI with stable operation IDs and inspect-first recovery.
4. Kubernetes Job + Kueue adapter and deletion-finality integration tests.

No custom GPU scheduler is part of this plan.
