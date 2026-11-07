# Provider management API v1 — restricted bootstrap slice

Status: **Draft, implemented as an unmounted HTTP handler only**. This is neither a
production management ingress nor proof of allocatable GPU capacity.

## Provenance and reuse

- Historical `gpu-compute-platform` (retired): management intent, immutable
  Provider identity, generation and lease fencing, create-or-adopt recovery,
  UID-bound cleanup, resource absence and late-ACK handling. See its
  `docs/PROVIDER_RECOVERY_CONTRACT.md`,
  `docs/WORKLOAD_LIFECYCLE.md` and
  `docs/RELEASE_ACCEPTANCE_V0_1.md`. The 2026-10-23 Stage B audit explicitly
  did **not** qualify physical GPU or a second production Provider.
- Historical `temp-runner`, now `openarc-labs/temp-runner`: preserve its
  pinned source-SHA, ephemeral kind/Temporal test environment, negative-control
  tests, source lock and artifacts model. It is an **experiment harness**, not
  a second CCP authority or a sandbox controller copied into this repository.
- Current CCP `main`: `postgres.Repository.RegisterProvider`,
  `GetProvider`, `BeginProviderDrain` own registry writes, read and
  one-way versioned Drain. PR #31 FakeProvider covers retry and UID/generation
  conformance, but only in the test-double boundary.

Keep **one** authoritative PostgreSQL Provider/Workload/Allocation state
machine in CCP. Do not reintroduce the historical Python/FastAPI management
database, Celery scheduler or a second ResourceClaim manager.

## Standalone HTTP contract

The constructor `httpapi.NewProviderManagementHandler` requires independent
service authentication, privileged Provider management authorization and a
durable Provider repository. No route is mounted by the default service.

| Method | Endpoint | Effect and response |
| --- | --- | --- |
| POST | `/api/v1/manage/providers` | Register immutable Provider fingerprint: `201` on creation, `200` on exact replay, `409` on conflicting fingerprint |
| GET | `/api/v1/manage/providers/{provider_id}` | Read recorded REGISTERED/DRAINING state and version: `200` or `404` |
| POST | `/api/v1/manage/providers/{provider_id}/drain` | `expected_version`/reason-bound one-way CAS to DRAINING: `200` or `409` |

All request bodies are bounded to 64 KiB and reject duplicate JSON keys,
explicit nulls, unknown fields, malformed bodies and unsupported fields.
The API restricts path Provider IDs to portable ASCII segments. All responses
are no-store. The `provider_id` is a global bootstrap registry key and
**must not be authorized merely because an authenticated service has Tenant
or Project Workload grants**. The injected administrator authorizer must
check the operation independently and **before any repository call**.

### Registration request

```json
{
  "provider_id": "provider-001",
  "cluster_scope": "cluster-a",
  "adapter_source": "kubernetes",
  "adapter_version": "v1",
  "capability_digest": "sha256:<64 lowercase hex digits>"
}
```

The returned state `REGISTERED` is **not** READY or eligible placement
evidence. A matching request never resurrects a DRAINING record.

### Drain request

```json
{
  "expected_version": 1,
  "reason_code": "PLANNED_MAINTENANCE"
}
```

Drain fences **new acceptance**, based on the existing transactional
Provider-drain guard. It does not delete any Job, release Kueue reservation,
prove DRA ResourceClaim absence, close Provider cost liability or finalize
the Provider as RETIRED. `READY`, `RETIRED`, `DELETE` and raw
reconciliation mutation are intentionally absent from this API.

## Further gates (not claimed)

1. Wire only after verified mTLS SPIFFE admin identities, explicit Provider
   admin policy, least-privilege PostgreSQL roles, audit and transport
   hardening are independently proved; do not infer privileges from the
   `Service` string alone.
2. Reuse `temp-runner` pinned-SHA scenario templates to exercise authenticated
   management `register → create → drain → race/replay` and durable
   PostgreSQL recovery; retain logs with caller identity redacted.
3. For eventual Resource release, require exact Provider resource UID and
   generation, independent Pod/Job and Kueue reservation disappearance, and
   DRA/adapter device-release evidence. Lost ACK and UNKNOWN are inspect-first
   and remain distinct from terminal absence.
4. Validate `#3`, `#32` and `#33` against real Kubernetes/Kueue/DRA
   and client identities. `#41` economic evidence and finance posting
   remain separate gated areas.

Current PR scope is just the unmounted Provider registry management transport,
its deterministic HTTP tests and this documentation; no new migrations,
runtime workers or external provider side effects.
