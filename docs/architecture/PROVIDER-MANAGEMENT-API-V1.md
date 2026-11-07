# Provider management API v1 — restricted bootstrap slice

Status: **Draft, implemented as an unmounted HTTP handler only**. This is neither a
production management ingress nor proof of allocatable GPU capacity.

## Provenance and reuse

- Historical `gpu-compute-platform` (retired): management intent, immutable
  Provider identity, generation and lease fencing, create-or-adopt recovery,
  UID-bound cleanup, resource absence and late-ACK handling. The
  2026-10-23 Stage B audit references the historical
  `docs/PROVIDER_ADAPTER_SPI.md`,
  `docs/VOLCANO_PROVIDER_RECOVERY_CONTRACT.md` and
  `docs/CAPABILITY_RELEASE_MATRIX.md` at archived master
  `bcb0631ed56b2bfeae9abeaefd8f91629c4e7987`. The legacy repository
  has moved; those archival paths have not been re-fetched in this review.
  The audit explicitly did **not** qualify physical GPU or a second
  production Provider.
- Historical `temp-runner`, now `openarc-labs/temp-runner`: preserve its
  pinned source-SHA, ephemeral kind/Temporal test environment, negative-control
  tests, source lock and artifacts model. It is an **experiment harness**, not
  a second CCP authority or a sandbox controller copied into this repository.
- Current CCP `main`: `postgres.Repository.RegisterProvider`,
  `GetProvider`, `BeginProviderDrain` own registry writes, read and
  one-way versioned Drain. PR #31 FakeProvider covers retry and UID/generation
  conformance, but only in the test-double boundary.

Keep **one** authoritative PostgreSQL Provider/Workload/Allocation state
machine in CCP. Do not introduce an alternative management database,
scheduler or second ResourceClaim manager.

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
explicit nulls, unknown fields (including case-folded aliases), malformed
bodies and unsupported fields. The GET read route explicitly rejects HEAD
rather than allowing Go ServeMux to invoke a privileged read.
The API restricts path Provider IDs to portable ASCII segments. All responses
are no-store. The `provider_id` is a global bootstrap registry key and
**must not be authorized merely because an authenticated service has Tenant
or Project Workload grants**. The injected administrator authorizer must
check the operation independently and **before any repository call**.

### Explicit administrator policy — staged, not production-wired

`NewExactProviderManagementAuthorizer` accepts only independently supplied,
explicit `ProviderManagementGrant` entries with **exact**
`Subject` (canonical SPIFFE URI), `TenantID`, `Service`,
`ProviderID` and one action: `register`, `read`, or `drain`.
No wildcard, inferred role, Project grant, caller-supplied action or
`Service=compute-admin` shortcut is accepted. Duplicate, missing and
invalid grants fail closed when constructing the authorizer.

The existing `NewSPIFFEWorkloadAuthenticator` is reused to verify TLS
client-certificate chains against the configured trust domain and resolve
the currently active principal from the service identity directory.
The privileged policy then compares the **resolved** principal, not a
caller header. The repository handler remains unmounted by the default
service entry point.

The exact-grant policy is an **in-process bootstrap allowlist**, not an
authorization database, dynamic policy engine, installed management
configuration or production readiness claim. Updating/revoking a static
grant requires trusted configuration replacement and handler rotation;
revoking the underlying SPIFFE identity in the authoritative directory
is enforced on subsequent authentication. Real deployment provisioning,
secret/config trust, revocation latency, policy auditing, least-privilege
database roles and independently verified production TLS admission remain
separate gates. Unit tests cover exact grant matching and a real
`httptest` TLS client-certificate handshake with an in-memory identity
resolver. An additional `DATABASE_URL`-gated integration test connects the
same verified TLS client to the **real PostgreSQL identity directory and
Provider registry**, exercising register/read/drain/replay, identity
revocation, and append-only audit counts against a disposable PG16 CI service.
That proves a reproducible integration scenario only: no deployed CA,
production listener, administrator provisioning workflow, lease/device
release, or real GPU has been tested.

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
