# HostIdentity PostgreSQL enrollment boundary

Status: **unmounted authenticated-ingress candidate**. Immutable HostIdentity bindings and durable session-epoch/heartbeat receipts exist in PostgreSQL. The dedicated host-only X.509 verifier, revocable subject-grant boundary and standalone session/heartbeat HTTP handler are implemented in PR #197; issuer-CRL enforcement is added in PR #203; atomic in-process CRL snapshot rotation is added in PR #204. The handler remains unmounted. No independent hardware attestation, administrator enrollment workflow, OCSP, automated CRL distribution, Kubernetes Lease ingestion, hardware trust, or GPU capacity authority is enabled.

## Authority and lifecycle

- CCP Provider Registry remains the sole writer and authority for Provider registration, cluster scope and Drain.
- Kubernetes remains authoritative for Node identity and Lease observations.
- HostIdentity binding stores an immutable tuple: Host ID, Provider ID, cluster scope, native Node UID and authenticated subject reference.
- A changed Node UID or subject requires a new binding and explicit supersession/quarantine; it is never an in-place adoption.
- A binding receipt timestamp is assigned by PostgreSQL after the Provider row lock. It is not a heartbeat or proof of a live host.

## PostgreSQL 16 least-privilege boundary

Migration `0022_host_identity_bindings.sql` requires a separately provisioned role before applying migrations:

```sql
CREATE ROLE ccp_host_identity_guard_owner
    NOLOGIN NOINHERIT NOSUPERUSER NOCREATEDB NOCREATEROLE
    NOREPLICATION NOBYPASSRLS;
```

The migration rejects unexpected ownership, membership, direct grants, default privileges or CREATE authority. It then grants this role only:

- `USAGE` on schema `public`.
- `SELECT(provider_id, cluster_scope, state)` on `public.compute_providers`.
- `UPDATE(provider_id)` on `public.compute_providers`.

PostgreSQL 16 requires UPDATE privilege on at least one column to execute `SELECT ... FOR SHARE`. The grant is deliberately limited to the immutable key column; the guard function never updates Provider state. The role is NOLOGIN, NOINHERIT, has no runtime members, and owns only the fixed-search-path `SECURITY DEFINER` trigger function. The function rejects invocation from any relation other than the permanent HostIdentity binding table, and EXECUTE is revoked from PUBLIC and the migration user; only the NOLOGIN guard owner retains it.

The migration temporarily grants schema CREATE only to transfer function ownership, then revokes it before commit. A non-superuser migrator must have the normal, separately approved ownership-transfer authority for this dedicated role. Never grant this role to an application or host-enrollment login.

A HostIdentity enrollment writer needs only `USAGE` on `public` and `INSERT` on `compute_host_identity_bindings`; it receives no Provider SELECT/UPDATE, no direct trigger-function execution, and no binding UPDATE/DELETE/TRUNCATE. The guard itself reads and locks the Provider row under its restricted owner.

## Durable session epoch and heartbeat receipt boundary

Migration `0023_host_session_fencing.sql` requires two independently provisioned roles before migration:

```sql
CREATE ROLE ccp_host_session_guard_owner
    NOLOGIN NOINHERIT NOSUPERUSER NOCREATEDB NOCREATEROLE
    NOREPLICATION NOBYPASSRLS;

CREATE ROLE ccp_host_session_writer
    LOGIN NOINHERIT NOSUPERUSER NOCREATEDB NOCREATEROLE
    NOREPLICATION NOBYPASSRLS;
```

The migration fails closed if either role has unrelated ownership, memberships, default privileges, direct grants or CREATE authority. The non-login guard owner receives only Provider identity/scope/state SELECT plus UPDATE(provider_id) for the PostgreSQL `FOR SHARE` lock, and HostIdentity binding SELECT plus UPDATE(host_id) for `FOR UPDATE`. It owns the two append-only session tables and fixed-search-path SECURITY DEFINER functions. Any temporary migration-role membership used for ownership transfer is revoked before commit.

The dedicated session writer receives schema USAGE and EXECUTE only on `compute_host_session_start_authenticated` and `compute_host_session_heartbeat_authenticated`. Migration `0024_host_session_subject_grants.sql` revokes its EXECUTE on the raw tuple-based functions. It receives no direct Provider, binding, session, heartbeat, subject-grant or audit-table privileges. Do not grant this role to a host or tenant principal; it is an internal ingress-service credential.

Session start is one database statement. The caller supplies a stable `RequestID` and must persist it before the first attempt, then reuse it for every retry of that same intent. PostgreSQL serializes the global request key with a transaction-scoped advisory lock and a unique constraint. An exact request replay returns the original server-issued session ID and timestamps (including after a later Provider Drain); reusing the key with a changed binding, expected epoch or TTL is rejected. A different request ID still goes through Epoch CAS and cannot replace an active predecessor.

1. Lock the Provider row `FOR SHARE` and require exact `REGISTERED` state/scope.
2. Lock the immutable HostIdentity binding row `FOR UPDATE` and require exact Host/Provider/cluster/Node UID/subject.
3. Lock the latest durable session epoch, require the caller's expected epoch, and reject replacement while its predecessor remains unexpired.
4. Generate the opaque session ID in PostgreSQL and record issued/expiry timestamps from `clock_timestamp()` after lock acquisition.

TTL is bounded to 1 second through 24 hours. Epoch 0 is valid only when no session history exists; a new session after expiry advances exactly one epoch. Provider Drain and session start serialize on the Provider row.

The session table intentionally has no foreign key to the binding table: session start validates and locks the exact immutable binding before insert, while the binding's own BEFORE TRUNCATE guard remains the authoritative rejection path (a PostgreSQL FK would reject TRUNCATE earlier with a different SQLSTATE). Heartbeat receipts use the same Provider → binding → latest-session lock order. The latest epoch, session ID, immutable binding, Provider REGISTERED state, DB-clock expiry, strictly increasing sequence and SHA-256 digest are checked in the same function. An exact sequence/digest replay returns the original receipt timestamp without inserting another row or extending expiry. A stale sequence, conflicting digest, foreign binding, old epoch, expired session or DRAINING Provider is rejected. Session and heartbeat history is append-only; all mutation and TRUNCATE triggers are ENABLE ALWAYS.

The raw tuple-based database entrypoints are internal persistence primitives, not host authentication. Production ingress must call only the authenticated wrappers. Migration 0024 adds an immutable DBA-provisioned subject grant, a one-way DB-timestamped revocation field, append-only grant audit, and a fixed-search-path SECURITY DEFINER wrapper that resolves the subject to the exact immutable binding. The writer cannot read or mutate the grant table or invoke the raw functions.

## Implemented authenticated host ingress candidate (PR #197; not mounted)

The transport identity is a **dedicated machine certificate**, not a workload/Console service-account identity. The standalone handler in `internal/httpapi/host_session_ingress.go` uses the host-only verifier in `internal/hostidentity/credential.go`; it does not call the workload SPIFFE authenticator or accept a tenant, service, subject, HostID, ProviderID, cluster scope or Node UID from request JSON.

### Host certificate profile and proof of possession

- The dedicated host trust bundle and trust domain are explicit handler dependencies. The serving listener must independently set `tls.RequireAndVerifyClientCert`, use the same host-only ClientCAs bundle, enforce TLS 1.3 or later, and never share workload/Console trust roots.
- The HTTP handler requires a TLS peer certificate and a non-empty `VerifiedChains` entry whose leaf matches the peer leaf. It then independently verifies the presented DER chain against the dedicated host roots.
- The leaf must be valid at the verifier's current clock, non-CA, carry `KeyUsageDigitalSignature`, explicitly include `ExtKeyUsageClientAuth`, and must not use `ExtKeyUsageAny`.
- The original SAN extension must contain exactly one URI GeneralName and no other SAN identity. The URI must be canonical `spiffe://<trust-domain>/host/<machine-id>`; workload namespace paths, aliases, extra SANs, foreign trust domains, malformed chains and expired certificates fail closed.
- TLS client authentication proves possession of the certificate's private key. It does **not** prove TPM/TEE state, secure boot, image integrity, physical device ownership or Kubernetes Node truth. Those facts still require an independent machine-attestation authority before an operator provisions a subject grant.
- The unmounted verifier requires an explicit, bounded issuer-CRL bundle. Every non-root certificate in every verified path must have exactly one current CRL signed by its actual issuer, with matching Authority Key Identifier, issuer `cRLSign` usage, valid signature and current `ThisUpdate` / `NextUpdate` window. Missing, malformed, duplicate-issuer, expired, not-yet-valid, wrong-key, unsupported delta/indirect or serial-revoking CRLs fail closed before repository access.
- CRLs are parsed into an immutable `HostCredentialRevocationListStore` snapshot together with the exact configured issuer certificates. A caller that already holds a trusted, pre-fetched CRL bundle may call `Replace` to publish a complete new snapshot atomically without restarting the handler. Before publication, every CRL must be current, cover exactly the configured issuer set, match issuer DN and AKI, and verify under an issuer CA with `cRLSign` usage. Replacement also rejects CRL-number or `ThisUpdate` rollback, refuses to remove any previously revoked serial, and accepts an equal CRL number only for byte-identical DER. Invalid updates leave the last known-good snapshot in place. The issuer set is immutable for the lifetime of a store; CA issuer changes require a separately reviewed trust-bundle rotation and store reconstruction. The store does not fetch distribution points, schedule refreshes, authenticate a CRL source, or implement OCSP; each request still verifies revocation against the actual presented certificate path and revoked serial before repository access. The update caller must therefore be a trusted configuration/control-plane component, not a host request or public endpoint. A request that loaded the previous snapshot before replacement may finish; deployments requiring quiescent revocation must drain in-flight ingress requests before acknowledging the rotation. Operational deployments requiring automated distribution or OCSP must remain unmounted until those policies are implemented and reviewed.
- Current software authorization revocation remains the one-way HostIdentity subject-grant revocation below. Emergency CA distrust still requires host trust-bundle rotation. CRL verification is not a complete PKI operations system.

### Subject grant and atomic authorization

Migration `0024_host_session_subject_grants.sql` creates `compute_host_identity_subject_grants` and its append-only audit table.

- A DBA/operator provisions a grant only after independent enrollment approval. The row binds one dedicated host URI subject (whose machine-id is an external identity, not necessarily the internal HostID) to one immutable HostID/Provider/cluster/native Node UID tuple and records the owning tenant. Subject is a primary key and HostID is unique, so one host certificate cannot silently become a shared identity for multiple hosts, nor can one HostID be silently rebound to another subject.
- Grants are not created by the host endpoint. Runtime writer credentials cannot SELECT/INSERT/UPDATE/DELETE/TRUNCATE grants or read the audit table. The grant owner remains a separate administrative boundary.
- Revocation is a one-way `revoked_at` / reason update. A trigger assigns the database timestamp and appends an audit event with `session_user`; grants and audit history cannot be deleted or truncated.
- The authenticated SQL wrappers accept only the verified certificate subject. They resolve the exact grant and fixed `compute-host-agent` service in PostgreSQL; tenant/service are not caller-supplied parameters. They acquire a transaction-scoped shared advisory lock keyed by subject. The revocation trigger acquires the matching exclusive lock, so an in-flight start/heartbeat commits before revocation, while a request queued behind revocation observes the revoked grant.
- Session start and heartbeat recheck the grant window after the raw Provider/binding/session operation returns. If the grant expires while waiting on row locks, the wrapper raises SQLSTATE `28000`; the transaction rolls back the tentative session/receipt. Requested session expiry may not exceed the grant expiry.
- The writer can execute only the authenticated wrappers. Raw tuple-based session start/heartbeat functions remain owned by the NOLOGIN guard role and are not executable by the writer.

Provisioning is an administrative operation, not an API. Example shape (replace all placeholders only from independently approved enrollment evidence):

```sql
INSERT INTO public.compute_host_identity_subject_grants
    (subject, tenant_id, service_name, host_id, provider_id, cluster_scope, node_uid, expires_at)
VALUES
    ('spiffe://ccp.example/host/<machine-id>', '<tenant-id>', 'compute-host-agent',
     '<internal-host-id>', '<provider-id>', '<cluster-scope>', '<native-node-uid>', '<authorization-expiry>');
```

Revoke a subject once; the trigger supplies the authoritative timestamp and appends the audit row:

```sql
UPDATE public.compute_host_identity_subject_grants
   SET revoked_at=pg_catalog.clock_timestamp(),
       revocation_reason='CREDENTIAL_REVOKED'
 WHERE subject='spiffe://ccp.example/host/<host-id>'
   AND revoked_at IS NULL;
```

The grant trigger rejects every other update, DELETE and TRUNCATE. Re-authorization requires a separately reviewed new grant/binding workflow; never clear `revoked_at`.

### Standalone HTTP contract

The handler registers routes only on a private mux returned by `NewHostSessionIngressHandler`; it is not mounted by `cmd/compute-control-panel/main.go`.

| Operation | Request | Server-owned behavior |
| --- | --- | --- |
| `POST /internal/v1/hosts/session/start` | `Idempotency-Key`, `expected_epoch`, `ttl_seconds` | Resolve certificate subject grant; create/replay durable epoch and DB-clock receipt |
| `POST /internal/v1/hosts/session/heartbeat` | `epoch`, `session_id`, `sequence`, `agent_version` | Resolve certificate subject grant; compute canonical SHA-256 digest server-side; persist ACCEPT/REPLAY receipt |

- JSON bodies are size-bounded, require one `application/json` Content-Type, reject duplicate keys, nulls, unknown/case-aliased fields and trailing data.
- The subject is extracted only from the verified certificate URI SAN. No identity or binding fields are accepted from JSON or forwarding headers.
- Heartbeat digest is computed over the canonical typed request with a versioned domain separator. The host cannot provide its own digest, wall clock, Node UID, health, device UUID or GPU readiness claim.
- The handler maps authorization denial to 403, stale/conflicting session fences to 409, malformed input to 400 and repository failure to 503 without leaking SQL details. It returns no-store responses.
- A real mTLS test proves the client certificate's private-key possession and host URI profile; a missing client certificate fails at the TLS handshake, while a workload URI signed by the same CA is rejected by the host profile verifier.

### Pre-mount acceptance matrix

| Case | Required outcome |
| --- | --- |
| No mTLS, wrong host CA, expired certificate, workload URI, ambiguous/non-canonical SAN | Reject before repository call |
| Certificate URI subject has no exact grant, maps to the wrong immutable binding, or has a revoked/expired grant | Reject; no session/heartbeat row |
| Same subject mapped to another HostID or another subject mapped to an existing HostID | Database uniqueness/FK/check rejection |
| Grant revocation races session start/heartbeat | Shared/exclusive subject lock gives one clear order; requests after revocation reject |
| Grant expires while waiting on Provider/binding locks | Postflight DB-clock check rejects and rolls back tentative rows |
| Requested session TTL exceeds grant expiry | Reject; no durable session row |
| Drain races with session start/heartbeat | Existing Provider row lock and session fence reject new effect |
| Lost start ACK / exact heartbeat replay | Original session/receipt only; heartbeat replay does not extend expiry |
| Certificate serial revoked, missing or expired issuer CRL | Reject before repository access; exact issuer signature, AKI, freshness and serial are checked on every request |
| CRL update malformed, stale, regressing, missing an issuer, or removing a revoked serial | Reject replacement and retain last known-good snapshot; expired snapshots still reject every host request |
| CRL distribution source unavailable or OCSP response required | No fetcher, scheduler or OCSP client exists; keep listener unmounted if automated distribution/OCSP is mandatory |
| Missing TPM/TEE/Node Lease/device evidence | No hardware trust, GPU READY/free/released or capacity assertion |

This is an internal software identity and durable session boundary only. It does not add an enrollment API, hardware attestation, OCSP validation or automated CRL refresh/distribution, Kubernetes Lease freshness, physical GPU UUID evidence, health, DRA release, G04/G05 or Cost FINAL. Keep the handler unmounted until the independent machine authority, operational grant provisioning, CRL refresh/OCSP operating requirements, dedicated listener configuration, independent security review, exact-head CI and branch governance are separately approved.

## Session verification

The exact-source PostgreSQL 16 acceptance suite proves first epoch issuance, database-generated ID/time, active predecessor rejection after repository restart, expiry-gated epoch increment, concurrent first-start single-winner CAS, Provider Drain rejection, heartbeat monotonicity, exact replay idempotency, conflict rejection, unchanged session expiry, function-only restricted LOGIN access, immutable history and SECURITY DEFINER/ACL catalog boundaries. CI requires these tests to run without SKIP.

## HostIdentity binding verification

The disposable PostgreSQL 16 acceptance test uses a real, distinct LOGIN with only HostIdentity INSERT. It proves:

- Enrollment succeeds for an exact REGISTERED Provider without Provider SELECT/UPDATE.
- The login cannot read or mutate Provider, change/delete/truncate a binding, execute the guard function directly, or assume the guard owner.
- A concurrent Provider Drain holding the row lock blocks enrollment; after Drain commits, enrollment fails with SQLSTATE `23514` and persists no binding.
- The guard function is SECURITY DEFINER, owned by the dedicated NOLOGIN role, has `search_path=pg_catalog, pg_temp`, no PUBLIC EXECUTE, and exactly the required Provider column privileges.
- All three immutable/insert/truncate triggers are ENABLE ALWAYS.

These are database-boundary tests only. Production enrollment still requires independently authenticated host attestation, authorization, revocation, durable session issuance/CAS and operational credential provisioning before any listener is mounted.
