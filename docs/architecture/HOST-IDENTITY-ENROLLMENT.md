# HostIdentity PostgreSQL enrollment boundary

Status: **unmounted persistence foundation**. Immutable HostIdentity bindings and durable session-epoch/heartbeat receipt persistence exist behind internal PostgreSQL entrypoints. No HTTP listener, host attestation, machine-certificate authentication, heartbeat ingress, hardware trust, or GPU capacity authority is mounted.

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

The dedicated session writer receives schema USAGE and EXECUTE on `compute_host_session_start` and `compute_host_session_heartbeat` only. It receives no direct Provider, binding, session-table or heartbeat-table privileges. Do not grant this role to a host or tenant principal; it is an internal service identity.

Session start is one database statement. The caller supplies a stable `RequestID` and must persist it before the first attempt, then reuse it for every retry of that same intent. PostgreSQL serializes the global request key with a transaction-scoped advisory lock and a unique constraint. An exact request replay returns the original server-issued session ID and timestamps (including after a later Provider Drain); reusing the key with a changed binding, expected epoch or TTL is rejected. A different request ID still goes through Epoch CAS and cannot replace an active predecessor.

1. Lock the Provider row `FOR SHARE` and require exact `REGISTERED` state/scope.
2. Lock the immutable HostIdentity binding row `FOR UPDATE` and require exact Host/Provider/cluster/Node UID/subject.
3. Lock the latest durable session epoch, require the caller's expected epoch, and reject replacement while its predecessor remains unexpired.
4. Generate the opaque session ID in PostgreSQL and record issued/expiry timestamps from `clock_timestamp()` after lock acquisition.

TTL is bounded to 1 second through 24 hours. Epoch 0 is valid only when no session history exists; a new session after expiry advances exactly one epoch. Provider Drain and session start serialize on the Provider row.

The session table intentionally has no foreign key to the binding table: session start validates and locks the exact immutable binding before insert, while the binding's own BEFORE TRUNCATE guard remains the authoritative rejection path (a PostgreSQL FK would reject TRUNCATE earlier with a different SQLSTATE). Heartbeat receipts use the same Provider → binding → latest-session lock order. The latest epoch, session ID, immutable binding, Provider REGISTERED state, DB-clock expiry, strictly increasing sequence and SHA-256 digest are checked in the same function. An exact sequence/digest replay returns the original receipt timestamp without inserting another row or extending expiry. A stale sequence, conflicting digest, foreign binding, old epoch, expired session or DRAINING Provider is rejected. Session and heartbeat history is append-only; all mutation and TRUNCATE triggers are ENABLE ALWAYS.

These database entrypoints are not host authentication. Before calling them, a future transport must independently validate the machine certificate/SPIFFE identity, revocation and subject-to-binding resolution. No enrollment/heartbeat HTTP route, hardware attestation, physical GPU identity, GPU health or capacity readiness is enabled by this migration.

## Proposed authenticated host ingress contract (not mounted)

The next implementation gate (#182 gate 3) is authentication and revocation, not a second session store. The existing workload SPIFFE verifier in `internal/httpapi/spiffe_auth.go` verifies a peer chain and resolves workload grants; a host is a distinct principal class. Do not let a workload/Console credential become a host credential simply because it has a URI SAN. Reuse audited certificate verification primitives where appropriate, not the workload principal's Tenant/Project grants.

### Identity and authority

- The administrative enrollment workflow uses an authenticated administrative principal and exact Provider management authorization. An independent machine-attestation authority must establish the canonical machine subject, registered Provider/cluster scope and native Node UID before the immutable binding is inserted. A host's self-reported Node name/UID, forwarded identity header, Kubernetes Lease or certificate CN is never sufficient.
- Host transport requires direct mutually authenticated TLS with a narrowly configured host trust bundle, a verified leaf and exactly one canonical allowed host URI identity. Reject ambiguous/malformed identities, expired credentials and unknown trust domains. Do not accept a host-supplied subject as authority. Certificate rotation preserves the binding only when the independent authority confirms the same subject and machine identity.
- Resolve the verified subject to exactly one current approved binding within the authenticated scope. Ambiguity, missing evidence, revoked identities, unavailable authority and cross-Provider/cluster/Node-UID claims fail closed. No lookup keyed solely by HostID or Node name. The existing immutable binding and Provider registry remain authoritative; no automatic rebind.
- A revocation check made before a database call is not atomic authorization: the caller could block on Provider/binding locks until the credential or grant expires. Before production ingress, design a versioned, independently administered active authorization fence that is checked under the same transactional lock boundary as session start/heartbeat, with database-clock expiry checked after waits. Explicitly test the revocation/expiry race. Migration 0023 does not provide this authority yet.

### Proposed typed request/response contract

| Operation | Caller input (untrusted) | Server-owned result |
| --- | --- | --- |
| Admin enrollment | Requested Provider/cluster/native Node identity and attestation reference | Immutable binding, DB receipt time; no live capacity claim |
| Session start | Stable request ID, expected epoch, bounded requested TTL | Exact persisted epoch, opaque session ID, issue/expiry times |
| Heartbeat | Epoch, session ID, positive sequence, bounded typed observation | ACCEPT or REPLAY and original DB receipt time |

The transport obtains the binding from the trusted authority for every operation, never from request JSON. It must compute the canonical SHA-256 digest over the accepted typed observation rather than trust a client digest. Define the canonical encoding/version and size limits before implementing the endpoint. The server owns the TTL policy cap and must not grant a session beyond the current credential/authorization window. No endpoint is registered by this design.

For a lost session-start ACK, retry the exact durable request ID and payload; never invent a new request ID or increment expected epoch until the previous intent is resolved. The repository can return a historical receipt even after Drain. Receipt replay is not authorization: ingress must still check current identity status, Provider state, expiry and latest epoch before allowing any new effect. Heartbeat replay never extends expiry.

### Pre-mount acceptance matrix

| Case | Required outcome |
| --- | --- |
| No mTLS, wrong trust bundle, workload credential or ambiguous URI | Reject before persistence |
| Unknown/revoked subject or foreign binding tuple | Reject; no session/heartbeat row |
| Authority unavailable or returns more than one binding | Reject; no fallback to a claimed identity |
| Revocation or expiry while blocked on Provider/binding lock | Reject at the durable effect boundary |
| Drain races with new session/heartbeat | Existing transaction fences reject the new effect |
| Duplicate start intent / lost ACK | Original receipt only, not a fresh grant |
| Same sequence with changed canonical payload | Reject |
| Stale epoch, expired session or clock rollback | Reject |
| Node replacement or machine identity mismatch | Quarantine/supersession workflow; no implicit adoption |
| Fresh authenticated heartbeat with missing physical device evidence | No physical GPU READY/free/released assertion |

Use a disposable real TLS client/server, restricted PostgreSQL roles, independent authorization data, exact-head race tests and a process restart fixture before mounting. Verify the role cannot directly mutate binding, Provider or session tables. This contract does not authorize public exposure, commands, hardware readiness, Allocation RELEASED, G04/G05 or Cost FINAL.

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
