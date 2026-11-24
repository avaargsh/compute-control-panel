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

Session start is one database statement:
1. Lock the Provider row `FOR SHARE` and require exact `REGISTERED` state/scope.
2. Lock the immutable HostIdentity binding row `FOR UPDATE` and require exact Host/Provider/cluster/Node UID/subject.
3. Lock the latest durable session epoch, require the caller's expected epoch, and reject replacement while its predecessor remains unexpired.
4. Generate the opaque session ID in PostgreSQL and record issued/expiry timestamps from `clock_timestamp()` after lock acquisition.

TTL is bounded to 1 second through 24 hours. Epoch 0 is valid only when no session history exists; a new session after expiry advances exactly one epoch. Provider Drain and session start serialize on the Provider row.

The session table intentionally has no foreign key to the binding table: session start validates and locks the exact immutable binding before insert, while the binding's own BEFORE TRUNCATE guard remains the authoritative rejection path (a PostgreSQL FK would reject TRUNCATE earlier with a different SQLSTATE). Heartbeat receipts use the same Provider → binding → latest-session lock order. The latest epoch, session ID, immutable binding, Provider REGISTERED state, DB-clock expiry, strictly increasing sequence and SHA-256 digest are checked in the same function. An exact sequence/digest replay returns the original receipt timestamp without inserting another row or extending expiry. A stale sequence, conflicting digest, foreign binding, old epoch, expired session or DRAINING Provider is rejected. Session and heartbeat history is append-only; all mutation and TRUNCATE triggers are ENABLE ALWAYS.

These database entrypoints are not host authentication. Before calling them, a future transport must independently validate the machine certificate/SPIFFE identity, revocation and subject-to-binding resolution. No enrollment/heartbeat HTTP route, hardware attestation, physical GPU identity, GPU health or capacity readiness is enabled by this migration.

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
