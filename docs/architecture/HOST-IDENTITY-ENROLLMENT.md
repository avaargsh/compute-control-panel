# HostIdentity PostgreSQL enrollment boundary

Status: **unmounted persistence foundation**. This slice does not authenticate a host, issue a session, ingest heartbeats, establish hardware trust, or declare GPU capacity available.

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

## Verification

The disposable PostgreSQL 16 acceptance test uses a real, distinct LOGIN with only HostIdentity INSERT. It proves:

- Enrollment succeeds for an exact REGISTERED Provider without Provider SELECT/UPDATE.
- The login cannot read or mutate Provider, change/delete/truncate a binding, execute the guard function directly, or assume the guard owner.
- A concurrent Provider Drain holding the row lock blocks enrollment; after Drain commits, enrollment fails with SQLSTATE `23514` and persists no binding.
- The guard function is SECURITY DEFINER, owned by the dedicated NOLOGIN role, has `search_path=pg_catalog, pg_temp`, no PUBLIC EXECUTE, and exactly the required Provider column privileges.
- All three immutable/insert/truncate triggers are ENABLE ALWAYS.

These are database-boundary tests only. Production enrollment still requires independently authenticated host attestation, authorization, revocation, durable session issuance/CAS and operational credential provisioning before any listener is mounted.
