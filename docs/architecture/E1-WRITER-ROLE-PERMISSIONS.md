# E1 PostgreSQL writer-role attestation (Draft)

Tracking: [Draft PR #44](https://github.com/avaargsh/compute-control-panel/pull/44), stacked on [Draft PR #41](https://github.com/avaargsh/compute-control-panel/pull/41). PostgreSQL 16 only. This is **not** a production grant recipe or independent IAM / policy-source attestation.

## Why the original v0007 deployment wording was insufficient

v0007 prevented anonymous `PUBLIC` DML on `compute_evidence_policy_fences`, but that does **not** ensure the runtime `DATABASE_URL` does not authenticate as a table owner/superuser, or through a role that can modify policy. The real E1 writer must use a distinct non-owner login. Furthermore, PostgreSQL 16 requires **UPDATE privilege on at least one column** for both `SELECT ... FOR SHARE` and `SELECT ... FOR UPDATE`; plain SELECT-only is insufficient. This also affects E1 immutable-row replay and Run/Operation/Allocation ownership reads.

## Implemented transaction-specific check

Before taking the policy fence lock or writing an immutable E1 FINAL row, `PersistUsageEvidenceSet` attests the **current database transaction session**. It denies if the effective role is a superuser, bypasses row-level security, can create objects in the application schema, or can INSERT/DELETE/TRUNCATE/TRIGGER/REFERENCES on the administrator policy table; it also rejects UPDATE privileges on any policy column **other than `updated_at`**. A missing SELECT, missing restricted lock capability or missing ability to insert E1 evidence fails closed.

The single column-level `UPDATE(updated_at)` grant is required to run `SELECT ... FOR SHARE`. The v0007 `compute_evidence_policy_revision_guard` trigger rejects an UPDATE which does not also increment `revision`. The E1 writer cannot update `revision`, making this lock-only column grant unusable for actual mutation. Admin UPDATEs must monotonically increment the revision and remain serialized against E1 writer transactions.

The immutable `compute_allocation_usage_evidence_sets` table also requires an UPDATE-column grant for its `FOR SHARE` conflict/replay SELECT. The CI restricted role receives only `UPDATE(recorded_at)` there: the v0006 append-only trigger rejects actual UPDATE. The existing `FOR SHARE OF o,w,a` Provider/Workload/Allocation binding SELECT also requires UPDATE-column permissions on the referenced tables; the CI role receives `UPDATE(updated_at)` as a demonstration **but these tables are not immutable**. Therefore the role template is **not production hardened** for those three mutable tables. Solve them via a reviewed scoped locking API, a guarded SQL function with tightly constrained EXECUTE, or a separately audited same-service authority boundary before deployment. Do not copy the broad CI fixture grants into production.

## CI evidence and deployment gates

- Real PostgreSQL test role is created with unique login credentials under a privileged test fixture, and evidence writes/replays execute on a separate SQL connection pool. Admin fixture setup stays on the privileged pool.
- Negative tests prove table-owner credentials cannot persist FINAL and the restricted writer cannot INSERT/UPDATE/DELETE/TRUNCATE policy data; E1 immutable-record UPDATE is rejected despite the lock-only column grant.
- The role-attestation query is checked on the same `sql.Tx` connection as the E1 INSERT. A separate startup-only pool query would not prove the transaction's session identity.
- Before production: review role membership/SET ROLE escalation, schema/database CREATE rights, security-definer functions and search path, trusted policy issuer, policy signature/approval, session pool resets, rotation/revocation and actual scoped Provider/Kueue/DRA source assertions. A PostgreSQL owner/superuser can disable triggers and bypass these constraints.
- Keep the parent and child PRs Draft, Issue #39 open, and E1 FINAL excluded from finance posting/cost settlement until a separately authenticated environment passes these gates.
