# E1 policy revocation fencing (Draft, PostgreSQL 16)

This is a **database-scoped trust barrier**, not a complete authenticated policy service or proof that Provider, Kueue or DRA observations are genuine. See [Issue #39](https://github.com/avaargsh/compute-control-panel/issues/39), [PR #41](https://github.com/avaargsh/compute-control-panel/pull/41), and [PR #43](https://github.com/avaargsh/compute-control-panel/pull/43).

## Enforcement and ordering

The previously integrated `PersistUsageEvidenceSet` requires a separately injected `EvidencePolicyAuthority`, but its authorization read occurred before E1 derivation and before the write transaction. An approval could be revoked during that interval. The additive checksummed v0007 migration introduces `compute_evidence_policy_fences`, keyed by `(tenant_id, provider_id, policy_version)`, with administrator-provisioned policy digest, effective window, monotonic revision and revoked flag.

The E1 writer:

1. Reads a tenant/Provider-scoped approved snapshot from the injected policy authority and rejects mismatched caller allowlists, time-window and skew configuration.
2. Derives E1 evidence using **only the resolved policy snapshot**.
3. Begins the **same PostgreSQL transaction** used for the E1 FINAL insert, locks the precise policy row with `SELECT ... FOR SHARE`, and verifies digest, revision, effective-window identity, revoked=false and database `clock_timestamp()`.
4. Verifies the successful Provider operation and immutable Workload/Allocation identities, inserts or idempotently replays E1 data, and commits. Any policy fence mismatch rolls back the entire transaction.

A policy revocation UPDATE that commits **before** the E1 writer takes the share lock is seen by the writer and rejected. If the E1 writer takes the lock **first**, that revocation waits for the E1 transaction to finish; its ordering is explicit. There is no promise that a revocation cancels a write which has already obtained its authoritative fence and is about to commit.

The domain-separated `sha256` policy digest covers tenant/Provider identity, version, policy content (including timestamp/certified-adapter allowlists, AsOf and skew) and effective interval. A revision number protects against stale snapshots after revocation and reapproval, even when the policy body has not changed. A digest is **not a signature**; the database policy row must originate from an independently trusted provisioning authority.

## Independent approval deadlines after lock waits

The v0009 FINAL authorization trigger captured its comparison time before waiting for an independently issued approval row. An issuer transaction could hold that row until after `expires_at`, then commit unchanged or roll back a revocation. Both direct SQL and Repository writes could consequently create FINAL evidence with an expired approval. Direct SQL could also miss policy expiry during the same wait.

Forward migration `0012_e1_authorization_lock_deadlines.sql` preserves policy-before-approval lock ordering and reads `clock_timestamp()` after each lock acquisition. After acquiring the exact approval, it rechecks both the locked policy window and the approval deadline. Expired policy returns `E1P01`; missing, revoked, mismatched or expired approval returns `E1A01`. Existing immutable FINAL replay remains unchanged. Migrations `0001` through `0011` retain their original bytes. The writer's catalog attestation now pins the v0012 function body and rejects restoration of the historical v0009 body.

`TestE1FinalRejectsApprovalExpiryDuringApprovalLock` uses a separate restricted writer LOGIN for direct SQL and Repository paths. It observes a real conflicting approval-row lock through `pg_blocking_pids`, waits past the deadline using PostgreSQL time, and checks that no FINAL row commits. The issuer cases include unchanged commit and rolled-back revocation, with additional policy-expiry coverage. These tests establish fresh validation after authority locks. They do not guarantee that a deadline remains valid at the exact durable COMMIT instant or certify production evidence sources.

The application and database function body must advance together. Drain old E1 writers, apply v0012 with the separate migration identity, then start the updated writers and recheck the catalog. An old writer rejects the new body, and an updated writer rejects the old body; mixed versions fail closed. This migration does not authorize production enrollment.

## Privileges and operational constraints

- The v0007 migration does **not** expose a public API to register or edit trusted policies. Deployers must implement a separately authenticated administrator policy lifecycle and restrict INSERT/UPDATE/DELETE/TRUNCATE privileges on `compute_evidence_policy_fences` to that identity. `REVOKE ... FROM PUBLIC` in the migration is a baseline, **not** a full role grant audit. An owner or superuser can bypass it.
- The evidence writer should only receive scoped SELECT and E1-specific INSERT privileges. It must not run as the same database owner as policy provisioning, mutate policy rows or disable validation/immutability triggers.
- A revocation must update the same authoritative fence row, bump the revision and commit durably before being declared effective. An external policy service cannot promise instant revocation merely by changing its own disconnected cache. Deletion is not a substitute for a versioned audit history.
- Existing v0006 evidence remains durable. No automatic retrospective invalidation, signed audit receipt, cost reversal or finance posting is implemented here.
- SQL row locking depends on PostgreSQL transaction semantics; this implementation and tests target PG16. No PostgreSQL-major upgrade certification is implied.

## Verification and remaining gates

PostgreSQL integration tests check absent registry entries, digest mismatch, revoked versions, stale revisions, valid current versions, and concurrent revocation blocked by an in-flight transaction's share lock. Existing typed E1 replay/concurrency tests still use a **test-only** policy provisioner; no business policy or real adapter credential is derived from those fixtures. The v0005 Provider and v0006 E1 migrations remain byte-for-byte unchanged.

Still required for production: signed/approved policy registry, administrative role isolation and audit, authentic Provider/Kueue/Kubernetes/DRA source identities and monotonically verified UID/resourceVersion, real clock/expiry/rotation/revocation staging, trusted policy digest signatures, out-of-band source reconciliation, and independent G03/G05 and finance authorization reviews. Keep the parent and child PRs Draft, and keep #39 open until all production controls are verified.
