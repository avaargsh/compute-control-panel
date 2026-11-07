# E1 evidence-policy authority boundary (Draft)

Status: **internal persistence gate implemented; production trust and finance authority NOT accepted**. Tracking: [#39](https://github.com/avaargsh/compute-control-panel/issues/39), [PR #41](https://github.com/avaargsh/compute-control-panel/pull/41).

## Threat and concrete enforcement

The v0006 `compute_allocation_usage_evidence_sets` schema verifies immutable, tenant/allocation-bound facts against successful Provider operations. The evidence derivation itself also uses policy-controlled source timestamp and device-adapter allowlists. Those allowlists are **authorization configuration**, not facts an untrusted evidence producer may self-assert.

`PersistUsageEvidenceSet` now requires an explicitly configured `EvidencePolicyAuthority` (via `NewRepositoryWithEvidencePolicyAuthority`); ordinary `NewRepository` **fails closed for E1 FINAL evidence**, while existing Workload/Allocation/ComputeOperation APIs remain unchanged.

Before derivation and before opening the evidence write transaction, the repository resolves a policy snapshot by `(tenant_id, provider_id, policy_version)` through its injected authority. It rejects a missing/typed-nil resolver, lookup failure, mismatched scope/version, revoked or not-current policy, a future policy `AsOf`, and changes between caller-supplied and authoritative `AsOf`, maximum clock skew, timestamp source and certified-device adapter maps. On successful match it uses copies of the **authority-provided** allowlists for derivation and the persisted E1 evidence envelope. A resolved policy is a precondition for internal persistence, not a billing authorization.

## Source of trust: MUST be deployed separately

The interface is dependency injection, **not** a built-in trustworthy policy registry or proof of an identity provider. A production implementation must read a separately administrator-approved, versioned immutable policy registry (not request JSON), and verify signer/administrator authority, tenant/Provider scope, schema digest, effective interval and revocation. Policy distribution must use authenticated transport and independent audit, with minimum-privilege database roles. Never inject a resolver that simply echoes `PersistUsageEvidenceParams.Policy`.

The CI test resolver is synthetic; it returns approved fixture policy values and must not be shipped as an actual policy source. A Go `FINAL` record remains **internal E1 derived evidence**, not authoritative metering, financial liability, cost ledger, invoice basis or release proof.

## Known remaining P0 boundaries

1. **Authenticating observations**: Provider, Kueue, Kubernetes, Pod UID and DRA/device evidence still need verified producer identity, UID/resourceVersion monotonicity, authenticated evidence references and current external-authority reconciliation.
2. **TOCTOU and revocation**: the authority lookup happens before the PostgreSQL transaction. Revocation or policy reconfiguration after lookup does not atomically fence the write; production ingestion requires a durable policy-revision fencing/snapshot mechanism and recheck at the protected commit boundary. The local clock is not an external time authority.
3. **Cryptographic snapshot**: v0006 stores policy version and full normalized evidence/policy JSON, but has no independently attested policy-signature or trusted registry digest column. Do not claim signed policy attestation.
4. **Lifecycle and economics**: optional source release evidence, provider resource release and UNKNOWN reconciliation remain external authorities. WorkloadCostEntry posting/reversal, tariff approvals, billing and margin calculations require separate gates.

## Regression evidence and rollout

- Test for missing authority, unavailable policy registry, wrong tenant/Provider, changed revision, expired/revoked/future policy, fake certified adapter, inserted trusted timestamp source, altered maximum clock skew and changed `AsOf`.
- Existing PostgreSQL 16 E1 tests use a deliberately injected *test-only* authority and still exercise exact replay, conflict rejection, immutable triggers, Provider operation binding, eight-way concurrency and migration 0005/0006 compatibility.
- No v0005/v0006 SQL bytes or migration lineage changed; no public ingestion endpoint was mounted.
- Keep PR #41 Draft pending code owner review and real independently configured authority + source attestations + revocation race tests. Do not close Issue #39 or lift production E1/finance blocks based only on CI.
