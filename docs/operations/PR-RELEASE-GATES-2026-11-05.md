# Compute Control Plane — PR and release-gate checkpoint

**Snapshot date:** 2026-11-05. **Repository:** `avaargsh/compute-control-panel`.
**Observed main before this governance PR:** `1f229ce89865959339e68d5a0ce1f18595c6e70b`
([exact-main CI #41942994314](https://github.com/avaargsh/compute-control-panel/actions/runs/41942994314), success).
This is an **auditable snapshot**, not a live deployment status. Refresh heads, bases,
review threads and CI before any merge decision. Old PR descriptions and historical
issue snapshots are not current acceptance evidence.

## Evidence vocabulary (never collapse these)

- **Designed:** contracts, ADRs, fixture schemas and proposed authority/behavior.
- **Implemented:** source code or migration exists in a specific commit.
- **Tested:** exact commit/merged-head test or CI passed; identify test environment.
- **Real-environment accepted:** independently documented deployed Provider, broker,
  Kueue/Kubernetes/DRA and authorization traces with identities/timestamps.
- **Unknown:** no directly verifiable evidence; do not infer from design or CI.

One stage does not imply the next. For production placement, release and metering,
**real-environment acceptance remains unknown** as of this snapshot.

## Domain ownership and P0 gates

| Gate | Authority / owning domain | Evidence required | Current status |
| --- | --- | --- | --- |
| Provider bootstrap and Drain | CCP owns Provider intent/version and durable audit; real Provider owns resource facts | Immutable identity/version, authenticated provisioning, acceptance-time exclusion of non-ready and draining Providers | **Partial test evidence**: merged #34 records REGISTERED -> DRAINING with CAS and audit; no authenticated readiness or admission-time gate |
| Workload / Allocation / Operation | CCP only; ACP owns Agent/RunIntent, Runtime owns Run/Attempt, EngageGuard owns protected actions/Permit | Authenticated external owner reference, versioned producer/consumer contract, tenant/Project authorization | **Partial test evidence**: main transactional acceptance and unmounted SPIFFE Workload handlers; live authorization and independent consumers unknown |
| Recovery and protected release | CCP durable ComputeOperation/Outbox and inspect-first UNKNOWN; Provider-native API is authority for operation outcome | Lease fencing, ACK-loss restart, exact resource ID+UID DELETE, approved protected actions, no blind retry | **Fake+PostgreSQL tested**; production Provider/Permit and real ACK-loss reconciliation unknown |
| GPU admission / capacity / isolation | Kueue owns queue/quota; Kubernetes owns Pod placement; DRA or certified device plugin owns device assignment | Exact UID/resourceVersion evidence, quota/device release, isolation, preemption/relist/restart, no double accounting | **Not real-environment accepted**; see #33 |
| Economics E1/E2 | CCP economics derives from verified Allocation usage; external telemetry and rate sources retain authority | trusted source/policy/version, complete usage intervals, immutable lineage, cost ledger/reversals; no pricing from PENDING or ACK | **Draft-only**; #26 migration collision is blocked by #35 |

Do not repurpose the shared cross-repository **G01–G05** names without ratified
ACP/Runtime/EngageGuard mapping. Track their independently executed consumer
tests against their own exact heads before promoting any CCP integration.

## Open PR and migration dependency matrix

| PR / scope | HEAD at review | Target branch | Code/contract evidence | Blocker / permitted action |
| --- | --- | --- | --- | --- |
| [#12](https://github.com/avaargsh/compute-control-panel/pull/12), architecture + OpenAPI/schema | `fff0b9ac` | main | Draft; contract & Go CI #41836962849 / #41836962846 historically green | Main advanced; contract consumers in ACP/Runtime/EngageGuard not independently tested; keep Draft |
| [#9](https://github.com/avaargsh/compute-control-panel/pull/9), E0 contracts | `3784dd4c` | main | Draft; fixture/schema tests on historical head | Integration with current main and currency revisions; do not assert live E1 |
| [#14](https://github.com/avaargsh/compute-control-panel/pull/14), currency metadata | `3f7021fe` | #9 branch | Draft; contract test #41791808327 | Requires #9 contract authority and fresh stacked verification |
| [#16](https://github.com/avaargsh/compute-control-panel/pull/16), E1 policy proposal | `bd0c1a26` | #9 branch | Draft; contract test #41796812564 | Design only; source trust and operational collection missing |
| [#23](https://github.com/avaargsh/compute-control-panel/pull/23), deterministic calculator + evidence derivation | `47219103` | main | Draft; exact-head Go CI #41812252270 succeeded historically | Main advanced; needs refreshed dependency and review; no live collectors |
| [#26](https://github.com/avaargsh/compute-control-panel/pull/26), immutable E1 persistence | `ae158a57` | #23 branch | Draft; exact-head Go CI #41812691473 succeeded historically | **Hard migration blocker [#35](https://github.com/avaargsh/compute-control-panel/issues/35):** this branch uses different `0005`; restack and move to next contiguous migration version before merge |

Only the **exact SHA** and relevant base-tree under test authorize a "Tested"
statement. A GitHub `mergeable=true` result against an old stacked base is
not current-main schema compatibility.

### Migration lineage guard policy

The `migrations/NNNN_name.sql` namespace is global and forward-only:
1. A version used in main may not be assigned to a different name in a PR.
2. A previously applied main migration's **blob content must remain identical**.
3. A PR branch may not include two files with the same four-digit version.
4. New migrations must be allocated the next unused contiguous version *after*
   all earlier main and parent migrations are included. The Go migration
   runner already enforces contiguous versions and recorded name/checksum.
5. In a **stacked PR**, compare migration trees against current **main**, not
   only the PR's older target branch. The read-only `pull_request_target`
   guard compares tree metadata without running untrusted head code.
6. Run new-schema tests from (a) empty PostgreSQL and (b) DB previously
   migrated to current main, including repeated migration application.

Concrete collision: main #34 added `0005_provider_registry_drain.sql`;
#26 adds `0005_allocation_usage_evidence_sets.sql`. Fix in dependency order:
refresh #23 onto main, restack #26, choose next contiguous version (currently
`0006`), and revalidate exact head. Never rewrite main's version 0005.

## Merge checklist (required before marking Ready)

- [ ] Read current main SHA, PR exact head SHA, actual PR base, mergeability,
      review threads and CI. Record them in PR.
- [ ] Confirm **current-main** migration version/name/blob compatibility;
      for stacked PRs verify parent branch history and reconcile first.
- [ ] Ensure release gate classifications separate designed, implemented,
      exact-head tested and real-environment accepted evidence.
- [ ] Execute focused red/green tests for changes in behavior and full Go CI
      (gofmt, module verification, twice-applied PostgreSQL migrations,
      schema constraints, vet, race, SIGKILL recovery).
- [ ] Require owner/authorization, unknown/retry, tenant DB-role/RLS and
      external trust evidence where the PR actually enables those paths.
- [ ] Ensure no fake source, PENDING Allocation, provider ACK, admitted
      Kueue Workload or Pod request becomes an allocation or release fact.
- [ ] Only merge a tightly scoped independently safe slice; keep other
      Draft work blocked with precise dependencies, risk and proof gaps.
- [ ] Recheck merge commit and **main CI** after merging; preserve immutable
      links to failures, fixes and outstanding gates.

## Ordered action queue

1. **P0**: #32 — authenticated Provider-ready evidence and acceptance-time
   Drain/Ready gate before enabling any selector, then durable UID cleanup.
2. **P0**: #33 — Kueue admission/quota, Pod UID/binding, DRA/device assignment
   and release proofs with real-environment cases.
3. **P0 integration**: #3 — ACL/least-privilege roles, cross-repo owner/Permit,
   broker/UNKNOWN and external recovery; #12 independently tested consumers.
4. **P1 merge hygiene**: #35 — reconcile #23 and #26 against main, resolve
   migration collision and check migration-upgrade tests.
5. **P1 economics**: #8 — live trusted evidence ingestion before cost ledger;
   cost/revenue margin claims remain prohibited without provenance.

**Responsible areas:** CCP owns Provider/Pool/Workload/Allocation/ComputeOperation
and compute-economics records; ACP/Runtime/EngageGuard, Provider, Kueue,
Kubernetes and device sources own their respective authoritative facts.
No individual human owner is assigned here without a verified assignment.
