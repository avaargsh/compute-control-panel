# gpu-compute-platform → compute-control-panel consolidation

Status: **adopted target ownership; staged implementation, NOT a completed code migration**.
Decision date: 2026-11-02.

## Decision and current evidence

`gpu-compute-platform` is retired as an active delivery repository. New features,
bug fixes, release gates, provider adapters and observability for Compute belong
to `compute-control-panel`. Do not recreate a second scheduler or a second
Compute Workload/Allocation source of truth. The historical repository could not
be read with current GitHub access (404/insufficient readable content); the
inventory below is informed by prior project discussions rather than a complete
diff-level audit. Therefore **no old implementation or test suite is marked
migrated solely on the basis of this document**.

Verified target repository as of this decision:
- PostgreSQL 16 schema: Workload, Allocation, Operation, ResourceObservation,
  tenant-scoped Outbox/Inbox and cursors.
- Transactional Workload and Operation creation, idempotent replay and Outbox;
  DB-backed operation lease/CAS/fencing and UNKNOWN persistence tests.
- Pure provider-operation state machine; fake-provider SPI is a conformance
  test target, not an actual Kueue / Kubernetes implementation.
- Read-only process HTTP endpoints only. None of this is production admission,
  provider execution, quota enforcement or GPU allocation.

## Single authoritative ownership

| Capability | Owner | Source of authoritative observation |
|---|---|---|
| Tenant / AgentRelease / RunIntent | agent-control-panel | Control Plane DB |
| Run / Task / Attempt / Temporal history | agent-runtime | Runtime DB / Temporal |
| Action permit / approval / protected side effect | EngageGuard | Guard journal / executor |
| ComputeProvider / ComputePool / ComputeProfile / ComputeWorkload / Allocation / ComputeOperation | compute-control-panel | Compute DB, with external observed evidence |
| Queueing/admission and cohort quota admission | Kubernetes Kueue | Kueue status and admission |
| Placement, Pod/Job lifecycle, device requests | Kubernetes scheduler / kube-apiserver / DRA drivers | UID-bound Kubernetes resources |
| Cloud provider node and device realization | Provider / DRA driver | Provider-specific authoritative inspect |

A Compute Workload is neither a business Run nor a Temporal Workflow. It
references upstream owner identity and generation, but does not claim Run
authority. Kueue owns admission, not compute-control-panel. The target repository
owns operations and observations, not Kubernetes scheduling decisions.

## Migrating capabilities: evidence and acceptance

| Legacy capability / known work | New module or interface | Current evidence level | Acceptance gate |
|---|---|---|---|
| Provider / Pool / Profile catalog | `internal/domain` + PostgreSQL + read APIs | Planned (schema partial) | Authenticated tenant-scoped APIs, immutable profile revision and CAS |
| GPU Workload request and Allocation lifecycle | `internal/store/postgres` and command API | Workload transaction and schema only | Workload+Operation+Outbox atomicity, Allocation FK, version CAS |
| Kueue queue / admission / preemption observations | Kubernetes/Kueue provider adapter | Planned | Fake cluster fixtures plus real disposable cluster observation tests |
| Stage B Kueue + DRA lost-ACK / recovery (historically tracked as #215) | Operation journal + Kubernetes UID-bound `Inspect` | Journal foundation only | restart + lost-ACK + stale-generation + DRA device ownership tests |
| GPU device allocation / DRA resource claims | Provider adapter using K8s ResourceClaims | Planned | UID/resource-version matching, no fabricated released state |
| Job create/update/delete and drift recovery | Kubernetes Job provider adapter | FakeProvider contract starting | No blind retry, exact UID delete finality, deterministic reconciliation |
| ResourceQuota / limits / Pool policy | Compute policy with Kueue authority respected | Planned | Tenant and quota rejection fixtures; Kueue remains final admission |
| GHCR candidate container images, distroless baseline (historical #209) | `.github/workflows` and release manifests | Planned | tagged immutable image, non-root/distroless runtime, provenance |
| cosign signatures, Trivy SBOM / scanning | supply-chain CI/CD under target repo | Planned | pinned actions, artifact digest, signature verification, vulnerability policy |
| Release notes / test matrices / Stage B acceptance | `docs/acceptance`, CI and evidence manifest | Planned | explicit run IDs, reproducible versions, recovery and supply-chain gates |

The legacy #215/#209 references are historical project identifiers, not
current target repository issues and not proof of migrated code.

## Chosen design

Option A (rejected): lift old GPU control loops into the new service and keep
their own scheduler, in-memory queue or separate deployment authority. This
duplicates Kueue/Kubernetes and creates inconsistent ownership.

Option B (chosen): domain-owned declarative Workload/Allocation + durable
ComputeOperation journal + `Provider.Apply/Inspect` adapter backed by Kueue,
Kubernetes and DRA. The adapter translates resources and observes real state;
it does not decide queue placement or mark a Run complete.

```text
Agent Control / Runtime Run owner
  → authenticated ComputeWorkload intent
  → tenant + profile + quota preflight
  → PostgreSQL (Workload, Operation, Outbox) atomic commit
  → durable worker claims operation lease (version + fencing)
  → provider Apply(operation_id, key, digest, generation)
  → Kubernetes Job / Kueue admission / DRA realization
  → provider Inspect(operation_id + immutable target UID)
  → versioned ResourceObservation
  → ComputeOperation reconcile and Allocation projection
  → Outbox event for upstream consumers
```

If Apply's ACK is missing or a worker expires:
```text
IN_FLIGHT → UNKNOWN (persist) → RECONCILING
  ├─ matching authoritative resource → SUCCEEDED / observed allocation
  ├─ authoritative NOT_APPLIED_FINAL → same operation ID/key/digest retry
  ├─ IN_PROGRESS or inconclusive → UNKNOWN, inspect later
  ├─ target UID mismatch / conflict → MANUAL_REVIEW
  └─ stale generation with created resource → SUPERSEDED + compensation intent
```

Not finding a Job in a cache or an eventually-consistent list does NOT prove
`NOT_APPLIED_FINAL`. For DELETE, absence must bind the exact prior resource UID.
Do not silently turn UNKNOWN into FAILED, nor bypass EngageGuard for protected
actions in an upstream workflow.

## Phased delivery

1. **P0-A done for initial slice:** durable operation create, idempotency,
   transactional Outbox, lease CAS and UNKNOWN persistence with DB integration.
   Requires additional repository audit and multi-process/restart execution
   tests before release.
2. **P0-B underway:** bounded Provider interface + FakeProvider lost-ACK,
   duplicate and conflict conformance; connect Inspect to durable journal only
   through a trusted adapter boundary. Reject fake data at external APIs.
3. **P0-C:** atomic Workload/Allocation/Operation workflow, tenant AuthZ,
   quotas and generation CAS; forward-only migrations and database recovery.
4. **P1:** Kubernetes Job + Kueue adapter; ResourceClaim/DRA inspection,
   admission/preemption, UID-bound target confirmation and cluster fixtures.
5. **P1 release gates:** GHCR, distroless/non-root, SBOM, cosign, Trivy,
   staged deployment/rollback, reproducible version and test matrix.
6. **Cutover:** freeze the legacy repo read-only; migrate only identified
   manifests/configuration with version and provenance; require
   `old_workload_id → new_compute_workload_id` mapping if live state exists;
   otherwise reject an inferred live-state import. Never double-dispatch a
   provider operation from both repositories.

## Explicit release blockers

- No production AuthN/AuthZ/RLS boundary or externally authenticated provider
  evidence; the fake adapter must not ship as a live provider.
- No verified Kubernetes/Kueue/DRA adapter, device claim lifecycle or cluster
  recovery evidence.
- No real process crash, restart, stale-fence or multi-worker delivery harness.
- No production migration/backup/restore, signed image or release rollback proof.
- Historical old-repo code and datasets remain inaccessible for exact
  feature-for-feature parity review.

Keep the implementation PR Draft until the relevant gates are evidenced.
