# ADR-003: Single-writer llm-d integration and inference lifecycle

- Status: Proposed (no production activation)
- Date: 2026-11-21
- Scope: v0.2 single-model Standalone Mode; v0.1 is read-only
- Related: [ADR-001](ADR-001-CROSS-REPO-OWNERSHIP.md), [ADR-002](ADR-002-LLMD-SERVING-BASELINE.md), [observation contract](../contracts/LLMD-INFERENCE-OBSERVATION-V0.md)

## Decision: one resource, one authoritative writer

| Fact/resource | Writer/authority | CCP role |
|---|---|---|
| Tenant/project, model intent, desired generation, operation/Allocation journal | CCP domain | Sole domain writer |
| Kubernetes model server Deployment and Service (v0.2) | CCP's *single* Kubernetes model-server adapter | Create/update/delete top-level objects, UID- and generation-fenced; observe owned descendants |
| One llm-d Standalone Router Helm release | CCP installation/release orchestrator via Helm | Operate pinned release, persist release identity, revision, values digest and operation ID |
| InferencePool, EPP, Proxy objects installed by that chart | Helm release | **Observe only**; no parallel direct creation, patch, or deletion of the same objects |
| GAIE CRDs (cluster-scoped) | Cluster administrator / explicit platform installation transaction | Preflight presence/version; never delete shared CRDs on service teardown |
| Pod binding and Kubernetes child objects | Kubernetes native scheduler/controllers | Read UID, node and state; do not patch native children |
| Physical GPU and sharing/device claim facts | Device Plugin, DRA driver or HAMi, per validated pool mode | Observe evidence and freshness; never infer allocation from `Pod Running` |
| Request endpoint selection | llm-d EPP/router | Observe EPP, pool, proxy and request outcomes; do not implement EPP routing |
| Usage/cost FINAL | CCP evidence-gated economics | Require authoritative allocation, release and time/rate coverage, not router metrics alone |

The v0.2 bootstrap is intentionally limited to one service/one chart release in a dedicated namespace. Any move to shared Router/InferencePool ownership requires a new ADR and migration plan. Helm operation completion is not proof that the model is serving.

## Intent, references and adapter contract

CCP retains an opaque `InferenceServiceBinding` associated with an authenticated tenant/project, `workload_id`, `desired_generation`, exact namespace, chosen model template revision, release name/revision/values digest, `InferencePool` reference (group/kind/namespace/name/UID/resourceVersion), Deployment and Service references (with UIDs), and endpoint exposure status. External observed facts carry their own source/time/revision.

Proposed interface (contract, NOT existing Go implementation):

```go
type ModelServerAdapter interface {
    Plan(ctx context.Context, intent WorkloadIntent) (DeploymentPlan, error)
    Apply(ctx context.Context, op FencedOperation, plan DeploymentPlan) (ResourceRefs, error)
    Inspect(ctx context.Context, refs ResourceRefs) (ObservedResources, error)
    Delete(ctx context.Context, op FencedOperation, refs ResourceRefs) error
}
type RouterReleaseAdapter interface {
    Plan(ctx context.Context, intent WorkloadIntent) (PinnedReleasePlan, error)
    InstallOrUpgrade(ctx context.Context, op FencedOperation, plan PinnedReleasePlan) (ReleaseRef, error)
    Inspect(ctx context.Context, ref ReleaseRef) (RouterObservation, error)
    Uninstall(ctx context.Context, op FencedOperation, ref ReleaseRef) error
}
```

`FencedOperation` must include exact tenant/project, workload, operation ID, expected generation, idempotency key, immutable request digest and lease/fence token. Inspect is read-only. If an external API or Helm call times out after possible side effects, the operation becomes `UNKNOWN`: inspect exact resource/release identity *before* retry. A stale generation must not overwrite a newer one.

## Three independent service readiness dimensions

1. **Allocation**: `UNKNOWN | PENDING | VERIFIED | RELEASING | RELEASED | FAILED`. Only device/provider evidence appropriate to the tested integration can establish `VERIFIED` and `RELEASED`.
2. **Model**: `UNKNOWN | PENDING | LOADING | READY | FAILED`. `READY` requires model-server health plus a versioned model-identity/probe contract; `Pod Running` alone is not enough.
3. **Route**: `UNKNOWN | PENDING | READY | FAILED`. `READY` requires an observed InferencePool/EPP/proxy path with endpoint discovery and a successful authorized, model-specific request; a pool existing does not imply route readiness.

Each dimension has `observed_at`, source, cluster/namespace, native object UID/resourceVersion (where applicable), evidence reference, expiry/staleness, and a human-readable reason. Missing, conflicting or expired observations display `UNKNOWN` or `STALE`, never green. Router request success and pool discovery are *not* proof of physical allocation.

## Apply, recovery and deletion

- **Preflight**: verify authentication, project scope, namespace policy, version matrix, GPU resources, privileged/Device Plugin conflicts, required CRDs, model image/weights and endpoint authentication.
- **Create**: durably accept intent/outbox, then apply model-server resources and a pinned router Helm release through their designated writers; record immutable external identities and observed state. Before public exposure require all readiness dimensions plus network and identity controls.
- **Recover**: repeatable Inspect on exact UID, Helm release/revision and generation; detect orphaned/stale identities, missing ACK, partial Helm install and controller restart. No blind retry after ambiguity; mismatch goes to manual review.
- **Delete**: disable admission/exposure and drain in-flight requests according to a recorded deadline; request model-server deletion and verify exact model Pod/Deployment UID absence plus independent device release evidence; uninstall the exact Helm release and verify pool/router endpoint absence. Keep operation `UNKNOWN` or `RELEASING` if any required proof is missing. Do not report `Allocation RELEASED` solely because a Helm uninstall succeeded. CRDs must not be deleted.
- **Security**: no publicly exposed inference endpoint by default, project-scoped RBAC, validated template inputs, secure secret references, no browser-supplied authority, no arbitrary Helm values from untrusted tenants.

## Required acceptance (must precede real actuation claims)

- L1: schema/UID/generation/tenant/operation fences; failed and stale observations; no double writers.
- L2: disposable kind/K3s with pinned llm-d chart and GAIE CRDs; double create, Helm timeout/partial install, wrong pool selector, wrong model, stale Pod UID, DELETE/recreate, lost ACK and EPP unavailable.
- L3: physical GPU + supported driver/plugin; actual device allocation and release proof, authenticated inference via llm-d, metrics/probe consistency, crash/restart/partial deletion and tenant isolation.
- Failure must be shown as separate allocation/model/route status with its source and last-seen time.
- Do not mark production-ready merely because L1/L2 are green.
