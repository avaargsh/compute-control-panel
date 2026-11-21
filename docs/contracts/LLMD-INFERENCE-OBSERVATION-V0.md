# llm-d Inference Service Observation Contract v0 (design-only)

**Status:** Proposed for v0.2; no new API, CRD, Helm release or runtime behavior is activated by this document. v0.1 may reuse the *envelope* below for read-only cluster/GPU observations.

## User story

A platform user sees exactly which layer is not ready: (1) allocated GPU, (2) model server, (3) inference route. Every status links to a source and timestamp. A pool may exist while no physical device is allocated. A Pod may run while the model cannot answer requests.

## Stable envelope (proposed wire shape)

```json
{
  "workload_id": "w-123",
  "project_id": "p-123",
  "desired_generation": 7,
  "model_ref": "qwen-small@revision",
  "allocation": {
    "phase": "UNKNOWN",
    "reason_code": "DEVICE_EVIDENCE_MISSING",
    "message": "No authoritative device allocation observation is available",
    "source": "kubernetes/dra",
    "observed_at": "2026-11-21T00:00:00Z",
    "expires_at": "2026-11-21T00:01:00Z",
    "resource_ref": {"cluster_id": "c-1", "namespace": "models", "kind": "ResourceClaim", "name": "claim-1", "uid": "uid-claim-1", "resource_version": "42"},
    "evidence_ref": "sha256:example-placeholder"
  },
  "model": {
    "phase": "LOADING",
    "reason_code": "MODEL_WEIGHTS_LOADING",
    "message": "Model identity / probe has not passed",
    "source": "model-server/health-probe",
    "observed_at": "2026-11-21T00:00:00Z",
    "expires_at": "2026-11-21T00:01:00Z",
    "resource_ref": {"cluster_id": "c-1", "namespace": "models", "kind": "Deployment", "name": "qwen-small", "uid": "uid-deploy-1", "resource_version": "15"},
    "evidence_ref": null
  },
  "route": {
    "phase": "PENDING",
    "reason_code": "EPP_NO_HEALTHY_ENDPOINT",
    "message": "Model-specific authorized request has not succeeded",
    "source": "llm-d/router",
    "observed_at": "2026-11-21T00:00:00Z",
    "expires_at": "2026-11-21T00:01:00Z",
    "resource_ref": {"cluster_id": "c-1", "namespace": "models", "kind": "InferencePool", "name": "qwen-small-pool", "uid": "uid-pool-1", "resource_version": "10"},
    "evidence_ref": null
  }
}
```

The example is **synthetic**, not operational evidence. Source-specific claim fields must remain truthful. For Device Plugin modes without an authoritative per-GPU UID, do not fabricate one or assert physical release from Pod status alone.

## Enforced display/decision rules

| Case | UI status | Allowed conclusion |
|---|---|---|
| Device claim/allocator proof absent | Allocation UNKNOWN | No claim of physical GPU allocation |
| Pod Running, model probe fails | Model LOADING / FAILED | No model readiness |
| InferencePool exists, EPP has no endpoint | Route PENDING / FAILED | No inference availability |
| Authorized request returns model-specific success but physical allocation not proven | Route READY, Allocation UNKNOWN | Inference route works; hardware attribution unproven |
| Observation age > expires_at | STALE with last-seen source/time | Never silently render green |
| Generation/UID contradicts current desired state | UNKNOWN, conflict reason, audit link | Never overwrite with stale observation |
| Teardown has incomplete device release proof | Allocation RELEASING / UNKNOWN | Never post RELEASED or finance FINAL |

All response handlers MUST authorize the authenticated tenant/project scope on the server. Client-supplied `project_id` is never an authority. The front end uses the same schema for Demo and real data; a top-level `data_mode: "demo" | "live"` discriminant MUST be present in future implementation so synthetic evidence cannot be mistaken for production data.

## Minimum acceptance fixtures

- Valid independent allocation/model/route observations, each with source and timestamps.
- No source, stale time, tampered UID, generation mismatch, duplicate/replayed observation.
- Model endpoint is healthy but Router EPP down.
- Router returns success but device allocation proof absent.
- Model Deployment deleted but old Pod UID remains.
- HAMi sharing enabled, EPP route healthy while device partition is partially occupied (v0.3 test).
- Demo and real responses render from the same UI components; demo data is explicitly labelled.

Do not register this proposed envelope as a production public API until its versioning, authenticating source adapters and schema tests are implemented.
