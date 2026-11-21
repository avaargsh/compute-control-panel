# Console observation read contract v0 (v0.1 candidate)

**Status:** implemented as an unmounted Go handler and pure observation projection; NOT a live cluster/GPU integration, authenticated browser API, or production-ready evidence authority.

## User outcome

For each Cluster/Node/GPU observation, the future console can display an observation status, explicit resource identity, source and revision, collection time, expiry time, and reason for degraded/unknown/stale state.

- This contract is deliberately read-only. Physical GPU assignment, `Allocation VERIFIED/RELEASED`, finance `FINAL`, and model/route readiness are separate facts.
- Demo and Live share the *same response body*. Only top-level `data_mode` differs.
- A Reader must set `data_mode` from a trusted server-side configuration, never from user input.
- A source's `AVAILABLE` condition is a source report, **not** independently attested device allocation.

## Response (synthetic example)

```json
{
  "data_mode": "demo",
  "generated_at": "2026-11-21T08:00:30Z",
  "items": [
    {
      "project_id": "project-a",
      "resource": {
        "cluster_id": "cluster-a",
        "kind": "GPU",
        "name": "node-a/GPU-1",
        "uid": "GPU-1",
        "resource_version": "6"
      },
      "status": "AVAILABLE",
      "source": "kubernetes/device-plugin",
      "source_revision": "rv-6",
      "observed_at": "2026-11-21T08:00:00Z",
      "expires_at": "2026-11-21T08:01:00Z"
    }
  ]
}
```

This example is synthetic and MUST NEVER be presented as production data.

## Projection rules

1. Resource identity requires tenant scope, project scope, cluster ID, kind and name. Missing identity rejects the record as invalid.
2. A missing source, invalid timestamps, unknown condition, or observation from the future is `UNKNOWN`; it must never be converted into `AVAILABLE`.
3. At `generated_at >= expires_at`, status becomes `STALE`, even if the source once reported `AVAILABLE`.
4. A non-empty native UID / resourceVersion is returned only if observed. Never invent physical GPU identity or imply a Device Plugin reported a physical GPU UID.
5. Do not conflate source revision with a verifiable signature. These values alone cannot authorize cost FINAL.

## Service boundary

A proposed `GET /api/v1/observations` handler exists as `NewObservationReadHandler`, **but is deliberately not mounted** on the default public HTTP mux. Before activation, production must provide:

- an identity authenticator backed by *verified* TLS or a separately reviewed browser session implementation,
- an independent authorizer for exact tenant/project grants,
- a tenant-and-project-filtered Reader with sourced, freshness-bounded real observations,
- a bounded endpoint and audited observation failure diagnostics.

The handler rejects missing dependencies at construction, accepts GET only, refuses client-supplied query filtering, rechecks every returned row's tenant/project scope, and rejects inconsistent batches rather than returning partial cross-scope data. It uses `no-store` for every response and a 1,000-item temporary bound; pagination should be designed before a production-scale rollout.

**Security:** the existing SPIFFE WorkloadAuthenticator is for authenticated service identity, not a browser login implementation. No UI or Kubernetes permissions are granted by this PR.

## Validation evidence

- RED: new tests initially failed with undefined model/handler.
- GREEN: isolated standard-library Go tests and local vet/race run passed on Go 1.23; not equivalent to the repository's Go 1.27 CI.
- Official GitHub CI on the exact PR HEAD (including `go test -race -count=1 ./...` and full repository PostgreSQL tests) remains the merge gate.
- No actual Kubernetes cluster, GPU driver, Device Plugin, DRA claim or inference service was tested.

## Next isolated changes

1. Connect real Kubernetes observation adapters with explicit source timestamp / stale tests, using client-go and verifying RBAC failures.
2. Implement Demo Reader using the exact same snapshot/projector and a visibly synthetic data source.
3. Introduce an authenticated browser-facing Console API and UI after a separate security review; do not mount this service-identity handler directly for browser use.
4. Add Cluster/Node/GPU views only after the read integration is verified.

Related: [ADR-001](../adr/ADR-001-CROSS-REPO-OWNERSHIP.md), [LLMD-INFERENCE-OBSERVATION](LLMD-INFERENCE-OBSERVATION-V0.md) (proposed on a separate Draft PR).
