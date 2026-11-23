# L2 acceptance: real Kubernetes Node inventory, without real GPU hardware

**Status:** executable acceptance candidate. This document is not evidence that the test succeeded; inspect the exact PR-head GitHub Actions run for results.

## Goal

Prove that the read-only `internal/kubeinventory.Reader` introduced in [PR #140](https://github.com/avaargsh/compute-control-panel/pull/140) can talk to a *real* Kubernetes API server with a dedicated least-privilege ServiceAccount and distinguish allowed, denied and rotated credentials. It deliberately does **not** attest GPU hardware or Kubernetes device allocation.

## Disposable workflow

- `.github/workflows/inventory-kind-smoke.yml` runs on pull requests touching the inventory source, its smoke script, or the workflow itself.
- Reuses the established pinned kind image baseline: `kindest/node:v1.34.0@sha256:7416a61b42b1662ca6ca89f02028ac133a309a2a30ba309614e8ec94d976dc5a`.
- Reuses the repository's Go 1.27.4 toolchain and commit-pinned checkout/setup actions.
- `scripts/kubeinventory_kind_smoke.sh` refuses to reuse a pre-existing kind cluster; it uses an isolated kubeconfig and deletes only its own named cluster.
- A real Kubernetes ClusterRole allows **only** `list` on the core `nodes` collection. Other node verbs and Pod mutating verbs are explicitly checked to be denied.
- A separate, unbound ServiceAccount has no Node list permission.
- The script issues short-lived ServiceAccount TokenRequest credentials, stores them in a private temporary directory, and obtains the API CA from the generated kind kubeconfig without disabling TLS verification.
- The integration test queries the actual `/api/v1/nodes` endpoint and inspects real Node UID, resourceVersion, provenance and observation validity.
- The integration test replaces a *copy* of the allowed token with the denied token and confirms the same Reader observes HTTP 403, then restores the valid token, then rejects an empty token. No TokenRequest credentials are logged.
- The disposable kind cluster has **no GPU Device Plugin**. The test forbids pretending it observed physical GPU identity or availability.

## Run commands (never against a production cluster)

```bash
# On an isolated machine with kind, kubectl, Docker and a compatible Go toolchain:
CCP_DISPOSABLE_KIND_SMOKE=1 bash scripts/kubeinventory_kind_smoke.sh
```

The script does not accept an external kubeconfig or existing cluster for testing. The test itself skips unless `CCP_KIND_INVENTORY_SMOKE=1` and all required API/CA/token-file environment variables are set.

## Explicit evidence boundaries

| Evidence | Source | This PR proves when CI passes |
|---|---|---|
| HTTPS and real Node API | disposable Kubernetes API | L2 positive real read |
| `list nodes` and denied other verbs | real Kubernetes RBAC authorizer | L2 least privilege |
| Auth failure | real, second ServiceAccount token | L2 fail-closed 403 |
| Rotation | token file updated between HTTPS reads | L2 reader reload behavior |
| Node UID/ResourceVersion | native Node metadata | L2 inventory provenance |
| GPU resource discovery and quantities | fake HTTPS test only | L1 parsing, **not L3** |
| Physical GPU UID, used/free quantity, DRA claim, release | absent | **not tested** |
| Console session auth and project RBAC | absent | **not activated** |

## Gates still open

- Independent review of the L2 script's temporary credential handling and Kubernetes resource permissions.
- GitHub-hosted actual kind execution and complete exit status on the **exact tested commit**.
- No public Console API is mounted; do not enable it until a production identity, session and reader path is reviewed.
- For v0.1 launch: installable Helm Quickstart, Demo reader and same-schema Web UI are separate work items.

This acceptance does not authorize a merge of PR #139 or #140 on its own. Their review and evidence gates remain applicable.
