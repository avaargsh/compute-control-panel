# Kubernetes Node/GPU read-only inventory adapter (v0.1 candidate)

**Status:** adapter code and fake HTTPS API tests only. The default CCP service still serves health/meta routes; no Kubernetes credential, Console browser session, GPU discovery API, Helm chart, or real device attestation is enabled by this branch.

## Purpose and authority

The `internal/kubeinventory.Reader` reads Kubernetes core/v1 `Node` objects using a dedicated least-privilege service-account credential. It produces the same `observation.Snapshot`/record model introduced in the parent [observation-read PR #139](https://github.com/avaargsh/compute-control-panel/pull/139).

It records only **Node-reported capacity and allocatable** values for explicitly configured extended resource names. It **does not** determine the number of currently unused GPU cards, independently attest physical GPU IDs, inspect DRA ResourceClaims, inspect HAMi allocations, or prove resource release. It does not touch llm-d.

There is no `client-go` dependency in this first narrow bootstrap. It uses the stable Kubernetes HTTPS `GET /api/v1/nodes?limit=100` API with a bounded, paginated, TLS-verified client. If v0.1 requires an informer/cache at scale, add client-go in a later audited dependency/adapter PR; do not mix those changes into the first observation contract.

## Trust and access

- API endpoint must be an exact HTTPS origin, with explicit CA trust roots; `InsecureSkipVerify` is never enabled. Redirects are not followed.
- Bearer credential is read from the configured token file on every page (supports rotated projected service-account tokens). No client-supplied token or project parameter is used.
- Reader is bound at construction to **one trusted cluster, tenant and project**; any tenant mismatch is denied and ungranted project scope returns an empty batch without contacting Kubernetes.
- Only the `nodes` resource needs the `list` verb at cluster scope. The credential must not have create/update/patch/delete privileges.
- Response payloads are size-bounded, pages are finite, repeated cursors, inconsistent list resourceVersions, duplicate node names and missing resource identities fail closed.
- Kubernetes API 401/403 errors become sanitized `ErrUnauthorized` / `ErrForbidden` values, without reflecting response bodies or tokens.
- Server-side source and observed-at/expiry fields are included in every record.

Example minimal Kubernetes RBAC for an explicitly configured read-only namespace `ccp-system`:

```yaml
apiVersion: v1
kind: ServiceAccount
metadata:
  name: ccp-inventory-reader
  namespace: ccp-system
---
apiVersion: rbac.authorization.k8s.io/v1
kind: ClusterRole
metadata:
  name: ccp-inventory-node-list
rules:
  - apiGroups: [""]
    resources: ["nodes"]
    verbs: ["list"]
---
apiVersion: rbac.authorization.k8s.io/v1
kind: ClusterRoleBinding
metadata:
  name: ccp-inventory-node-list
roleRef:
  apiGroup: rbac.authorization.k8s.io
  kind: ClusterRole
  name: ccp-inventory-node-list
subjects:
  - kind: ServiceAccount
    name: ccp-inventory-reader
    namespace: ccp-system
```

This is a documentation example, not a chart auto-install. A service-account token and API CA are provisioned via a trusted operator path; avoid copying credentials into browser configuration or Git.

## Response facts

For Node `worker-1` reporting `nvidia.com/gpu=4` capacity and `3` allocatable, the reader generates two *separate* records:

1. `Node:worker-1`, with the **Node UID**, Kubernetes resourceVersion, Ready/Unschedulable condition and source timestamp.
2. `NodeGPUResource:worker-1/nvidia.com/gpu`, with `reported_quantity: {"resource_name": "nvidia.com/gpu", "capacity": "4", "allocatable": "3"}`, no physical GPU UID and `UNKNOWN / NODE_ALLOCATABLE_NOT_FREE_CAPACITY`.

The example is not a claim that three GPU cards are free. Kubernetes `status.allocatable` is a node-advertised allocatable **total**, not a live reservation ledger or physical device identity.

Device plugins for other vendors may report different extended resource names, such as an Ascend resource. These names must be explicitly configured and verified against the installed plugin; their presence does not mean CCP's inference integration for that vendor is supported.

## Limits

- First deployment: a single configured Kubernetes cluster/project, `<=400` Nodes per fetch, `<=16` explicitly configured resource keys, `<=10` pages of 100 each, and `<=2 MiB` per page.
- Integer Node resource quantities only in this candidate. A non-integer or conflicting quantity fails closed until reviewed; do not silently coerce units.
- No informer, Pod request aggregation, device UID, PCI topology, in-use/free GPU calculation, node metrics, or audit-signature ingestion. All require follow-up implementation and separate evidence gates.
- No public mounting of `GET /api/v1/observations`; continue using the reviewed authentication/authorization boundary in the parent PR.

## Verification requirements

- L1 tests: verified HTTPS/CA and SA token, paginated NodeList, source revision/time, resource count, Ready/Unschedulable projection, unknown GPU availability, tenant/project isolation, invalid resource quantity, duplicate cursor and RBAC 403.
- Exact-head GitHub Actions: gofmt, `go vet`, `go test -race` plus repository-wide PostgreSQL schema/migration tests.
- L2: kind with a minimally privileged ServiceAccount and real Kubernetes API; verify missing SA list permission, token rotation, pagination and API outage.
- L3: GPU node/driver/device plugin and physical allocation/UID/cost evidence (not covered by this PR).

**Do not merge into main merely because a fake Kubernetes API test passes.**
