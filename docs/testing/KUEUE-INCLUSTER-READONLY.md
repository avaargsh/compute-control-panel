# Kueue read-only observer: in-cluster smoke (NOT Golden E2E)

Evidence level: **Code + TLS mock tests only** until a real cluster run produces immutable, reviewed logs and an accepted observation manifest.

This slice offers a one-shot, read-only Kubernetes API inspection of an **existing** Job pinned to `namespace/name/UID`. It **cannot create Jobs**, patch Kueue admission, mark a Compute Allocation READY, verify GPU device assignment, prove quota release, or finalize deletion. It does not replace the historical kind + Kueue + Fake GPU Golden Path.

## Trusted input and minimal ServiceAccount

Use a disposable namespace for the test, for example `tenant-a`, and grant only the three API reads already used by `internal/provider/kueue_reader.go`:

```yaml
apiVersion: v1
kind: ServiceAccount
metadata:
  name: ccp-kueue-observer
  namespace: tenant-a
---
apiVersion: rbac.authorization.k8s.io/v1
kind: Role
metadata:
  name: ccp-kueue-observer
  namespace: tenant-a
rules:
- apiGroups: ["batch"]
  resources: ["jobs"]
  verbs: ["get"]
- apiGroups: [""]
  resources: ["pods"]
  verbs: ["list"]
- apiGroups: ["kueue.x-k8s.io"]
  resources: ["workloads"]
  verbs: ["list"]
---
apiVersion: rbac.authorization.k8s.io/v1
kind: RoleBinding
metadata:
  name: ccp-kueue-observer
  namespace: tenant-a
roleRef:
  apiGroup: rbac.authorization.k8s.io
  kind: Role
  name: ccp-kueue-observer
subjects:
- kind: ServiceAccount
  name: ccp-kueue-observer
  namespace: tenant-a
```

The source cluster and namespace must be operator-approved; the command does **not** authorize the Job owner or make a tenant-to-namespace policy decision itself. Bind this Role only to the test namespace. No verbs for `create/update/patch/delete`, no DRA or Node permissions, and no cluster-wide `ClusterRole`.

## Run a pinned image inside a test Pod

Build the binary from the chosen exact source commit:

```bash
go build -trimpath -o kueue-observe ./cmd/kueue-observe
```

Package that binary into a non-root container image using your deployment pipeline and record its immutable digest. In a test Pod, use `serviceAccountName: ccp-kueue-observer` with the projected token/CA mount enabled. Do not use the control-plane API credential or a general cluster-admin kubeconfig.

The binary's command is:

```text
/kueue-observe \
  --tenant tenant-a \
  --workload workload-a \
  --generation 7 \
  --namespace tenant-a \
  --job job-a \
  --uid <EXACT_KUBERNETES_JOB_UID>
```

It reads `KUBERNETES_SERVICE_HOST` and `KUBERNETES_SERVICE_PORT_HTTPS` from Kubernetes and uses the mounted `/var/run/secrets/kubernetes.io/serviceaccount/{ca.crt,token}` files. Token content is not output or persisted, and rotated projected tokens are read on each API GET.

Output is a JSON `KueueJobCollection` with a structural stage `UNPROVEN`, `KUEUE_ADMITTED`, or `POD_PLACED`. Provenance source remains explicitly `kubernetes-https-readonly-unattested`. **POD_PLACED does not prove device assignment or Allocation READY.** For contradictory, forbidden, missing or stale resources, the command returns nonzero and emits no successful observation JSON.

## Acceptance evidence needed before next stage

Record source commit and image digest; cluster and Kueue versions; namespace and ServiceAccount role; Job UID/resourceVersion; Kueue Workload UID/resourceVersion and `Admitted` condition; Pod UID/resourceVersion and Node binding; timestamp plus API errors. Run at least: queued without Pod; admitted without Node binding; scheduled Pod without GPU device claim; Job recreation with different UID; token rotation; revoked RBAC; invalid CA; lost API response. All mismatches must remain `UNPROVEN` or return a nonzero error.

Further required before #33 can close: version-pinned kind/Kueue/Fake GPU installation, durable Workload/Operation -> Job creation, unknown/lost-ACK restart, single-authority finalizers, Kueue quota release and independently verified device release. Unit TLS tests and this command are **not** real-cluster E2E.
