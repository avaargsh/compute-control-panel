# Kueue read-only observer: in-cluster smoke (NOT Golden E2E)

Evidence level: **Code + TLS mock tests only** until a real cluster run produces immutable, reviewed logs and an accepted observation manifest.

This slice offers a one-shot, read-only Kubernetes API inspection of an **existing** Job pinned to `namespace/name/UID`. The `kueue-observe` command **cannot create Jobs**, patch Kueue admission, mark a Compute Allocation READY, verify GPU device assignment, prove quota release, or finalize deletion. It does not replace the historical kind + Kueue + Fake GPU Golden Path. A separate library CREATE and DELETE path is described at the end of this document and is not this command.

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

## CREATE and DELETE then observe (NOT Golden E2E)

`provider.KueueJobAdapter` is a library slice only. It is not mounted on a production HTTP handler, it does not run inside `kueue-observe`, and it does not close #33.

`Apply(CREATE)` posts one suspended `batch/v1` Job into the adapter's configured namespace and pins the Job UID returned by the API. The Job carries the tenant, workload, and desired-generation annotations the reader already checks, plus operation, digest, idempotency, and provider annotations. It has no Kueue queue label, no second Kueue Workload object, no finalizer, and no GPU or DRA request. A lost or unreadable create response returns `ErrAcknowledgementLost`. The next `Apply` or `Inspect` reads the existing object by deterministic name and will not post a second Job or adopt a replacement UID.

`Apply(DELETE)` GETs that Job and deletes it only when the object UID is the request's target UID. When this process still has the CREATE pin, that pin must be the same UID. The DELETE body is a Kubernetes `DeleteOptions` UID precondition, so a later object that reused the name is not deleted. A different UID or desired-generation annotation is `ErrOperationConflict` (`JOB_UID_REPLACED` or `RESOURCE_GENERATION_SUPERSEDED`). A lost, unreadable, or unexpected server response returns `ErrAcknowledgementLost`; the next `Apply` or `Inspect` re-GETs and will not delete a different UID. HTTP 400, 401, 403, and 422 are `ErrInvalidRequest`. After a delete acknowledgement, a replay does not create a Job and does not issue another DELETE.

`Inspect` after that process-local intended delete reports `TARGET_ABSENT` for a 404 of that name. A 404 with no intended-delete record stays `MISSING_AUTHORITATIVE_OPERATION`, not `OPERATION_NOT_APPLIED_FINAL` and not proof the Job never existed. **That absence is not Allocation cleanup proof, Kueue quota release, or closure of #33.** Process restart plus a 404 can still be ambiguous until a later durable tombstone exists; this slice does not add one, and CREATE still writes no finalizer.

`Inspect` for CREATE reuses `KubernetesObservationReader.Collect`. `UNPROVEN`, `KUEUE_ADMITTED`, and `POD_PLACED` are structural Job observations. **POD_PLACED is not Allocation READY.** Absence of the Job on a CREATE inspect is a conflict, not proof that CREATE never happened.

Use a different ServiceAccount and Role from `ccp-kueue-observer`. `create` and `delete` are the only mutable verbs. Do not add these rules to the read-only observer Role, and do not grant `update`, `patch`, `watch`, or cluster-admin:

```yaml
apiVersion: rbac.authorization.k8s.io/v1
kind: Role
metadata:
  name: ccp-kueue-job-creator
  namespace: tenant-a
rules:
- apiGroups: ["batch"]
  resources: ["jobs"]
  verbs: ["create", "get", "delete"]
- apiGroups: [""]
  resources: ["pods"]
  verbs: ["list"]
- apiGroups: ["kueue.x-k8s.io"]
  resources: ["workloads"]
  verbs: ["list"]
```

List Pods and Kueue Workloads are the same reads the observer already performs, so Inspect can classify a later admission without promoting readiness. No Kueue CR self-scheduling, DRA, cost ledger, or #39 billing-trust work is in this slice. A TLS mock that returns 201 or 200 is not a kind/Kueue/Fake GPU Golden Path.
