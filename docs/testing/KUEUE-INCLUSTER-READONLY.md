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

Further required before #33 can close: version-pinned kind/Kueue/Fake GPU installation, wiring the Job pin into the durable Workload and Operation journal, unknown/lost-ACK recovery of that journal, single-authority finalizers, Kueue quota release, and independently verified device release. Migration 0014 persists only the adapter CREATE pin and DELETE tombstone. Unit TLS tests and this command are **not** real-cluster E2E.

## CREATE and DELETE then observe (NOT Golden E2E)

`provider.KueueJobAdapter` is a library slice only. It is not mounted on a production HTTP handler, it does not run inside `kueue-observe`, and it does not close #33.

`Apply(CREATE)` posts one suspended `batch/v1` Job into the adapter's configured namespace and pins the Job UID returned by the API in the adapter binding store. The Job carries the tenant, workload, and desired-generation annotations the reader already checks, plus operation, digest, idempotency, and provider annotations. It has no Kueue queue label, no second Kueue Workload object, no finalizer, and no GPU or DRA request. A lost, unreadable, or unexpected create response returns `ErrAcknowledgementLost` and does not write a pin. The pin is written only from that acknowledged UID. It is not reconstructed from the Job name or annotations.

`NewKueueJobAdapter` keeps a private in-memory store. A freshly constructed adapter that cannot load that store stays fail-closed: `Apply` returns `ErrAcknowledgementLost` without another POST, and `Inspect` returns `OperationInProgress` with no resource UID so the durable operation remains `UNKNOWN`. A freshly constructed adapter that loads the same store treats an identical CREATE replay as idempotent when the live Job UID matches the stored pin and `exactSuspendedJobSpec` still holds. `Inspect` then returns `ResourcePresent` with that pinned UID. A same-name successor with a different UID is `JOB_UID_REPLACED`; the stored pin is the trusted UID, and the successor is left in place. `POD_PLACED` remains Job presence, not Allocation READY.

The binding store is the adapter-facing lineage for this CREATE pin and for the DELETE tombstone. `MemoryKueueJobBindingStore` is process-local. It outlives one adapter only when the same value is passed to `NewKueueJobAdapterWithBindings`, and it is gone when that process is gone. `PostgresKueueJobBindingStore` is the durable backend for the same interface. Migration `0014_kueue_job_binding.sql` stores one create-pin row and one delete-tombstone row, each keyed by namespace, tenant id, and operation id. A row holds the Job name, Job UID, the request fingerprint `sameRequest` compares, and timestamps. A tombstone also holds the acknowledged flag. SQL enforces the unique key, a non-empty UID, and rejects rewriting that UID, deleting or truncating the row, and moving acknowledged from true back to false. An identical put is idempotent. A different UID, name, or request fingerprint is `ErrOperationConflict`. Acknowledgement stays true. Reads are namespace-scoped. A database error is returned to the adapter, which fails closed. The error is not treated as a missing row, `OPERATION_NOT_APPLIED_FINAL`, or `TARGET_ABSENT`. A missing row means there is no pin or tombstone: CREATE stays unbound, and a DELETE 404 stays `MISSING_AUTHORITATIVE_OPERATION`.

The restart test for that backend stays inside one test process. It opens adapter A on one database connection, discards that adapter, the store, and the connection, then opens a new connection, a new `PostgresKueueJobBindingStore`, and adapter B. That is not a SIGKILL of a separate operating-system process. Adapter B replays the same CREATE with no second POST, and `Inspect` returns `ResourcePresent` for the pinned UID. A same-name successor is `JOB_UID_REPLACED` and is left in place. An acknowledged DELETE is `TARGET_ABSENT` after the new connection. An unacknowledged tombstone is still stored: B re-GETs and does not delete a different UID. A lost CREATE acknowledgement writes no row, so B does not adopt the Job. A closed database makes the next `Apply` and `Inspect` return an error.

`NewKueueJobAdapter` still defaults to a private memory store. These tables are not wired to a production HTTP handler, `kueue-observe`, or `cmd/kueue-provider-smoke`. `compute_operations.provider_resource_uid` is not this pin: the operation row records a provider UID only after `SUCCEEDED`, and that success already required the pin. This slice does not register Providers, drain them, or settle cost liability. It does not attest a signed receipt, and it does not admit, unsuspend, or schedule the Job. **`TARGET_ABSENT` is still not Allocation RELEASED, Kueue quota release, or device or claim deallocation.** #33 and #32 stay open.

`Apply(DELETE)` GETs that Job and deletes it only when the object UID is the request's target UID. When the store still has the CREATE pin, that pin must be the same UID. The DELETE body is a Kubernetes `DeleteOptions` UID precondition, so a later object that reused the name is not deleted. A different UID or desired-generation annotation is `ErrOperationConflict` (`JOB_UID_REPLACED` or `RESOURCE_GENERATION_SUPERSEDED`). A lost, unreadable, or unexpected server response returns `ErrAcknowledgementLost` and records an unacknowledged tombstone when the intended UID was already known. The next `Apply` or `Inspect` re-GETs and will not delete a different UID, including after a new adapter loads that unacknowledged tombstone. HTTP 400, 401, 403, and 422 are `ErrInvalidRequest` and do not record a tombstone. After a delete acknowledgement, a replay does not create a Job and does not issue another DELETE. A new adapter that loads the same store makes the same replay.

`Inspect` after that stored tombstone reports `TARGET_ABSENT` for a 404 of that name and stored UID, including when a new adapter loads the store. A 404 with no stored CREATE pin and no stored DELETE tombstone stays `MISSING_AUTHORITATIVE_OPERATION`, not `OPERATION_NOT_APPLIED_FINAL`, and not proof that CREATE never happened or that the Job never existed. **`TARGET_ABSENT` is not Allocation cleanup proof, Kueue quota release, device or claim deallocation, or closure of #33.** It is also not Provider drain or reclamation. CREATE still writes no finalizer.

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
