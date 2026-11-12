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

The Go restart test for that backend still stays inside one test process. It opens adapter A on one database connection, discards that adapter, the store, and the connection, then opens a new connection, a new `PostgresKueueJobBindingStore`, and adapter B. That test is not a SIGKILL of a separate operating-system process. Adapter B replays the same CREATE with no second POST, and `Inspect` returns `ResourcePresent` for the pinned UID. A same-name successor is `JOB_UID_REPLACED` and is left in place. An acknowledged DELETE is `TARGET_ABSENT` after the new connection. An unacknowledged tombstone is still stored: B re-GETs and does not delete a different UID. A lost CREATE acknowledgement writes no row, so B does not adopt the Job. A closed database makes the next `Apply` and `Inspect` return an error.

The disposable kind job is a separate-process proof of that same store, and only of that store. It starts PostgreSQL 16 in a disposable container on the runner, applies the checksummed migrations, including the `ccp_kueue_job_binding` grants in migration 0015, and runs `cmd/kueue-provider-smoke` `durable` phases as separate operating-system processes. Each process calls `NewServiceAccountKueueJobAdapterWithBindings` with `PostgresKueueJobBindingStore` and `CCP_KUEUE_BINDING_DSN`. The no-argument smoke path is unchanged: one Pod, and an explicit private memory store passed to `NewServiceAccountKueueJobAdapterWithBindings`. It does not call the in-cluster constructor. Process A applies CREATE and is SIGKILLed (`kill -9`) after the create-pin row is durable. Process B, a new process with the same DSN, replays that CREATE. The namespace still has exactly one Job, with the pinned UID and the original creation timestamp, so the replay did not POST a second Job. A further process inspects and reports `ResourcePresent` for that UID. Process C applies the UID-fenced DELETE and is SIGKILLed after the acknowledged tombstone is durable. Process D replays that DELETE. A further process inspects and reports `TARGET_ABSENT` only for that stored UID. The script then creates a same-name successor out of band, not through the adapter. A fresh process reports `JOB_UID_REPLACED` and leaves that successor's UID and creation timestamp unchanged. A fresh process with an empty `CCP_KUEUE_BINDING_DSN` stops before it builds an adapter. A fresh process whose database is unreachable still builds the adapter, and Apply, Inspect, and DELETE all return errors: it does not adopt the successor and it does not delete it. The database login in this job is a disposable superuser. It is not the trusted `ccp_kueue_job_binding` role, and `NewServiceAccountKueueJobAdapter` would refuse it. The job is not production HTTP wiring.

The same disposable cluster also runs an admission-versus-placement case. After the node is tainted `ccp-kueue-smoke/unplaceable=true:NoSchedule`, a fresh process calls `NewQueuedServiceAccountKueueJobAdapter` with the `ccp_kueue_job_binding` login from migration 0015. That constructor refuses the superuser DSN. The process submits one queued Job through the existing opt-in path. Kueue reserves quota and admits its Workload. The Pod is created and stays unscheduled because it does not tolerate the taint. `ProjectAdmissionWithoutPlacement` records the Workload namespace, name, UID, and resourceVersion, the quota reservation, and the Job and Pod UIDs separately from node binding. The projection phase is `NOT_READY`. `allocated`, `active`, `ready`, `costable`, and `released` stay false. A later process deletes only that Job UID with the existing UID precondition. Another process Inspects and reports `TARGET_ABSENT` for that UID. The script then creates a same-name successor outside the adapter. Inspect reports `JOB_UID_REPLACED` and leaves the successor UID and creation timestamp unchanged. `TARGET_ABSENT` is still not Allocation RELEASED.

The same disposable job also runs `journal` phases against the real ComputeOperation journal. Those phases do not add an operation-journal migration. Process A creates one suspended Job, persists that CREATE as `UNKNOWN`, and is SIGKILLed. A fresh process runs only `Worker.RecoverUnknown`, which Inspects and records `RESOURCE_PRESENT` for the pinned UID. After that, process A issues a UID-fenced DELETE of that same suspended Job through the CA-pinned ServiceAccount API, persists the DELETE operation as `UNKNOWN` with the acknowledged tombstone, and is SIGKILLed. A fresh process builds a new PostgreSQL repository and adapter and runs only `Worker.RecoverUnknown`. That recovery persists one `TARGET_ABSENT` observation whose `TargetResourceUID` is the deleted UID, plus the reconciliation outbox event. The DELETE `SUCCEEDED` row leaves `compute_operations.provider_resource_uid` null; that column is not the tombstone and this smoke does not read it as one. Recovery does not call `Apply`, so it does not issue a second DELETE. An empty DSN stops before an adapter is built. An unreachable DSN fails the journal read and leaves the DELETE `UNKNOWN` with no observation. After the name is absent, the script creates a same-name successor out of band and records a second DELETE of the original UID without calling Kubernetes. Inspect-only recovery of that operation is `JOB_UID_REPLACED` / `MANUAL_REVIEW`. The successor's UID and creation timestamp stay in place. **`TARGET_ABSENT` is still not Allocation RELEASED, Kueue quota release, or device or claim deallocation, and it does not close #33 or #32.**

`NewKueueJobAdapter` is the explicit in-memory constructor for tests and fakes. `NewServiceAccountKueueJobAdapter` is the in-cluster constructor. It opens `PostgresKueueJobBindingStore` from a binding DSN and never substitutes the memory store. An empty DSN, an unreachable database, a missing binding schema, or a connected role with broader privileges than migration 0015 grants fails closed at construction. Provision `ccp_kueue_job_binding` before that migration as `LOGIN NOINHERIT NOSUPERUSER NOCREATEDB NOCREATEROLE NOREPLICATION NOBYPASSRLS`. The migration grants `SELECT` and `INSERT` on both binding tables, `UPDATE (updated_at)` on the pin table, and `UPDATE (acknowledged, updated_at)` on the tombstone table, plus `EXECUTE` on the two guard functions. The pin column grant exists because PostgreSQL 16 requires `UPDATE` on one column for `SELECT ... FOR UPDATE`. The pin guard rejects every `UPDATE`, so that column cannot be rewritten. The login has no DDL and no privileges on Provider, Allocation, Workload, Operation, or Evidence tables. Each connection pins `search_path` to `pg_catalog, public, pg_temp`, so `pg_temp` is not searched first, and the binding SQL stays schema-qualified. The startup check attests the connected login's effective privileges, not only the role name. These tables are not wired to a production HTTP handler or `kueue-observe`. `cmd/kueue-provider-smoke` reaches them through the disposable `durable`, `journal`, and `admission` phases. The `admission` phase is the one that calls `NewQueuedServiceAccountKueueJobAdapter` with the restricted login. The `durable` and `journal` phases still pass the disposable superuser DSN to the unchecked store constructor. `compute_operations.provider_resource_uid` is not this pin: a CREATE row records a provider UID only after `SUCCEEDED`, and that success already required the pin. A DELETE `SUCCEEDED` row does not record one. This slice does not register Providers, drain them, or settle cost liability. It does not attest a signed receipt, and it does not admit, unsuspend, or schedule the Job. **`TARGET_ABSENT` is still not Allocation RELEASED, Kueue quota release, or device or claim deallocation.** #33 and #32 stay open.

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


## Optional exact queue-identity proof (PR #87)

The library method `KubernetesObservationReader.ObserveQueueBinding` is a separate **read-only, structurally attested** queue-mapping check, not part of the `kueue-observe` CLI or an Allocation lifecycle/finance write path. An authorized controller must supply a pinned `KueueQueueBindingQuery` with the Job scope and **expected LocalQueue and ClusterQueue names and UIDs** from a trusted policy or registry. A tenant-provided name/UID is not an authoritative QuotaBinding.

The method first uses the existing Job/Workload/Pod observation and then GETs the exact Kueue Workload, namespaced LocalQueue and cluster-scoped ClusterQueue. It verifies `Workload.spec.queueName -> LocalQueue.spec.clusterQueue -> Workload.status.admission.clusterQueue` (when quota is reserved), and checks exact resource UIDs/resourceVersions. All three queue-path objects are re-read; the Job is read again, and a stale/missing/rewritten identity fails closed. The returned `ClusterQueue.Namespace` is intentionally empty because ClusterQueue is cluster-scoped.

**Additional narrow RBAC required only by clients that invoke this optional library method:** extend that client's namespace Role with `get` on `workloads` and `localqueues` in `kueue.x-k8s.io`; separately grant `get` on `clusterqueues` through a cluster-scoped ClusterRole restricted to the expected `resourceNames` and bind it only to the observer's dedicated ServiceAccount. Do not grant mutating verbs or apply these permissions to other tenants indiscriminately. The existing `kueue-observe` command and its three-read Role remain unchanged.

The method does **not** authenticate the cluster independently, resolve a server-owned Tenant/Project/Pool QuotaBinding, verify ResourceFlavor object identities, provide a single transactional Kubernetes snapshot, or attest DRA/device-plugin allocation or release. It cannot mark Allocation READY/RELEASED or UsageInterval FINAL. CI TLS-mock tests and disposable CPU-only kind tests do not constitute real GPU evidence.
