# Remote Cluster Agent v1

Status: **implementation contract / staged rollout**  
Date: 2026-11-10  
Scope: `compute-control-panel`

## Decision

Add one long-running Cluster Agent deployment inside each managed Kubernetes cluster. The Agent establishes an **outbound mTLS HTTPS** connection to Compute Control Plane (CCP), advertises its version and observed capabilities, polls a durable command channel, executes only typed Provider operations and returns bounded results.

The existing Provider SPI remains the control-plane boundary:

```go
type Adapter interface {
    Apply(context.Context, Request) (Acknowledgement, error)
    Inspect(context.Context, Request) (operation.Observation, error)
}
```

A `RemoteClusterAgentAdapter` implements this interface on the CCP side. It submits an immutable operation command to the durable command journal and waits for the Agent result. The Cluster Agent uses a local executor which implements the same semantic boundary. This preserves Recovery Worker behavior: its call to Provider.Inspect crosses the remote command channel without changing the durable ComputeOperation state machine.

## Authority and ownership

| Fact | Authority |
|---|---|
| Project, ComputePool, Accelerator desired state and revisions | Compute CP |
| ComputeWorkload, Allocation, ComputeOperation, operation lease/fence | Compute CP PostgreSQL |
| Cluster Agent identity, cluster binding, lifecycle and session epoch | Compute CP PostgreSQL |
| Remote command identity, delivery lease/fence and completion receipt | Compute CP PostgreSQL |
| Actual Job/Pod/Kueue/DRA/Node and Provider-native resource facts | Kubernetes, Kueue, DRA/device plugin or Provider API |
| Agent process health and local command deduplication | Cluster Agent process + its persistent local journal |

The Agent does not become a second Compute control plane. It cannot create Workloads, choose Projects/Pools, mutate Allocations, update ComputeOperation rows, allocate quota, schedule Pods or declare device readiness. It executes a narrow `APPLY` or `INSPECT` request already authorized and resolved by CCP. It returns evidence; CCP's Recovery Worker and reconciliation journal remain the only owners of ComputeOperation outcomes.

A Provider is mapped to one registered `cluster_agent_id` through a tenant-independent, operator-managed binding. Several Provider IDs may use one Agent in the same `cluster_scope`. Agent registration is separate from Provider registration: a Provider fingerprint does not authenticate a remote process.

## Network and identity

- The Agent initiates every connection. The managed cluster needs outbound access to the CCP HTTPS endpoint; CCP requires no route into the cluster.
- TLS is mutual. The Agent certificate must chain to the configured client CA and contain exactly one SPIFFE URI SAN in the configured trust domain.
- CCP resolves that exact SPIFFE ID to a pre-provisioned `cluster_agent_id` and `cluster_scope`. Caller-provided agent IDs, cluster names, tenant IDs or Provider IDs are never identity claims.
- Agent registration and Provider-to-Agent binding are privileged bootstrap operations, not self-service Agent endpoints.
- Each process start supplies a fresh `instance_id`. The server issues a monotonically increasing `lease_epoch`. A new instance fences completions from the previous process.
- The Agent does not receive a database credential, kubeconfig, Provider credential, Permit or arbitrary URL from a command.

## Transport

Protocol schema: `compute.cluster-agent/v1`. Initial transport is HTTPS JSON with bounded request/response bodies and short polling; this avoids introducing a broker or gRPC control dependency before the first remote-cluster golden path exists.

| Method | Endpoint | Purpose |
|---|---|---|
| POST | `/api/v1/cluster-agent/heartbeat` | Refresh Agent lease; publish Agent version and capability snapshot |
| POST | `/api/v1/cluster-agent/commands/claim` | Claim one eligible command using current instance and lease epoch |
| POST | `/api/v1/cluster-agent/commands/{command_id}/complete` | Submit the result under the exact command fence |

No GET endpoint reveals cluster data. Responses are `no-store`. A heartbeat response returns the server-issued lease epoch and heartbeat interval. The Agent backs off on 429/5xx and certificate/identity errors fail closed.

## Command envelope

A command binds all dimensions needed to prevent cross-resource replay:

- `command_id`, `schema_version`, `command_kind` (`APPLY` or `INSPECT`);
- `agent_id`, `provider_id`, `cluster_scope`;
- `operation_id`, `tenant_id`, `workload_id`, `desired_generation`;
- `idempotency_key`, `request_digest`, exact target resource ID + UID when applicable;
- `command_fence`, `agent_lease_epoch`, `lease_expires_at`.

The request body is the existing typed Provider Request. No shell, arbitrary executable, generic HTTP proxy, raw Kubernetes manifest or caller-supplied credentials are accepted. The Agent validates the complete envelope before the local Provider executor is invoked.

## Delivery and recovery

Command delivery is at-least-once at the transport boundary; provider effects remain idempotent by the immutable ComputeOperation identity and request digest.

- The command journal uses `PENDING → LEASED → SUCCEEDED | FAILED | UNKNOWN`.
- Every claim increments a command fence. Completion must match `command_id + agent_id + instance_id + lease_epoch + command_fence`.
- A stale Agent instance or stale command lease cannot complete a command.
- An expired `APPLY` command becomes `UNKNOWN`; it is **never automatically re-dispatched**. This covers a crash after the remote side effect but before the completion response.
- An expired `INSPECT` command may return to `PENDING`, because inspection is read-only.
- A late completion after expiry is rejected. The CCP Provider adapter reports the outcome as ambiguous; the Recovery Worker then creates a new `INSPECT` command through the same Provider SPI.
- The Agent persists `command_id + request_digest + execution state + result` locally. Repeated delivery of the same completed command replays the stored result. A command left `EXECUTING` after process restart is treated as ambiguous; it is not run a second time.
- A remote Agent ACK is not proof of Job admission, Pod placement, GPU device ownership, Allocation READY or release. Only the authoritative Kubernetes/Kueue/DRA/Provider inspection path may provide those facts.

## Lifecycle

Cluster Agent identity is stable across process restarts. Session identity is not.

```text
REGISTERED → CONNECTED ↔ SUSPECT
                  ↓
               DRAINING → RETIRED
```

- Heartbeat renews the lease and reports Agent version/capabilities.
- Lease expiry makes the Agent unavailable for new command claims; the control plane may project it SUSPECT.
- DRAINING stops new claims. Existing leased commands are allowed to finish until their lease expires or are explicitly reconciled.
- RETIRED is terminal. Replacing a certificate or Agent binary does not silently create a new cluster identity.
- Capabilities are timestamped Agent observations. They are not Provider readiness or GPU inventory truth.

## Initial deployment profile

Run as a Deployment with one active command-polling process per registered Agent identity. Use a dedicated ServiceAccount and namespace-scoped RBAC for the exact operations supported by the selected Provider adapter. Disable service-account token automount when not needed; otherwise use projected short-lived tokens. Mount the persistent command journal on a dedicated PVC with restrictive file permissions. Do not use cluster-admin.

Initial Kueue Job support remains bounded by the existing adapter contract. In particular, a suspended Job with `POD_PLACED` evidence does not prove a GPU device was assigned. DRA, queue quota release, GPU readiness and production release attestation remain separate acceptance work.

## Acceptance gates

1. Valid mTLS/SPIFFE identity resolves to exactly one registered Agent; malformed, duplicate or untrusted identities are denied.
2. An Agent cannot claim or complete another Agent's command or a command from another cluster scope.
3. A process restart increments `lease_epoch`; an old process completion is fenced.
4. An expired APPLY becomes UNKNOWN and is never claimed again; Recovery Worker resolves it only with a fresh INSPECT.
5. An expired INSPECT can be reclaimed with a higher command fence.
6. Same command ID + same digest is idempotent; same ID + changed digest is rejected.
7. Local journal restart replays a completed result; an EXECUTING command returns UNKNOWN and never executes twice.
8. Agent outage blocks new remote Apply/Inspect without changing ComputeOperation authority. Heartbeat recovery restores dispatch only after a new lease.
9. Kueue/Kubernetes evidence remains UID-bound and provenance-labelled; Agent heartbeat never implies capacity readiness.
10. Disposable kind + PostgreSQL SIGKILL test proves Agent, control-plane worker and provider effect recovery independently.

## Rollout boundary

The first implementation slice adds the typed protocol, durable Agent/command schema, pull/heartbeat/complete server boundary and a remote Provider SPI adapter. Production mounting, CA issuance/rotation, RBAC installation, persistent Agent deployment, and real GPU/DRA acceptance remain gated until end-to-end tests establish the complete path.
