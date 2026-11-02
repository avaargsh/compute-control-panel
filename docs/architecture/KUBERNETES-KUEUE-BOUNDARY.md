# Kubernetes and Kueue Integration Boundary

Status: Proposed v0.2  
Date: 2026-11-02  
Decision for v0.1 POC: Kubernetes Job + Kueue built-in Job integration

## 1. Terms are not interchangeable

| Platform concept | External Kubernetes/Kueue concept |
|---|---|
| ComputeWorkload | Platform-owned compute request; not a Kueue CRD |
| Allocation | Platform-owned provider binding and evidence |
| ComputePool | Logical platform pool; not a ClusterQueue |
| QuotaBinding | Mapping from tenant/project/pool to queue references |
| Kueue Workload | Kueue-owned unit of admission |
| LocalQueue | Namespaced Kueue queue selected by a Job/workload |
| ClusterQueue | Cluster-scoped quota, cohort and fair-sharing policy |
| ResourceFlavor | Kueue description of resource/node characteristics |
| Kubernetes Job | Execution object created by the Compute Kubernetes adapter |
| Pod | Child object managed by the Job controller |
| Node binding | Decision made by Kubernetes Scheduler |

The Kueue Workload must never be called or persisted as the platform's ComputeWorkload. They have different owners, schemas and lifecycle semantics.

## 2. v0.1 integration choice

Use one path:

~~~text
Agent Control Panel
  -> Compute CP API: Create ComputeWorkload
       -> durable ComputeWorkload + ComputeOperation
            -> Compute Kubernetes adapter creates one suspended Job
                 -> Kueue built-in Job integration observes Job
                      -> Kueue creates/updates its native Workload
                           -> LocalQueue -> ClusterQueue quota/admission
                                -> Kueue admits Job
                                     -> Job controller creates Pods
                                          -> Kubernetes Scheduler binds Pods to Nodes
                                               -> kubelet / device plugins / provider run resources
~~~

The Compute Kubernetes adapter is the sole writer for the top-level Job create/delete request in this path. Kueue's built-in Job integration owns its native Workload object and admission status. The Job controller owns Pods and Job status. Kubernetes Scheduler owns Pod placement.

Compute CP reads and projects the external status; it does not write Kueue admission conditions, quota reservations, Pod bindings or controller-owned child state.

### Required implementation rules

- Create the Job in a tenant-scoped namespace with stable labels/annotations for compute_workload_id, allocation_id and desired_generation.
- Select the namespaced LocalQueue through the supported Kueue Job integration label/configuration for the deployed Kueue version. Keep the mapping in QueueBinding; do not expose raw Kueue objects as the Agent API.
- Create the Job suspended/managed according to the supported Kueue integration contract. Do not manually create a second Kueue Workload for the same Job.
- Treat the Kueue-generated Workload as an observed external resource. Record its UID/name/resourceVersion, but do not patch or delete it directly.
- Kueue admission and quota conditions are authoritative for queue admission. Pod Scheduled/NodeName is authoritative for actual node placement.
- Delete the Job through the same ComputeOperation journal. Let the Kueue integration reconcile the associated Workload and reservation; verify both resource absence and applicable quota release before marking Allocation RELEASED.

## 3. Ownership of each external object

| Object / decision | Create or decision owner | Update/status owner | Delete/cleanup owner | Compute CP behavior |
|---|---|---|---|---|
| ComputeWorkload | Compute CP API | Compute CP reconciler | Compute CP | Full domain authority |
| Kubernetes Job | Compute Kubernetes adapter | Job controller / Kueue integration for managed fields | Compute adapter requests deletion; Kubernetes controllers finalize | Own intent; observe actual status |
| Kueue Workload | Kueue Job integration | Kueue controllers | Kueue integration/controller | Read-only projection |
| LocalQueue | Cluster/platform admin or separately governed config manager | Kueue/admin | Cluster/platform admin | Reference through QueueBinding |
| ClusterQueue | Cluster/platform admin | Kueue/admin | Cluster/platform admin | Reference only in v0.1 |
| ResourceFlavor | Cluster/platform admin | Kueue/admin | Cluster/platform admin | Reference only in v0.1 |
| Pod | Kubernetes Job controller | Kubernetes controllers / kubelet | Kubernetes controllers | Read-only observation |
| Pod-to-Node binding | Kubernetes Scheduler | Kubernetes Scheduler | Kubernetes control plane | Never choose or patch NodeName |
| Device claims / device allocation | Kubernetes DRA driver and supported controllers | Kubernetes DRA/device controllers | Corresponding controllers | Capability-gated observation |

## 4. Queue and pool mapping

ComputePool is a platform grouping and policy boundary. It may map to a Kubernetes cluster plus a permitted set of LocalQueues, ClusterQueues and ResourceFlavors, but it is not a second quota ledger.

QuotaBinding maps tenant/project and workload class to:
- Kubernetes cluster/provider identity;
- namespace and LocalQueue;
- permitted ClusterQueue/ResourceFlavor domain;
- policy revision and effective time.

Kueue remains authoritative for:
- quota reservation and release;
- admission and requeue;
- borrowing, lending and fair sharing;
- preemption;
- flavor assignment;
- AdmissionChecks and topology-aware admission.

Compute CP may reject a request for platform policy reasons before submission. It must not predict or claim Kueue admission by recomputing available quota locally.

## 5. Alternative: direct Kueue Workload integration

Directly creating Kueue Workload objects is not the v0.1 path. It is only considered for a custom workload type that cannot be represented by a supported Job integration and has a dedicated Kueue integration/controller with explicit ownership of the workload-to-execution mapping.

If a future workload class uses direct Kueue Workload creation:
- it must not also use the built-in Job integration for the same logical execution;
- one controller owns creation and mutation of that Kueue Workload;
- the controller owns the mapping to the execution object;
- deletion and quota release semantics are contract-tested;
- the decision is recorded in a new ADR and compatibility matrix.

## 6. GPU, DRA and vendor integrations

GPU and DRA support is capability-gated. Compute CP describes normalized requirements and records provider observations. Kueue and the device/provider integration own actual admission and device allocation.

Do not assume every cluster supports the same:
- extended resource names;
- DRA APIs and driver versions;
- ResourceFlavor labels;
- topology constraints;
- MIG/vGPU slicing semantics;
- HAMi or vendor-specific admission behavior.

A provider capability is usable only after discovery and compatibility validation. Unsupported or unknown capabilities fail closed.

## 7. POC acceptance

The real-cluster POC must demonstrate:

1. Compute CP creates exactly one suspended Job for a ComputeWorkload generation.
2. Kueue creates exactly one corresponding native Workload through the selected Job integration.
3. With insufficient quota, the Job remains queued/suspended and no Pod is scheduled.
4. After admission, the Job is unsuspended and Pods are created; Kubernetes Scheduler selects Nodes.
5. Kueue preemption/requeue or admission rejection is reflected as an observation, not rewritten as a Compute CP scheduler decision.
6. Deleting the ComputeWorkload causes the Job and associated Kueue reservation to be cleaned up through their owners.
7. A lost create/delete ACK is recovered through ComputeOperation UNKNOWN and authoritative inspection.
8. No controller writes fields owned by another controller.

## 8. Official references

- Kueue concepts: https://kueue.sigs.k8s.io/docs/concepts/
- Kueue admission: https://kueue.sigs.k8s.io/docs/concepts/admission/
- Kueue Workload: https://kueue.sigs.k8s.io/docs/concepts/workload/
- Kueue run workloads: https://kueue.sigs.k8s.io/docs/tasks/run/
- Kubernetes Jobs: https://kubernetes.io/docs/concepts/workloads/controllers/job/
- Kubernetes scheduler: https://kubernetes.io/docs/concepts/scheduling-eviction/kube-scheduler/

Pin the Kueue and Kubernetes compatibility matrix per deployment. These links describe the upstream concepts; they do not imply that a cluster integration has been tested.
