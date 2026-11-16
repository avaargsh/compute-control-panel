# Kueue/DRA patch regression gate (2026-11-16)

Status: **test specification only — NOT cluster evidence or release approval**.
Authority: [GPU consolidation](../migration/GPU-COMPUTE-PLATFORM-CONSOLIDATION.md), [issue #33](https://github.com/avaargsh/compute-control-panel/issues/33).

## Version matrix
- Kubernetes version and feature gates: record from actual cluster; do not assume DRA is enabled.
- Kueue v0.19.12 -> v0.19.13 (DRA invalid ResourceSlice pool exclusion and quota behavior).
- Kueue v0.20.5 -> v0.20.6 (CRD conversion webhook and DQO stale capacity).
- DRA driver, DeviceClass, ResourceClaim and API group versions: capture before execution.
- Use a disposable cluster. Do not combine patch lines or assert that either is deployed.

## Gated test cases

| ID | Input / fault injection | Required observations | Fail-closed CCP assertion |
| --- | --- | --- | --- |
| G01 | Create Kueue-admitted Workload while Pod is unscheduled | Kueue Workload UID, admission condition, Job/Pod UID, empty node binding | Allocation stays NOT_READY; no busy GPU billing |
| G02 | Advertise incomplete/stale ResourceSlice pool, then repair it | ResourceSlice pool generation, DeviceClass, ResourceClaim allocation and Kueue quota | Invalid pool never becomes allocatable; no false reservation or device assignment |
| G03 | Upgrade v0.20.5 to v0.20.6 with CRDs previously configured for v1beta1 conversion | CRD conversion spec, apiserver list/watch success for ClusterQueue, Cohort, LocalQueue, MultiKueueCluster, Workload | No silent watch-cache failure; observation freshness expires closed |
| G04 | DQO aggregation failure with >64 distinct resources for a flavor | EffectiveCapacityComputed=False / AggregationFailed; effectiveCapacity cleared | Pool capacity projection must not reuse stale value |
| G05 | Lose CREATE ACK, restart worker, recreate same-name Job with new UID | Durable operation ID, fence, Inspect, exact Job/Pod UIDs and resourceVersions | No blind CREATE retry, no successor UID adoption |
| G06 | Delete old Job UID while retaining Kueue quota or DRA claim | Exact target UID absence, quota release and claim/device deallocation | RELEASED and liability closeout blocked until all three proven |
| G07 | Drop watch events, relist, then preempt and restart | Source UID/version, list RV, observed timestamps, journal transitions | Deduplicate stale observations; UNKNOWN stays inspect-first |
| G08 | Compare on-demand vs preemptible for same workload and GPU class | Provider pricing provenance, billed seconds, allocated/busy/successful GPU hours, retries, wait, reclaim | No cost posting without authenticated usage evidence; calculate effective GPU-hour cost with ancillary charges |

## Execution protocol

1. Pin CCP commit, Kubernetes/Kueue/driver versions, feature gates, test manifests and immutable artifact digests.
2. Provision isolated tenant namespace, LocalQueue, ClusterQueue, ResourceFlavor, DeviceClass and test GPU driver; capture RBAC and source identities.
3. Execute each case with fault injection; record timestamped raw API GET/LIST/WATCH and authenticated Provider Inspect alongside CCP PostgreSQL journal rows.
4. For G03 verify CRD served/storage versions and conversion webhook *before and after* upgrade, plus API server watch health.
5. For G05-G07 kill/restart the CCP worker; verify monotonic fence and no duplicate side effects.
6. For G08 compute cost per successful busy GPU hour and report queue wait, allocated-to-busy ratio, preemption waste and p95 reclaim latency. Compare only matching GPU, region and workload input.
7. Record exact commands, expected vs actual observations, exit codes and cleanup results. Mark every unexecuted case UNKNOWN, never PASS.

## Evidence manifest required per case

```yaml
case_id: G01
status: UNKNOWN # PASS | FAIL | UNKNOWN
ccp_commit: null
kubernetes_version: null
kueue_version: null
dra_driver_version: null
feature_gates: []
tenant_id: null
provider_id: null
pool_id: null
workload_id: null
allocation_id: null
operation_id: null
operation_generation: null
operation_fence: null
kueue_workload_uid: null
job_uid: null
pod_uid: null
resourceclaim_uid: null
resource_versions: {}
source_identity: null
observed_at: null
raw_evidence_paths: []
test_command: null
exit_code: null
cleanup_evidence: null
```

## Release decision

Do not mark any case GREEN from fake-provider unit tests, successful repository CI or an admitted Workload alone. Real cluster device allocation/release, authenticated source identity, exact UID finality, independent DB roles and restart/recovery must be evidenced separately. This specification adds no claim of a completed legacy migration.
