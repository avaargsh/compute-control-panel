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

## Executed scope and next executable slice

The merged [#108](https://github.com/avaargsh/compute-control-panel/pull/108) at
`745848e6` adds a **disposable-kind, UID-pinned Pod placement** observation.
Its exact feature-head `2965129` passed [Go/race/PG](https://github.com/avaargsh/compute-control-panel/actions/runs/43721009082)
and [real-controller kind smoke](https://github.com/avaargsh/compute-control-panel/actions/runs/43721009084).
The smoke uses a real Kubernetes scheduler and Kueue controller, but no authenticated
device driver or physical GPU allocation: `POD_PLACED` is still `NOT_READY`.
This is a **partial G01 observation**, not a G01–G08 PASS or a release certificate.

Next implementation should target **G06 three-way release** with a disposable,
pinned DRA-capable cluster and an authenticated source adapter. This is the
smallest missing proof that closes the dangerous distinction between
"Job no longer visible" and "GPU cost liability can safely end":

1. Persist the target Provider Job/Pod and ResourceClaim **UIDs**, tenant,
   Allocation generation, Provider operation/fence and claim-owner binding before DELETE.
2. Collect three independent, timestamped authorities: exact Provider/Pod
   UID absence, exact Kueue Workload reservation release, and exact ResourceClaim
   device deallocation. Include source identity, resourceVersion and adapter version.
3. Inject missing or stale evidence **one authority at a time**. Every gap,
   conflicting UID, changed generation, unreachable source or unconfirmed
   deallocation must leave `RELEASED=false` and cost finality blocked.
4. Recreate a same-name Job/Claim with a new UID. Verify old-UID cleanup
   cannot delete, adopt, or mistake the successor for the old target.
5. Restart the observer/controller between DELETE and Inspect; replay the
   same events and verify monotonic fencing with no duplicate finality.
6. Attach an exact-head run manifest and raw read-only API/Provider traces.
   A simulated DRA driver may prove **API claim-state correctness only**;
   physical GPU/device release remains UNKNOWN without hardware-driver evidence.

When G06 is not executable because the driver or authenticated source does not
exist, record `UNKNOWN`, do not change the scope to a fake-green test.

### Release evidence manifest extension

The following statuses are deliberately nullable: `null` means missing or
unverified, **never** success. The three sources must refer to the same
tenant, Allocation generation and original target UIDs.

```yaml
release_proof:
  provider_target:
    uid: null
    exact_uid_absent: null
    observed_at: null
    source_identity: null
  kueue:
    workload_uid: null
    reservation_released: null
    resource_version: null
    observed_at: null
    source_identity: null
  device:
    resourceclaim_uid: null
    device_deallocated: null
    resource_version: null
    observed_at: null
    source_identity: null
  successor_uid_if_any: null
  allocation_released: false
  liability_closeout_authorized: false
```

Do not promote the final two fields on the basis of one or two of the
three authorities. Financial posting and provider-liability closeout are
additional policy/authorization decisions under #8, #32 and #39.

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
