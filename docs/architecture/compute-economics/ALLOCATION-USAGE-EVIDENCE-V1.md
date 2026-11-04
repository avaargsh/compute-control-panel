# Allocation Usage Evidence v1

**Status:** E1 evidence-gate proposal; implementation and real-cluster validation pending  
**Parent:** [Compute Economics Domain](../COMPUTE-ECONOMICS-DOMAIN.md)  
**Tracking:** [Issue #8](https://github.com/avaargsh/compute-control-panel/issues/8)

## 1. Purpose and hard boundary

This contract defines when a platform Allocation may produce costable GPU usage. It joins the platform's durable intent to independently authoritative Provider, scheduler and device-allocation facts.

The initial Workload acceptance transaction creates a Workload, an Allocation in `PENDING`, a CREATE ComputeOperation and outbox events atomically. That transaction proves durable intent and stable identity only. It does not prove that a Provider resource exists, Kueue admitted a job, a Pod was scheduled, or a GPU device was assigned.

A Provider API acknowledgement alone is also insufficient. The ComputeOperation journal records the intent and reconciliation result; ResourceObservation records facts reported by an authoritative external source. Those axes remain separate.

**No FINAL UsageInterval or WorkloadCostEntry may be produced from:**
- an initial `PENDING` Allocation;
- a provider create/accept ACK without a correlated inspect observation;
- a Kubernetes Job existing without Kueue admission;
- Kueue admission without an exact workload/Allocation/generation join;
- a Pod request or node label without authoritative device-allocation evidence;
- a stale observation, conflicting resource UID, or unknown desired generation.

## 2. Existing source facts and their authority

| Evidence source | What it can prove | What it cannot prove alone |
|---|---|---|
| Workload + Allocation projection | Tenant-scoped logical identity, requested Provider/Pool/Profile, generation and current platform phase | External capacity exists or is admitted |
| ComputeOperation | Durable external mutation intent, request digest, fencing and reconciled operation outcome | Kueue admission or physical device ownership |
| Provider inspect observation | Provider resource ID/UID, resource kind, Provider scope, lifecycle and Provider-side lease/binding facts | Kueue quota admission or Kubernetes device assignment |
| Kueue native Workload observation | Native Kueue Workload UID, queue relationship, admission condition, ClusterQueue/flavor assignment and admitted resource request | Provider resource existence or exact device allocation |
| Kubernetes Job/Pod observation | Immutable Job/Pod UID, owner relationship, Pod scheduling and Node binding | A device's physical allocation identity unless the device integration exposes it |
| DRA or certified device-adapter observation | ResourceClaim/device allocation identity, driver/pool/device references and the exact allocated device count | Provider procurement lease interval unless linked to Provider evidence |

The Compute Kubernetes adapter creates the top-level Job. Kueue's built-in Job integration owns its native Workload and admission state. Job/Pod controllers own child resources. Kubernetes Scheduler owns Pod placement. DRA/device drivers own device allocation. Compute CP observes and projects these facts; it does not patch controller-owned fields.

The source model already provides useful provenance fields on ResourceObservation: `source`, `adapter_version`, `observed_at`, `source_timestamp`, `response_digest`, `evidence_ref` and freshness metadata. E1 must add role-specific validation around these facts instead of treating a generic `details` JSON object as self-authenticating.

## 3. Required evidence set

A costable evidence set is one immutable, tenant-scoped join for exactly one Workload, Allocation, desired generation, Provider, Pool and GPU Profile. Its logical shape is:

~~~json
{
  "schemaVersion": "compute.economics.allocation-usage-evidence.v1",
  "evidenceSetId": "evidence-demo-001",
  "tenantId": "tenant-demo",
  "workloadId": "workload-demo",
  "allocationId": "allocation-demo",
  "allocationGeneration": 1,
  "providerId": "provider-demo",
  "poolId": "pool-h100",
  "profileId": "gpu-h100-80gb",
  "providerBindingObservationIds": ["provider-observation-001"],
  "kueueAdmissionObservationIds": ["kueue-observation-001"],
  "deviceAssignmentObservationIds": ["dra-observation-001"],
  "podObservationIds": ["pod-observation-001"],
  "meteringPolicyVersion": "allocation-evidence-v1",
  "evidenceDigest": "sha256:..."
}
~~~

This is an illustrative logical record, not yet a released JSON Schema or a production API payload. The future typed schema must reference immutable source observations; it must not duplicate source status fields as independently writable truth.

Every referenced observation must:
1. Be tenant-scoped and match the exact Workload, Allocation, Provider and desired generation.
2. Carry a stable source event/observation identity, source system and adapter version.
3. Preserve the native immutable object UID and resourceVersion/revision where the source provides them.
4. Preserve source timestamp, collector `observedAt`, durable `recordedAt`, response digest and a durable evidence reference.
5. Match the expected resource kind, Pool/Profile and quantity; one observation from another generation or Provider is never substituted.
6. Be deduplicated by source identity and native UID/revision. Re-delivery must not create a second interval or add GPU time.

### 3.1 Provider binding

Require a successful ComputeOperation for the exact Allocation generation plus an authoritative Provider inspect observation that binds the exact Provider resource ID and immutable UID to that operation and Allocation.

The adapter must prove the resource's Provider/Pool/Profile identity and whole-device capacity. A provider request ACK, an uncorrelated resource name, or a resource ID without immutable UID is insufficient. Provider-specific billing/lease timestamps and release evidence must be retained separately from the operation's local timestamps.

### 3.2 Kueue admission

For the Job-based v1 path, require the Kueue-owned native Workload created by the supported Job integration. Retain its UID, namespace/name, owning Job UID, LocalQueue, ClusterQueue, selected ResourceFlavor, admitted condition and admitted resource request.

Admission is authoritative only when the native Workload's admitted condition is true for the expected Job/Allocation generation and the admitted GPU resource quantity/profile matches the requested Allocation. A suspended Job, queued Workload, quota estimate, or locally recomputed availability is not admission evidence.

### 3.3 Device assignment

Require evidence that the exact whole GPU devices have been assigned to the expected execution resource. The preferred v1 source is a DRA ResourceClaim allocation linked by immutable UID to the expected Pod/Workload and exposing the driver/pool/device identities.

A non-DRA device path is admissible only through a separately versioned, capability-certified Provider/device adapter contract that returns equivalent immutable assignment evidence. A Pod spec resource request, extended-resource limit, NodeName, node label or GPU telemetry sample alone does not prove device assignment.

The assigned device count must equal the costable GPU count. MIG slices, fractional devices and ambiguous device-to-Pod relationships remain out of v1.

## 4. Cost attribution window

Do not use Pod `RUNNING` duration or measured GPU activity as the cost basis. E0 prices allocated capacity time; an idle GPU that remains assigned to the Workload is still cost.

E1 must keep two intervals distinct:

- **Provider cost-liability interval:** when the Provider contract/lease makes the capacity cost-bearing, backed by Provider source evidence and the selected rate basis.
- **Workload attribution interval:** when the exact Provider capacity is reserved to this Allocation and the Kueue/device evidence proves the expected Workload owns that capacity.

The direct Workload costable window is the intersection of those intervals:

`costableStart = max(providerCostStart, workloadAttributionStart)`

`costableEnd = min(providerCostEnd, workloadAttributionEnd)`

The workload attribution interval begins only when the required Provider binding, Kueue admission and device assignment facts are all true for the same identities. Its start is the latest effective transition among those required facts. It ends when authoritative evidence shows that the Allocation no longer owns the admitted/device capacity or the Provider resource is no longer cost-bearing. Any continuing Provider lease after Workload attribution ends is tracked as reserved/unallocated capacity cost; it is not silently attached to the former Workload.

This intersection rule keeps procurement liability separate from ownership attribution, prevents charging a Workload before the actual device assignment is proven, and avoids hiding idle Provider capacity after the Workload releases it.

### 4.1 Effective timestamps

For each source transition, preserve both:
- source-effective time: the upstream transition timestamp, only when the adapter's versioned source contract defines it as trustworthy;
- collector observation time: when the adapter first observed the transition.

Never accept a caller-supplied time as financial evidence. If the source does not expose a trustworthy effective timestamp, use the first authoritative observation time and do not backdate the costable window. Clock skew, out-of-order delivery or timestamp ambiguity makes the affected boundary INCOMPLETE until reconciled.

The metering policy must define timestamp trust and maximum observation freshness for each source/adapter version. Do not infer transition time from event arrival order alone.

## 5. Lifecycle gates

| Platform phase / evidence | Cost treatment |
|---|---|
| Allocation `PENDING` | Intent only; zero costable GPU-hours |
| Provider operation `IN_FLIGHT` or `UNKNOWN` | No final usage; reconcile the same operation and Provider resource |
| Provider resource exists, but Kueue is not admitted | Keep Provider liability visible; do not attribute direct Workload cost under this v1 gate |
| Kueue admitted, device allocation absent/ambiguous | Attribution remains INCOMPLETE; no final WorkloadCostEntry |
| Provider + Kueue + device evidence match; Allocation `BOUND` | Start the derived Workload attribution interval |
| Allocation `RELEASING` | Continue only to the authoritative attribution/provider-liability end boundary; never infer release from DELETE ACK alone |
| Provider cost liability ended and Kueue/device ownership released | Close the attribution interval and finalize complete usage evidence |
| Stale generation, changed UID, conflicting device count or missing source | INCOMPLETE with a reason code; no partial FINAL posting |

The Allocation projection should expose the evidence-set reference and version that justified `BOUND`. Release should retain a separate immutable release evidence set. The platform may remain `RELEASING` while cleanup is ambiguous even when one cost-liability interval has ended; lifecycle completion and cost timing are related but distinct facts.

## 6. UsageInterval derivation

A FINAL UsageInterval may be emitted only when:
- the evidence set is complete for the deployment's required capability profile;
- tenant, Workload, Allocation, generation, Provider, Pool and Profile identities match across every source;
- provider capacity, admitted queue resources and device count reconcile to the same whole-GPU quantity;
- start/end windows are positive, offset-aware and covered by the evidence;
- event IDs and observation references are unique and immutable;
- no conflicting newer generation or unresolved operation can recreate/rebind the resource inside the window;
- the applicable metering policy and source freshness policy are pinned.

The interval's `sourceEventIds` must identify the complete source set. The evidence digest must bind canonical identity, source observation IDs, native UIDs/revisions, exact effective boundaries, GPU count, Provider/Pool/Profile, capability profile and metering policy version. Corrections append a superseding evidence set and a linked recalculation/reversal; they never rewrite the historical interval or ledger entry.

Possible non-final reason codes include `PROVIDER_BINDING_MISSING`, `PROVIDER_IDENTITY_MISMATCH`, `KUEUE_NOT_ADMITTED`, `KUEUE_IDENTITY_MISMATCH`, `DEVICE_ASSIGNMENT_MISSING`, `DEVICE_COUNT_MISMATCH`, `SOURCE_STALE`, `SOURCE_CONFLICT`, `GENERATION_MISMATCH`, `TIMESTAMP_UNTRUSTED` and `RELEASE_UNCONFIRMED`.

## 7. Required failure and replay tests

1. Initial PENDING Allocation cannot create UsageInterval or WorkloadCostEntry.
2. Lost Provider create ACK followed by successful inspect binds one existing resource; no duplicate Provider resource or usage interval is created.
3. Provider resource exists while Kueue is pending: no Workload-attributed final interval.
4. Kueue admitted without exact Job/Allocation UID or generation match: fail closed.
5. DRA claim/device identity absent, stale, duplicated or mismatched in count: fail closed.
6. Two Providers expose the same resource name but different Provider IDs/UIDs: evidence remains isolated.
7. Duplicate/out-of-order Provider/Kueue/DRA observations do not double-count or move a boundary backwards.
8. A stale worker/observation from a superseded generation cannot change current Allocation attribution.
9. Pod activity falls to zero while the Allocation still owns the GPU: allocated time continues to accrue.
10. Workload attribution ends while Provider lease remains: subsequent capacity cost moves to reserved/unallocated accounting, not the old Workload.
11. Provider release ACK is lost: cost end is not inferred until authoritative Provider lease/absence evidence is reconciled.
12. Release evidence is incomplete: no FINAL interval and no partial WorkloadCostEntry.
13. Recalculation after corrected evidence creates a superseding interval and linked reversal/replacement, preserving original evidence.

## 8. Implementation order and open decisions

1. Pin the deployed Kubernetes/Kueue/DRA compatibility matrix and the capability-specific evidence adapters.
2. Define the typed immutable AllocationUsageEvidence schema and the source-observation role contracts.
3. Implement the evidence-set builder/reconciler with tenant/generation/UID fencing and explicit INCOMPLETE reason codes.
4. Add PostgreSQL persistence for evidence-set identity, source links, unique event/revision keys and append-only correction lineage.
5. Implement UsageInterval derivation and run FakeProvider fault-injection/replay tests.
6. Run the real Kubernetes Job + Kueue + DRA cluster POC before enabling any production metering.

Open decisions before code promotion:
- Which Provider/Pool capability profile is the first real-cluster E1 target?
- Is DRA the required device evidence source for that profile, or is a certified device-plugin adapter available with equivalent immutable assignment facts?
- Which Provider contract field defines the start/end of procurement cost liability, and how is it reconciled to the rate-card basis?
- Which upstream source timestamps are trustworthy for effective time, and what freshness windows apply?

Until those decisions and tests are completed, this document is a proposed evidence gate, not an implemented capability. Production cost posting remains disabled.
