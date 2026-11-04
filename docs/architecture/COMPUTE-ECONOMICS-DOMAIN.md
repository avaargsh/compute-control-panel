# Compute Economics Domain — Design v0.1

**Status:** Proposal  
**Scope:** Product and domain contract only. No production metering, billing or pricing implementation is implied.  
**Tracking:** [Issue #8 — Compute economics and GPU FinOps](https://github.com/avaargsh/compute-control-panel/issues/8)  
**Prerequisite:** [P0 integration gate #3](https://github.com/avaargsh/compute-control-panel/issues/3)

## 1. Product objective

Evolve Compute Control Plane from resource lifecycle control into a system that explains the economic consequences of compute-capacity decisions.

The product has four cooperating planes:

1. **Resource control** — Provider, Pool, Profile, Workload, Allocation, quota bindings and lifecycle.
2. **Workload control** — admission and execution intent, with Kubernetes/Kueue/DRA retaining their respective authorities.
3. **Observability and metering** — provider/device telemetry, usage intervals, health and workload-output evidence.
4. **Compute economics** — cost attribution, commercial revenue facts, unit-cost comparison and capacity economics.

The economics domain consumes trusted identity and evidence from the control and metering domains. It does not schedule workloads, mutate provider resources, or become a general ledger/invoicing system.

## 2. Domain boundaries

### Existing domain remains authoritative

- Compute Control Plane owns ComputeProvider, ComputePool, ComputeProfile, Workload intent, Allocation, quota bindings, ComputeOperation and ResourceObservation.
- Kueue owns queue admission and quota admission.
- Kubernetes scheduler owns Pod placement.
- Device plugins, DRA and Provider adapters own resource/device realization.
- Runtime and upstream control planes own their respective execution identities and intent.

### New economics domain owns

- normalized usage facts and metering policies;
- effective-dated Provider procurement rates;
- cost calculation entries and attribution explanations;
- commercial price policies and revenue facts when an authoritative contract/billing source exists;
- capacity supply/demand snapshots and derived economic analytics.

Economics records reference existing domain identities. They must not create a competing Workload or Allocation identity.

## 3. Core metric vocabulary

These measures are deliberately separate. API fields, reports and dashboards must use the same names and definitions.

| Measure | Definition | Primary use |
|---|---|---|
| capacity_gpu_hours | Healthy/eligible physical capacity multiplied by elapsed time; denominator must state whether it is installed, provisioned or schedulable capacity | Capacity inventory and utilization denominator |
| allocated_gpu_hours | GPU device quantity assigned to a stable Allocation multiplied by its accepted Allocation interval | Initial direct Workload cost attribution |
| reserved_gpu_hours | Capacity contractually held by a Provider commitment or customer reservation, independent of Workload activity | Commitment and reservation economics |
| active_gpu_hours | Allocated GPU time above an explicitly versioned activity threshold | Operational activity reporting |
| activity_weighted_gpu_hours | Integral of normalized activity signal (0–1) over allocated GPU time | Utilization-weighted analysis; not billing |
| unallocated_idle_gpu_hours | Healthy/schedulable capacity time with no Allocation | Unallocated capacity aging |
| allocated_inactive_gpu_hours | Allocated time below the activity threshold | Underutilized allocation analysis |
| billable_gpu_hours | Quantity chargeable under a commercial contract and BillingPolicy | Revenue calculation |
| effective_workload_output | Workload-specific measured output, e.g. tokens, samples or jobs completed | Delivered-cost comparison |

Rules:

- GPU utilization is an operational observation, not a universal compute unit, billing unit or proof of useful output.
- active_gpu_hours uses a versioned threshold and sampling/aggregation policy. activity_weighted_gpu_hours is a separate weighted metric; neither is a commercial billing quantity.
- unallocated_idle_gpu_hours and allocated_inactive_gpu_hours are mutually distinct. An inactive allocation must not be counted as unallocated idle capacity.
- capacity_gpu_hours must disclose its denominator (installed, provisioned, healthy or schedulable); unavailable hardware must not silently inflate eligible capacity.
- effective_workload_output is workload-specific. Do not collapse unlike workloads into one synthetic GPU-hour measure.
- allocated, reserved, active, idle and billable time can overlap by design. Preserve dimensions instead of summing them as if they were disjoint.

## 4. GPU telemetry and effective-workload analytics

### Normalized observation envelope

A normalized observation should retain at least:

- tenant/project scope where known;
- Provider, Pool, host/node and device identity;
- GPU vendor, model, memory capacity and partition identity (for example MIG or another vendor's slice);
- observed timestamp, collection timestamp and source;
- metric name, numeric value, unit and aggregation window;
- adapter/source version and raw-source reference;
- quality status (valid, missing, stale, estimated, rejected).

Candidate metrics include compute activity, memory utilization and bandwidth, power, temperature, device health, PCIe/interconnect/RDMA activity, and workload throughput.

### Comparability

A common schema makes signals queryable; it does not make their semantics identical. Retain vendor/model/source provenance and document per-adapter mappings. Cross-model comparisons require either a comparable workload benchmark or an explicitly qualified normalization method.

A GPU reporting 90% activity is not necessarily delivering more useful work than a different model reporting 60% activity. Communication wait, memory pressure, throttling and workload shape can materially change useful throughput.

### Attribution

Telemetry can be linked to Workload cost only through a stable Allocation/device binding and a valid time interval. Unattributed device metrics remain infrastructure observations; do not guess the Workload owner.

A costable Allocation interval requires the accepted Allocation projection and authoritative boundary evidence under the P0 contract (Provider resource realization plus applicable Kueue admission and DRA/device ownership evidence). A Provider acknowledgement or a raw device metric alone does not establish a costable Workload Allocation.

## 5. Metering domain

### UsageSample

Immutable source observation or counter point. It preserves source identity, source timestamp, collection timestamp, units, adapter version and quality metadata.

### UsageInterval

A normalized interval derived from samples and lifecycle evidence. It identifies the resource, Allocation, Workload, start/end, quantity/unit, aggregation policy version and source sample references.

The v1 UsageInterval schema uses full GPU device counts only (GPU_DEVICE_COUNT). Fractional GPU sharing, MIG slices and vendor-specific partitions require an explicit profile-scoped unit contract before they can be charged; they must not be coerced to whole-GPU counts.

The interval pipeline must define behavior for duplicate samples/events, missing samples and telemetry gaps, late arrival and out-of-order events, clock skew and timestamp correction, Allocation start/end boundaries, GPU partition changes and device replacement, counter reset and Provider identity changes.

A gap must be exposed as missing coverage or an explicit estimate. It must not silently become zero usage.

### MeteringPolicy

A versioned policy for sampling, aggregation, attribution and handling incomplete intervals. Each derived interval stores the policy version used.

## 6. Cost accounting

### Initial direct-cost basis

For the first auditable workload-cost slice:

**direct_workload_cost = priced_allocation_quantity × effective_provider_rate**

For GPU-hour rates, the quantity is derived from the exact Allocation interval and GPU quantity/fraction. The rate defines its own unit and basis; the system must reject incompatible units.

**Important:** the initial cost basis is allocated/provisioned capacity time, not GPU active time. A Provider may charge for reserved capacity even while a workload is idle. Active time is an operational efficiency metric and must not silently discount the Provider's actual charge.

### ProviderRateCard

Effective-dated procurement price facts, keyed by Provider and applicable resource dimension (Pool, Profile, model or resource class). Each rate must include:

- rate ID and immutable version;
- currency and unit;
- rate basis (for example per allocated GPU-hour, reserved GPU-hour, host-hour or fixed-period commitment);
- effective start/end;
- source/contract reference and provenance;
- tax/inclusion metadata where known;
- optional minimum commitment, tier or discount terms.

A future rate change must not rewrite historical cost calculations.

### WorkloadCostEntry

Append-only calculated cost fact with:

- stable Workload and Allocation references;
- Provider, Pool, Profile and resource identity;
- source UsageInterval IDs and covered time range;
- priced quantity and unit;
- rate-card version and calculation version;
- amount and currency;
- allocation method and completeness/quality status;
- correction/reversal linkage.

Cost entries must be reproducible from source usage facts and immutable rate/policy versions.

### Shared and idle overhead

Keep these separate from direct Workload cost:

- unallocated capacity cost;
- reserved commitment not yet assigned;
- idle capacity cost;
- shared storage/network/power overhead;
- platform operations overhead.

A later explicit allocation policy may distribute shared costs to projects or tenants. The report must expose the policy and allocation basis. Never silently spread idle capacity cost into direct Workload cost.

## 7. Provider unit-cost analysis

Start with directly comparable procurement measures:

- procurement cost per GPU-hour for the same resource/rate basis;
- cost of available/healthy capacity;
- committed versus on-demand rate;
- effective capacity after unavailable or unhealthy resources.

Add workload-specific delivered-cost measures only when benchmark context is comparable:

**delivered_cost_per_output = attributable_cost / measured_workload_output**

Examples include cost per million generated tokens or cost per training sample. Comparisons must identify model, precision/quantization, batch/concurrency, sequence length, parallelism, software stack and benchmark version where relevant.

Provider ranking must disclose GPU/profile and workload comparability, data coverage and missing telemetry, availability/failure context, and rate-card source and validity window.

Do not rank dissimilar GPUs by hourly price or raw utilization alone.

## 8. Commercial revenue and gross margin

Keep the following concepts independent:

- CommercialRateCard / BillingPolicy: sell price and billable unit/rules;
- BillableUsage: quantity eligible for charging under a contract;
- RevenueEntry: revenue fact and recognition state from an authoritative contract/billing source;
- Invoice/Collection state: only when an authoritative billing integration provides it;
- WorkloadCostEntry: direct compute cost.

A reservation for eight GPUs can be billable for the contracted reservation window even when activity is low. Therefore billable hours must not be inferred from active hours.

A preliminary gross-margin view can be defined as:

**gross_profit = recognized_revenue - defined_direct_COGS**

**gross_margin_rate = gross_profit / recognized_revenue**

The definition of direct COGS must be explicit (Provider procurement cost and any included direct power/network/storage costs). Fixed overhead, depreciation, tax, refunds, credits, FX and accounting recognition rules require separate policies or authoritative source integrations.

When input facts are incomplete, report margin as incomplete/unknown with coverage details. Do not display a precise profitability claim from partial data.

## 9. Capacity supply-demand analytics

Use time-indexed facts to represent:

- total, healthy, schedulable, allocated, reserved and idle capacity;
- pending and admitted demand by Pool/Profile;
- running workload demand;
- idle capacity aging and fragmentation;
- provider-specific shortage/surplus;
- demand history and forecast scenarios.

Recommendations (acquire, reduce, rebalance, discount or retire capacity) are advisory outputs with evidence and assumptions. The economics module must not directly bypass Kueue admission or Kubernetes placement.

## 10. Proposed context map

    Provider / Pool / Profile
              |
              v
      Workload + Allocation ------> Lifecycle / identity evidence
              |                                  |
              v                                  v
        UsageSample -----> UsageInterval <--- Device telemetry adapters
              |                   |
              |                   +------> GPU utilization / idle analytics
              |                   |
              v                   v
       ProviderRateCard ------> WorkloadCostEntry
                                      |
       CommercialRateCard --> BillableUsage --> RevenueEntry
                                      |
                                      v
                             Provider / Pool economics
                                      |
       CapacitySnapshot + DemandSignal + BenchmarkObservation
                                      |
                                      v
                       Supply, demand and margin analysis

## 11. Delivery phases and gates

### E0 — Contract and vocabulary (can begin before P0 closes)

- [ ] Freeze the metric glossary in section 3.
- [ ] Define schemas and event envelopes for UsageSample, UsageInterval, ProviderRateCard and WorkloadCostEntry.
- [ ] Freeze provenance, idempotency, correction, retention and completeness rules.
- [ ] Define direct versus shared/idle overhead treatment.
- [ ] Define commercial pricing and revenue-recognition boundaries.
- [ ] Add calculation examples and deterministic golden fixtures.

### E1 — Metering foundation (after stable Allocation identity)

- [ ] Ingest normalized device metrics with vendor/model/source provenance.
- [ ] Bind metrics and time intervals to exact Allocation/Workload identities.
- [ ] Produce allocated/active/idle GPU time and coverage indicators.
- [ ] Test duplicates, late samples, missing data, clock skew, counter reset and lifecycle boundaries.

### E2 — Cost accounting v1

- [ ] Add effective-dated ProviderRateCard with currency/unit/basis validation.
- [ ] Calculate auditable WorkloadCostEntry from Allocation-hours.
- [ ] Query cost by Workload, Project, Tenant, Provider, Pool and Profile.
- [ ] Support reconciliation and append-only reversal/replacement for corrected inputs.

### E3 — Provider economics and revenue

- [ ] Compare provider procurement cost per GPU-hour.
- [ ] Add benchmarked cost-per-output comparisons for qualified workload profiles.
- [ ] Add independent commercial pricing, billable usage and RevenueEntry.
- [ ] Calculate gross profit/margin only under an explicit completeness policy.

### E4 — Capacity economics

- [ ] Add supply/demand snapshots and idle-capacity aging.
- [ ] Identify profile-level surplus, shortage and fragmentation.
- [ ] Produce explainable purchase/reduce/rebalance recommendations.

## 12. Acceptance tests

1. A deterministic fixture calculates workload cost from Allocation intervals and a versioned rate card with exact reproducibility.
2. Replaying the same usage source does not double-charge.
3. Late or corrected usage produces traceable reversal/replacement entries; history remains explainable.
4. Every cost result traces to source usage, Allocation, Provider and rate-card/calculation versions.
5. Reserved, allocated, active, idle and billable GPU-hours are separately queryable.
6. Unlike GPU models are not compared by raw utilization alone.
7. Delivered-cost comparisons expose benchmark configuration and coverage.
8. Revenue and margin require commercial source facts and direct-cost coverage; incomplete values remain explicitly incomplete.
9. Capacity analytics distinguish healthy/schedulable supply, allocation, pending demand and idle capacity without changing scheduler authority.

## 14. E0 machine-readable contract assets

The initial schemas and examples are cataloged in [schemas/compute-economics](../../schemas/compute-economics/README.md):

- UsageSample and tenant-scoped Event Envelope;
- UsageInterval, ProviderRateCard and WorkloadCostEntry;
- golden device-telemetry event and effective-rate-boundary cost fixture.

The cost calculation contract is [Workload Cost Calculation v1](compute-economics/WORKLOAD-COST-CALCULATION-V1.md). Data lifecycle and audit lineage are defined in [Retention and Evidence Policy](compute-economics/RETENTION-AND-EVIDENCE-POLICY.md).

These files are proposed contracts under PR #9. They do not imply production ingestion, a live broker, a billing integration or accepted Allocation projection.

## 13. Dependencies and non-goals

**P0 blockers:** stable Allocation identity/projection; authenticated Provider and Kubernetes/Kueue/DRA evidence; trusted tenant/project scope; durable event delivery and replay semantics. Track these in [Issue #3](https://github.com/avaargsh/compute-control-panel/issues/3).

**Non-goals for this design slice:**

- implementing a custom scheduler or replacing Kueue/Kubernetes;
- treating GPU activity as a billing unit;
- asserting accounting-grade invoices, taxes, revenue recognition or general-ledger integration without authoritative sources;
- implementing automated Provider purchasing, repricing or capacity mutation;
- treating FakeProvider observations as production economics evidence.

This document is a product/domain proposal. Implementation begins only after the required P0 evidence gates are accepted.
