# Workload Cost Calculation v1

**Status:** E0 normative calculation proposal  
**Parent:** [Compute Economics Domain](../COMPUTE-ECONOMICS-DOMAIN.md)  
**Tracking:** [Issue #8](https://github.com/avaargsh/compute-control-panel/issues/8)

## 1. Scope

This calculation defines the initial direct compute cost for an exact, stable Compute Allocation using an effective-dated Provider procurement rate.

It does not calculate commercial revenue, billable usage, active GPU time, shared overhead, power, storage or network cost. It does not claim accounting-grade COGS.

## 2. Inputs and versioned contracts

- Usage interval: [usage-interval.schema.json](../../../schemas/compute-economics/usage-interval.schema.json)
- Provider procurement rate: [provider-rate-card.schema.json](../../../schemas/compute-economics/provider-rate-card.schema.json)
- Cost posting: [workload-cost-entry.schema.json](../../../schemas/compute-economics/workload-cost-entry.schema.json)
- Golden fixture: [workload-cost-allocation-rate-boundary-v1.json](../../../schemas/compute-economics/examples/workload-cost-allocation-rate-boundary-v1.json)

For v1, the costable usage interval must be exactly attributed to one tenant, project, Workload, Allocation, Provider, Pool and GPU Profile. Ambiguous or missing identity is not costable input. GPU quantity is a positive whole-device count; fractional GPU/MIG pricing requires a separate profile-scoped contract. The accepted Allocation interval must be supported by the P0 projection/evidence contract (including applicable Provider resource, Kueue admission and DRA/device evidence); a Provider acknowledgement alone is insufficient. V1 supports full GPU device counts only. Fractional shares and MIG/device partitions require a separate profile-scoped unit contract.

## 3. Calculation

For each Allocation interval, split the time range at every matching rate-card effective boundary.

Rate intervals use **start-inclusive, end-exclusive** semantics:

- effectiveFrom <= segment.start
- effectiveTo is null or segment.end <= effectiveTo

For each resulting segment:

`duration_hours = (segment_end - segment_start) in exact seconds / 3600`

`priced_gpu_hours = gpu_count × duration_hours`

`segment_cost = priced_gpu_hours × rate_amount_per_gpu_hour`

The rate basis must be ALLOCATED_GPU_HOUR and must match the usage quantity. A different rate basis requires a different evidence source and a separate calculation contract.

## 4. Rate selection and coverage

For every instant in the costable interval, exactly one matching ProviderRateCard must exist for Provider + Pool/Profile dimensions, currency and currency precision. V1 rates are fixed ALLOCATED_GPU_HOUR prices; tiered/minimum-commitment pricing is deferred.

- A rate gap means the calculation is INCOMPLETE.
- Overlapping matching rates mean the calculation is ambiguous and must fail closed.
- A rate change inside a usage interval requires splitting the interval at the effective boundary.
- A rate-card update must never rewrite already-posted historical entries; recalculate through linked reversal/replacement entries.
- Do not choose a "closest" rate, interpolate a missing rate, or silently fall back to another Provider/Profile.

A final cost entry requires complete time and identity coverage. Incomplete telemetry or rate coverage must remain visible as incomplete; it must not be converted to zero cost.

## 5. Arithmetic and rounding

- Parse decimal strings into exact decimal arithmetic; do not use IEEE-754 binary floating point for quantities, rates or currency.
- Compute duration from timestamp instants, not local wall-clock arithmetic. Store all calculation boundaries as offset-aware timestamps; UTC is preferred for persisted examples.
- Resolve the currency's minor-unit scale from a versioned ISO 4217 currency registry at the boundary. ProviderRateCard and WorkloadCostEntry both persist currencyMinorUnit; a currency precision change inside one usage interval fails closed. Round each posted segment amount to that scale using HALF_UP. Sum the posted segment amounts for the Workload total so the total reconciles exactly to its constituent postings.
- Preserve the calculation version and source facts so a result can be reproduced.

## 6. Idempotency, correction and replay

A cost posting is identified by a stable calculation identity derived from:

- usage interval identity and version/content digest;
- exact priced segment window;
- ProviderRateCard ID + version;
- calculation version;
- currency and unit.

Replaying identical inputs must return the same posting identity and must not create a second charge. If any source fact or rate is corrected, append an explicit reversal of the superseded posting and a replacement posting linked to it. Never mutate or delete historical financial evidence.

## 7. Allocation boundary and inactive GPUs

The initial direct-cost basis is **allocated capacity time**, not active time. If a Workload holds two GPUs for three hours, it accrues six allocated GPU-hours even when the measured compute activity is low. That idleness is an efficiency signal; it does not erase a Provider procurement cost incurred for the Allocation.

Capacity that is reserved but has no Workload Allocation is tracked as reserved/unallocated capacity cost, not silently attached to a Workload.

## 8. Golden fixture

See [workload-cost-allocation-rate-boundary-v1.json](../../../schemas/compute-economics/examples/workload-cost-allocation-rate-boundary-v1.json).

Expected result:

- Two GPUs allocated from 09:00 to 12:00 = 6.0 GPU-hours.
- Rate 10 CNY/GPU-hour applies from 09:00 to 10:30: 3.0 × 10 = 30.00 CNY.
- Rate 12 CNY/GPU-hour applies from 10:30 to 12:00: 3.0 × 12 = 36.00 CNY.
- Total direct Workload cost = 66.00 CNY.

## 9. Acceptance checks

1. Split exactly at rate-effective boundaries and price every segment once.
2. Reject rate gaps, overlaps, currency mismatch, currency-precision mismatch and unsupported rate basis.
3. Reject non-positive interval duration, non-positive GPU quantity and fractional GPU device counts.
4. Repeated source events or calculation attempts do not double-post.
5. A correction creates a traceable reversal/replacement chain.
6. Missing identity, incomplete interval coverage or missing rate produces INCOMPLETE, never a fabricated zero.
7. The golden fixture returns 6.0 GPU-hours and 66.00 CNY.
