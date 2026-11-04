# Compute Economics Contracts v1

These files define the first documentation-only contract slice for Compute Economics.

| Contract | Purpose |
|---|---|
| [UsageSample](usage-sample.schema.json) | Immutable normalized device telemetry with provider/source provenance |
| [UsageInterval](usage-interval.schema.json) | A normalized full-GPU-device-count interval attributed to one accepted Allocation |
| [Event Envelope](event-envelope.schema.json) | Tenant-scoped at-least-once event identity and aggregate-version contract |
| [ProviderRateCard](provider-rate-card.schema.json) | Effective-dated Provider procurement rate per allocated GPU-hour |
| [WorkloadCostEntry](workload-cost-entry.schema.json) | Append-only, fully covered direct Workload cost posting |

The [golden fixture](examples/workload-cost-allocation-rate-boundary-v1.json) and [calculation rules](../../docs/architecture/compute-economics/WORKLOAD-COST-CALCULATION-V1.md) define rate-boundary splitting, exact arithmetic, rounding, idempotency and correction behavior.

## v1 limits

- Full GPU devices only; no fractional GPU, MIG slice or vendor-specific partition pricing.
- Only ALLOCATED_GPU_HOUR procurement rate basis.
- Workload direct cost requires an accepted Allocation identity/projection and authoritative resource/admission evidence.
- Commercial billable usage, customer sell prices, revenue recognition, invoices and collection remain separate future contracts.
- Missing source coverage or missing/overlapping rate windows produces an incomplete calculation outcome; it must not create a zero or partial final WorkloadCostEntry.

Retention and audit lineage are specified in [Retention and Evidence Policy](../../docs/architecture/compute-economics/RETENTION-AND-EVIDENCE-POLICY.md). These are proposed contracts under review in PR #9. They are not yet merged into main and are not consumed by the runtime.
