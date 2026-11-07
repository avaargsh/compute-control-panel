# Compute Economics Retention and Evidence Policy

**Status:** E0 policy contract proposal  
**Parent:** [Compute Economics Domain](../COMPUTE-ECONOMICS-DOMAIN.md)  
**Tracking:** [Issue #8](https://github.com/avaargsh/compute-control-panel/issues/8)

## 1. Purpose

Compute cost and utilization results must remain explainable from source facts and the policy versions used to derive them. Retention therefore follows evidence purpose, not a single global TTL.

No universal retention duration is specified here. Deployment policy must set durations based on customer contracts, applicable law, operational needs and dispute/correction windows. A missing configured policy is a configuration error for production ingestion.

## 2. Retention classes

| Retention class | Typical records | Required behavior |
|---|---|---|
| RAW_TELEMETRY | High-frequency UsageSample and raw provider metric references | Apply configured TTL; downsampling is allowed only after the raw retention window and must preserve provenance to the aggregate |
| USAGE_EVIDENCE | UsageInterval, Allocation boundary evidence and metering provenance | Retain through the longest dependent cost-correction, audit and customer-dispute window |
| FINANCIAL_LEDGER | WorkloadCostEntry, RevenueEntry, reversals and rate-card versions | Append-only; retention follows applicable accounting, contract and legal requirements |
| AUDIT_EVENT | Policy/rate-card changes, authorization decisions and correction actions | Retain under the security/audit policy; preserve actor, timestamp and reason |

## 3. Policy requirements

Every production retention decision must resolve to an immutable policy identity and version that specifies:

- retention class and owning tenant/data scope;
- retention duration or contractual/legal retention reference;
- permitted aggregation/downsampling transformations;
- deletion/expiration behavior and any required evidence pointer;
- legal-hold behavior and approval authority;
- policy effective time and version.

Policy changes are effective-dated. New durations do not silently rewrite historical records or retroactively authorize deletion of data still within a prior retention obligation.

## 4. Evidence and lineage

- A UsageSample preserves provider, device, adapter/source version, observation time and raw evidence reference.
- A UsageInterval preserves source event/sample IDs, exact time window, Allocation identity and metering policy version.
- A WorkloadCostEntry preserves UsageInterval IDs, rate-card ID/version, calculation version and correction/reversal lineage.
- A RevenueEntry (future contract) must preserve authoritative contract/billing source and recognition policy version.
- Event envelopes preserve stable event ID, producer, tenant, aggregate identity/version, occurrence/emission timestamps and payload schema identity.

Downsampling or expiry of RAW_TELEMETRY must not silently sever the lineage needed to reproduce a retained cost or audit result. Where raw payload deletion is required, preserve a durable immutable evidence reference or a formally approved "source expired" tombstone with the original digest and timestamps.

## 5. Tenant and access boundaries

- Retention and deletion jobs are tenant-scoped and must not cross tenant boundaries.
- Financial/audit evidence deletion requires an explicit policy decision and any required legal-hold check.
- Data minimization applies to high-cardinality labels and raw payloads; do not collect customer prompts or model payloads as GPU telemetry unless a separately authorized product requirement exists.
- Retention policy reads and enforcement actions must be auditable.

## 6. Acceptance checks

1. Ingestion rejects production writes when no applicable retention policy can be resolved.
2. A policy change creates a new version and does not mutate prior records.
3. Raw telemetry downsampling preserves references/digests required by retained UsageInterval and cost entries.
4. Cost corrections remain possible for the full configured correction/dispute window.
5. Legal hold prevents scheduled deletion and emits an audit event.
6. Tenant-scoped retention tests prove one tenant's expiration cannot delete another tenant's records.

