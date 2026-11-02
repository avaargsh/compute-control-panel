# Compute Control Plane Baseline

Status: Proposed v0.1

## Authority
This service owns ComputeProvider registrations, ComputePool, Workload intent, Allocation records, quota bindings, provider operation journal and observed-state projections. Kubernetes/Kueue/provider APIs remain authoritative for actual admission, scheduling and execution.

## Architecture
Go API + separate reconciler process; PostgreSQL per service; transactional outbox/inbox; Kubernetes client adapter; Kueue admission integration; provider SPI for future non-Kubernetes backends. No custom scheduler in v0.1.

## Resource model
ComputeProvider, ComputePool, ComputeProfile, Workload, Allocation, ComputeOperation, ResourceObservation, QuotaBinding.

## Golden Path
Create Workload → persist intent/outbox → reconciler creates Kubernetes Job/Workload → Kueue admission → observe admitted/running/completed → delete intent → verify provider absence → terminal release evidence.

## Invariants
- Stable operation_id and desired_generation for every provider mutation.
- Lost acknowledgement is UNKNOWN; inspect before retry.
- Delete ACK is not release proof.
- Stale controller cannot overwrite newer generation.
- No tenant-crossing references.
- Do not run multiple schedulers for the same workload class.
- DRA/HAMi/GPU-specific integration is capability-gated, not assumed.

## Initial stack
Go, PostgreSQL/pgx, Kubernetes client-go/controller-runtime only where justified, Kueue API, OpenTelemetry, OpenAPI. Choose one controller framework after POC; do not add both generic controller-runtime and custom pollers for the same resource.
