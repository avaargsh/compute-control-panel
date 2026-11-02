# Compute Control Panel

Go-based Compute Control Plane for self-hosted, Kubernetes-first heterogeneous compute.

## Product boundary

Owns compute provider registrations, pools, workload intent, allocation references, quota bindings, reconciliation and compute evidence. Kubernetes, Kueue, device plugins and provider APIs remain authoritative for actual scheduling and execution.

## Initial architecture

- Go API and controller/reconciler processes.
- PostgreSQL as the control-plane domain store.
- Kubernetes + Kueue as the initial workload admission and execution substrate.
- GPU support is an adapter capability, not a custom scheduler.
- Provider operations are idempotent, durable and reconciled; ambiguous outcomes remain UNKNOWN.

Architecture baseline and implementation plan are being established in docs/architecture/BASELINE.md and tasks/plan.md.
