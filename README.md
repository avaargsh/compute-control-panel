# Compute Control Plane

Go-based control plane for heterogeneous compute capacity and workload allocation in a self-hosted Kubernetes-first platform.

## Authority

This service owns:
- ComputeProvider registrations and capability inventory.
- ComputePool and ComputeProfile.
- Workload intent and desired generation.
- Allocation records and quota bindings.
- Compute provider mutation journal, leases/fencing and reconciliation.
- Append-only ResourceObservation evidence and read projections.

Kubernetes, Kueue, device plugins/DRA and provider APIs remain authoritative for actual admission, Pod placement, device realization and provider resource state. This service is not a custom scheduler.

## Boundary

| Concern | Authority |
|---|---|
| Agent / Session / Run / RunIntent | Agent Control Plane / Agent Runtime |
| ComputeProvider / Pool / Profile / Workload / Allocation | Compute Control Plane |
| Provider operation journal for compute mutations | Compute Control Plane |
| Queue admission and quota admission | Kueue |
| Pod placement | Kubernetes scheduler |
| Device allocation / accelerator realization | Device plugin / DRA / provider |
| Actual external resource existence and UID | Provider / Kubernetes API |
| Sandbox identity, isolation and lifecycle | selected Sandbox Provider |

Agent Runtime requests a Workload using a canonical Run reference and immutable compute requirements. Compute selects a compatible provider and pool. A SandboxProvider may request capacity, but it remains the only owner of the sandbox lifecycle; it must not create a duplicate compute allocation.

## State model

Three separate records must not be collapsed:
1. **Workload** — desired state and a read-only observed lifecycle projection.
2. **ComputeOperation** — one provider mutation, with stable operation ID, request digest, lease and fencing token.
3. **ResourceObservation** — immutable provider inspection/watch evidence with provider identity, resource UID, generation and observed time.

An UNKNOWN operation enters RECONCILING. It is never blindly reissued. A retry reuses the same operation ID and idempotency key only after provider inspection proves the prior operation was not applied. Delete is not complete until the exact target resource UID is observed absent.

## Golden Path

`Create Workload → persist intent + Outbox → claim provider operation → Kueue admission → Kubernetes scheduling → inspect provider state → update observation projection → delete intent → inspect exact UID absence → release Allocation`

## Architecture principles

1. PostgreSQL is authoritative for Compute domain state; no shared writable DB.
2. Domain mutation and Outbox row commit atomically.
3. Inbox dedupe and projection update commit atomically.
4. Every provider mutation has stable operation ID, idempotency key, request digest and desired generation.
5. Version CAS, lease owner and fencing token prevent stale controllers overwriting newer work.
6. UNKNOWN → RECONCILING precedes retry; ambiguous effects are not guessed.
7. Provider ACK is not verified completion; delete ACK is not release proof.
8. Kueue is admission/queueing, not a second Pod scheduler.
9. GPU/DRA/HAMi/vendor integration is capability-gated; no assumed universal accelerator API.

## Implementation order

- [Architecture baseline](docs/architecture/BASELINE.md)
- [Workload and operation contract v1](docs/architecture/WORKLOAD-OPERATION-V1.md)
- [Implementation plan](tasks/plan.md)

No custom scheduler or provider-specific API leaks into the public Workload contract.
