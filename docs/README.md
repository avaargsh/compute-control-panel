# Technical Documentation Index

This index organizes the design, contracts, and operational documents for the Compute Control Plane.

Documents under `docs/` are **design contracts and architecture decisions**. They are not proof that every integration or production path is implemented and accepted. See the main [README](../README.md#current-status-2026-11-10) for current implementation status and open gates.

## Quick navigation

| Area | Purpose | Start here |
|------|---------|------------|
| Architecture | Authority boundaries, domain model, state machines | [BASELINE.md](architecture/BASELINE.md) |
| Economics | Evidence, cost calculation, retention | [COMPUTE-ECONOMICS-DOMAIN.md](architecture/COMPUTE-ECONOMICS-DOMAIN.md) |
| Contracts | API, events, JSON Schemas, fixtures | [COMPUTE-WORKLOAD-V1.md](contracts/COMPUTE-WORKLOAD-V1.md) |
| Implementation | Go foundation notes | [GO-FOUNDATION.md](implementation/GO-FOUNDATION.md) |
| Operations | Identity, release gates | [SPIFFE-WORKLOAD-IDENTITY-PROVISIONING.md](operations/SPIFFE-WORKLOAD-IDENTITY-PROVISIONING.md) |
| Testing | Acceptance criteria | [RELIABILITY-ACCEPTANCE.md](testing/RELIABILITY-ACCEPTANCE.md) |
| Decisions | Cross-repo ownership | [ADR-001](adr/ADR-001-CROSS-REPO-OWNERSHIP.md) |

## Directory layout

```
docs/
├── adr/                          # Architecture Decision Records
├── architecture/                 # Core design
│   ├── BASELINE.md               # Authority, domain records, acceptance
│   ├── DOMAIN-OWNERSHIP.md
│   ├── STATE-MACHINES.md
│   ├── KUBERNETES-KUEUE-BOUNDARY.md
│   ├── PROVIDER-MANAGEMENT-API-V1.md
│   ├── WORKLOAD-*.md
│   ├── E1-*.md                   # Evidence policy & writer roles
│   └── compute-economics/         # Cost & usage evidence details
├── contracts/                   # OpenAPI, schemas, fixtures, tests
├── implementation/               # Implementation notes
├── migration/                   # Consolidation from retired repos
├── operations/                  # Provisioning & release process
└── testing/                     # Reliability & acceptance matrices
```

## Reading order for new contributors / interview prep

1. Main [README](../README.md) — product intent, highlights, current status.
2. [BASELINE.md](architecture/BASELINE.md) — authority model and domain records.
3. [DOMAIN-OWNERSHIP.md](architecture/DOMAIN-OWNERSHIP.md) + [ADR-001](adr/ADR-001-CROSS-REPO-OWNERSHIP.md).
4. [STATE-MACHINES.md](architecture/STATE-MACHINES.md) and [WORKLOAD-OPERATION-V1.md](architecture/WORKLOAD-OPERATION-V1.md).
5. Economics: [COMPUTE-ECONOMICS-DOMAIN.md](architecture/COMPUTE-ECONOMICS-DOMAIN.md) then the files under `compute-economics/`.
6. Contracts: OpenAPI + schemas under `contracts/`.
7. Open issues #3, #32, #33, #39 for remaining acceptance gates.

## Status convention

- Design documents describe intended invariants and boundaries.
- Implementation status is tracked in the main README and linked issues, not inside individual design docs.
- Older review notes (e.g. `REVIEW-DISPOSITION-*.md`) are historical snapshots.
