# Offline capacity evidence extraction — main promotion scope

**Source:** PR #115 draft, exact snapshot `33ac22204b586c6612c22f5c9f733505fb3a8897`. This split is intentionally independent of the old branch's PD JobSet/PostgreSQL/Provider path.

## Implemented and proposed scope

`internal/capacity` and `internal/capacity/pd` are read-only mathematical and evidence verifiers. The modules validate independently supplied, signed benchmark profiles, pinned raw JSONL digest and metrics, physical topology representations, PD per-phase data and conservative capacity estimates. The data origin, evaluator/deployment configuration, policy/signing keys and inventory evidence **must** be authenticated by separately implemented trusted adapters. In-process `Attested` flags and caller-provided claims are not independent proof.

There is no API route, Kubernetes JobSet mutation, DRA device allocation/release, Provider command, Workload/Allocation state transition, usage ledger or finance FINAL capability in this split. `ESTIMATED_FIT` means a calculation only; it is never quota admission, physical availability or production readiness.

## CI acceptance

- `go test -count=1 ./internal/capacity/...`: base, rawbenchmark, PD rawphase, topology, signatures and proposal generation tests.
- Merge-tree `go test -race -count=1 ./...`, migrations/schema, SIGKILL recovery and source-pinned PostgreSQL fence checks must remain green at the exact PR head.
- The Source #115 branch remains Draft and does **not** inherit success from this split. The integrated PD activation gates, real paired JobSet controller lifecycle and physical GPU acceptance are **not** tested or satisfied here.

## Fail-closed boundary

Do not route these outputs into control-plane writes until independent provenance and fresh policy checks, generation CAS, Kueue admission, DRA device assignment, driver NodePrepare/NodeUnprepare, readback and teardown are proven under the appropriate authority. No actual physical GPU measurements exist in this extraction.
