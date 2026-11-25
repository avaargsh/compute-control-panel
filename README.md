# Compute Control Panel

Go-based Compute Control Plane for self-hosted, Kubernetes-first heterogeneous compute.

## Try the local Demo

The synthetic-only Dashboard, Clusters and GPUs preview is available on `main`. It needs a Go toolchain matching `go.mod` (Go 1.27.0 or newer), but no Kubernetes cluster, GPU, PostgreSQL or cloud credentials. GitHub Go validation pins Go 1.27.4 with `GOTOOLCHAIN=local`.

```bash
make demo
```

That runs `go run ./cmd/ccp-demo -addr 127.0.0.1:8081`. Open `http://127.0.0.1:8081` on the same machine. See the [Demo Quickstart](docs/product/DEMO-CONSOLE-QUICKSTART.md) for verification and limitations. This is not an authenticated live Console; never expose the Demo publicly.

## Local checks

From a checkout that contains the Makefile, select an installed Go SDK on PATH. These targets set `GOTOOLCHAIN=local` and do not download a different Go toolchain:

```bash
make help
make lint   # gofmt -l ., go vet ./..., go mod verify
make test   # go test -race -count=1 ./...
make build  # writes bin/ccp-demo, bin/compute-control-panel, bin/migrate, bin/kueue-observe
make clean  # deletes bin/ only; it does not clear the Go build cache
```

`make lint` and `make test` do not start PostgreSQL or a kind cluster. Tests that need `DATABASE_URL` skip when it is unset, so a passing local `make test` is the race command from Go validation, not that whole job. The job also applies migrations, checks schema constraints, and repeats the SIGKILL recovery test. Kind workflows are separate and are not invoked by these targets.

`bin/compute-control-panel` serves `GET /livez`, `GET /readyz`, and `GET /api/v1/meta`. It listens on `127.0.0.1:8080` unless `LISTEN_ADDR` is set. For a free local port, run `LISTEN_ADDR=127.0.0.1:0 bin/compute-control-panel` and use the address printed in the startup log. It does not mount workload, provider, host-session, or receipt handlers. `bin/migrate` applies SQL from `MIGRATIONS_DIR` (default `./migrations`) only when `DATABASE_URL` is set; running it is not part of `make build`. `bin/kueue-observe` is a one-shot read-only Kubernetes Job observer. With no flags it exits before contacting Kubernetes (`all Job identity and credential-path flags are required`). In-cluster execution was not verified.

## Interview highlights

- **Evidence-first Compute Economics**: Separates allocated GPU time, active time, attributable direct cost, reserved/unallocated capacity, and revenue/margin. Costing is derived from immutable, provenance-tracked evidence with exact decimal arithmetic; incomplete or contradictory evidence fails closed.
- **Strict authority boundaries**: Does not implement a custom scheduler. The control plane owns intent, reconciliation, allocation references, and economics; Kubernetes, Kueue, DRA/device plugins, and Providers remain authoritative for admission, placement, and resource realization.
- **Production-oriented foundations in main**: Atomic Workload acceptance with Outbox, SPIFFE identity, independent Provider audit writer role, generation-fenced state machines, UNKNOWN reconciliation invariants, and migration lineage guards.
- **Current honest status**: Go/PostgreSQL core, authenticated handlers, E1 policy/approval persistence, and deterministic cost calculator are implemented and CI-passing. Live Provider/Kueue/DRA evidence and production metering remain open (issues #3, #32, #33, #39).

## Product boundary

Owns compute provider registrations, pools, workload intent, allocation references, quota bindings, reconciliation, normalized compute evidence and workload-attributed economics. Kubernetes, Kueue, device plugins/DRA and provider APIs remain authoritative for actual scheduling, admission, placement and resource realization.

## Product intent

Evolve into an evidence-backed **Compute Economics Control Plane**:

**Compute Control Plane = Resource Control + Workload Control + Observability + Compute Economics.**

It answers:

- What capacity is registered, allocatable, admitted, placed, active, idle or unavailable?
- Which Workload/Project/Tenant consumed each accepted allocation, and what evidence supports that?
- What is the attributable direct compute cost for a Workload over a proven usage interval?
- How do comparable Provider/Pool/GPU-profile cost and effective throughput differ?
- Which capacity is reserved but unallocated, underused or economically unattractive?

This is additive. It must not introduce a custom scheduler or weaken Kubernetes, Kueue, DRA or Provider authority boundaries.

## Economic metric vocabulary

Keep these facts separate (do not collapse into one generic utilization number):

| Measure | Meaning | Evidence boundary |
|---|---|---|
| Allocated GPU time | Capacity assigned to an accepted Allocation | Durable Allocation lifecycle |
| Active GPU time | Allocated time weighted by a named activity signal | Timestamped telemetry with source/adapter provenance |
| Effective workload output | Useful output (e.g. tokens/s) | Workload-linked benchmark or execution evidence |
| Attributable direct cost | Usage inside Provider liability ∩ proven Workload attribution, priced by effective-dated rate | Trusted Provider + Kueue + K8s + device evidence |
| Reserved / unallocated cost | Provider liability outside proven Workload attribution | Provider lease / reservation evidence |
| Revenue and gross margin | Customer charge less attributable cost | Explicit commercial contract evidence |

Initial scope is intentionally narrow: whole-GPU quantities and fixed allocated-GPU-hour Provider rates. Fractional/MIG, tiered pricing, billing and revenue recognition require separate versioned contracts. Financial values use exact decimal arithmetic; incomplete evidence fails closed; corrections are append-only.

## Authority boundaries

- **Compute Control Plane**: ComputeProvider, Pool, Profile, Workload, Allocation, ComputeOperation, reconciliation, trusted-evidence references, UsageInterval, WorkloadCostEntry.
- **Provider**: native resource identity, lease/cost-liability, operation outcome.
- **Kueue**: queue admission and quota (not Pod placement or device ownership).
- **Kubernetes scheduler**: Pod placement and binding.
- **DRA / device plugin**: device claim and assignment evidence.
- **Agent Control Plane / Runtime**: Agent/Session/Run identity (Compute only stores authenticated external references).
- **EngageGuard**: protected-action Permit and operation evidence.

A provider ACK, PENDING Allocation, Pod request or queue admission alone is not proof of GPU allocation, attribution, readiness or release.

## Architecture principles

- Go API + controller/reconciler; PostgreSQL as domain store.
- Kubernetes + Kueue as initial admission/execution substrate.
- GPU support is an adapter, not a custom scheduler.
- Provider operations are idempotent, durable and reconciled; ambiguous outcomes stay UNKNOWN.
- Tenant/Project identity comes from authenticated service identity, never from client payload.
- Cost is derived from immutable evidence and effective-dated rates.

## Current status (2026-11-24)

Latest merged main: `04b4608fd8debdb30080e090fc33359c33157fed` (documentation alignment for HostIdentity TLS 1.3). Runtime hardening is in `48e3ba6c49291cd3451e396f669f2862470b0e06` (PR #205 DRA Receipt transport; PR #206 HostSession TLS 1.3). Post-runtime-merge [Go Validation #45358412924](https://github.com/avaargsh/compute-control-panel/actions/runs/45358412924) completed successfully, including PostgreSQL 16 migrations, static analysis, race-enabled tests and SIGKILL recovery.

Implemented and CI-verified foundations:

- Go/PostgreSQL control-plane foundation, Workload acceptance/Outbox, Provider lifecycle primitives, operation journaling and migration lineage checks.
- HostIdentity immutable Provider-scoped bindings, durable HostSession epochs and heartbeat receipts, authenticated subject grants, host-only certificate verification and CRL revocation checks. The standalone HostSession ingress remains unmounted; the handler rejects TLS below 1.3. CRL monotonicity/rollback protection is currently in-process only.
- DRA NodeUnprepare receipt journal, DBA-provisioned source-key checks, signed receipt correlation and read-only same-transaction release preflight. The standalone receipt ingress requires verified SPIFFE mTLS, TLS 1.3 and exactly one `application/json` Content-Type; it remains unmounted and evidence-only.
- Kueue/Kubernetes/DRA observation and correlation primitives, including admission/placement separation, UID/resourceVersion checks and fail-closed release prerequisites.

Not yet accepted as production capability:

- A production authenticated Provider adapter with authoritative Inspect and exact-UID cleanup/reconciliation.
- A production DRA receipt evidence resolver and authenticated collector pipeline that ties a real driver RPC result to independently observed device deallocation.
- Real-cluster Kueue/Kubernetes/DRA GPU allocation, release and restart evidence. Simulated DRA/kind tests are not physical GPU acceptance.
- Durable cross-restart CRL anti-rollback, independent hardware attestation and production mounting of the HostSession listener.
- Production tenant-isolation acceptance, the complete E1 trusted-source authorization gate and Cost FINAL eligibility.

Governance:

- GitHub currently reports `main.protected=false` and `GET /rulesets` returns no rulesets. Issue [#166](https://github.com/avaargsh/compute-control-panel/issues/166) is open because required-check enforcement acceptance remains unmet.
- Green Go, Migration Lineage and Main PR Evidence workflows are real test results, but they are not required GitHub merge checks while branch protection/rulesets are absent.

Key open gates:

- [#166 main branch ruleset enforcement](https://github.com/avaargsh/compute-control-panel/issues/166)
- [#3 P0 integration gate](https://github.com/avaargsh/compute-control-panel/issues/3)
- [#32 Provider readiness / reclamation](https://github.com/avaargsh/compute-control-panel/issues/32)
- [#33 Kubernetes/Kueue/DRA evidence](https://github.com/avaargsh/compute-control-panel/issues/33)
- [#39 E1 trusted evidence policy and source authentication](https://github.com/avaargsh/compute-control-panel/issues/39)
- [#182 HostIdentity enrollment and durable session fencing](https://github.com/avaargsh/compute-control-panel/issues/182)

Older issue/PR descriptions are snapshots, not live status. Do not infer physical GPU allocation/release, production listener readiness or financial finality from CI alone.

## Delivery sequence

1. **P0** — durable resource control (authenticated acceptance, idempotency, Outbox/Inbox, UNKNOWN reconciliation).
2. **P0** — trusted allocation evidence (real Provider inspect + exact Kueue/K8s/DRA evidence).
3. **P1** — immutable evidence persistence with provenance and replay safety.
4. **P1** — cost ledger (WorkloadCostEntry from complete UsageIntervals; append-only corrections).
5. **P1** — utilization and effectiveness (normalized telemetry with explicit trust).
6. **P2** — commercial economics (sell rates, revenue, margin) only after commercial evidence exists.
7. **P2** — cross-plane attribution with Agent Runtime (without transferring Run identity).

## Non-negotiable invariants

1. No custom scheduler; preserve Kubernetes and Kueue authority boundaries.
2. UNKNOWN must be inspected/reconciled before retry; never blindly retry an ambiguous side effect.
3. Stale generations cannot overwrite current Workload state.
4. DELETE finality requires absence of the exact Provider resource UID plus applicable release evidence.
5. FakeProvider is test-only until a real authenticated Provider adapter exists.
6. A single PostgreSQL transaction does not provide distributed exactly-once delivery.
7. PENDING, UNKNOWN, RELEASING, queue admission, Pod placement and device allocation remain separate facts.
8. No Workload cost posting without complete, tenant-scoped, digest-bound evidence and full rate coverage.
9. Provider liability after Workload attribution ends belongs to reserved/unallocated accounting.
10. Revenue and gross margin require explicit commercial source evidence.

## Retired repository

`gpu-compute-platform` is retired. All new Compute implementation, adapters, economics and release evidence belong here. See [migration matrix](docs/migration/GPU-COMPUTE-PLATFORM-CONSOLIDATION.md).

## Documentation

Start with the [documentation index](docs/README.md) for a map of architecture, contracts, operations and testing docs.

Key entry points:

- [Architecture baseline](docs/architecture/BASELINE.md)
- [Domain ownership](docs/architecture/DOMAIN-OWNERSHIP.md)
- [Cross-repository ownership ADR](docs/adr/ADR-001-CROSS-REPO-OWNERSHIP.md)
- [OpenAPI + schemas](docs/contracts/openapi.json)
- [Reliability acceptance](docs/testing/RELIABILITY-ACCEPTANCE.md)

These are design contracts; external acceptance (live Provider/Kueue/DRA, production tenant isolation) remains open.
