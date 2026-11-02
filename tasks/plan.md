# Compute Control Plane Plan

## Phase 0: contracts
1. Freeze resource model, provider SPI, operation state machine and ownership.
2. Define Workload API/OpenAPI and event envelope.
3. Define FakeProvider contract suite.

## Phase 1: foundation
4. Go API bootstrap, config, health, telemetry and shutdown.
5. PostgreSQL schema, migrations, idempotency and outbox.
6. Workload create/get/list/cancel API.

## Phase 2: reconciliation
7. Durable operation leases/generation fencing.
8. FakeProvider create/inspect/delete and UNKNOWN recovery.
9. Kubernetes Job adapter and Kueue admission POC.

## Phase 3: proof
10. Lost ACK, stale generation, restart, quota exhaustion, delete finality tests.
11. Cluster integration evidence and operational runbook.

## Exit gate
No duplicate provider resource on retry; no release until absence is verified; restart resumes from durable intent.
