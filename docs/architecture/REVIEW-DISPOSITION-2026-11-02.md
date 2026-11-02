# Compute Architecture Review Disposition — 2026-11-02

Status: proposed v0.2 corrections incorporated; not a formal GitHub approval.

| Review finding | Required correction | Disposition |
|---|---|---|
| Workload, provider operation and observed resource were one lifecycle concept | Separate Workload desired/observed projection, ComputeOperation journal and append-only ResourceObservation | Fixed in BASELINE and WORKLOAD-OPERATION-V1 |
| UNKNOWN could be treated as failure or retried directly | UNKNOWN → RECONCILING → SUCCEEDED / PENDING / FAILED / MANUAL_REVIEW | Fixed in operation package and tests |
| Delete ACK could be mistaken for released allocation | Verify absence of exact provider resource ID + UID | Fixed in operation package and tests |
| Operation status lacked durable concurrency protection | Version CAS, lease owner/expiry and fencing token | Domain prototype fixed; PostgreSQL implementation remains P0 |
| Stale generation could overwrite current desired state | Compare operation generation with current Workload generation; supersede stale work | Fixed in operation model |
| Stale UPDATE could be treated like stale CREATE cleanup | CREATE may need compensating delete; UPDATE must reconcile latest desired state without deleting target | Fixed in operation model and regression test |
| Provider observation could be correlated to wrong provider/operation | Require tenant/workload/provider/operation/generation and provider resource UID | Fixed in operation model and tests |
| Kueue could be described as the scheduler | Kueue admission/quota; Kubernetes scheduler places Pods; device plugins/DRA/providers realize resources | Fixed in BASELINE and WORKLOAD-OPERATION-V1 |
| SandboxProvider and ComputeProvider could both create the same resource | SandboxProvider owns sandbox lifecycle; Compute owns capacity/Workload/Allocation | Fixed in cross-domain boundary docs |
| API success could imply provider success | Workload create/delete intent returns 202 after DB + Outbox commit; reconcile asynchronously | Fixed in API contract documentation |

## Remaining acceptance gaps

- PostgreSQL migrations, Workload/Allocation/Operation persistence and tenant/project authorization.
- Idempotency key + canonical digest uniqueness and 409 behavior.
- Transactional Outbox/Inbox and aggregate version gap handling.
- Durable operation leases/fencing and controller restart recovery.
- FakeProvider implementation and full provider contract suite.
- Kubernetes Job and Kueue adapter, then real-cluster POC.
- Provider-specific authoritative operation lookup and exact UID absence proof.

No custom scheduler is approved. The Go operation package is a pure domain prototype; it is not a durable provider controller.
