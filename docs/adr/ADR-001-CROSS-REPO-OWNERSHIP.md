# ADR-001: Cross-Repository Compute Ownership and References

- Status: Proposed v0.3; implementation and contract-test gate
- Date: 2026-11-02
- Owners: compute-control-panel maintainers
- Scope: agent-control-panel, agent-runtime, EngageGuard, Compute CP and selected SandboxProvider

## Context

Compute CP is one service in a multi-repository platform. A resource may be referenced by several services, but each business fact and each external side effect must have one authoritative writer. The current ACP architecture already separates RunIntent from Runtime Run identity; Compute CP must consume that contract rather than preserve an obsolete identity conflict.

Two operation journals are required because they record different side effects: EngageGuard records the protected Agent action and authorization/receipt lifecycle; Compute CP records the provider-side compute mutation and reconciliation. They must not share an identifier or overwrite each other's state.

## Decision

### 1. Compute domain authority

Compute CP is the sole authority for:
- ComputeProvider registration identity and adapter configuration;
- ComputePool, ComputeProfile and QuotaBinding;
- ComputeWorkload intent and lifecycle projection;
- Allocation identity and provider-resource mapping;
- compute_operation_id, provider dispatch attempts, provider request digest and UNKNOWN reconciliation;
- timestamped ResourceObservation projections.

Compute CP is not an Agent lifecycle service, generic sandbox manager, Kubernetes scheduler, Kueue admission controller or provider inventory source of truth.

### 2. Agent identity references

- Agent Control Plane owns run_intent_id and its immutable request digest.
- Agent Runtime consumes run_intent_id and creates run_id, workflow_id and run_attempt_id. Runtime derives workflow_id as agent-run/v1/<run_id>.
- Compute CP does not accept RunIntent as the canonical owner of an allocation. For non-sandbox Run capacity, owner_ref is agent-runtime/run/<run_id>.
- For sandboxed execution, owner_ref is sandbox-provider/sandbox-claim/<claim_id>. SandboxProvider owns the SandboxClaim and is the sole Compute CP caller for the capacity required by that claim.
- Compute CP treats owner_ref as an opaque, tenant-scoped reference. It does not query ACP, Runtime or SandboxProvider databases.

Run identity is aligned at the architecture level by ACP ADR-003 and Runtime's StartRun contract. The remaining gate is executable Runtime idempotency/lookup behavior and cross-service tests, not a reason for Compute CP to invent an alternative Run identity.

### 3. Sandbox and capacity boundary

SandboxProvider owns sandbox identity, isolation, sandbox-session state and sandbox cleanup. Compute CP owns CPU/GPU/accelerator capacity, Workload, Allocation and provider mutations.

Two mutually exclusive request paths are allowed:

1. Non-sandbox Run capacity: Runtime calls Compute CP directly with owner_ref = agent-runtime/run/<run_id>.
2. Sandbox capacity: Runtime calls SandboxProvider Claim; SandboxProvider calls Compute CP once with owner_ref = sandbox-provider/sandbox-claim/<claim_id>.

Runtime and SandboxProvider must not create separate ComputeWorkloads for the same sandbox capacity claim. SandboxProvider may create sandbox-specific objects only inside or against the Allocation returned by Compute CP. For the v0.1 Kubernetes Job POC, the Compute Kubernetes adapter is the only creator of the top-level Job.

### 4. Two operation journals

EngageGuard Action Operation:
- Runtime creates one stable action_operation_id per admitted ActionProposal; ActionRequest v1 serializes it as operation_id.
- EngageGuard owns the Action Operation record, authorization decision, approval, Permit, action_hash, executor_attempt_id and Receipt.
- Reusing operation_id with the same canonical ActionRequest returns the same operation. Reusing it with a different action/binding is a conflict.

Compute Provider Operation:
- Compute CP creates one compute_operation_id for each immutable provider mutation intent.
- Compute CP owns provider_request_digest, compute_attempt_id records, provider receipt/observation and UNKNOWN reconciliation.
- A provider retry reuses compute_operation_id, provider idempotency token and provider_request_digest. A changed target/payload/generation is not a retry.

Delegation:
- For an Agent-proposed protected compute action, EngageGuard authorizes the exact action and issues a Permit for the Compute CP audience.
- Compute CP validates the delegated Permit/action binding at its API enforcement point before accepting the protected mutation. It does not recompute action_hash; it treats the Guard digest as opaque.
- The Guard action_operation_id and Compute compute_operation_id are linked by references. Guard reconciles the delegated action against Compute CP status; Compute CP reconciles the provider effect.
- A Compute 202 means durable Workload acceptance, not Allocation readiness. Neither Guard nor Runtime may claim the resource is ready until Compute CP reports a verified BOUND Allocation.

Platform-internal Run capacity needed to execute a Run is an infrastructure orchestration request, not an Agent ActionProposal. Runtime may call Compute CP under authenticated workload identity and Compute policy. It does not create a Guard Operation solely to mirror a ComputeOperation.

### 5. Authorization binding contract

Direct Runtime/SandboxProvider infrastructure requests are authenticated by service workload identity and Compute tenant/project/profile policy.

An Agent-originated protected compute action must include a Guard operation reference, Permit reference, action_hash and Compute CP audience. Compute CP must validate the Permit with EngageGuard and bind it to the exact normalized Compute request digest. A caller-supplied permit_ref or action_hash is not proof of authorization. The Guard-to-Compute verification/attestation schema and golden vectors remain an implementation gate; until they exist, protected Agent-originated Compute mutations fail closed.

Compute CP computes its own provider_request_digest from the normalized provider mutation. It must not reuse action_hash as the provider request digest or attempt to reproduce Guard's canonicalizer.

### 6. Retry ownership

| Layer | Retry responsibility |
|---|---|
| Agent Control Plane | Retry StartRun dispatch with the same run_intent_id and request digest; reconcile only through Runtime's authoritative registry |
| Agent Runtime | Retry Guard submission with the same action_operation_id; retry Compute API only with the same per-request Idempotency-Key; query status |
| EngageGuard | Own Action Operation, Permit, executor attempt and action-level UNKNOWN; query downstream Compute status when delegated |
| Compute CP | Own compute_operation_id, provider dispatch, provider inspection and provider UNKNOWN |
| Provider adapter | Apply provider-specific idempotency/conditional writes and normalize receipts |
| Temporal | Retry Workflow/Activity orchestration; never bypass Guard or Compute to dispatch a provider mutation |

A timeout is not proof of failure. No layer may create a replacement operation to escape UNKNOWN.

### 7. Kubernetes and Kueue boundary

Kueue owns queue admission, quota reservation, borrowing, fair sharing and preemption. Kubernetes Scheduler owns Pod-to-Node placement. Kubernetes controllers own Job/Pod lifecycle. Provider/device controllers own actual resource realization. Compute CP owns only its desired Workload, Allocation binding and reconciliation projections.

The v0.1 integration path is one Kubernetes Job plus Kueue's built-in Job integration. Compute CP does not create or patch the Kueue-native Workload for that Job.

## Alternatives rejected

- Runtime owns provider mutation: rejected because it duplicates ComputeOperation and couples Run lifecycle to provider state.
- SandboxProvider independently provisions CPU/GPU: rejected because it creates competing capacity writers.
- EngageGuard owns the provider journal: rejected because it would make Guard a second authority for Kubernetes/Kueue/provider state.
- Compute CP owns Run identity: rejected because it would create an Agent lifecycle authority in the compute domain.
- Compute CP implements its own scheduler: rejected because it duplicates Kueue admission and Kubernetes placement.

## Consequences and remaining gates

- One writer exists for each platform domain fact and provider-side mutation.
- Guard and Compute journals can both be UNKNOWN without contradiction because they describe different boundaries.
- Cross-repository schemas and contract tests are required before enabling the golden path.
- The Permit-to-Compute request digest attestation is not implemented and remains a fail-closed gate.
- PostgreSQL persistence, FakeProvider conformance and real Kubernetes/Kueue evidence remain separate acceptance levels.