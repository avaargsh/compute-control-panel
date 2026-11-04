# Workload Acceptance API v1

## Scope

This document describes the dependency-injected HTTP acceptance boundary for creating a Compute Workload. The handler implements `POST /api/v1/workloads` for `compute.platform/v1`. It is a staged application component; it is not mounted by the service's default `main` entry point.

The handler's only write path is `Repository.AcceptWorkload`, which atomically persists the Workload, initial Allocation identity, CREATE Operation, and the two dispatchable Outbox events.

## Request contract

- Exactly one `Idempotency-Key` header is required; maximum length is 256 bytes.
- The request body is capped at 1 MiB.
- Unknown fields, malformed JSON, duplicate object keys, trailing JSON values and invalid field combinations are rejected.
- Exactly one of `compute_profile_ref` or `resources` is required.
- `project_id` and `owner_ref` are part of the normalized request intent.
- Tenant ID, caller service identity, Provider ID, Pool ID and provider-native resource identity are never accepted from the request body.

## Trust and authorization boundary

1. **Authentication** — the injected `WorkloadAuthenticator` must validate the caller's service credential and return a trusted principal containing Tenant, Subject, Service and allowed Project IDs.
2. **Scope** — the handler checks that the requested Project belongs to the principal's project grants and that `owner_ref.service` matches the authenticated service. The initial owner mapping is intentionally allowlisted (`agent-runtime/run` and `sandbox-provider/sandbox-claim`).
3. **Authorization** — the injected `WorkloadCreateAuthorizer` must enforce the service policy and any required Guard Permit. It receives the fully decoded request and normalized request digest. It runs before idempotency lookup and before Provider resolution, so rejected callers cannot use Provider selection as a capacity oracle.
4. **Idempotency replay** — the repository is queried by Tenant + Idempotency-Key and the full immutable request identity. An exact replay returns the original Workload, Allocation, Operation and event identities without asking the Provider selector to make a new decision. Reusing the key with a different digest or immutable owner/project/profile identity returns a conflict.
5. **Provider resolution** — only a new authorized request invokes the injected `WorkloadProviderSelector`. Provider and Pool identities are selected server-side.
6. **Atomic acceptance** — `AcceptWorkload` commits the Workload, initial `PENDING` Allocation, bound CREATE Operation and both Outbox events in one PostgreSQL transaction. The operation request digest binds the Workload request intent; the selected Provider remains a durable fact on the Operation and Allocation rather than being part of the replay identity. Concurrent first submissions with the same key converge on the same durable acceptance.

## Initial response semantics

A successful new acceptance or exact replay returns HTTP `202 Accepted` with:

- `workload_id` and `desired_generation`
- `desired_state` and `observed_phase`
- `allocation_id` and `allocation_phase`
- `compute_operation_id`

Acceptance means the control plane durably recorded the intent. The initial Allocation phase is `PENDING`. It does not prove Kueue admission, Pod placement, DRA device ownership, Provider execution or capacity readiness.

The response deliberately does not advertise a status URL until the authenticated Workload read/status endpoint is implemented.

## Dependency interfaces and rollout

The handler constructor rejects missing authentication, authorization, Provider-selection or repository dependencies. ID generation defaults to cryptographically random identifiers.

No concrete service-token authenticator, project/Guard authorizer or production Provider selector is wired yet. The handler must remain unmounted until those implementations are integrated and independently tested. The default service entry point continues to expose only health and metadata endpoints.

## Verification

The PR test suite covers unauthenticated requests, Project scope denial, owner/service mismatch, duplicate JSON keys, client-selected Provider rejection, authorization-before-provider ordering, idempotency conflict, replay before Provider resolution, server-side Provider selection, and binding of the CREATE Operation to the initial Allocation. PostgreSQL integration tests cover durable lookup, changed Provider selection on replay, concurrent replay and stable event identity.

Production rollout remains gated on real Provider Inspect/authentication, broker/outbox worker durability, tenant AuthN/AuthZ and RLS, and Kueue/DRA/Pod evidence.
