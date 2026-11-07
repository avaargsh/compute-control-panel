# Agent task boundaries

- Derive the requested outcome, permitted files, invariants, and acceptance checks before editing. Search narrowly when paths are unknown.
- Read-only review/diagnosis means report only; do not modify files or run active probes unless requested.
- Make the smallest sufficient change using existing contracts, dependencies, tests, and CI. Do not add frameworks, compatibility layers, feature flags, or generic abstractions without a demonstrated requirement.
- Fix failures caused by the requested change and rerun affected checks. Expand validation only for a concrete risk or failure; never treat an older commit's CI as exact-head evidence.
- Preserve security, authorization, durable state, idempotency, and recovery invariants; these require risk-appropriate regression evidence.
- Finding an unrelated defect does not authorize fixing it. Report nonblocking findings separately.
- Do not create a new Draft PR, stacked branch, or governance document when an existing PR or artifact can carry the change.
- Within the authorized scope, proceed without repeated approvals. Stop when acceptance is met; report changed files, evidence, and unknowns. Do not deploy or change production resources without explicit authorization.

## Repository-specific invariants

- CCP owns Provider/Pool/Workload/Allocation authority. Do not invent a second scheduler, provider lifecycle authority, or billing engine. Never infer physical GPU readiness from repository tests.
