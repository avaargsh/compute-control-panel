# ADR-002: Adopt llm-d for inference routing, not as the Compute control plane

- Status: Proposed (v0.2 implementation gate)
- Date: 2026-11-21
- Scope: Compute Control Panel (CCP) inference serving, not v0.1
- Supersedes: planning references to AIBrix as the default serving/routing integration
- Does not supersede: ADR-001, Kubernetes/Kueue/DRA/Provider authority boundaries

## Context

CCP owns enterprise-facing compute intent, tenant/project identity, operations, trusted observations and usage/cost attribution. Its first product release (v0.1) is a *read-only*, installable cluster/GPU console. v0.2 must serve one real model via a supported external routing implementation, without importing a former employer's API gateway and without building a custom request scheduler.

llm-d composes a proxy, Endpoint Picker (EPP), Gateway API Inference Extension (GAIE) InferencePool and model servers. The official standalone Router chart packages InferencePool, EPP and a proxy. Gateway Mode integrates with an existing compatible Gateway controller. These are materially different deployment contracts.

## Decision

1. **Choose llm-d as the default external routing/serving integration for v0.2**. Use the upstream Standalone Mode first, with one model, one InferencePool, and one CCP-managed Helm release. Pin chart, image and GAIE CRD versions and immutable digests in a tested compatibility manifest; `latest` is forbidden for the release baseline.
2. **Do not install llm-d in v0.1**. The v0.1 acceptance gate is authenticated, provenance-aware, read-only Kubernetes GPU inventory and an installable console, not a serving demo.
3. Keep llm-d *outside* CCP's domain model: CCP owns the compute intent and its deployment/recovery journal; llm-d owns request routing policy and decisions. Kubernetes and the respective device allocator own Pod placement and actual device allocation. The platform must never reinterpret request routing health as a physical GPU allocation fact.
4. Model deployment is a separate v0.2 adapter boundary. vLLM is the initial tested engine; SGLang and additional accelerators require their own release-matrix entries and physical integration evidence.
5. Defer Gateway Mode, multi-model pools, P/D disaggregation, advanced autoscaling and shared Router ownership until the single-model lifecycle has passed independent acceptance.

## Alternatives considered

- **AIBrix**: viable inference infrastructure but wider overlapping serving-controller surface for CCP's initial integration; not rejected on comparative performance grounds.
- **Custom OpenAI-compatible gateway or custom scheduler**: duplicates upstream functionality and adds lifecycle, security, and maintenance burden.
- **Direct Service-only routing**: simplest smoke baseline, but does not validate the chosen llm-d route/EPP/InferencePool integration.

## Consequences and gates

- Record the tested combination of Kubernetes, GAIE CRDs, llm-d chart/EPP/proxy, inference engine, driver, GPU and isolation mode.
- The standalone chart's resources have one owning Helm release. Do not separately `kubectl apply` or reconcile the same InferencePool from a CCP controller.
- Authentication, endpoint exposure and project isolation remain CCP/product concerns; a working inference route alone is not an authorized public API.
- Follow [ADR-003](ADR-003-LLMD-RESOURCE-OWNERSHIP.md) for ownership and lifecycle.
- Adoption is **not** an assertion of tested interoperability or production readiness. L1 contract tests, L2 disposable cluster testing and L3 physical GPU evidence remain independent gates.

## References

- https://llm-d.ai/docs/architecture/router
- https://llm-d.ai/docs/api-reference/artifacts
- https://llm-d.ai/docs/getting-started/quickstart
- https://github.com/llm-d/llm-d/releases/tag/v1.0.0
