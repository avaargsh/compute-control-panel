# CCP v0.1 Demo Console — localhost-only product preview

**Status:** synthetic developer preview available on `main`. Not a production Console, not an authenticated browser session, not real Kubernetes device discovery, and not a replacement for the planned React/Vite web application.

## Try the first three screens

Requirements: a locally installed Go toolchain matching `go.mod` (currently Go 1.27.x). No running Kubernetes cluster, GPU, PostgreSQL service, npm or cloud credentials are needed.

```bash
git switch main
go test ./internal/demoinventory ./cmd/ccp-demo

# Start via Makefile (preferred for local iteration)
make demo

# Or directly
# go run ./cmd/ccp-demo -addr 127.0.0.1:8081
```

Open **http://127.0.0.1:8081** on the same machine. Use Dashboard / Clusters / GPUs in the sidebar and the Refresh button.

```bash
curl -fsS http://127.0.0.1:8081/api/v1/observations
```

The endpoint is **read-only synthetic Demo data**. The JSON is the same shape as the live observer contract (`data_mode`, `generated_at`, `items`, timestamps, resource identity, source and `reported_quantity`) and must always return `"data_mode":"demo"` and `"source":"synthetic/demo"`.

The standalone Demo process accepts **only** `127.0.0.1:<port>` listen addresses; it rejects `0.0.0.0`, wildcard and LAN addresses. It loads no credentials and does not mount any of the real Compute write APIs. Do **not** put the Demo process behind a public reverse proxy or treat the unauthenticated synthetic endpoint as a production auth design.

## User-visible scope

| Page | Shown | Not claimed |
|---|---|---|
| Dashboard | synthetic cluster count, Node count and Node-reported GPU capacity | true free GPUs, active usage or costs |
| Clusters | Node Ready, degraded/unknown diagnostics, last observed time | actual fleet registration or remediation |
| GPUs | extended-resource name, capacity, allocatable, source and UNKNOWN status | physical device UID, utilization or allocation/release evidence |

All views are powered by the same in-memory `observation.Snapshot`. There is no separate Demo JSON schema. The browser refuses a payload marked as `live` or containing any non-synthetic source, and shows a failure rather than retaining a false green status.

## Why the frontend has no dependencies

This is a **small, isolated product-experience proof**, not the production frontend. It uses embedded static HTML/CSS/browser JavaScript so that a new contributor can try the three-screen experience using only `go run`, without npm installs or mock Kubernetes services.

The product's planned `web/` implementation remains **React + TypeScript + Vite + a tested component library**, with project/session authentication, TanStack Query, live Kubernetes-backed observations and versioned API contracts. Do not copy this Demo server's lack of authentication into the production API.

## Verification / acceptance boundaries

- Local Go tests exercise Demo/Live schema parity, project isolation, source provenance, timestamp validity, read-only route behavior and loopback-only binding.
- GitHub CI executes gofmt, vet, repository-wide race tests and separately checks `app.js` syntax.
- Real kind/ServiceAccount L2 acceptance lives in [PR #141](https://github.com/avaargsh/compute-control-panel/pull/141).
- This preview does **not** demonstrate physical GPU discovery, reliable allocation evidence, model deployment or production metering.

## Next product increments

1. The read-only observation contract, Kubernetes reader and local kind acceptance (#139–#141) are integrated foundations.
2. Track the opt-in authenticated live Console and secure browser session in [#143](https://github.com/avaargsh/compute-control-panel/issues/143). Never mount the existing workload SPIFFE service-only handler directly as a browser auth flow.
3. Rebuild the three-screen preview as the real React console, keeping the observation wire contract and provenance/freshness rules.
4. Provide Helm and Demo Quickstart for an installed, authenticated v0.1 release.

The first UI should explain **why** a resource is UNKNOWN rather than claim that a green node condition proves physical GPU capacity.
