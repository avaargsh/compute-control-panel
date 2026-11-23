# GET-only paired PD JobSet admission / DRA claim correlation (extracted from #115)

Source: Draft #115 exact `33ac22204b586c6612c22f5c9f733505fb3a8897`; promotion base `77086e38d962d877710236338515cea2f398d8b0`.

## Included

The Provider `KubernetesObservationReader` **GET-only** methods `ObservePairedPDAdmission` and `ObservePairedPDClaims` bind an independently pinned JobSet UID, owner Kueue Workload UID, expected tenant/workload/intent/generation, per-phase replica counts and GPU quota assignments to child Job UID, Pod UID and v1 ResourceClaim/allocated direct device sets. Both paired native objects and child resources are read repeatedly; resourceVersion/identity/pool drift and mismatched or duplicate device tuples reject. The included tests cover positive, negative and fake TLS multi-device integration cases.

These are **structural scheduler-side observations**, not autonomous permission, source attestation or device allocation. The caller must independently authenticate authoritative durable PD intent, Provider identity and Kubernetes API endpoint and restrict read credentials. Returned `Stage: PAIRED_QUOTA_ADMITTED`, `ReservedGPUCount`, or Claim device tuple is not evidence of actual physical GPU readiness.

## Explicitly excluded

- All `PDJobSetAdapter` writers, service-account staging APIs, kube POST/PATCH/DELETE, JobSet activation/unsuspend, a running ValidatingAdmissionWebhook, public HTTP endpoints and policy issuer
- All `internal/store/postgres/pd_*` intent/promotion/migration/Operation code, allocations and finance/ledger effects
- Physical DRA `NodePrepareResources` / `NodeUnprepareResources`, signed driver device receipts, true GPU occupation, release or cost FINAL

A multi-device `ResourceClaim.status.allocation` is a Kubernetes allocation observation, not driver preparation. Sequential GETs are not atomic snapshots. `PhysicalReady` is always false; downstream state transitions must remain fail-closed.

## Validation

Exact HEAD full Go vet/test/race, PostgreSQL exact-source fence, SIGKILL recovery and Evidence CI. This PR touches `internal/provider/**`, so disposable Kubernetes 1.34 + Kueue controller kind with **simulated** DRA should also succeed on the exact head. Genuine hardware release/readiness requires separate independent signed driver evidence and physical test.
