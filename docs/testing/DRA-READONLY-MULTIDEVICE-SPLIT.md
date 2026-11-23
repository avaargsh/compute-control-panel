# DRA read-only device set and ResourceSlice identity join

Source: Draft #115 (snapshot `33ac22204b586c6612c22f5c9f733505fb3a8897`) extracted on current main without its PD JobSet, writer, stage or Postgres migration dependencies.

The `KubernetesObservationReader` GET-only methods `ObserveDRAPodDeviceSet` and `ObserveDRAMultiSlice` verify complete per-Pod ResourceClaim device sets, exact Pod/Job/Claim UIDs and opaque resourceVersions, the expected Node UID, and stable ResourceSlice pool generations. They reject incomplete, shared, duplicated and mismatched devices, drift across successive Kubernetes reads, and unsupported Node selection.

Metadata is represented by a local `draDeviceReadMeta`, independent of the PD JobSet writer's `pdNativeMeta`. No POST/PATCH/PUT/DELETE, Kubernetes workload write, Provider command, database transaction, Allocation release, or financial finality is introduced.

Success is **scheduler-side evidence only**. No physical GPU identifier, driver `NodePrepareResources` or `NodeUnprepareResources`, hardware health, actual physical allocation, release or Cost FINAL is independently established. GETs are not atomic snapshots; source provenance, signed hardware inventory and device-level verification remain separate gates.

Required acceptance: exact-head Go merge-tree tests/race/SIGKILL and source-pinned PostgreSQL release fence; focused `go test -count=1 ./internal/provider`; Kueue kind smoke with simulated DRA is useful but not physical GPU acceptance. The original #115 remains Draft.
