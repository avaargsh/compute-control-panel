# M5.7 multi-device DRA ResourceSlice and Node readback

Status: read-only Kubernetes identity proof. Not physical GPU, Driver Prepare or release evidence.

The `provider.ObserveDRAMultiSlice` reader takes independently pinned per-Pod ResourceClaim UID, expected full direct-request Driver/Pool/Device set, Job/Pod/Node IDs, expected Node UID and ClaimTemplate name. It reuses `ObserveDRAPodDeviceSet`, the existing `currentDRAPool` complete-generation validation, and bounded HTTPS Kubernetes GETs.

- Validates the complete non-shared Claim device set, Pod owner/consumer and Claim UID/RV; no caller-reported device count can replace the real v1 ResourceClaim allocation result.
- Pins the Kubernetes Node exact UID and resourceVersion before and after reading the complete current ResourceSlice pool generation twice. Rejects missing/duplicate devices, Node UID replacement, ResourceSlice UID/RV drift, incomplete pool generations and unsupported non-explicit Node selection modes.
- Every observed Driver/Pool/Device is mapped to exactly one ResourceSlice UID/RV and same independently pinned Node UID.
- Only same Driver/Pool per Claim is supported in v1. Cross-pool devices, sharing, admin access, consumed capacity and fractional devices reject.
- Reader output is scheduler-side `DRAMultiSliceObservation` with `PhysicalPrepared=false`; Kubernetes publisher and Node API do not authenticate actual physical GPU UUID, current health, driver Prepare, KV movement, release or cost FINAL. The signed physical hardware inventory remains a **separate** authority. An authorized adapter must correlate this readback with `pd.BindScheduledDevices` and signed source provenance, not manufacture a hardware UID from ResourceSlice device name.

The test uses a fake TLS API to reproduce two allocated devices, two ResourceSlice objects in one complete pool generation and an independently pinned Node UID. It covers a missing device, second-read resourceVersion drift, Node successor and incomplete pool. Real Kubernetes/DRA and physical GPU acceptance are separate integration gates.
