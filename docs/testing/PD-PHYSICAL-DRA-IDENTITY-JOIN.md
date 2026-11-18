# M5.5 physical UID and ResourceSlice cross-source join

This new narrow join is **non-actuating**, and it never authorizes GPU READY, RELEASED, or cost FINAL.

## Exact source requirements

1. **Independently signed inventory and Fabric policy:** the existing `pd.VerifyTopology` authenticates scope, cluster/provider, source adapters, independent signing keys, freshness, content digest and sorted whole-GPU tuples.
2. **Authenticated Kubernetes/DRA source:** the caller must independently collect and pin the exact JobSet/Job/Pod/Claim UID/RV, allocated Driver/Pool/Device tuples, live ResourceSlice UID/RV and Node UID from the same CA-pinned cluster. Passing a hand-constructed `ScheduledDevice` is only an offline test. The Kubernetes collector must re-read each object and enforce UID/RV consistency; the pure join does not perform I/O or authenticate that caller.
3. **One-to-one join:** the Device tuple must occur in signed inventory; Node UID and ResourceSlice UID/RV must be exact, physical GPU UUID unique, whole and healthy, and signed state RESERVED or ACTIVE. FREE is inconsistent with an allocated Claim. Multiple GPUs may share a Claim only for the same Pod UID. The output is sorted and has a domain-separated content digest.
4. **No inference:** the digest binds the *identity join*, not driver Prepare, real GPU occupancy, actual fabric transfer, workload execution, release finality or finance. The signed inventory records hashes of occupancy and health artifacts, but without source authentication and retrieval of those immutable artifacts the verifier cannot claim they establish real hardware state.

## Next integration gates

- Read and authenticate every ResourceSlice and Kubernetes Node UID for all devices in a paired JobSet; link to the durable PD Intent and Provider UID without importing caller-provided authority flags.
- Receive independent GPU/driver Prepare receipts and exact PodResources/driver health evidence with registered source keys; an allocated DRA Claim alone is not prepared.
- Conduct real PD serving run with raw phase-level data and independent evaluator; check TTFT, TPOT, link saturation, failed attempts, recovery and placement.
- Reuse DRA unprepare journal + Pod/Claim/queue release proof and E1 independent FINAL policy before any Allocation RELEASED or cost FINAL transition.
