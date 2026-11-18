# M5.5 physical topology and fabric evidence v1

Status: offline verification boundary, not deployed collector or physical allocation proof.

## Source authorities
- Inventory adapter: an independently administered key, scoped to tenant/project/pool/model/GPU/shape/fabric, cluster UID, Provider and adapter version. Establish a complete whole-device inventory and stable physical GPU UUID-to-DRA driver/pool/device mapping. Capture exact ResourceSlice UID/RV, Node UID, source revisions and corroborating immutable health/occupancy/connectivity artifact digests. Reuse the existing Kubernetes/DRA evidence readers for structural joins; ResourceSlice, Claim absence, Pod deletion or device name alone is not proof of FREE. A driver/Provider-native inventory reader must reconcile device-plugin and DRA aliases, authoritative reservations, driver state, quarantine and health. Never treat a simulated DRA device as physical GPU proof.
- Fabric adapter: a different key and collector, pinned to the same scope/provider/cluster/revisions and exact signed inventory content digest. Covers every known island; test a conservative shared aggregate bottleneck across the applicable paths, not the sum of NIC speeds. Each contiguous measurement window reports measured aggregate bytes, duration, already committed bandwidth and explicit headroom. These are source observations, not a QoS guarantee; authenticate raw probe data and bind hardware/driver/topology/configuration before production.
- Benchmark verifier must be distinct from both source keys. Independent source policies come from trusted configuration; they cannot be self-authored by the report caller. Production registration, key rotation/revocation, artifact retrieval and adapter authorization remain integration requirements.

## Derived advisory
Reject incomplete inventories, duplicate physical UID or DRA tuple, same UID cross-island, same ResourceSlice identity disagreement, a multi-node island in this narrow v1, non-whole devices, missing evidence hashes and unhealthy FREE state. Only explicitly healthy FREE devices count; RESERVED, ACTIVE, DRAINING, UNKNOWN and UNHEALTHY contribute zero. Derive sorted island counts and maximum island size. The exact source must prove compatibility and connectivity; the code checks consistency of its declaration.

Recompute the floor of bytes*1000/window_ms with exact integer arithmetic, subtract reserved bandwidth and headroom saturating at zero, take the minimum across contiguous measured windows. A zero window budget is insufficient for positive transfer demand. No oversubscription, per-path guarantee, physical placement or network reservation is inferred.

ProposeFromSources accepts no caller-set Attested input: VerifyTopology derives the internal observation, ProposeRaw verifies the original PD trace and signed benchmark, then a domain-separated receipt digest binds full source data/signatures/policies. Different physical mappings with identical GPU counts produce different content digests under the same generation key. VerifiedProposal exposes a candidate, Provider ID and exclusive earliest evidence deadline but has no public constructor for a positive receipt. The store still must enforce identity, current authorized policy and source revision fences, generation CAS and durable replay.

## Remaining gates
- Implement/authenticate native physical GPU/driver/Kubelet inventory + complete claim and reservation collection, independent probe/measurement and immutable artifact retrieval, ensure source revisions are authoritative and never recycled.
- Treat device availability as advisory until Kueue admission and exact K8s/DRA/Provider placement/prepare/release evidence; actual link saturation and physical GPU acceptance need an authorized hardware environment.
- No workload autoacceptance, scaling actuator, custom scheduler or finance posting in this candidate.
