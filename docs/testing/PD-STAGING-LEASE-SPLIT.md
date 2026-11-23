# PD staging snapshot and fenced lease — unmounted source split

Source: historical Draft #115 `7ea4e9de1e2ba74c9df5653a01fbd6fc3a5d87a4`; based on `main@48fae945ff70b7d80655e68c78bd63ee4898eb46` with merged #158 generic dispatch rejection, #159 immutable PD Intent and #160 atomic non-dispatchable Promotion.

`ReadCurrentPDStagingSnapshot` returns a **read-only** evidence view only when the canonical Workload, current Provider, immutable Intent/Promotion, allocation, reserved Operation, current source authority/deadline and desired Generation all match within locked PostgreSQL records. It **rejects** multi-GPU-per-replica and missing independently confirmed per-phase cardinality. A snapshot is not itself an authorization to Kubernetes CREATE or an activation permit.

`ClaimPDStagingOperation` is a separate, restricted, same-transaction source-checked lease path for a promoted `pd-operation-` PENDING Operation. It locks canonical Workload/source/Operation, checks source current and clips its lease deadline below source expiry. `StartPDStagingOperation` rechecks current authority before the same fenced Operation transitions to IN_FLIGHT. Both operate **only in the database**, not via generic Claim/Start. They do not authorize a side effect; any actual JobSet CREATE must be guarded again at its external boundary and uncertain results remain UNKNOWN/inspect-first.

The package contains **no** Kubernetes ServiceAccount, side-effect callback, JobSet POST/PATCH, Provider adapter deployment or runtime worker registration. In-flight does not mean JobSet exists, device prepared, GPU ready or release. No physical GPU UID/driver NodePrepare/NodeUnprepare or Cost FINAL is established.

Tests must exercise current owner/generation/revocation, Provider status, old Allocation state, exact Proposal and per-phase single GPU shape; concurrent one-winner lease, stale/expired source rejection, generic dispatch denial and independently gated Start. Exact-head merged-tree Go/race/SIGKILL, real PostgreSQL source-pinned release fence and Evidence must all succeed. Do not equate simulated SQL/test fixtures with physical G04/G05 acceptance.
