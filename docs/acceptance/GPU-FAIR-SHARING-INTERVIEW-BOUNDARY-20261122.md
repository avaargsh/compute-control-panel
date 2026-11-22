# GPU fair-share interview claims → CCP evidence boundary (2026-11-22)

Status: **reason-projection patch and synthetic tests only**. No real cross-tenant 16-GPU
borrowing/reclaim cluster run, no checkpoint-aware preemption implementation, and
no physical GPU release proof is added by this PR.

## Authority and runnable regression

Kueue is the authority for ClusterQueue nominal quota, borrowing limit, Cohort
sharing, admission, and preemption. CCP observes the Job/Workload and does not
invent a second scheduler or infer that quota eviction released a GPU.

The existing `internal/provider/kueue_preemption_projection.go` only accepted
`Preempted.reason=InClusterQueue`. That misses documented Kueue cohort
reclamation/fair-share cases. The projection now accepts *only* the pinned
documented reasons:

- `InClusterQueue`
- `InCohortReclamation`
- `InCohortFairSharing`
- `InCohortReclaimWhileBorrowing`

The exact observed reason is preserved, with `NOT_READY / PREEMPTED`,
`QuotaReserved=false`, `Admitted=false`, and every
`Allocated / Active / Ready / Costable / Released=false`.
Unrecognized or generic reasons fail closed, including unexpected future API
values until version compatibility is reviewed.

Run the focused unit tests at the exact PR HEAD:

```bash
go test ./internal/provider -run 'TestKueueSupportedPreemptionReasonsRemainInactive|TestPreemptionProjectionRejectsMissingOrContradictoryEvidence|TestPreemptionFenceRejectsStaleAndCrossProjection' -count=1
go test ./internal/provider -count=1
```

These are synthetic observation fixtures, not proof that two live ClusterQueues
actually borrowed/reclaimed GPU quota.

## Minimum real-cluster acceptance still outstanding

For a disposable cluster whose tested Kueue version supports the policies,
define two tenant LocalQueues bound to distinct ClusterQueues in one Cohort,
with a compatible GPU ResourceFlavor. Pin the exact Kueue API version,
extended resource name, DRA/device-plugin mode, and node availability.

1. Establish quota A=8, B=8 for **16 verified allocatable GPUs**.
   Observe A using 4, B using 8. Submit B demand for four more;
   verify *Kueue Workload* quota reservation and admission, not merely
   a CCP-derived available number. If B already uses 8, the requested
   additional four will bring its total to 12.
2. Submit A demand for the same four cards. Confirm the permitted cohort
   reclaim policy, which workload was selected, its precise preemption
   reason/UID, and independent Kueue quota/accounting evidence.
3. For any evicted Job, prove Pods are gone, the device claims/unprepare
   released actual resources, stale identities were fenced, and the
   tenant's workload can be re-admitted only after authoritative capacity
   becomes available. Quota eviction **alone** cannot set Allocation RELEASED
   or Cost FINAL.
4. Run separate tests for non-preemptible serving and checkpoint-capable
   offline work. Kueue preemption does **not** automatically checkpoint
   training processes or drain inference connections. Require a workload
   owner/coordinator to implement checkpoint, readiness/traffic transfer,
   restart, and state-integrity checks, with an explicit fallback on failure.
5. Capture per-tenant guaranteed quota attainment under demand,
   p95 time from queued to admitted, borrow/reclaim counts and latency,
   checkpoint restore success, inference availability, and the minimum
   tenant share over time. In a constrained cluster, returning quota may
   require waiting; never claim a strict recovery SLA without measuring it.

## Scope

This PR is a read-only projection compatibility fix. It does not enable
preemption policies, provision quota, perform a reclaim, change billing,
or claim live physical-GPU tests. Existing same-ClusterQueue disposable-kind
smoke remains separate from the unimplemented two-tenant cohort acceptance.

Upstream compatibility references:
- https://kueue.sigs.k8s.io/docs/concepts/preemption/
- https://kueue.sigs.k8s.io/docs/concepts/cluster_queue/
- https://github.com/kubernetes-sigs/kueue/blob/main/apis/kueue/v1beta1/workload_types.go
