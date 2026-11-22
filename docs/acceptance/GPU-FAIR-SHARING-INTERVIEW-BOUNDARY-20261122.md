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

## Disposable real-controller Cohort test — first controller pass verified

`scripts/kueue_cohort_kind_smoke.sh` is now sourced after the existing
Kueue v0.19.6 / Kubernetes 1.34 disposable-kind suite. The fixture
**writes** the legacy `v1beta1 spec.cohort` API and **reads back** the
served `v1beta2 spec.cohortName` representation; a real Actions failure
at commit `c9ad4d9` proved these fields cannot be interchanged.

**First passed real-controller smoke:** GitHub Actions
[#44984809622](https://github.com/avaargsh/compute-control-panel/actions/runs/44984809622),
reported PR HEAD `cf6e838ab157604938f4227846cfdb4f2fbd34f1`,
with native `cohort_acceptance` evidence. This run's source checkout,
however, was the GitHub PR synthetic merge SHA
`e5a2545b4a3dd3d67ab59bd8945777d59445256f`, not the literal
PR HEAD. That source identity difference is a **distinct outstanding
gate** for exact-head reproducibility, not a reason to erase the
successful controller evidence. The workflow now pins
`github.event.pull_request.head.sha || github.sha` and asserts
`git rev-parse HEAD` equality; that new workflow needs its own
successful latest-HEAD run.

Two distinct namespaces, LocalQueues and ClusterQueues share one Cohort,
each with 100m CPU nominal quota. A begins at 50m, B starts at 100m
then admits a 50m lower-priority Job by borrowing A's unused quota.
A submits a higher-priority 50m Job; A's reclaim policy must cause the
borrowed B Job's *native Kueue Workload* to report
`Evicted=True / Preempted=True / InCohortReclamation`,
`QuotaReserved=False`, and `Admitted=False`.
The fixture also requires the A reclaim Job to be admitted,
the original A/B nominal Jobs to remain admitted, and the evicted
borrower's Pods to disappear. Every observation is tied to the
Job/Workload UID; assertions fail closed rather than being skipped.

The CPU requests are intentionally small so this test exercises Kueue
**quota policy without physical GPU hardware**. The runtime output
explicitly marks `gpu_physical_release_verified=false`,
`allocation_released=false`, and `cost_final=false`.
It also does not prove CCP's own controller invoked the reclaim.

Evidence from the first passing controller test:
`quota_borrowing_observed=true`,
`preempted_reason=InCohortReclamation`,
`borrowed_workload_uid=27903bb5-02b1-4630-9f31-82f6d131ae4e`,
and `reclaimed_workload_uid=34e2536b-e6fa-48f3-8f95-702f085bacea`.
The test does not attest actual GPU allocation or release; no `READY`
or physical `RELEASED` promotion follows. **For merge:** confirm a
new exact-checkout HEAD with successful shell syntax, Go/race and
controller-run evidence; independently review the diff.

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

This PR fixes a read-only projection and adds a **disposable CPU-only
controller acceptance fixture**. The fixture configures its own disposable
Kueue preemption policy, but does not change production policy, create
CCP provider writes, mutate billing, or claim physical-GPU results.
Same-ClusterQueue smoke and the CPU Cohort fixture do not replace
the separate 16-GPU physical acceptance.

Upstream compatibility references:
- https://kueue.sigs.k8s.io/docs/concepts/preemption/
- https://kueue.sigs.k8s.io/docs/concepts/cluster_queue/
- https://github.com/kubernetes-sigs/kueue/blob/main/apis/kueue/v1beta1/workload_types.go
