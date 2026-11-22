#!/usr/bin/env bash
# Kueue v0.19.6 / v1beta1 implicit Cohort CPU-quota acceptance.
# Source ONLY after scripts/kueue_kind_smoke.sh creates the isolated cluster.
# Kueue admission/eviction is not GPU/DRA release or cost finality.
if [[ "$BASH_SOURCE" == "$0" ]]; then
  echo "source this from kueue_kind_smoke.sh only" >&2
  exit 2
fi
[[ "$CCP_DISPOSABLE_KIND_SMOKE" == "1" && -n "$dir" && -n "$KUBECONFIG" ]] || exit 2
command -v kubectl >/dev/null && command -v jq >/dev/null || exit 2

cohort_fail() {
  echo "cohort FAIL: $*" >&2
  kubectl get clusterqueue ccp-cohort-a ccp-cohort-b -o yaml >&2 || true
  kubectl -n ccp-cohort-a get jobs,pods,workloads -o wide >&2 || true
  kubectl -n ccp-cohort-b get jobs,pods,workloads -o wide >&2 || true
  exit 1
}
cohort_wait() {
  local label="$1"
  shift
  for _ in $(seq 1 100); do
    if "$@"; then return 0; fi
    sleep 2
  done
  cohort_fail "timed out waiting for $label"
}

# v1beta1 uses spec.cohort; newer v1beta2 uses cohortName.
kubectl create namespace ccp-cohort-a
kubectl create namespace ccp-cohort-b
cat > "$dir/cohort-queues.yaml" <<'YAML'
apiVersion: kueue.x-k8s.io/v1beta1
kind: ResourceFlavor
metadata: {name: ccp-cohort-cpu}
---
apiVersion: kueue.x-k8s.io/v1beta1
kind: WorkloadPriorityClass
metadata: {name: ccp-cohort-low}
value: 100
---
apiVersion: kueue.x-k8s.io/v1beta1
kind: WorkloadPriorityClass
metadata: {name: ccp-cohort-high}
value: 10000
---
apiVersion: kueue.x-k8s.io/v1beta1
kind: ClusterQueue
metadata: {name: ccp-cohort-a}
spec:
  namespaceSelector:
    matchLabels: {kubernetes.io/metadata.name: ccp-cohort-a}
  cohort: ccp-cohort-isolated
  queueingStrategy: BestEffortFIFO
  preemption:
    reclaimWithinCohort: LowerPriority
    withinClusterQueue: Never
  resourceGroups:
  - coveredResources: [cpu, memory]
    flavors:
    - name: ccp-cohort-cpu
      resources:
      - {name: cpu, nominalQuota: 100m}
      - {name: memory, nominalQuota: 64Mi}
---
apiVersion: kueue.x-k8s.io/v1beta1
kind: ClusterQueue
metadata: {name: ccp-cohort-b}
spec:
  namespaceSelector:
    matchLabels: {kubernetes.io/metadata.name: ccp-cohort-b}
  cohort: ccp-cohort-isolated
  queueingStrategy: BestEffortFIFO
  resourceGroups:
  - coveredResources: [cpu, memory]
    flavors:
    - name: ccp-cohort-cpu
      resources:
      - {name: cpu, nominalQuota: 100m, borrowingLimit: 50m}
      - {name: memory, nominalQuota: 64Mi}
---
apiVersion: kueue.x-k8s.io/v1beta1
kind: LocalQueue
metadata: {name: ccp-cohort-a, namespace: ccp-cohort-a}
spec: {clusterQueue: ccp-cohort-a}
---
apiVersion: kueue.x-k8s.io/v1beta1
kind: LocalQueue
metadata: {name: ccp-cohort-b, namespace: ccp-cohort-b}
spec: {clusterQueue: ccp-cohort-b}
YAML
kubectl apply -f "$dir/cohort-queues.yaml" || cohort_fail "Kueue Cohort config rejected"
kubectl get clusterqueue ccp-cohort-a -o json | jq -e \
  '.spec.cohort == "ccp-cohort-isolated" and .spec.preemption.reclaimWithinCohort == "LowerPriority"' >/dev/null ||
  cohort_fail "A reclaim policy not persisted"
kubectl get clusterqueue ccp-cohort-b -o json | jq -e \
  '.spec.cohort == "ccp-cohort-isolated" and
   .spec.resourceGroups[0].flavors[0].resources[0].borrowingLimit == "50m"' >/dev/null ||
  cohort_fail "B borrowing limit not persisted"

cohort_create_job() {
  local namespace="$1" name="$2" cpu="$3" priority="$4"
  # Upstream fixture via kubectl; not a CCP provider write.
  cat <<YAML | kubectl apply -f - >/dev/null
apiVersion: batch/v1
kind: Job
metadata:
  name: $name
  namespace: $namespace
  labels:
    kueue.x-k8s.io/queue-name: $namespace
    kueue.x-k8s.io/priority-class: $priority
spec:
  suspend: true
  parallelism: 1
  completions: 1
  backoffLimit: 0
  template:
    spec:
      restartPolicy: Never
      terminationGracePeriodSeconds: 0
      containers:
      - name: hold
        image: registry.k8s.io/pause:3.10
        resources:
          requests: {cpu: "$cpu", memory: 16Mi}
          limits: {memory: 16Mi}
YAML
}
cohort_uid() { kubectl -n "$1" get job "$2" -o jsonpath='{.metadata.uid}'; }
cohort_workload() {
  kubectl -n "$1" get workloads -o json | jq -ec --arg uid "$2" '
    [.items[] | select(any(.metadata.ownerReferences[]?;
      .kind == "Job" and .controller == true and .uid == $uid))]
    | if length == 1 then .[0] else empty end'
}
cohort_admitted() {
  local namespace="$1" jobuid="$2" cq="$3"
  cohort_workload "$namespace" "$jobuid" | jq -e --arg cq "$cq" --arg uid "$jobuid" '
    .metadata.uid != $uid and
    .status.admission.clusterQueue == $cq and
    any(.status.conditions[]?; .type == "QuotaReserved" and .status == "True") and
    any(.status.conditions[]?; .type == "Admitted" and .status == "True")' >/dev/null
}
cohort_evicted() {
  cohort_workload "$1" "$2" | jq -e '
    .status.admission == null and
    any(.status.conditions[]?; .type == "Evicted" and .status == "True" and .reason == "Preempted") and
    any(.status.conditions[]?; .type == "Preempted" and .status == "True" and .reason == "InCohortReclamation") and
    any(.status.conditions[]?; .type == "QuotaReserved" and .status == "False") and
    any(.status.conditions[]?; .type == "Admitted" and .status == "False")' >/dev/null
}
cohort_pods_gone() {
  kubectl -n "$1" get pods -o json | jq -e --arg uid "$2" '
    [.items[] | select(.metadata.labels["batch.kubernetes.io/controller-uid"] == $uid)]
    | length == 0' >/dev/null
}

echo "cohort: A holds 50m; B holds 100m and borrows 50m"
cohort_create_job ccp-cohort-a ccp-cohort-a-base 50m ccp-cohort-high
a_base="$(cohort_uid ccp-cohort-a ccp-cohort-a-base)"
cohort_wait "A base admitted" cohort_admitted ccp-cohort-a "$a_base" ccp-cohort-a
cohort_create_job ccp-cohort-b ccp-cohort-b-base 100m ccp-cohort-high
b_base="$(cohort_uid ccp-cohort-b ccp-cohort-b-base)"
cohort_wait "B base admitted" cohort_admitted ccp-cohort-b "$b_base" ccp-cohort-b
cohort_create_job ccp-cohort-b ccp-cohort-b-borrow 50m ccp-cohort-low
b_borrow="$(cohort_uid ccp-cohort-b ccp-cohort-b-borrow)"
cohort_wait "B borrow admitted" cohort_admitted ccp-cohort-b "$b_borrow" ccp-cohort-b

kubectl get clusterqueue ccp-cohort-b -o json > "$dir/cohort-b-borrowed.json"
jq -e 'any(.status.flavorsReservation[]?.resources[]?;
  .name == "cpu" and .total == "150m")' "$dir/cohort-b-borrowed.json" >/dev/null ||
  cohort_fail "B reservation did not prove 150m > 100m nominal"

echo "cohort: A demands remaining nominal 50m and reclaims B's low-priority borrow"
cohort_create_job ccp-cohort-a ccp-cohort-a-reclaim 50m ccp-cohort-high
a_reclaim="$(cohort_uid ccp-cohort-a ccp-cohort-a-reclaim)"
cohort_wait "B borrower evicted by cohort reclaim" cohort_evicted ccp-cohort-b "$b_borrow"
cohort_wait "A reclaim admitted" cohort_admitted ccp-cohort-a "$a_reclaim" ccp-cohort-a
cohort_wait "preempted B pods gone" cohort_pods_gone ccp-cohort-b "$b_borrow"
cohort_admitted ccp-cohort-b "$b_base" ccp-cohort-b || cohort_fail "B nominal base disrupted"
cohort_admitted ccp-cohort-a "$a_base" ccp-cohort-a || cohort_fail "A nominal base disrupted"

b_workload="$(cohort_workload ccp-cohort-b "$b_borrow")"
a_workload="$(cohort_workload ccp-cohort-a "$a_reclaim")"
jq -n --arg sha "$CCP_SMOKE_SOURCE_COMMIT" \
  --arg a_base "$a_base" --arg b_base "$b_base" \
  --arg b "$b_borrow" --arg a "$a_reclaim" \
  --argjson wb "$b_workload" --argjson wa "$a_workload" '{
    source_sha:$sha, case:"Kueue-v0.19.6-CPU-Cohort",
    a_base_job_uid:$a_base, b_base_job_uid:$b_base,
    borrowed_job_uid:$b, reclaimed_job_uid:$a,
    borrowed_workload_uid:$wb.metadata.uid, reclaimed_workload_uid:$wa.metadata.uid,
    preempted_reason:([ $wb.status.conditions[] |
      select(.type == "Preempted") | .reason ][0]),
    quota_borrowing_observed:true, gpu_physical_release_verified:false,
    allocation_released:false, cost_final:false
  }' > "$dir/cohort-acceptance.json"
jq -e '.preempted_reason == "InCohortReclamation" and
  .borrowed_job_uid != .reclaimed_job_uid and
  .borrowed_workload_uid != .reclaimed_workload_uid and
  .gpu_physical_release_verified == false and
  .allocation_released == false and .cost_final == false' "$dir/cohort-acceptance.json" >/dev/null ||
  cohort_fail "cohort report missing identity/release fail-closed checks"
echo "cohort_acceptance=$(jq -c . "$dir/cohort-acceptance.json")"
echo "PASS: Kueue real-controller Cohort CPU quota borrow/reclaim; GPU release UNPROVEN."
