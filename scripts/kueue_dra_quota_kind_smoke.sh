#!/usr/bin/env bash
# Disposable kind only: exercise Kueue v0.19 device-count quota with two
# independent tenant namespaces and one shared ClusterQueue. No real GPU
# ownership, driver Unprepare or Allocation RELEASED is asserted.
set -Eeuo pipefail

[[ "$CCP_DISPOSABLE_KIND_SMOKE" == "1" ]] || { echo "disposable opt-in required" >&2; exit 2; }
[[ "$(kubectl config current-context)" == "kind-ccp-kueue-smoke" ]] || {
  echo "refusing non-disposable Kubernetes context" >&2; exit 2
}
for tool in kubectl jq; do command -v "$tool" >/dev/null || exit 2; done

tenant_a="ccp-dra-quota-a"
tenant_b="ccp-dra-quota-b"
queue="ccp-dra-quota"
job="ccp-dra-quota"
resource="example.com/gpu"
dir="$(mktemp -d)"
trap 'rm -rf "$dir"' EXIT

# The shared ClusterQueue has enough CPU/RAM for BOTH jobs, but can reserve
# only one logical DRA device. Existing Kueue Configuration maps DeviceClass
# gpu.example.com -> example.com/gpu; a missing mapping must fail this test.
kubectl apply -f - <<YAML
apiVersion: v1
kind: Namespace
metadata:
  name: $tenant_a
  labels: {ccp-dra-quota-test: "true"}
---
apiVersion: v1
kind: Namespace
metadata:
  name: $tenant_b
  labels: {ccp-dra-quota-test: "true"}
---
apiVersion: kueue.x-k8s.io/v1beta1
kind: ClusterQueue
metadata: {name: $queue}
spec:
  namespaceSelector:
    matchLabels: {ccp-dra-quota-test: "true"}
  resourceGroups:
  - coveredResources: [cpu, memory, example.com/gpu]
    flavors:
    - name: ccp-smoke
      resources:
      - {name: cpu, nominalQuota: "2"}
      - {name: memory, nominalQuota: "2Gi"}
      - {name: example.com/gpu, nominalQuota: "1"}
---
apiVersion: kueue.x-k8s.io/v1beta1
kind: LocalQueue
metadata: {name: $queue, namespace: $tenant_a}
spec: {clusterQueue: $queue}
---
apiVersion: kueue.x-k8s.io/v1beta1
kind: LocalQueue
metadata: {name: $queue, namespace: $tenant_b}
spec: {clusterQueue: $queue}
---
apiVersion: resource.k8s.io/v1
kind: ResourceClaimTemplate
metadata: {name: $job, namespace: $tenant_a}
spec:
  spec:
    devices:
      requests:
      - name: gpu
        exactly: {deviceClassName: gpu.example.com, count: 1}
---
apiVersion: resource.k8s.io/v1
kind: ResourceClaimTemplate
metadata: {name: $job, namespace: $tenant_b}
spec:
  spec:
    devices:
      requests:
      - name: gpu
        exactly: {deviceClassName: gpu.example.com, count: 1}
YAML

[[ "$(kubectl get deviceclass gpu.example.com -o jsonpath='{.metadata.name}')" == "gpu.example.com" ]] || exit 1
queue_uid="$(kubectl get clusterqueue "$queue" -o jsonpath='{.metadata.uid}')"
[[ -n "$queue_uid" ]] || { echo "missing ClusterQueue UID" >&2; exit 1; }

submit_job() {
  local ns="$1"
  kubectl create -f - <<YAML
apiVersion: batch/v1
kind: Job
metadata:
  name: $job
  namespace: $ns
  labels:
    kueue.x-k8s.io/queue-name: $queue
spec:
  backoffLimit: 0
  template:
    spec:
      restartPolicy: Never
      # Quota admission must be observed separately from Pod binding and
      # physical device preparation. Neither tenant has an eligible node.
      nodeSelector:
        ccp-dra-quota-test/unplaced: "true"
      containers:
      - name: consumer
        image: registry.k8s.io/pause:3.10
        resources:
          requests: {cpu: 100m, memory: 32Mi}
          claims:
          - name: gpu
      resourceClaims:
      - name: gpu
        resourceClaimTemplateName: $job
YAML
}
# Jobs overlap in time; which tenant wins Kueue admission is intentionally
# nondeterministic. The correctness assertion does not depend on winner order.
submit_job "$tenant_a" > "$dir/create-a.log" 2>&1 &
pid_a=$!
submit_job "$tenant_b" > "$dir/create-b.log" 2>&1 &
pid_b=$!
if ! wait "$pid_a"; then cat "$dir/create-a.log" >&2; exit 1; fi
if ! wait "$pid_b"; then cat "$dir/create-b.log" >&2; exit 1; fi

job_a="$(kubectl -n "$tenant_a" get job "$job" -o json)"
job_b="$(kubectl -n "$tenant_b" get job "$job" -o json)"
uid_a="$(jq -er '.metadata.uid' <<< "$job_a")"
uid_b="$(jq -er '.metadata.uid' <<< "$job_b")"
[[ -n "$uid_a" && -n "$uid_b" && "$uid_a" != "$uid_b" ]] || exit 1

workload_for_job() {
  local ns="$1" uid="$2"
  kubectl -n "$ns" get workloads.kueue.x-k8s.io -o json | jq -cer --arg uid "$uid" '
    [.items[] | select(any(.metadata.ownerReferences[]?; .kind == "Job" and .uid == $uid))] |
    if length == 1 then .[0] else empty end'
}
has_device_charge() {
  jq -e --arg r "$resource" '
    (.status.admission.podSetAssignments | length) == 1 and
    ((.status.admission.podSetAssignments[0].resourceUsage[$r] // null) | tostring) == "1" and
    any(.status.conditions[]?; .type == "QuotaReserved" and .status == "True") and
    any(.status.conditions[]?; .type == "Admitted" and .status == "True")'
}
quota_denied() {
  jq -e --arg r "$resource" '
    (.status.admission == null) and
    any(.status.conditions[]?; .type == "QuotaReserved" and .status == "False" and
      ((.message // "") | contains($r)))'
}
snapshot() {
  local a b cq
  a="$(workload_for_job "$tenant_a" "$uid_a")" || return 1
  b="$(workload_for_job "$tenant_b" "$uid_b")" || return 1
  cq="$(kubectl get clusterqueue "$queue" -o json)" || return 1
  jq -e --arg uid "$queue_uid" '
    .metadata.uid == $uid and
    .status.reservingWorkloads == 1 and .status.admittedWorkloads == 1' <<< "$cq" >/dev/null || return 1
  if has_device_charge <<< "$a" >/dev/null && quota_denied <<< "$b" >/dev/null; then
    winner="$tenant_a"; loser="$tenant_b"
  elif has_device_charge <<< "$b" >/dev/null && quota_denied <<< "$a" >/dev/null; then
    winner="$tenant_b"; loser="$tenant_a"
  else
    return 1
  fi
  for w in "$a" "$b"; do
    jq -e --arg n "$job" --arg q "$queue" '
      (.metadata.uid | type == "string" and length > 0) and
      (.metadata.resourceVersion | type == "string" and length > 0) and
      .spec.queueName == $q and
      (.metadata.ownerReferences | any(.kind == "Job" and .name == $n))' <<< "$w" >/dev/null || return 1
  done
  printf '%s\n' "$a" > "$dir/a.json"
  printf '%s\n' "$b" > "$dir/b.json"
  printf '%s\n' "$cq" > "$dir/queue.json"
}
stable=0
for attempt in $(seq 1 90); do
  if snapshot; then stable=1; break; fi
  sleep 2
done
if [[ "$stable" != 1 ]]; then
  echo "FAIL: two tenants were not constrained by a one-device DRA quota" >&2
  kubectl get clusterqueue "$queue" -o yaml >&2 || true
  for ns in "$tenant_a" "$tenant_b"; do
    kubectl -n "$ns" get workloads,jobs,pods -o yaml >&2 || true
  done
  exit 1
fi

# Reserve a single DRA device by Kueue quota. Because neither job is node
# bound, this is NOT a claim that the driver allocated or prepared a device.
winner_uid="$(kubectl -n "$winner" get job "$job" -o jsonpath='{.metadata.uid}')"
loser_uid="$(kubectl -n "$loser" get job "$job" -o jsonpath='{.metadata.uid}')"
if [[ "$winner" == "$tenant_a" ]]; then winner_record="$dir/a.json"; loser_record="$dir/b.json"; else winner_record="$dir/b.json"; loser_record="$dir/a.json"; fi
winner_workload_uid="$(jq -er '.metadata.uid' "$winner_record")"
loser_workload_uid="$(jq -er '.metadata.uid' "$loser_record")"
[[ "$winner_uid" != "$loser_uid" && "$winner_workload_uid" != "$loser_workload_uid" ]] || exit 1
echo "dra_quota_before=$(jq -cn --arg winner "$winner" --arg loser "$loser" \
  --arg a "$uid_a" --arg b "$uid_b" --arg quota "$queue_uid" \
  --slurpfile x "$winner_record" --slurpfile y "$loser_record" '{
    case_id:"M3-DRA-QUOTA-CONTENTION",device_authority:"SIMULATED_DEVICE_ONLY",
    quota_resource:"example.com/gpu",quota_limit:1,admitted_tenant:$winner,pending_tenant:$loser,
    job_uids:[$a,$b],clusterqueue_uid:$quota,
    admitted_workload:{uid:$x[0].metadata.uid,rv:$x[0].metadata.resourceVersion,
      usage:$x[0].status.admission.podSetAssignments[0].resourceUsage},
    pending_workload:{uid:$y[0].metadata.uid,rv:$y[0].metadata.resourceVersion,
      conditions:$y[0].status.conditions},
    physical_gpu_verified:false,allocation_ready:false,cost_final:false
  }')"

# This is fixture cleanup, not the CCP UID-fenced Provider DELETE contract.
# Independently observe Kueue release and re-admission of the other tenant.
[[ "$(kubectl -n "$winner" get job "$job" -o jsonpath='{.metadata.uid}')" == "$winner_uid" ]] || exit 1
kubectl -n "$winner" delete job "$job" --cascade=background --wait=true
re_admitted() {
  local fresh
  fresh="$(workload_for_job "$loser" "$loser_uid")" || return 1
  [[ "$(jq -er '.metadata.uid' <<< "$fresh")" == "$loser_workload_uid" ]] || return 1
  has_device_charge <<< "$fresh" >/dev/null || return 1
  local queue_state
  queue_state="$(kubectl get clusterqueue "$queue" -o json)" || return 1
  jq -e --arg uid "$queue_uid" '
    .metadata.uid == $uid and .status.reservingWorkloads == 1 and
    .status.admittedWorkloads == 1' <<< "$queue_state" >/dev/null || return 1
  local old
  old="$(kubectl -n "$winner" get workloads.kueue.x-k8s.io -o json)" || return 1
  jq -e --arg uid "$winner_workload_uid" 'all(.items[]; .metadata.uid != $uid)' <<< "$old" >/dev/null || return 1
  printf '%s\n' "$fresh" > "$dir/re-admitted.json"
}
stable=0
for attempt in $(seq 1 90); do
  if re_admitted; then stable=1; break; fi
  sleep 2
done
[[ "$stable" == 1 ]] || {
  echo "FAIL: quota did not transfer to pending tenant after winner cleanup" >&2
  kubectl get clusterqueue "$queue" -o yaml >&2 || true
  exit 1
}
echo "dra_quota_after=$(jq -cn --arg loser "$loser" --arg orig "$loser_workload_uid" \
  --arg queue "$queue_uid" --slurpfile w "$dir/re-admitted.json" '{
   case_id:"M3-DRA-QUOTA-READMISSION",readmitted_tenant:$loser,
   same_workload_uid:($w[0].metadata.uid == $orig),clusterqueue_uid:$queue,
   resource_usage:$w[0].status.admission.podSetAssignments[0].resourceUsage,
   device_release_attested:false,allocation_released:false,cost_final:false
  }')"
echo "PASS: two namespaces share an exact one-device Kueue DRA quota; denial and re-admission observed; no physical GPU or release finality attested"
