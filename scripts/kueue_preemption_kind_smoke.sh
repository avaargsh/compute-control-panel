#!/usr/bin/env bash
# Sourced by scripts/kueue_kind_smoke.sh after the admission case.
# Reuses run_admission and the ccp_kueue_job_binding login. Preemption is not release.
if [[ "${BASH_SOURCE[0]}" == "$0" ]]; then
  echo "source this from scripts/kueue_kind_smoke.sh" >&2
  exit 2
fi
[[ -n "${dir:-}" && -n "${ns:-}" && -n "${api_host:-}" && -n "${api_port:-}" && -n "${pg_name:-}" && -n "${binding_role_dsn:-}" ]] || {
  echo "preemption smoke requires the disposable kind harness" >&2
  exit 1
}
command -v kubectl >/dev/null && command -v jq >/dev/null || exit 2

preempt_fail() {
  echo "$1" >&2
  for f in "$dir"/preempt-*.json "$dir"/preempt-*.err; do
    [[ -f "$f" ]] || continue
    echo "----- $(basename "$f") -----" >&2
    tail -n 80 "$f" >&2 || true
  done
  exit 1
}

export CCP_KUEUE_JOURNAL_DSN="postgres://postgres:compute-ci@127.0.0.1:${pg_port}/postgres?sslmode=disable"
[[ "$CCP_KUEUE_JOURNAL_DSN" != "$binding_role_dsn" ]] || preempt_fail "journal DSN aliased the binding login"
[[ "$(psql_at "SELECT rolsuper::text || ':' || rolname FROM pg_roles WHERE rolname='ccp_kueue_job_binding'")" == "false:ccp_kueue_job_binding" ]] || {
  preempt_fail "ccp_kueue_job_binding is not the restricted login"
}

cat > "$dir/preempt-queues.yaml" <<YAML
apiVersion: kueue.x-k8s.io/v1beta1
kind: ResourceFlavor
metadata: {name: ccp-preempt}
---
apiVersion: kueue.x-k8s.io/v1beta1
kind: WorkloadPriorityClass
metadata: {name: ccp-preempt-low}
value: 100
description: disposable low priority
---
apiVersion: kueue.x-k8s.io/v1beta1
kind: WorkloadPriorityClass
metadata: {name: ccp-preempt-high}
value: 10000
description: disposable high priority
---
apiVersion: kueue.x-k8s.io/v1beta1
kind: ClusterQueue
metadata: {name: ccp-preempt}
spec:
  namespaceSelector: {}
  queueingStrategy: BestEffortFIFO
  preemption:
    reclaimWithinCohort: Never
    withinClusterQueue: LowerPriority
  resourceGroups:
  - coveredResources: [cpu, memory]
    flavors:
    - name: ccp-preempt
      resources:
      - {name: cpu, nominalQuota: 100m}
      - {name: memory, nominalQuota: 32Mi}
---
apiVersion: kueue.x-k8s.io/v1beta1
kind: LocalQueue
metadata: {name: ccp-preempt, namespace: $ns}
spec: {clusterQueue: ccp-preempt}
YAML
queue_ready=false
for attempt in $(seq 1 15); do
  if kubectl apply -f "$dir/preempt-queues.yaml" >"$dir/preempt-queues.out" 2>"$dir/preempt-queues.err"; then
    queue_ready=true
    break
  fi
  if ! grep -Eq 'failed calling webhook|conversion webhook|connect: connection refused|no endpoints available' "$dir/preempt-queues.err"; then
    cat "$dir/preempt-queues.err" >&2
    preempt_fail "non-retryable preemption queue apply"
  fi
  echo "Kueue webhook not ready; retry preemption queue apply $attempt/15" >&2
  sleep 2
done
[[ "$queue_ready" == true ]] || preempt_fail "preemption queue was not accepted"
kubectl get clusterqueue ccp-preempt -o json > "$dir/preempt-clusterqueue.json"
jq -e '
  .spec.preemption.withinClusterQueue == "LowerPriority" and
  .spec.resourceGroups[0].flavors[0].resources[0].name == "cpu" and
  .spec.resourceGroups[0].flavors[0].resources[0].nominalQuota == "100m" and
  .spec.resourceGroups[0].flavors[0].resources[1].nominalQuota == "32Mi"
' "$dir/preempt-clusterqueue.json" >/dev/null || preempt_fail "ClusterQueue is not one-Job LowerPriority preemption"

echo "preemption: low-priority queued Job through the restricted binding login"
if ! run_admission "$dir/preempt-submit-low.json" preemption submit-low; then
  preempt_fail "low-priority submit failed"
fi
jq -e '
  .phase == "submit-low" and .projection == "ADMITTED" and .disposition == "ADMITTED" and
  .evicted == false and .preempted == false and .quota_reserved == true and .admitted == true and
  .cluster_queue == "ccp-preempt" and
  (.workload_uid | type == "string" and length > 0) and
  (.workload_resource_version | type == "string" and length > 0) and
  (.job_uid | type == "string" and length > 0) and .job_uid != .workload_uid and
  (.job_name | type == "string" and length > 0) and
  .allocated == false and .active == false and .ready == false and .costable == false and .released == false and
  .release_attested == false and .pin_durable == true and .operation_id == "ccp-kind-preempt-low"
' "$dir/preempt-submit-low.json" >/dev/null || preempt_fail "low-priority projection was not admitted and inactive"
low_job_uid="$(jq -r .job_uid "$dir/preempt-submit-low.json")"
low_job_name="$(jq -r .job_name "$dir/preempt-submit-low.json")"
low_workload_uid="$(jq -r .workload_uid "$dir/preempt-submit-low.json")"
low_workload_rv="$(jq -r .workload_resource_version "$dir/preempt-submit-low.json")"
low_job_rv="$(jq -r .job_resource_version "$dir/preempt-submit-low.json")"
[[ "$(psql_at "SELECT job_uid FROM public.kueue_job_create_pins WHERE namespace='ccp-kueue-smoke' AND operation_id='ccp-kind-preempt-low'")" == "$low_job_uid" ]] || {
  preempt_fail "low-priority pin was not stored by the restricted login"
}
workloads_json | jq -e --arg id "$low_job_uid" --arg uid "$low_workload_uid" --arg rv "$low_workload_rv" '
  [.items[] | select(any(.metadata.ownerReferences[]?; .kind == "Job" and .uid == $id))]
  | if length == 1 then .[0] else empty end
  | .metadata.uid == $uid and .metadata.resourceVersion == $rv
    and any(.status.conditions[]?; .type == "QuotaReserved" and .status == "True")
    and any(.status.conditions[]?; .type == "Admitted" and .status == "True")
    and (any(.status.conditions[]?; .type == "Evicted" and .status == "True") | not)
' >/dev/null || preempt_fail "live Workload did not match the admitted projection"

echo "preemption: high-priority queued Job must evict the low-priority Workload"
export CCP_KUEUE_PREEMPTION_PRIOR="$dir/preempt-submit-low.json"
if ! run_admission "$dir/preempt-submit-high.json" preemption submit-high; then
  preempt_fail "high-priority submit did not observe preemption"
fi
jq -e --arg low "$low_job_uid" --arg high_forbidden "$low_workload_uid" --arg prior_rv "$low_workload_rv" '
  .phase == "submit-high" and .stale_rejected == true and .cross_rejected == true and .deduped == true and
  .release_attested == false and
  .low.projection == "NOT_READY" and .low.disposition == "PREEMPTED" and
  .low.evicted == true and .low.evicted_reason == "Preempted" and
  .low.preempted == true and .low.preempted_reason == "InClusterQueue" and
  .low.quota_reserved == false and .low.admitted == false and
  .low.job_uid == $low and .low.workload_uid != $low and .low.job_uid != .low.workload_uid and
  (.low.workload_resource_version | tonumber) > ($prior_rv | tonumber) and
  .low.allocated == false and .low.active == false and .low.ready == false and
  .low.costable == false and .low.released == false and .low.pin_durable == true and
  .high.projection == "ADMITTED" and .high.disposition == "ADMITTED" and
  .high.quota_reserved == true and .high.admitted == true and .high.evicted == false and
  .high.cluster_queue == "ccp-preempt" and
  .high.job_uid != $low and .high.workload_uid != .low.workload_uid and
  .high.workload_uid != $high_forbidden and .high.job_uid != .high.workload_uid and
  .high.allocated == false and .high.active == false and .high.ready == false and
  .high.costable == false and .high.released == false and .high.pin_durable == true and
  .high.operation_id == "ccp-kind-preempt-high" and .low.operation_id == "ccp-kind-preempt-low"
' "$dir/preempt-submit-high.json" >/dev/null || preempt_fail "preemption projection crossed identities or became release"
high_job_uid="$(jq -r .high.job_uid "$dir/preempt-submit-high.json")"
high_job_name="$(jq -r .high.job_name "$dir/preempt-submit-high.json")"
high_workload_uid="$(jq -r .high.workload_uid "$dir/preempt-submit-high.json")"
high_workload_rv="$(jq -r .high.workload_resource_version "$dir/preempt-submit-high.json")"
low_preempt_rv="$(jq -r .low.workload_resource_version "$dir/preempt-submit-high.json")"
[[ "$low_job_name" != "$high_job_name" && "$low_job_uid" != "$high_job_uid" && "$low_workload_uid" != "$high_workload_uid" ]] || {
  preempt_fail "A and B did not keep distinct Job and Workload identities"
}
[[ "$(psql_at "SELECT job_uid FROM public.kueue_job_create_pins WHERE namespace='ccp-kueue-smoke' AND operation_id='ccp-kind-preempt-high'")" == "$high_job_uid" ]] || {
  preempt_fail "high-priority pin was not its own binding"
}
workloads_json | jq -e --arg id "$low_job_uid" --arg uid "$low_workload_uid" --arg rv "$low_preempt_rv" '
  [.items[] | select(any(.metadata.ownerReferences[]?; .kind == "Job" and .uid == $id))]
  | if length == 1 then .[0] else empty end
  | .metadata.uid == $uid and .metadata.resourceVersion == $rv and .status.admission == null
    and any(.status.conditions[]?; .type == "Evicted" and .status == "True" and .reason == "Preempted")
    and any(.status.conditions[]?; .type == "Preempted" and .status == "True" and .reason == "InClusterQueue")
    and any(.status.conditions[]?; .type == "QuotaReserved" and .status == "False")
    and any(.status.conditions[]?; .type == "Admitted" and .status == "False")
' >/dev/null || preempt_fail "live low-priority Workload conditions were not Evicted/Preempted with quota and admission false"
workloads_json | jq -e --arg id "$high_job_uid" --arg uid "$high_workload_uid" --arg rv "$high_workload_rv" '
  [.items[] | select(any(.metadata.ownerReferences[]?; .kind == "Job" and .uid == $id))]
  | if length == 1 then .[0] else empty end
  | .metadata.uid == $uid and .metadata.resourceVersion == $rv
    and .status.admission.clusterQueue == "ccp-preempt"
    and any(.status.conditions[]?; .type == "QuotaReserved" and .status == "True")
    and any(.status.conditions[]?; .type == "Admitted" and .status == "True")
' >/dev/null || preempt_fail "live high-priority Workload was not admitted on its own UID"
low_pods_gone() {
  kubectl -n "$ns" get pods -o json | jq -e --arg uid "$low_job_uid" '
    [ .items[] | select(.metadata.labels["batch.kubernetes.io/controller-uid"] == $uid) ] | length == 0
  ' >/dev/null
}
wait_for 'preempted Job pods absent' low_pods_gone || preempt_fail "preempted Job still had pods"
echo "kueue_preemption=$(jq -c '{low:{job:.low.job_uid,workload:.low.workload_uid,rv:.low.workload_resource_version,projection:.low.projection,disposition:.low.disposition,released:.low.released},high:{job:.high.job_uid,workload:.high.workload_uid,rv:.high.workload_resource_version,projection:.high.projection,released:.high.released},stale:.stale_rejected,cross:.cross_rejected}' "$dir/preempt-submit-high.json")"

save_preempt_successor() {
  local name="$1" out="$2"
  kubectl -n "$ns" get job "$name" -o json | jq '
    .spec |= (
      del(.selector) |
      if .template.metadata.labels == null then . else
        .template.metadata.labels |= del(
          .["batch.kubernetes.io/controller-uid"],
          .["batch.kubernetes.io/job-name"],
          .["controller-uid"],
          .["job-name"]
        )
      end
    ) | {
      apiVersion, kind,
      metadata: {
        name: .metadata.name, namespace: .metadata.namespace, annotations: .metadata.annotations,
        labels: {
          "kueue.x-k8s.io/queue-name": .metadata.labels["kueue.x-k8s.io/queue-name"],
          "kueue.x-k8s.io/priority-class": .metadata.labels["kueue.x-k8s.io/priority-class"]
        }
      },
      spec: (.spec + {suspend: true})
    }' > "$out"
}
save_preempt_successor "$low_job_name" "$dir/preempt-low-successor.json"
save_preempt_successor "$high_job_name" "$dir/preempt-high-successor.json"

sigkill_preempt() {
  local outfile="$1"
  shift
  CCP_DISPOSABLE_KIND_SMOKE=1 \
    POD_NAMESPACE="$ns" \
    KUBERNETES_SERVICE_HOST="$api_host" \
    KUBERNETES_SERVICE_PORT_HTTPS="$api_port" \
    CCP_KUEUE_SA_CA="$dir/kube-ca.crt" \
    CCP_KUEUE_SA_TOKEN="$dir/sa.token" \
    CCP_KUEUE_BINDING_DSN="$binding_role_dsn" \
    CCP_KUEUE_JOURNAL_DSN="$CCP_KUEUE_JOURNAL_DSN" \
    "$dir/kueue-provider-smoke" "$@" >"$outfile" 2>"$outfile.err" &
  local pid=$!
  local ready=false
  for _ in $(seq 1 450); do
    if jq -e '(.tombstone_durable == true or (.phase == "successor" and .status == "UNKNOWN")) and (.uid != "" and .target_uid != "")' "$outfile" >/dev/null 2>&1; then
      ready=true
      break
    fi
    if ! kill -0 "$pid" 2>/dev/null; then
      cat "$outfile.err" >&2 || true
      preempt_fail "preemption process exited before the journal was durable"
    fi
    sleep 0.2
  done
  if [[ "$ready" != true ]]; then
    kill -9 "$pid" >/dev/null 2>&1 || true
    cat "$outfile.err" >&2 || true
    preempt_fail "timed out waiting for a durable preemption journal"
  fi
  kill -9 "$pid" 2>/dev/null || true
  set +e
  wait "$pid"
  local code=$?
  set -e
  [[ "$code" -eq 137 ]] || preempt_fail "expected SIGKILL status 137, got $code"
}

echo "preemption: later process UID-fenced DELETE of only the owned Job UIDs"
sigkill_preempt "$dir/preempt-delete.json" preemption delete --hold
jq -e --arg low "$low_job_uid" --arg high "$high_job_uid" '
  .phase == "delete" and .status == "UNKNOWN" and .tombstone_durable == true and .acknowledged == true and
  .uid == $low and .target_uid == $high and .released == false and .ready == false and .release_attested == false and
  .low_operation_id == "ccp-kind-preempt-low-delete" and .high_operation_id == "ccp-kind-preempt-high-delete"
' "$dir/preempt-delete.json" >/dev/null || preempt_fail "delete journal was not the two owned UIDs"

echo "preemption: fresh process Inspects TARGET_ABSENT"
if ! run_admission "$dir/preempt-recover-delete.json" preemption recover-delete; then
  preempt_fail "delete recovery failed"
fi
jq -e --arg low "$low_job_uid" --arg high "$high_job_uid" '
  .phase == "recover-delete" and .status == "SUCCEEDED" and .tombstone_durable == true and
  .uid == $low and .target_uid == $high and .released == false and .ready == false and .release_attested == false
' "$dir/preempt-recover-delete.json" >/dev/null || preempt_fail "recovery was not TARGET_ABSENT"
for op in ccp-kind-preempt-low-delete ccp-kind-preempt-high-delete; do
  if [[ "$op" == ccp-kind-preempt-low-delete ]]; then want="$low_job_uid"; else want="$high_job_uid"; fi
  row="$(psql_at "SELECT status || '|' || coalesce(target_resource_uid,'') || '|' || (provider_resource_uid IS NULL)::text FROM public.compute_operations WHERE tenant_id='tenant-a' AND compute_operation_id='${op}'")"
  [[ "$row" == "SUCCEEDED|${want}|true" ]] || preempt_fail "delete finality was not exact-UID absence: ${row:-empty}"
  obs="$(psql_at "SELECT operation_result || '|' || coalesce(details->>'TargetResourceUID','') FROM public.compute_resource_observations WHERE tenant_id='tenant-a' AND compute_operation_id='${op}'")"
  [[ "$obs" == "TARGET_ABSENT|${want}" ]] || preempt_fail "observation was not TARGET_ABSENT for ${want}: ${obs:-empty}"
done
preempt_uid_absent() {
  local uid="$1"
  kubectl -n "$ns" get jobs -o json | jq -e --arg uid "$uid" 'all(.items[]; .metadata.uid != $uid)' >/dev/null
}
wait_for 'low Job UID absent' preempt_uid_absent "$low_job_uid" || preempt_fail "low Job UID remained"
wait_for 'high Job UID absent' preempt_uid_absent "$high_job_uid" || preempt_fail "high Job UID remained"

echo "preemption: same-name successors stay in place"
kubectl apply -f "$dir/preempt-low-successor.json" >/dev/null
kubectl apply -f "$dir/preempt-high-successor.json" >/dev/null
low_successor_uid="$(kubectl -n "$ns" get job "$low_job_name" -o jsonpath='{.metadata.uid}')"
high_successor_uid="$(kubectl -n "$ns" get job "$high_job_name" -o jsonpath='{.metadata.uid}')"
low_successor_created="$(kubectl -n "$ns" get job "$low_job_name" -o jsonpath='{.metadata.creationTimestamp}')"
high_successor_created="$(kubectl -n "$ns" get job "$high_job_name" -o jsonpath='{.metadata.creationTimestamp}')"
[[ -n "$low_successor_uid" && "$low_successor_uid" != "$low_job_uid" && -n "$high_successor_uid" && "$high_successor_uid" != "$high_job_uid" ]] || {
  preempt_fail "successors were not new UIDs"
}
sigkill_preempt "$dir/preempt-successor.json" preemption successor --hold
jq -e --arg low "$low_job_uid" --arg high "$high_job_uid" '
  .phase == "successor" and .status == "UNKNOWN" and .uid == $low and .target_uid == $high and
  .released == false and .release_attested == false
' "$dir/preempt-successor.json" >/dev/null || preempt_fail "successor journal did not keep the original UIDs"
if ! run_admission "$dir/preempt-recover-successor.json" preemption recover-successor; then
  preempt_fail "successor recovery failed"
fi
jq -e --arg low "$low_job_uid" --arg high "$high_job_uid" '
  .phase == "recover-successor" and .status == "MANUAL_REVIEW" and .uid == $low and .target_uid == $high and
  .released == false and .ready == false and .release_attested == false
' "$dir/preempt-recover-successor.json" >/dev/null || preempt_fail "successor recovery did not stay MANUAL_REVIEW"
[[ "$(kubectl -n "$ns" get job "$low_job_name" -o jsonpath='{.metadata.uid}')" == "$low_successor_uid" &&
  "$(kubectl -n "$ns" get job "$low_job_name" -o jsonpath='{.metadata.creationTimestamp}')" == "$low_successor_created" &&
  -z "$(kubectl -n "$ns" get job "$low_job_name" -o jsonpath='{.metadata.deletionTimestamp}')" &&
  "$(kubectl -n "$ns" get job "$high_job_name" -o jsonpath='{.metadata.uid}')" == "$high_successor_uid" &&
  "$(kubectl -n "$ns" get job "$high_job_name" -o jsonpath='{.metadata.creationTimestamp}')" == "$high_successor_created" &&
  -z "$(kubectl -n "$ns" get job "$high_job_name" -o jsonpath='{.metadata.deletionTimestamp}')" ]] || {
  preempt_fail "JOB_UID_REPLACED process deleted or replaced a same-name successor"
}
if [[ -n "${admission_job_name:-}" && -n "${admission_successor_uid:-}" ]]; then
  [[ "$(kubectl -n "$ns" get job "$admission_job_name" -o jsonpath='{.metadata.uid}')" == "$admission_successor_uid" ]] || {
    preempt_fail "preemption cleanup touched the admission successor"
  }
fi
echo "preemption_deleted_low=$low_job_uid preemption_deleted_high=$high_job_uid preemption_successor_low=$low_successor_uid preemption_successor_high=$high_successor_uid prior_job_rv=$low_job_rv"
echo "PASS: Kueue withinClusterQueue LowerPriority preemption of a queued Job is projected NOT_READY from Evicted/Preempted conditions. Preemption is not Allocation RELEASED."
