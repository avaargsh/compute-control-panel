#!/usr/bin/env bash
# Disposable relist/restart acceptance. Kubernetes resourceVersion is compared
# only for equality. This is not Allocation RELEASED or a GPU readiness claim.
set -Eeuo pipefail
if [[ "${CCP_DISPOSABLE_KIND_SMOKE:-}" != "1" ]]; then
  echo "Explicit CCP_DISPOSABLE_KIND_SMOKE=1 required" >&2
  exit 2
fi
for tool in kubectl jq docker; do
  command -v "$tool" >/dev/null || { echo "missing $tool" >&2; exit 2; }
done
dir="${CCP_RELIST_DIR:?}"
ns="${CCP_RELIST_NS:?}"
pg_port="${CCP_RELIST_PG_PORT:?}"
pg_name="${CCP_RELIST_PG_NAME:?}"
api_host="${CCP_RELIST_API_HOST:?}"
api_port="${CCP_RELIST_API_PORT:?}"
binding_role_dsn="postgres://ccp_kueue_job_binding:kueue-binding-ci-only@127.0.0.1:${pg_port}/postgres?sslmode=disable"
journal_dsn="postgres://postgres:compute-ci@127.0.0.1:${pg_port}/postgres?sslmode=disable"
[[ "$binding_role_dsn" != "$journal_dsn" ]] || { echo "relist journal login must not be the binding login" >&2; exit 1; }
smoke="$dir/kueue-provider-smoke"
[[ -x "$smoke" ]] || { echo "missing kueue-provider-smoke" >&2; exit 1; }

run_relist() {
  local outfile="$1"
  shift
  set +e
  CCP_DISPOSABLE_KIND_SMOKE=1 \
    POD_NAMESPACE="$ns" \
    KUBERNETES_SERVICE_HOST="$api_host" \
    KUBERNETES_SERVICE_PORT_HTTPS="$api_port" \
    CCP_KUEUE_SA_CA="$dir/kube-ca.crt" \
    CCP_KUEUE_SA_TOKEN="$dir/sa.token" \
    CCP_KUEUE_BINDING_DSN="$binding_role_dsn" \
    CCP_KUEUE_JOURNAL_DSN="$journal_dsn" \
    CCP_KUEUE_RELIST_PRIOR="${CCP_KUEUE_RELIST_PRIOR:-}" \
    CCP_KUEUE_RELIST_RESTART="${CCP_KUEUE_RELIST_RESTART:-}" \
    "$smoke" "$@" >"$outfile" 2>"$outfile.err"
  local code=$?
  set -e
  return "$code"
}

sigkill_relist() {
  local outfile="$1"
  shift
  CCP_DISPOSABLE_KIND_SMOKE=1 \
    POD_NAMESPACE="$ns" \
    KUBERNETES_SERVICE_HOST="$api_host" \
    KUBERNETES_SERVICE_PORT_HTTPS="$api_port" \
    CCP_KUEUE_SA_CA="$dir/kube-ca.crt" \
    CCP_KUEUE_SA_TOKEN="$dir/sa.token" \
    CCP_KUEUE_BINDING_DSN="$binding_role_dsn" \
    CCP_KUEUE_JOURNAL_DSN="$journal_dsn" \
    "$smoke" "$@" >"$outfile" 2>"$outfile.err" &
  local pid=$!
  local ready=false
  for _ in $(seq 1 450); do
    if jq -e '(.tombstone_durable == true or (.phase == "successor" and .status == "UNKNOWN" and .target_uid != "")) and .target_uid != ""' "$outfile" >/dev/null 2>&1; then
      ready=true
      break
    fi
    if ! kill -0 "$pid" 2>/dev/null; then
      echo "relist process exited before the journaled delete was durable" >&2
      cat "$outfile.err" >&2 || true
      exit 1
    fi
    sleep 0.2
  done
  if [[ "$ready" != true ]]; then
    kill -9 "$pid" >/dev/null 2>&1 || true
    echo "timed out waiting for the relist delete journal" >&2
    cat "$outfile.err" >&2 || true
    exit 1
  fi
  kill -9 "$pid" 2>/dev/null || true
  set +e
  wait "$pid"
  local code=$?
  set -e
  [[ "$code" -eq 137 ]] || { echo "expected SIGKILL status 137, got $code" >&2; exit 1; }
}

workload_for_job() {
  kubectl -n "$ns" get workloads -o json | jq -c --arg id "$1" '
    [.items[] | select(any(.metadata.ownerReferences[]?; .kind == "Job" and .uid == $id))]
    | if length == 1 then .[0] else empty end'
}

echo "relist: record a queued Job observation, then stop CCP"
if ! run_relist "$dir/relist-record.json" relist record; then
  cat "$dir/relist-record.json.err" >&2 || true
  exit 1
fi
jq -e '
  .phase == "record" and (.projection == "NOT_READY" or .projection == "UNKNOWN") and
  .result == "RESOURCE_PRESENT" and .status == "UNKNOWN" and .pin_durable == true and
  (.workload_uid | type == "string" and length > 0) and
  (.workload_resource_version | type == "string" and length > 0) and
  (.job_uid | type == "string" and length > 0) and
  (.job_resource_version | type == "string" and length > 0) and
  .job_uid != .workload_uid and
  (.attempts | type == "number" and . >= 1) and
  (.read_sequence | type == "number" and . >= 1) and
  .allocated == false and .active == false and .ready == false and
  .costable == false and .released == false and .release_attested == false and
  .kept_stale == false and .operation_id == "ccp-kind-relist-create"' \
  "$dir/relist-record.json" >/dev/null
job_uid="$(jq -r '.job_uid' "$dir/relist-record.json")"
job_name="$(jq -r '.job_name' "$dir/relist-record.json")"
workload_uid="$(jq -r '.workload_uid' "$dir/relist-record.json")"
recorded_job_rv="$(jq -r '.job_resource_version' "$dir/relist-record.json")"
recorded_workload_rv="$(jq -r '.workload_resource_version' "$dir/relist-record.json")"
[[ -n "$job_uid" && -n "$job_name" && -n "$workload_uid" && -n "$recorded_job_rv" && -n "$recorded_workload_rv" ]] || {
  echo "relist record did not include job and workload identity" >&2
  exit 1
}
[[ "$(kubectl -n "$ns" get job "$job_name" -o jsonpath='{.metadata.uid}')" == "$job_uid" ]] || {
  echo "recorded job uid did not match the live job" >&2
  exit 1
}
pin_uid="$(docker exec -e PGPASSWORD=compute-ci "$pg_name" psql -U postgres -d postgres -tA -v ON_ERROR_STOP=1 -c \
  "SELECT job_uid FROM public.kueue_job_create_pins WHERE namespace='ccp-kueue-smoke' AND operation_id='ccp-kind-relist-create'")"
[[ "$pin_uid" == "$job_uid" ]] || { echo "restricted binding pin was not the recorded job" >&2; exit 1; }
[[ "$(docker exec -e PGPASSWORD=compute-ci "$pg_name" psql -U postgres -d postgres -tA -v ON_ERROR_STOP=1 -c \
  "SELECT rolsuper::text FROM pg_roles WHERE rolname='ccp_kueue_job_binding'")" == "false" ]] || {
  echo "ccp_kueue_job_binding is not the restricted login" >&2
  exit 1
}

echo "relist: CCP is stopped; wait for admission and pod placement"
admitted_for_job() {
  workload_for_job "$job_uid" | jq -e --arg uid "$workload_uid" '
    .metadata.uid == $uid and
    any(.status.conditions[]?; .type == "QuotaReserved" and .status == "True") and
    any(.status.conditions[]?; .type == "Admitted" and .status == "True") and
    .status.admission.clusterQueue == "ccp-smoke"' >/dev/null
}
pod_placed_for_job() {
  kubectl -n "$ns" get pods -l "batch.kubernetes.io/controller-uid=$job_uid" -o json | jq -e '
    [.items[] | select((.spec.nodeName // "") != "" and
      any(.status.conditions[]?; .type == "PodScheduled" and .status == "True"))] | length == 1' >/dev/null
}
for _ in $(seq 1 90); do
  if admitted_for_job && pod_placed_for_job; then
    break
  fi
  sleep 2
done
admitted_for_job || { echo "job was not admitted while CCP was stopped" >&2; exit 1; }
pod_placed_for_job || { echo "pod was not scheduled while CCP was stopped" >&2; exit 1; }
kubectl -n "$ns" annotate job "$job_name" ccp.example/relist-watch=dropped --overwrite >/dev/null
live_job_rv="$(kubectl -n "$ns" get job "$job_name" -o jsonpath='{.metadata.resourceVersion}')"
[[ -n "$live_job_rv" && "$live_job_rv" != "$recorded_job_rv" ]] || {
  echo "job resourceVersion did not change while CCP was stopped" >&2
  exit 1
}

echo "relist: restart the Kueue controller and re-list during the window"
kubectl rollout restart -n kueue-system deploy/kueue-controller-manager >/dev/null
CCP_KUEUE_RELIST_RESTART=1
if ! run_relist "$dir/relist-window.json" relist window; then
  cat "$dir/relist-window.json.err" >&2 || true
  exit 1
fi
unset CCP_KUEUE_RELIST_RESTART
jq -e --arg uid "$job_uid" '
  .phase == "window" and .controller_restart == true and .job_uid == $uid and
  (.projection == "NOT_READY" or .projection == "UNKNOWN") and
  (.attempts | type == "number" and . >= 1 and . <= 20) and
  .allocated == false and .active == false and .ready == false and
  .costable == false and .released == false and .release_attested == false' \
  "$dir/relist-window.json" >/dev/null
kubectl rollout status -n kueue-system deploy/kueue-controller-manager --timeout=180s
kubectl wait -n kueue-system deploy/kueue-controller-manager --for=condition=Available --timeout=180s

echo "relist: fresh process converges from the API re-list"
stable_job_rv=""
stable_workload_rv=""
for _ in $(seq 1 15); do
  now_job_rv="$(kubectl -n "$ns" get job "$job_name" -o jsonpath='{.metadata.resourceVersion}')"
  now_workload_rv="$(workload_for_job "$job_uid" | jq -r '.metadata.resourceVersion')"
  if [[ -n "$stable_job_rv" && "$now_job_rv" == "$stable_job_rv" && "$now_workload_rv" == "$stable_workload_rv" && -n "$now_workload_rv" ]]; then
    break
  fi
  stable_job_rv="$now_job_rv"
  stable_workload_rv="$now_workload_rv"
  sleep 2
done
[[ -n "$stable_job_rv" && "$stable_job_rv" != "$recorded_job_rv" && -n "$stable_workload_rv" ]] || {
  echo "live identity was not ready to compare with the recorded observation" >&2
  exit 1
}
export CCP_KUEUE_RELIST_PRIOR="$dir/relist-record.json"
if ! run_relist "$dir/relist-converge.json" relist converge; then
  cat "$dir/relist-converge.json.err" >&2 || true
  exit 1
fi
unset CCP_KUEUE_RELIST_PRIOR
recorded_read="$(jq -r '.read_sequence' "$dir/relist-record.json")"
jq -e --arg uid "$job_uid" --arg workload "$workload_uid" --arg prior_job "$recorded_job_rv" --arg prior_workload "$recorded_workload_rv" --argjson prior_read "$recorded_read" '
  .phase == "converge" and .projection == "NOT_READY" and .status == "UNKNOWN" and
  .job_uid == $uid and .workload_uid == $workload and
  .prior_job_resource_version == $prior_job and .prior_workload_resource_version == $prior_workload and
  .job_resource_version != .prior_job_resource_version and
  .prior_read_sequence == $prior_read and .read_sequence > .prior_read_sequence and
  .stale_rejected == true and .cross_rejected == true and .deduped == true and
  .kept_stale == false and .pin_durable == true and
  (.attempts | type == "number" and . >= 1 and . <= 20) and
  .allocated == false and .active == false and .ready == false and
  .costable == false and .released == false and .release_attested == false and
  (.job_resource_version | type == "string" and length > 0) and
  (.workload_resource_version | type == "string" and length > 0)' \
  "$dir/relist-converge.json" >/dev/null
converge_job_rv="$(jq -r '.job_resource_version' "$dir/relist-converge.json")"
converge_workload_rv="$(jq -r '.workload_resource_version' "$dir/relist-converge.json")"
live_job_rv="$(kubectl -n "$ns" get job "$job_name" -o jsonpath='{.metadata.resourceVersion}')"
live_workload_rv="$(workload_for_job "$job_uid" | jq -r '.metadata.resourceVersion')"
if [[ "$live_job_rv" != "$converge_job_rv" || "$live_workload_rv" != "$converge_workload_rv" ]]; then
  live_job_rv="$(kubectl -n "$ns" get job "$job_name" -o jsonpath='{.metadata.resourceVersion}')"
  live_workload_rv="$(workload_for_job "$job_uid" | jq -r '.metadata.resourceVersion')"
fi
[[ "$live_job_rv" == "$converge_job_rv" && "$live_workload_rv" == "$converge_workload_rv" ]] || {
  echo "converge output does not match a fresh API read" >&2
  exit 1
}
admitted_for_job || { echo "converged job is no longer admitted" >&2; exit 1; }
[[ "$(docker exec -e PGPASSWORD=compute-ci "$pg_name" psql -U postgres -d postgres -tA -v ON_ERROR_STOP=1 -c \
  "SELECT status || '|' || coalesce(provider_resource_uid,'') FROM public.compute_operations WHERE tenant_id='tenant-a' AND compute_operation_id='ccp-kind-relist-create'")" == "UNKNOWN|" ]] || {
  echo "fresh relist process replayed the create journal" >&2
  exit 1
}

echo "relist: journaled UID-fenced DELETE, then a fresh Inspect"
kubectl -n "$ns" get job "$job_name" -o json | jq '
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
    metadata: {name: .metadata.name, namespace: .metadata.namespace, annotations: .metadata.annotations, labels: .metadata.labels},
    spec: .spec
  }' > "$dir/relist-successor-template.json"
sigkill_relist "$dir/relist-delete.json" relist delete --hold
jq -e --arg uid "$job_uid" --arg name "$job_name" '
  .phase == "delete" and .status == "UNKNOWN" and .tombstone_durable == true and
  .acknowledged == true and .job_uid == $uid and .job_name == $name and
  .target_uid == $uid and .released == false and .ready == false and
  .operation_id == "ccp-kind-relist-delete"' "$dir/relist-delete.json" >/dev/null
job_uid_absent() {
  kubectl -n "$ns" get jobs -o json | jq -e --arg uid "$job_uid" 'all(.items[]; .metadata.uid != $uid)' >/dev/null
}
for _ in $(seq 1 90); do
  if job_uid_absent; then
    break
  fi
  sleep 2
done
job_uid_absent || { echo "deleted job uid is still present" >&2; exit 1; }
if ! run_relist "$dir/relist-recover-delete.json" relist recover-delete; then
  cat "$dir/relist-recover-delete.json.err" >&2 || true
  exit 1
fi
jq -e --arg uid "$job_uid" '
  .phase == "recover-delete" and .status == "SUCCEEDED" and .result == "TARGET_ABSENT" and
  .target_uid == $uid and .absence_observed == true and .job_uid == "" and
  .allocated == false and .active == false and .ready == false and
  .costable == false and .released == false and .release_attested == false' \
  "$dir/relist-recover-delete.json" >/dev/null

echo "relist: same-name successor stays in place"
kubectl apply -f "$dir/relist-successor-template.json" >/dev/null
successor_uid="$(kubectl -n "$ns" get job "$job_name" -o jsonpath='{.metadata.uid}')"
successor_created="$(kubectl -n "$ns" get job "$job_name" -o jsonpath='{.metadata.creationTimestamp}')"
[[ -n "$successor_uid" && "$successor_uid" != "$job_uid" && -n "$successor_created" ]] || {
  echo "relist successor was not a new uid" >&2
  exit 1
}
sigkill_relist "$dir/relist-successor.json" relist successor --hold
jq -e --arg uid "$job_uid" --arg name "$job_name" '
  .phase == "successor" and .status == "UNKNOWN" and .job_name == $name and
  .target_uid == $uid and .released == false and .ready == false' \
  "$dir/relist-successor.json" >/dev/null
if ! run_relist "$dir/relist-recover-successor.json" relist recover-successor; then
  cat "$dir/relist-recover-successor.json.err" >&2 || true
  exit 1
fi
jq -e --arg uid "$job_uid" '
  .phase == "recover-successor" and .status == "MANUAL_REVIEW" and
  .result == "CONFLICT" and .reason_code == "JOB_UID_REPLACED" and
  .target_uid == $uid and .job_uid == "" and
  .ready == false and .released == false and .release_attested == false' \
  "$dir/relist-recover-successor.json" >/dev/null
[[ "$(kubectl -n "$ns" get job "$job_name" -o jsonpath='{.metadata.uid}')" == "$successor_uid" &&
  "$(kubectl -n "$ns" get job "$job_name" -o jsonpath='{.metadata.creationTimestamp}')" == "$successor_created" &&
  -z "$(kubectl -n "$ns" get job "$job_name" -o jsonpath='{.metadata.deletionTimestamp}')" ]] || {
  echo "JOB_UID_REPLACED process deleted or replaced the relist successor" >&2
  exit 1
}
echo "relist_restart_job=$job_uid relist_workload=$workload_uid relist_successor=$successor_uid"
echo "PASS: relist converged from a full API re-list after a dropped watch and Kueue controller restart. TARGET_ABSENT is not Allocation RELEASED."
