#!/usr/bin/env bash
# Disposable kind/Kueue controller acceptance. Not GPU/DRA or production READY evidence.
set -Eeuo pipefail
if [[ "$CCP_DISPOSABLE_KIND_SMOKE" != "1" ]]; then
  echo "Explicit CCP_DISPOSABLE_KIND_SMOKE=1 required" >&2; exit 2
fi
for tool in kind kubectl jq go docker curl; do
  command -v "$tool" >/dev/null || { echo "missing $tool" >&2; exit 2; }
done

dir="$(mktemp -d)"
export KUBECONFIG="$dir/kubeconfig"
name="ccp-kueue-smoke"
ns="ccp-kueue-smoke"
job="ccp-test-job"
pg_name="ccp-kueue-smoke-pg-${dir##*/}"
pg_id_file="$dir/pg-container-id"
pg_port="54329"
pg_boot_pid=""
migrate_pid=""
image="kindest/node:v1.34.0@sha256:7416a61b42b1662ca6ca89f02028ac133a309a2a30ba309614e8ec94d976dc5a"
created=false
cleanup() {
  status=$?
  if [[ "$status" -ne 0 ]] && [[ "$created" == true ]]; then
    kubectl --request-timeout=10s -n "$ns" get jobs,workloads,pods -o wide || true
    kubectl --request-timeout=10s -n "$ns" logs ccp-provider-lifecycle --tail=50 || true
    kubectl --request-timeout=10s -n kueue-system logs deploy/kueue-controller-manager --tail=80 || true
  fi
  if [[ "$status" -ne 0 ]]; then
    for f in "$dir"/durable-*.json "$dir"/durable-*.err "$dir"/journal-*.json "$dir"/journal-*.err "$dir"/admission-*.json "$dir"/admission-*.err "$dir"/preempt-*.json "$dir"/preempt-*.err "$dir"/pg-boot.log "$dir"/migrate.log; do
      [[ -f "$f" ]] || continue
      echo "----- $(basename "$f") -----" >&2
      tail -n 40 "$f" >&2 || true
    done
    for f in "$dir"/relist-*.json "$dir"/relist-*.err; do
      [[ -f "$f" ]] || continue
      echo "----- $(basename "$f") -----" >&2
      tail -n 40 "$f" >&2 || true
    done
  fi
  if [[ -n "${migrate_pid:-}" ]]; then kill "$migrate_pid" >/dev/null 2>&1 || true; fi
  if [[ -n "${pg_boot_pid:-}" ]]; then kill "$pg_boot_pid" >/dev/null 2>&1 || true; fi
  # Only remove the container created by this invocation, never a foreign
  # container whose name happened to match the disposable smoke fixture.
  if [[ -s "$pg_id_file" ]]; then
    pg_id="$(< "$pg_id_file")"
    if [[ "$pg_id" =~ ^[a-f0-9]{64}$ ]]; then
      docker rm -f "$pg_id" >/dev/null 2>&1 || true
    fi
  fi
  if [[ "$created" == true ]]; then kind delete cluster --name "$name" || true; fi
  rm -rf "$dir"
  exit "$status"
}
trap cleanup EXIT

# Never reuse an existing cluster or a kubeconfig from the host.
if kind get clusters | grep -Fxq "$name"; then
  echo "existing disposable cluster; refusing to reuse" >&2; exit 2
fi
created=true
# Boot disposable Postgres while kind starts. The binding smoke uses it later.
start_disposable_postgres() {
  docker pull postgres:16-alpine
  # Docker must reject a collision; never remove an existing named container.
  docker run -d --name "$pg_name" \
    -e POSTGRES_PASSWORD=compute-ci \
    -e POSTGRES_USER=postgres \
    -e POSTGRES_DB=postgres \
    -p "127.0.0.1:${pg_port}:5432" \
    postgres:16-alpine > "$pg_id_file"
  for _ in $(seq 1 60); do
    if docker exec "$pg_name" pg_isready -U postgres -d postgres >/dev/null 2>&1; then
      return 0
    fi
    sleep 1
  done
  echo "disposable postgres did not become ready" >&2
  return 1
}
start_disposable_postgres >"$dir/pg-boot.log" 2>&1 &
pg_boot_pid=$!
kind create cluster --name "$name" --image "$image" --wait 180s
[[ "$(kubectl config current-context)" == "kind-$name" ]] || exit 2
printf 'source_sha=%s\n' "$(git rev-parse HEAD)"
kubectl version --client
kubectl get nodes -o wide

kueue_release="v0.19.6"
curl -fLSs --retry 3 --retry-delay 2 -o "$dir/kueue.yaml" \
  "https://github.com/kubernetes-sigs/kueue/releases/download/$kueue_release/manifests.yaml"
printf 'kueue_release=%s manifest_sha256=%s\n' "$kueue_release" "$(sha256sum "$dir/kueue.yaml" | cut -d' ' -f1)"
kubectl apply --server-side -f "$dir/kueue.yaml"
kubectl wait -n kueue-system deploy/kueue-controller-manager --for=condition=Available --timeout=300s
kubectl create namespace "$ns"

kubectl apply -f - <<YAML
apiVersion: v1
kind: ServiceAccount
metadata: {name: ccp-kueue-observer, namespace: $ns}
---
apiVersion: rbac.authorization.k8s.io/v1
kind: Role
metadata: {name: ccp-kueue-observer, namespace: $ns}
rules:
- apiGroups: [batch]
  resources: [jobs]
  verbs: [get]
- apiGroups: ['']
  resources: [pods]
  verbs: [list]
- apiGroups: [kueue.x-k8s.io]
  resources: [workloads]
  verbs: [list]
---
apiVersion: rbac.authorization.k8s.io/v1
kind: RoleBinding
metadata: {name: ccp-kueue-observer, namespace: $ns}
roleRef: {apiGroup: rbac.authorization.k8s.io, kind: Role, name: ccp-kueue-observer}
subjects:
- kind: ServiceAccount
  name: ccp-kueue-observer
  namespace: $ns
YAML
for verb in create update patch delete; do
  answer="$(kubectl auth can-i "$verb" jobs.batch --as="system:serviceaccount:$ns:ccp-kueue-observer" -n "$ns" || true)"
  [[ "$answer" == no ]] || { echo "observer has unauthorized $verb permission" >&2; exit 1; }
done

# The writer is a separate identity; the read-only observer Role stays GET/LIST.
kubectl apply -f - <<YAML
apiVersion: v1
kind: ServiceAccount
metadata: {name: ccp-kueue-lifecycle, namespace: $ns}
---
apiVersion: rbac.authorization.k8s.io/v1
kind: Role
metadata: {name: ccp-kueue-lifecycle, namespace: $ns}
rules:
- apiGroups: [batch]
  resources: [jobs]
  verbs: [create, get, delete]
- apiGroups: ['']
  resources: [pods]
  verbs: [list]
- apiGroups: [kueue.x-k8s.io]
  resources: [workloads]
  verbs: [list]
---
apiVersion: rbac.authorization.k8s.io/v1
kind: RoleBinding
metadata: {name: ccp-kueue-lifecycle, namespace: $ns}
roleRef: {apiGroup: rbac.authorization.k8s.io, kind: Role, name: ccp-kueue-lifecycle}
subjects:
- kind: ServiceAccount
  name: ccp-kueue-lifecycle
  namespace: $ns
YAML
for verb in create get delete; do
  answer="$(kubectl auth can-i "$verb" jobs.batch --as="system:serviceaccount:$ns:ccp-kueue-lifecycle" -n "$ns" || true)"
  [[ "$answer" == yes ]] || { echo "writer missing required $verb permission" >&2; exit 1; }
done
for verb in update patch; do
  answer="$(kubectl auth can-i "$verb" jobs.batch --as="system:serviceaccount:$ns:ccp-kueue-lifecycle" -n "$ns" || true)"
  [[ "$answer" == no ]] || { echo "writer has forbidden $verb permission" >&2; exit 1; }
done

# Zero CPU/memory quota gives a real unadmitted Kueue Workload.
# Keep this manifest stable so transient webhook startup failures can be retried.
cat > "$dir/kueue-queues.yaml" <<YAML
apiVersion: kueue.x-k8s.io/v1beta1
kind: ResourceFlavor
metadata: {name: ccp-smoke}
---
apiVersion: kueue.x-k8s.io/v1beta1
kind: ClusterQueue
metadata: {name: ccp-smoke}
spec:
  namespaceSelector: {}
  resourceGroups:
  - coveredResources: [cpu, memory]
    flavors:
    - name: ccp-smoke
      resources:
      - {name: cpu, nominalQuota: '0'}
      - {name: memory, nominalQuota: '0'}
---
apiVersion: kueue.x-k8s.io/v1beta1
kind: LocalQueue
metadata: {name: ccp-smoke, namespace: $ns}
spec: {clusterQueue: ccp-smoke}
YAML

# Deployment Available does not guarantee the webhook listener is accepting
# admission/conversion calls yet. Retry only transient webhook transport errors,
# never swallow invalid schema, permission failures or a deadline expiration.
queue_ready=false
for attempt in $(seq 1 30); do
  if kubectl apply -f "$dir/kueue-queues.yaml" >"$dir/kueue-queues.out" 2>"$dir/kueue-queues.err"; then
    cat "$dir/kueue-queues.out"
    queue_ready=true
    break
  fi
  cat "$dir/kueue-queues.err" >&2
  if ! grep -Eq 'failed calling webhook|conversion webhook|connect: connection refused|no endpoints available' "$dir/kueue-queues.err"; then
    echo "non-retryable Kueue queue apply failure" >&2
    exit 1
  fi
  echo "Kueue webhook not ready; retry queue apply $attempt/30" >&2
  sleep 2
done
[[ "$queue_ready" == true ]] || { echo "Kueue webhook never became ready" >&2; exit 1; }
kubectl get resourceflavor ccp-smoke -o name
kubectl get clusterqueue ccp-smoke -o name
kubectl -n "$ns" get localqueue ccp-smoke -o name

CGO_ENABLED=0 go build -trimpath -o "$dir/kueue-observe" ./cmd/kueue-observe
CGO_ENABLED=0 go build -trimpath -o "$dir/kueue-provider-smoke" ./cmd/kueue-provider-smoke
set +e
wait "$pg_boot_pid"
pg_boot_code=$?
set -e
pg_boot_pid=""
[[ "$pg_boot_code" -eq 0 ]] || { echo "disposable postgres failed to start" >&2; cat "$dir/pg-boot.log" >&2; exit 1; }
{
  docker exec -e PGPASSWORD=compute-ci "$pg_name" \
    psql -U postgres -d postgres -v ON_ERROR_STOP=1 \
    -c "CREATE ROLE ccp_provider_audit_owner NOLOGIN NOINHERIT NOSUPERUSER NOCREATEDB NOCREATEROLE NOREPLICATION NOBYPASSRLS" \
    -c "CREATE ROLE ccp_kueue_job_binding LOGIN PASSWORD 'kueue-binding-ci-only' NOINHERIT NOSUPERUSER NOCREATEDB NOCREATEROLE NOREPLICATION NOBYPASSRLS" \
    -c "CREATE ROLE ccp_dra_receipt_ingest LOGIN PASSWORD 'dra-ingest-ci-only' NOINHERIT NOSUPERUSER NOCREATEDB NOCREATEROLE NOREPLICATION NOBYPASSRLS"
  DATABASE_URL="postgres://postgres:compute-ci@127.0.0.1:${pg_port}/postgres?sslmode=disable" \
    go run ./cmd/migrate
} >"$dir/migrate.log" 2>&1 &
migrate_pid=$!
cat > "$dir/Dockerfile" <<'DOCKER'
FROM scratch
COPY kueue-observe /kueue-observe
COPY kueue-provider-smoke /kueue-provider-smoke
USER 65532:65532
ENTRYPOINT ["/kueue-observe"]
DOCKER
docker build --quiet -t ccp-kueue-observe:smoke "$dir"
kind load docker-image ccp-kueue-observe:smoke --name "$name"

create_job() {
  kubectl apply -f - <<YAML
apiVersion: batch/v1
kind: Job
metadata:
  name: $job
  namespace: $ns
  annotations:
    compute-control-panel.io/tenant-id: tenant-a
    compute-control-panel.io/workload-id: workload-a
    compute-control-panel.io/desired-generation: '7'
  labels:
    kueue.x-k8s.io/queue-name: ccp-smoke
spec:
  parallelism: 1
  completions: 1
  template:
    spec:
      restartPolicy: Never
      nodeSelector: {ccp-kueue-smoke/eligible: 'true'}
      containers:
      - name: hold
        image: busybox:1.36
        command: [sh, -c, 'sleep 600']
        resources:
          requests: {cpu: 100m, memory: 32Mi}
YAML
}

wait_for() {
  description="$1"; shift
  for _ in $(seq 1 90); do if "$@"; then return 0; fi; sleep 2; done
  echo "timed out: $description" >&2
  return 1
}
job_uid() { kubectl -n "$ns" get job "$job" -o jsonpath='{.metadata.uid}'; }
pods_json() { kubectl -n "$ns" get pods -l "batch.kubernetes.io/controller-uid=$1" -o json; }
workloads_json() { kubectl -n "$ns" get workloads -o json; }
observer_succeeded() { [[ "$(kubectl -n "$ns" get pod "ccp-observe-$1" -o jsonpath='{.status.phase}')" == Succeeded ]]; }
observer_failed() { [[ "$(kubectl -n "$ns" get pod "ccp-observe-$1" -o jsonpath='{.status.phase}')" == Failed ]]; }
old_job_absent() {
  kubectl -n "$ns" get jobs -o json | jq -e --arg uid "$uid" 'all(.items[]; .metadata.uid != $uid)' >/dev/null
}
old_pod_absent() {
  kubectl -n "$ns" get pods -o json | jq -e --arg uid "$old_pod_uid" 'all(.items[]; .metadata.uid != $uid)' >/dev/null
}
old_workload_absent() {
  kubectl -n "$ns" get workloads -o json | jq -e --arg uid "$old_workload_uid" 'all(.items[]; .metadata.uid != $uid)' >/dev/null
}
quota_released() {
  kubectl get clusterqueue ccp-smoke -o json | jq -e '(.status.admittedWorkloads // 0) == 0 and (.status.reservingWorkloads // 0) == 0' >/dev/null
}
workload_for_uid() {
  workloads_json | jq -e --arg id "$1" '[.items[] | select(any(.metadata.ownerReferences[]?; .kind == "Job" and .uid == $id))] | length == 1' >/dev/null
}
workload_unreserved() {
  # Quota reservation and admission are distinct Kueue facts. No default-to-
  # true when a condition is absent or the owner UID does not match.
  workloads_json | jq -e --arg id "$1" '
    [.items[] | select(any(.metadata.ownerReferences[]?; .kind == "Job" and .uid == $id))
      | select((.metadata.uid | type) == "string" and (.metadata.uid | length) > 0
        and (.metadata.resourceVersion | type) == "string" and (.metadata.resourceVersion | length) > 0
        and .status.admission == null
        and any(.status.conditions[]?; .type == "QuotaReserved" and .status == "False")
        and (any(.status.conditions[]?; .type == "Admitted" and .status == "True") | not))] | length == 1' >/dev/null
}
workload_admitted() {
  workloads_json | jq -e --arg id "$1" '
    [.items[] | select(any(.metadata.ownerReferences[]?; .kind == "Job" and .uid == $id))
      | select((.metadata.uid | type) == "string" and (.metadata.uid | length) > 0
        and (.metadata.resourceVersion | type) == "string" and (.metadata.resourceVersion | length) > 0
        and .status.admission.clusterQueue == "ccp-smoke"
        and (.status.admission.podSetAssignments | type) == "array"
        and (.status.admission.podSetAssignments | length) > 0
        and any(.status.admission.podSetAssignments[]; (.resourceUsage | type) == "object" and (.resourceUsage | length) > 0)
        and any(.status.conditions[]?; .type == "QuotaReserved" and .status == "True")
        and any(.status.conditions[]?; .type == "Admitted" and .status == "True"))] | length == 1' >/dev/null
}
quota_reserved_in_clusterqueue() {
  kubectl get clusterqueue ccp-smoke -o json | jq -e '
    .status.admittedWorkloads == 1 and .status.reservingWorkloads == 1' >/dev/null
}
pod_scheduled() {
  pods_json "$1" | jq -e --arg node "$node" '
    [.items[] | select((.metadata.uid | type) == "string" and (.metadata.uid | length) > 0
      and (.metadata.resourceVersion | type) == "string" and (.metadata.resourceVersion | length) > 0
      and .spec.nodeName == $node
      and any(.status.conditions[]?; .type == "PodScheduled" and .status == "True"))] | length == 1' >/dev/null
}

observer() {
  tag="$1" target_uid="$2" expected="$3"
  kubectl apply -f - <<YAML
apiVersion: v1
kind: Pod
metadata: {name: ccp-observe-$tag, namespace: $ns}
spec:
  serviceAccountName: ccp-kueue-observer
  restartPolicy: Never
  containers:
  - name: observer
    image: ccp-kueue-observe:smoke
    imagePullPolicy: Never
    args: ['--tenant', tenant-a, '--workload', workload-a, '--generation', '7', '--namespace', '$ns', '--job', '$job', '--uid', '$target_uid']
    securityContext:
      allowPrivilegeEscalation: false
      capabilities: {drop: [ALL]}
      runAsNonRoot: true
      readOnlyRootFilesystem: true
YAML
  wait_for "observer $tag terminates" observer_succeeded "$tag"
  kubectl -n "$ns" logs "ccp-observe-$tag" > "$dir/$tag.json"
  jq -e --arg stage "$expected" --arg uid "$target_uid" --arg ns "$ns" \
    '.Stage == $stage and .Evidence.Job.UID == $uid and .Evidence.Job.Namespace == $ns and .Provenance.Source == "kubernetes-https-readonly-unattested"' "$dir/$tag.json" >/dev/null
  echo "observer_$tag=$(jq -c '{Stage:.Stage, JobUID:.Evidence.Job.UID, JobRV:.Evidence.Job.ResourceVersion, KueueUID:.Evidence.KueueWorkload.UID, KueueRV:.Evidence.KueueWorkload.ResourceVersion, PodUID:.Evidence.Pod.UID, PodRV:.Evidence.Pod.ResourceVersion}' "$dir/$tag.json")"
}

create_job
uid="$(job_uid)"
wait_for 'Kueue Workload created for Job UID' workload_for_uid "$uid"
wait_for 'Kueue explicitly denies quota reservation at zero quota' workload_unreserved "$uid"
if workload_admitted "$uid"; then echo "zero quota incorrectly admitted" >&2; exit 1; fi
[[ "$(pods_json "$uid" | jq '.items | length')" == 0 ]] || { echo "Pod created before admission" >&2; exit 1; }
echo "kueue_unreserved_evidence=$(workloads_json | jq -c --arg id "$uid" '.items[] | select(any(.metadata.ownerReferences[]?; .kind == "Job" and .uid == $id)) | {uid:.metadata.uid,rv:.metadata.resourceVersion,conditions:.status.conditions,admission:.status.admission}')"
observer queued "$uid" UNPROVEN

# Enable admission while no node matches, to prove admission != placement.
kubectl patch clusterqueue ccp-smoke --type=json -p='[{"op":"replace","path":"/spec/resourceGroups/0/flavors/0/resources/0/nominalQuota","value":"2"},{"op":"replace","path":"/spec/resourceGroups/0/flavors/0/resources/1/nominalQuota","value":"2Gi"}]'
wait_for 'Kueue reserved quota and admitted Job' workload_admitted "$uid"
wait_for 'ClusterQueue tracks exact quota reservation' quota_reserved_in_clusterqueue

# Actual disposable Kueue API object identities: not Provider, GPU or DRA
# evidence. Join by the original Job UID; never infer queue ownership by name
# alone or treat an empty UID/RV as a verified reservation.
wl_queue_json="$(workloads_json | jq -ce --arg id "$uid" '
  [.items[] | select(any(.metadata.ownerReferences[]?; .kind == "Job" and .uid == $id))]
  | if length == 1 then .[0] else empty end')"
lq_queue_json="$(kubectl -n "$ns" get localqueue ccp-smoke -o json)"
cq_queue_json="$(kubectl get clusterqueue ccp-smoke -o json)"
jq -nce --argjson w "$wl_queue_json" --argjson l "$lq_queue_json" --argjson c "$cq_queue_json" '
  if (
    $w.spec.queueName == "ccp-smoke"
    and $w.metadata.namespace == $l.metadata.namespace
    and $l.metadata.name == "ccp-smoke"
    and $l.spec.clusterQueue == $c.metadata.name
    and $c.metadata.name == "ccp-smoke"
    and $w.status.admission.clusterQueue == $c.metadata.name
    and ([ $w.metadata.uid, $w.metadata.resourceVersion,
           $l.metadata.uid, $l.metadata.resourceVersion,
           $c.metadata.uid, $c.metadata.resourceVersion ]
         | all(.[]; type == "string" and length > 0))
  ) then {
    workload: {uid:$w.metadata.uid, rv:$w.metadata.resourceVersion, queueName:$w.spec.queueName},
    localQueue: {uid:$l.metadata.uid, rv:$l.metadata.resourceVersion, namespace:$l.metadata.namespace, name:$l.metadata.name, clusterQueue:$l.spec.clusterQueue},
    clusterQueue: {uid:$c.metadata.uid, rv:$c.metadata.resourceVersion, name:$c.metadata.name},
    podSetAssignments: $w.status.admission.podSetAssignments
  } else empty end' > "$dir/kueue-queue-binding.json" || {
  echo "Kueue queue UID / ownership chain could not be proven" >&2; exit 1
}
echo "kueue_queue_binding_evidence=$(cat "$dir/kueue-queue-binding.json")"
if [[ "$(pods_json "$uid" | jq '[.items[] | select((.spec.nodeName // "") != "")] | length')" != 0 ]]; then
  echo "Pod bound before node opt-in" >&2; exit 1
fi
echo "kueue_reserved_evidence=$(workloads_json | jq -c --arg id "$uid" '.items[] | select(any(.metadata.ownerReferences[]?; .kind == "Job" and .uid == $id)) | {uid:.metadata.uid,rv:.metadata.resourceVersion,conditions:.status.conditions,admission:.status.admission}')"
echo "clusterqueue_reserved_evidence=$(kubectl get clusterqueue ccp-smoke -o json | jq -c '{uid:.metadata.uid,rv:.metadata.resourceVersion,reserving:.status.reservingWorkloads,admitted:.status.admittedWorkloads}')"
observer admitted "$uid" KUEUE_ADMITTED

node="$(kubectl get nodes -o jsonpath='{.items[0].metadata.name}')"
kubectl label node "$node" ccp-kueue-smoke/eligible=true --overwrite
wait_for 'Pod placed on exact node' pod_scheduled "$uid"
echo "pod_binding_evidence=$(pods_json "$uid" | jq -c --arg node "$node" '.items[] | select(.spec.nodeName == $node) | {uid:.metadata.uid,rv:.metadata.resourceVersion,node:.spec.nodeName,conditions:.status.conditions}')"
observer placed "$uid" POD_PLACED
old_pod_uid="$(pods_json "$uid" | jq -r '.items[0].metadata.uid')"
old_workload_uid="$(workloads_json | jq -r --arg id "$uid" '.items[] | select(any(.metadata.ownerReferences[]?; .kind == "Job" and .uid == $id)) | .metadata.uid')"

# Only disposable test teardown: NOT an Allocation RELEASED proof.
kubectl -n "$ns" delete job "$job" --cascade=background --wait=true
wait_for 'old Job UID absent' old_job_absent
wait_for 'old Pod UID absent' old_pod_absent
wait_for 'old Kueue Workload UID absent' old_workload_absent
wait_for 'queue reservation released' quota_released
echo "kueue_release_observation=$(kubectl get clusterqueue ccp-smoke -o json | jq -c --arg job "$uid" --arg pod "$old_pod_uid" --arg workload "$old_workload_uid" '{oldJobUID:$job,oldPodUID:$pod,oldKueueUID:$workload,clusterqueueUID:.metadata.uid,clusterqueueRV:.metadata.resourceVersion,reserving:.status.reservingWorkloads,admitted:.status.admittedWorkloads}')"


create_job
new_uid="$(job_uid)"
[[ "$new_uid" != "$uid" ]] || { echo "recreated Job retained old UID" >&2; exit 1; }
kubectl apply -f - <<YAML
apiVersion: v1
kind: Pod
metadata: {name: ccp-observe-stale, namespace: $ns}
spec:
  serviceAccountName: ccp-kueue-observer
  restartPolicy: Never
  containers:
  - name: observer
    image: ccp-kueue-observe:smoke
    imagePullPolicy: Never
    args: ['--tenant', tenant-a, '--workload', workload-a, '--generation', '7', '--namespace', '$ns', '--job', '$job', '--uid', '$uid']
YAML
wait_for 'successor Job refused for stale UID' observer_failed stale
stale_logs="$(kubectl -n "$ns" logs ccp-observe-stale)"
[[ "$stale_logs" == *"kueue-observe: read-only observation failed:"* ]] || {
  echo "stale UID failed without an observable refusal reason" >&2; exit 1;
}
if printf '%s\n' "$stale_logs" | jq -e '.Stage or .Evidence' >/dev/null 2>&1; then
  echo "stale UID emitted successful observation JSON" >&2; exit 1
fi
kubectl -n "$ns" delete job "$job" --cascade=background --wait=true

# This extra smoke invokes CCP's actual provider.Adapter CREATE/Inspect/DELETE
# against the same real API. Its binding store is process-local memory.
# Separate-process Postgres recovery follows below and is not this Pod.
kubectl apply -f - <<YAML
apiVersion: v1
kind: Pod
metadata: {name: ccp-provider-lifecycle, namespace: $ns}
spec:
  serviceAccountName: ccp-kueue-lifecycle
  restartPolicy: Never
  containers:
  - name: lifecycle
    image: ccp-kueue-observe:smoke
    imagePullPolicy: Never
    command: ["/kueue-provider-smoke"]
    env:
    - name: CCP_DISPOSABLE_KIND_SMOKE
      value: '1'
    - name: POD_NAMESPACE
      valueFrom:
        fieldRef: {fieldPath: metadata.namespace}
    securityContext:
      allowPrivilegeEscalation: false
      capabilities: {drop: [ALL]}
      runAsNonRoot: true
      readOnlyRootFilesystem: true
YAML
# A failed lifecycle Pod is a hard failure, not a three-minute timeout.
lifecycle_phase=''
for _ in $(seq 1 60); do
  lifecycle_phase="$(kubectl -n "$ns" get pod ccp-provider-lifecycle -o jsonpath='{.status.phase}' 2>/dev/null || true)"
  [[ "$lifecycle_phase" == Succeeded ]] && break
  if [[ "$lifecycle_phase" == Failed ]]; then
    kubectl -n "$ns" logs ccp-provider-lifecycle >&2 || true
    echo "provider lifecycle Pod failed" >&2
    exit 1
  fi
  sleep 2
done
[[ "$lifecycle_phase" == Succeeded ]] || { echo "timed out: provider lifecycle Pod" >&2; exit 1; }
kubectl -n "$ns" logs ccp-provider-lifecycle > "$dir/lifecycle.json"
jq -e '.created_uid != "" and
  .created_result == "RESOURCE_PRESENT" and
  .after_adapter_reinit == "OPERATION_IN_PROGRESS" and
  .bound_adapter_reinit_create == "RESOURCE_PRESENT" and
  .rejected_wrong_uid == true and
  .deleted_result == "TARGET_ABSENT" and
  .after_delete_reinit == "CONFLICT" and
  .bound_adapter_reinit_delete == "TARGET_ABSENT" and
  .release_attested == false' "$dir/lifecycle.json" >/dev/null
kubectl -n "$ns" get jobs -o json | jq -e '
  all(.items[]; .metadata.annotations["compute-control-panel.io/operation-id"] != "ccp-kind-lifecycle-create")' >/dev/null
echo "provider_lifecycle=$(jq -c '.' "$dir/lifecycle.json")"

# The previous queued Job was created with kubectl. Verify that CCP's actual
# HTTPS/ServiceAccount writer can create a Kueue-managed Job too (no direct
# Workload CR writes, no manual unsuspend, no GPU device attestations).
kubectl apply -f - <<YAML
apiVersion: v1
kind: Pod
metadata: {name: ccp-queued-submit, namespace: $ns}
spec:
  serviceAccountName: ccp-kueue-lifecycle
  restartPolicy: Never
  containers:
  - name: submit
    image: ccp-kueue-observe:smoke
    imagePullPolicy: Never
    command: ["/kueue-provider-smoke", "queued"]
    env:
    - name: CCP_DISPOSABLE_KIND_SMOKE
      value: '1'
    - name: POD_NAMESPACE
      valueFrom:
        fieldRef: {fieldPath: metadata.namespace}
    securityContext:
      allowPrivilegeEscalation: false
      capabilities: {drop: [ALL]}
      runAsNonRoot: true
      readOnlyRootFilesystem: true
YAML
queued_writer_completed() {
  phase="$(kubectl -n "$ns" get pod ccp-queued-submit -o jsonpath='{.status.phase}')"
  if [[ "$phase" == Failed ]]; then
    kubectl -n "$ns" logs ccp-queued-submit >&2 || true
    kubectl -n "$ns" describe pod ccp-queued-submit >&2 || true
    echo "queued writer Pod failed" >&2
    return 2
  fi
  [[ "$phase" == Succeeded ]]
}
# A FAILED Pod is terminal, not a three-minute Succeeded timeout.
for _ in $(seq 1 90); do
  if queued_writer_completed; then break; fi
  if [[ "$phase" == Failed ]]; then exit 1; fi
  sleep 2
done
[[ "$phase" == Succeeded ]] || { echo "timed out: queued writer Pod" >&2; exit 1; }
kubectl -n "$ns" logs ccp-queued-submit > "$dir/queued.json"
jq -e '.resource_id != "" and .resource_uid != "" and .attested_gpu == false and .released == false' "$dir/queued.json" >/dev/null
queued_ref="$(jq -r .resource_id "$dir/queued.json")"
[[ "$queued_ref" == "$ns/"* ]] || { echo "queued writer escaped namespace" >&2; exit 1; }
queued_job="${queued_ref#*/}"
queued_uid="$(jq -r .resource_uid "$dir/queued.json")"
kubectl -n "$ns" get job "$queued_job" -o json | jq -e --arg uid "$queued_uid" \
  '.metadata.uid == $uid and .metadata.labels["kueue.x-k8s.io/queue-name"] == "ccp-smoke" and
   .spec.template.spec.containers[0].resources.requests.cpu == "100m" and
   .spec.template.spec.containers[0].resources.requests.memory == "32Mi"' >/dev/null
wait_for 'queued CCP Job creates owner-linked Kueue Workload' workload_for_uid "$queued_uid"
wait_for 'queued CCP Job is really admitted by Kueue' workload_admitted "$queued_uid"
wait_for 'queued CCP Job is unsuspended by Kueue' bash -c \
  '[[ "$(kubectl -n "$1" get job "$2" -o jsonpath="{.spec.suspend}")" == false ]]' _ "$ns" "$queued_job"
wait_for 'queued CCP Job Pod placed by scheduler' pod_scheduled "$queued_uid"
queued_pod_uid="$(pods_json "$queued_uid" | jq -er '.items[0].metadata.uid')"
queued_workload_uid="$(workloads_json | jq -er --arg id "$queued_uid" \
  '.items[] | select(any(.metadata.ownerReferences[]?; .kind == "Job" and .uid == $id)) | .metadata.uid')"
echo "ccp_queued_admission_placement=$(jq -cn --arg job "$queued_uid" --arg pod "$queued_pod_uid" --arg workload "$queued_workload_uid" \
  '{jobUID:$job,podUID:$pod,workloadUID:$workload,deviceAssignment:"UNPROVEN",release:"UNPROVEN"}')"
kubectl -n "$ns" delete job "$queued_job" --cascade=background --wait=true
queued_job_absent() {
  kubectl -n "$ns" get jobs -o json | jq -e --arg uid "$queued_uid" 'all(.items[]; .metadata.uid != $uid)' >/dev/null
}
wait_for 'queued Job original UID absent' queued_job_absent
queued_pod_absent() {
  kubectl -n "$ns" get pods -o json | jq -e --arg uid "$queued_pod_uid" 'all(.items[]; .metadata.uid != $uid)' >/dev/null
}
queued_workload_absent() {
  kubectl -n "$ns" get workloads -o json | jq -e --arg uid "$queued_workload_uid" 'all(.items[]; .metadata.uid != $uid)' >/dev/null
}
queued_quota_clear() {
  kubectl get clusterqueue ccp-smoke -o json | jq -e '.status.reservingWorkloads == 0 and .status.admittedWorkloads == 0' >/dev/null
}
wait_for 'queued Pod original UID absent' queued_pod_absent
wait_for 'queued Kueue Workload original UID absent' queued_workload_absent
wait_for 'queued Kueue reservation counters explicitly zero' queued_quota_clear
echo "ccp_queued_release_observation=$(kubectl get clusterqueue ccp-smoke -o json | jq -c --arg job "$queued_uid" --arg pod "$queued_pod_uid" --arg workload "$queued_workload_uid" '{oldJobUID:$job,oldPodUID:$pod,oldWorkloadUID:$workload,clusterQueueUID:.metadata.uid,clusterQueueRV:.metadata.resourceVersion,reserving:.status.reservingWorkloads,admitted:.status.admittedWorkloads,deviceRelease:"UNPROVEN",allocationReleased:false}')"

# Separate OS processes, one disposable Postgres. SIGKILL after the pin or
# tombstone is durable. This is not Allocation RELEASED or quota release.
set +e
wait "$migrate_pid"
migrate_code=$?
set -e
migrate_pid=""
[[ "$migrate_code" -eq 0 ]] || { echo "disposable migration failed" >&2; cat "$dir/migrate.log" >&2; exit 1; }
server="$(kubectl config view --minify --raw -o jsonpath='{.clusters[0].cluster.server}')"
server="${server#https://}"
server="${server%/}"
api_host="${server%%:*}"
api_port="${server##*:}"
[[ -n "$api_host" && "$api_port" != "$server" && "$api_port" =~ ^[0-9]+$ ]] || {
  echo "unexpected kind API server URL" >&2
  exit 1
}
kubectl config view --minify --raw -o jsonpath='{.clusters[0].cluster.certificate-authority-data}' | base64 -d > "$dir/kube-ca.crt"
kubectl create token ccp-kueue-lifecycle -n "$ns" --duration=2h > "$dir/sa.token"
[[ -s "$dir/kube-ca.crt" && -s "$dir/sa.token" ]] || { echo "missing ServiceAccount material" >&2; exit 1; }
# Same CA and API address kubectl uses. Fail here if the runner cannot verify that cert.
curl --cacert "$dir/kube-ca.crt" --max-time 15 -sS -o "$dir/api-version.json" \
  "https://${api_host}:${api_port}/version" || { echo "runner cannot reach the kind API with the cluster CA" >&2; exit 1; }
(umask 077; printf 'Authorization: Bearer %s\n' "$(tr -d '[:space:]' < "$dir/sa.token")" > "$dir/sa.header")
token_code="$(curl --cacert "$dir/kube-ca.crt" --max-time 15 -sS -o /dev/null -w '%{http_code}' \
  -H @"$dir/sa.header" \
  "https://${api_host}:${api_port}/apis/batch/v1/namespaces/${ns}/jobs/ccp-durable-preflight")"
[[ "$token_code" == "404" ]] || { echo "lifecycle ServiceAccount was not accepted (HTTP ${token_code})" >&2; exit 1; }
binding_dsn="postgres://postgres:compute-ci@127.0.0.1:${pg_port}/postgres?sslmode=disable"
durable_dsn="$binding_dsn"
durable_target_name=""
durable_target_uid=""

run_durable() {
  local outfile="$1"
  shift
  set +e
  CCP_DISPOSABLE_KIND_SMOKE=1 \
    POD_NAMESPACE="$ns" \
    KUBERNETES_SERVICE_HOST="$api_host" \
    KUBERNETES_SERVICE_PORT_HTTPS="$api_port" \
    CCP_KUEUE_SA_CA="$dir/kube-ca.crt" \
    CCP_KUEUE_SA_TOKEN="$dir/sa.token" \
    CCP_KUEUE_BINDING_DSN="$durable_dsn" \
    CCP_KUEUE_SMOKE_TARGET_NAME="$durable_target_name" \
    CCP_KUEUE_SMOKE_TARGET_UID="$durable_target_uid" \
    "$dir/kueue-provider-smoke" "$@" >"$outfile" 2>"$outfile.err"
  local code=$?
  set -e
  return "$code"
}

sigkill_durable() {
  local outfile="$1"
  shift
  CCP_DISPOSABLE_KIND_SMOKE=1 \
    POD_NAMESPACE="$ns" \
    KUBERNETES_SERVICE_HOST="$api_host" \
    KUBERNETES_SERVICE_PORT_HTTPS="$api_port" \
    CCP_KUEUE_SA_CA="$dir/kube-ca.crt" \
    CCP_KUEUE_SA_TOKEN="$dir/sa.token" \
    CCP_KUEUE_BINDING_DSN="$durable_dsn" \
    "$dir/kueue-provider-smoke" "$@" >"$outfile" 2>"$outfile.err" &
  local pid=$!
  local ready=false
  for _ in $(seq 1 450); do
    if jq -e '(.pin_durable == true or .tombstone_durable == true or (.phase == "journal-successor" and .status == "UNKNOWN" and .target_uid != "")) and (.uid != "" or .target_uid != "")' "$outfile" >/dev/null 2>&1; then
      ready=true
      break
    fi
    if ! kill -0 "$pid" 2>/dev/null; then
      echo "provider process exited before the binding was durable" >&2
      cat "$outfile.err" >&2 || true
      exit 1
    fi
    sleep 0.2
  done
  if [[ "$ready" != true ]]; then
    kill -9 "$pid" >/dev/null 2>&1 || true
    echo "timed out waiting for a durable binding" >&2
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

psql_at() {
  docker exec -e PGPASSWORD=compute-ci "$pg_name" \
    psql -U postgres -d postgres -tA -v ON_ERROR_STOP=1 -c "$1"
}

echo "durable: process A CREATE, then SIGKILL"
sigkill_durable "$dir/durable-create-a.json" durable create --hold
pinned_uid="$(jq -r '.uid' "$dir/durable-create-a.json")"
pinned_name="$(jq -r '.job_name' "$dir/durable-create-a.json")"
pinned_op="$(jq -r '.operation_id' "$dir/durable-create-a.json")"
[[ "$pinned_op" == "ccp-kind-durable-create" && -n "$pinned_uid" && -n "$pinned_name" ]] || {
  echo "create process did not report a pin" >&2
  exit 1
}
created_at="$(kubectl -n "$ns" get job "$pinned_name" -o jsonpath='{.metadata.creationTimestamp}')"
live_uid="$(kubectl -n "$ns" get job "$pinned_name" -o jsonpath='{.metadata.uid}')"
[[ "$live_uid" == "$pinned_uid" && -n "$created_at" ]] || { echo "live Job UID != pinned UID" >&2; exit 1; }
kubectl -n "$ns" get job "$pinned_name" -o json > "$dir/durable-original-job.json"

echo "durable: process B replays CREATE"
if ! run_durable "$dir/durable-replay-create.json" durable replay create; then
  cat "$dir/durable-replay-create.json.err" >&2
  exit 1
fi
jq -e --arg uid "$pinned_uid" --arg name "$pinned_name" \
  '.phase == "replay-create" and .result == "applied" and .uid == $uid and .job_name == $name and .release_attested == false' \
  "$dir/durable-replay-create.json" >/dev/null
replay_uid="$(kubectl -n "$ns" get job "$pinned_name" -o jsonpath='{.metadata.uid}')"
replay_created="$(kubectl -n "$ns" get job "$pinned_name" -o jsonpath='{.metadata.creationTimestamp}')"
job_count="$(kubectl -n "$ns" get jobs -o json | jq -r --arg id "$pinned_op" --arg name "$pinned_name" \
  '[.items[] | select(.metadata.name == $name and .metadata.annotations["compute-control-panel.io/operation-id"] == $id)] | length')"
[[ "$replay_uid" == "$pinned_uid" && "$replay_created" == "$created_at" && "$job_count" == "1" ]] || {
  echo "CREATE replay posted or replaced the Job" >&2
  exit 1
}
pin_uid="$(psql_at "SELECT job_uid FROM public.kueue_job_create_pins WHERE namespace = 'ccp-kueue-smoke' AND operation_id = 'ccp-kind-durable-create'")"
pin_count="$(psql_at "SELECT count(*) FROM public.kueue_job_create_pins WHERE namespace = 'ccp-kueue-smoke' AND operation_id = 'ccp-kind-durable-create'")"
[[ "$pin_uid" == "$pinned_uid" && "$pin_count" == "1" ]] || { echo "create pin row drifted" >&2; exit 1; }

echo "durable: fresh process Inspect CREATE"
if ! run_durable "$dir/durable-inspect-create.json" durable inspect create; then
  cat "$dir/durable-inspect-create.json.err" >&2
  exit 1
fi
jq -e --arg uid "$pinned_uid" \
  '.phase == "inspect-create" and .result == "RESOURCE_PRESENT" and .uid == $uid and .release_attested == false' \
  "$dir/durable-inspect-create.json" >/dev/null

echo "durable: process C DELETE, then SIGKILL"
sigkill_durable "$dir/durable-delete-c.json" durable delete --hold
jq -e --arg uid "$pinned_uid" --arg name "$pinned_name" \
  '.phase == "delete" and .tombstone_durable == true and .acknowledged == true and .uid == $uid and .job_name == $name and .release_attested == false' \
  "$dir/durable-delete-c.json" >/dev/null

echo "durable: process D replays DELETE"
if ! run_durable "$dir/durable-replay-delete.json" durable replay delete; then
  cat "$dir/durable-replay-delete.json.err" >&2
  exit 1
fi
jq -e --arg uid "$pinned_uid" \
  '.phase == "replay-delete" and .result == "applied" and .uid == $uid and .release_attested == false' \
  "$dir/durable-replay-delete.json" >/dev/null
if ! run_durable "$dir/durable-inspect-delete.json" durable inspect delete; then
  cat "$dir/durable-inspect-delete.json.err" >&2
  exit 1
fi
jq -e --arg uid "$pinned_uid" \
  '.phase == "inspect-delete" and .result == "TARGET_ABSENT" and .target_uid == $uid and .uid == "" and .release_attested == false' \
  "$dir/durable-inspect-delete.json" >/dev/null
if kubectl -n "$ns" get job "$pinned_name" >/dev/null 2>&1; then
  echo "pinned Job still present after TARGET_ABSENT" >&2
  exit 1
fi
tomb_uid="$(psql_at "SELECT job_uid FROM public.kueue_job_delete_tombstones WHERE namespace = 'ccp-kueue-smoke' AND operation_id = 'ccp-kind-durable-delete' AND acknowledged")"
[[ "$tomb_uid" == "$pinned_uid" ]] || { echo "delete tombstone drifted: ${tomb_uid:-<empty>}" >&2; exit 1; }

echo "durable: same-name successor must be JOB_UID_REPLACED and left in place"
jq -n --arg name "$pinned_name" --arg ns "$ns" --slurpfile src "$dir/durable-original-job.json" '{
  apiVersion: "batch/v1",
  kind: "Job",
  metadata: {name: $name, namespace: $ns, annotations: $src[0].metadata.annotations},
  spec: {
    suspend: true,
    parallelism: 1,
    completions: 1,
    template: {spec: {
      restartPolicy: "Never",
      containers: [{name: "ccp-suspended", image: "registry.k8s.io/pause:3.10"}]
    }}
  }
}' > "$dir/durable-successor.json"
kubectl apply -f "$dir/durable-successor.json" >/dev/null
successor_uid="$(kubectl -n "$ns" get job "$pinned_name" -o jsonpath='{.metadata.uid}')"
successor_created="$(kubectl -n "$ns" get job "$pinned_name" -o jsonpath='{.metadata.creationTimestamp}')"
[[ -n "$successor_uid" && "$successor_uid" != "$pinned_uid" && -n "$successor_created" ]] || {
  echo "successor Job was not a new UID" >&2
  exit 1
}
if ! run_durable "$dir/durable-successor-inspect.json" durable inspect create; then
  cat "$dir/durable-successor-inspect.json.err" >&2
  exit 1
fi
jq -e '.phase == "inspect-create" and .result == "CONFLICT" and .reason_code == "JOB_UID_REPLACED" and .uid == "" and .release_attested == false' \
  "$dir/durable-successor-inspect.json" >/dev/null
[[ "$(kubectl -n "$ns" get job "$pinned_name" -o jsonpath='{.metadata.uid}')" == "$successor_uid" ]] || {
  echo "JOB_UID_REPLACED process replaced the successor" >&2
  exit 1
}
[[ "$(kubectl -n "$ns" get job "$pinned_name" -o jsonpath='{.metadata.creationTimestamp}')" == "$successor_created" ]] || {
  echo "JOB_UID_REPLACED process recreated the successor" >&2
  exit 1
}
[[ -z "$(kubectl -n "$ns" get job "$pinned_name" -o jsonpath='{.metadata.deletionTimestamp}')" ]] || {
  echo "JOB_UID_REPLACED process deleted the successor" >&2
  exit 1
}

echo "durable: empty DSN and unreachable database fail closed"
durable_target_name="$pinned_name"
durable_target_uid="$successor_uid"
durable_dsn=""
if run_durable "$dir/durable-empty.json" durable fail-closed; then
  echo "empty DSN did not fail" >&2
  exit 1
fi
jq -e '.phase == "fail-closed" and .failed_closed == true and .reason == "empty_dsn" and .create_applied == false and .delete_applied == false and .inspect_present == false' \
  "$dir/durable-empty.json" >/dev/null
durable_dsn="postgres://postgres:compute-ci@127.0.0.1:1/postgres?sslmode=disable&connect_timeout=2"
if run_durable "$dir/durable-unreachable.json" durable fail-closed; then
  echo "unreachable database did not fail" >&2
  exit 1
fi
jq -e '.phase == "fail-closed" and .failed_closed == true and .reason == "unreachable" and .create_applied == false and .delete_applied == false and .inspect_present == false' \
  "$dir/durable-unreachable.json" >/dev/null
[[ "$(kubectl -n "$ns" get job "$pinned_name" -o jsonpath='{.metadata.uid}')" == "$successor_uid" ]] || {
  echo "fail-closed process deleted or replaced the successor" >&2
  exit 1
}
[[ "$(kubectl -n "$ns" get job "$pinned_name" -o jsonpath='{.metadata.creationTimestamp}')" == "$successor_created" ]] || {
  echo "fail-closed process recreated the successor" >&2
  exit 1
}
[[ "$(psql_at "SELECT job_uid FROM public.kueue_job_create_pins WHERE namespace = 'ccp-kueue-smoke' AND operation_id = 'ccp-kind-durable-create'")" == "$pinned_uid" ]] || {
  echo "fail-closed process rewrote the create pin" >&2
  exit 1
}
echo "durable_sigkill_uid=$pinned_uid successor_uid=$successor_uid"

# This additional test joins the actual PostgreSQL ComputeOperation Journal and
# recovery.Worker to the real kind API Server and durable UID binding. The
# first process commits UNKNOWN after CREATE, then is genuinely SIGKILLed.
# Only a fresh-process, Inspect-only Worker may finalize that exact UID.
durable_dsn="$binding_dsn"
echo "journal: real Kueue Job CREATE and persisted UNKNOWN, then SIGKILL"
sigkill_durable "$dir/journal-create.json" journal create --hold
jq -e '.phase == "journal-create" and .status == "UNKNOWN" and
  .pin_durable == true and .release_attested == false and
  .uid != "" and .job_name != "" and .operation_id == "ccp-kind-journal-create"'   "$dir/journal-create.json" >/dev/null
journal_uid="$(jq -r '.uid' "$dir/journal-create.json")"
journal_name="$(jq -r '.job_name' "$dir/journal-create.json")"
journal_previous_fence="$(jq -r '.fence_token' "$dir/journal-create.json")"
journal_created="$(kubectl -n "$ns" get job "$journal_name" -o jsonpath='{.metadata.creationTimestamp}')"
[[ -n "$journal_created" &&
  "$(kubectl -n "$ns" get job "$journal_name" -o jsonpath='{.metadata.uid}')" == "$journal_uid" ]] || {
  echo "journal Job UID was not present after SIGKILL" >&2
  exit 1
}
[[ "$(psql_at "SELECT status FROM public.compute_operations WHERE tenant_id='tenant-a' AND compute_operation_id='ccp-kind-journal-create'")" == "UNKNOWN" ]] || {
  echo "ComputeOperation journal did not retain UNKNOWN after SIGKILL" >&2
  exit 1
}
[[ "$(psql_at "SELECT job_uid FROM public.kueue_job_create_pins WHERE namespace='ccp-kueue-smoke' AND tenant_id='tenant-a' AND operation_id='ccp-kind-journal-create'")" == "$journal_uid" ]] || {
  echo "ComputeOperation Job UID pin was not durable" >&2
  exit 1
}

echo "journal: fresh process fenced Worker.RecoverUnknown with Inspect only"
if ! run_durable "$dir/journal-recover.json" journal recover; then
  cat "$dir/journal-recover.json.err" >&2
  exit 1
fi
jq -e --arg uid "$journal_uid" --argjson previous "$journal_previous_fence"   '.phase == "journal-recover" and .status == "SUCCEEDED" and
   .pin_durable == true and .uid == $uid and .fence_token > $previous and
   .observation == "RESOURCE_PRESENT" and .observation_rows == 1 and
   .outbox_events == 2 and .release_attested == false' "$dir/journal-recover.json" >/dev/null
[[ "$(kubectl -n "$ns" get job "$journal_name" -o jsonpath='{.metadata.uid}')" == "$journal_uid" &&
  "$(kubectl -n "$ns" get job "$journal_name" -o jsonpath='{.metadata.creationTimestamp}')" == "$journal_created" &&
  "$(kubectl -n "$ns" get job "$journal_name" -o jsonpath='{.spec.suspend}')" == "true" ]] || {
  echo "recovery replaced, unsuspended or recreated the original Job" >&2
  exit 1
}
journal_job_count="$(kubectl -n "$ns" get jobs -o json | jq -r --arg id ccp-kind-journal-create   '[.items[] | select(.metadata.annotations["compute-control-panel.io/operation-id"] == $id)] | length')"
[[ "$journal_job_count" == "1" ]] || { echo "journal recovery created duplicate Jobs" >&2; exit 1; }
[[ "$(psql_at "SELECT status || ':' || coalesce(provider_resource_uid,'') FROM public.compute_operations WHERE tenant_id='tenant-a' AND compute_operation_id='ccp-kind-journal-create'")" == "SUCCEEDED:$journal_uid" ]] || {
  echo "journal finality did not persist exactly for the pinned UID" >&2
  exit 1
}
echo "journal_recovered_uid=$journal_uid journal_fence=$(jq -r '.fence_token' "$dir/journal-recover.json")"
kubectl -n "$ns" get job "$journal_name" -o json > "$dir/journal-original-job.json"

# DELETE recovery uses the same suspended Job. TARGET_ABSENT is the tombstone
# for that UID plus a 404. It is not Allocation RELEASED, quota release, or
# device release, and it does not use compute_operations.provider_resource_uid.
echo "journal: UID-fenced DELETE persisted as UNKNOWN, then SIGKILL"
sigkill_durable "$dir/journal-delete.json" journal delete --hold
jq -e --arg uid "$journal_uid" --arg name "$journal_name" \
  '.phase == "journal-delete" and .status == "UNKNOWN" and
   .tombstone_durable == true and .acknowledged == true and
   .uid == $uid and .target_uid == $uid and .job_name == $name and
   .operation_id == "ccp-kind-journal-delete" and .release_attested == false' \
  "$dir/journal-delete.json" >/dev/null
journal_delete_fence="$(jq -r '.fence_token' "$dir/journal-delete.json")"
for _ in $(seq 1 50); do
  if ! kubectl -n "$ns" get job "$journal_name" >/dev/null 2>&1; then
    break
  fi
  sleep 0.2
done
if kubectl -n "$ns" get job "$journal_name" >/dev/null 2>&1; then
  echo "UID-fenced DELETE left the original Job in place" >&2
  exit 1
fi
journal_delete_row="$(psql_at "SELECT status || '|' || coalesce(target_resource_uid,'') || '|' || (provider_resource_uid IS NULL)::text FROM public.compute_operations WHERE tenant_id='tenant-a' AND compute_operation_id='ccp-kind-journal-delete'")"
[[ "$journal_delete_row" == "UNKNOWN|${journal_uid}|true" ]] || {
  echo "DELETE ComputeOperation was not UNKNOWN for the exact target UID: ${journal_delete_row:-<empty>}" >&2
  exit 1
}
[[ "$(psql_at "SELECT job_uid FROM public.kueue_job_delete_tombstones WHERE namespace='ccp-kueue-smoke' AND tenant_id='tenant-a' AND operation_id='ccp-kind-journal-delete' AND acknowledged")" == "$journal_uid" ]] || {
  echo "acknowledged DELETE tombstone was not the original UID" >&2
  exit 1
}
[[ "$(kubectl -n "$ns" get job "$pinned_name" -o jsonpath='{.metadata.uid}')" == "$successor_uid" ]] || {
  echo "DELETE process removed a different UID" >&2
  exit 1
}

echo "journal: empty and unreachable DSN fail closed before DELETE recovery"
durable_dsn=""
if run_durable "$dir/journal-delete-empty.json" journal fail-closed; then
  echo "empty journal DSN did not fail" >&2
  exit 1
fi
jq -e '.phase == "journal-fail-closed" and .failed_closed == true and .reason == "empty_dsn" and .delete_applied == false and .release_attested == false' \
  "$dir/journal-delete-empty.json" >/dev/null
durable_dsn="postgres://postgres:compute-ci@127.0.0.1:1/postgres?sslmode=disable&connect_timeout=2"
if run_durable "$dir/journal-delete-unreachable.json" journal fail-closed; then
  echo "unreachable journal DSN did not fail" >&2
  exit 1
fi
jq -e '.phase == "journal-fail-closed" and .failed_closed == true and .reason == "unreachable" and .delete_applied == false and .release_attested == false' \
  "$dir/journal-delete-unreachable.json" >/dev/null
durable_dsn="$binding_dsn"
[[ "$(psql_at "SELECT status FROM public.compute_operations WHERE tenant_id='tenant-a' AND compute_operation_id='ccp-kind-journal-delete'")" == "UNKNOWN" ]] || {
  echo "unreachable DSN changed the DELETE journal" >&2
  exit 1
}
[[ "$(psql_at "SELECT count(*) FROM public.compute_resource_observations WHERE tenant_id='tenant-a' AND compute_operation_id='ccp-kind-journal-delete'")" == "0" ]] || {
  echo "unreachable DSN persisted a DELETE observation" >&2
  exit 1
}
[[ "$(kubectl -n "$ns" get job "$pinned_name" -o jsonpath='{.metadata.uid}')" == "$successor_uid" ]] || {
  echo "unreachable DSN deleted a different UID" >&2
  exit 1
}

echo "journal: fresh process fenced Worker.RecoverUnknown Inspects DELETE only"
if ! run_durable "$dir/journal-recover-delete.json" journal recover-delete; then
  cat "$dir/journal-recover-delete.json.err" >&2
  exit 1
fi
jq -e --arg uid "$journal_uid" --argjson previous "$journal_delete_fence" \
  '.phase == "journal-recover-delete" and .status == "SUCCEEDED" and
   .tombstone_durable == true and .acknowledged == true and
   .uid == "" and .target_uid == $uid and .fence_token > $previous and
   .observation == "TARGET_ABSENT" and .observation_rows == 1 and
   .outbox_events == 2 and .release_attested == false' \
  "$dir/journal-recover-delete.json" >/dev/null
journal_delete_final="$(psql_at "SELECT status || '|' || coalesce(target_resource_uid,'') || '|' || (provider_resource_uid IS NULL)::text FROM public.compute_operations WHERE tenant_id='tenant-a' AND compute_operation_id='ccp-kind-journal-delete'")"
[[ "$journal_delete_final" == "SUCCEEDED|${journal_uid}|true" ]] || {
  echo "DELETE finality wrote provider_resource_uid or lost the target UID: ${journal_delete_final:-<empty>}" >&2
  exit 1
}
[[ "$(psql_at "SELECT operation_result || '|' || coalesce(details->>'TargetResourceUID','') || '|' || coalesce(provider_resource_uid,'') FROM public.compute_resource_observations WHERE tenant_id='tenant-a' AND compute_operation_id='ccp-kind-journal-delete'")" == "TARGET_ABSENT|${journal_uid}|" ]] || {
  echo "TARGET_ABSENT observation was not the exact deleted UID" >&2
  exit 1
}
if kubectl -n "$ns" get job "$journal_name" >/dev/null 2>&1; then
  echo "DELETE recovery recreated the Job" >&2
  exit 1
fi
[[ "$(kubectl -n "$ns" get job "$pinned_name" -o jsonpath='{.metadata.uid}')" == "$successor_uid" ]] || {
  echo "DELETE recovery removed a different UID" >&2
  exit 1
}

echo "journal: same-name successor is JOB_UID_REPLACED and is not deleted"
jq -n --arg name "$journal_name" --arg ns "$ns" --slurpfile src "$dir/journal-original-job.json" '{
  apiVersion: "batch/v1",
  kind: "Job",
  metadata: {name: $name, namespace: $ns, annotations: $src[0].metadata.annotations},
  spec: {
    suspend: true,
    parallelism: 1,
    completions: 1,
    template: {spec: {
      restartPolicy: "Never",
      containers: [{name: "ccp-suspended", image: "registry.k8s.io/pause:3.10"}]
    }}
  }
}' > "$dir/journal-successor-job.json"
kubectl apply -f "$dir/journal-successor-job.json" >/dev/null
journal_successor_uid="$(kubectl -n "$ns" get job "$journal_name" -o jsonpath='{.metadata.uid}')"
journal_successor_created="$(kubectl -n "$ns" get job "$journal_name" -o jsonpath='{.metadata.creationTimestamp}')"
[[ -n "$journal_successor_uid" && "$journal_successor_uid" != "$journal_uid" && -n "$journal_successor_created" ]] || {
  echo "journal successor was not a new UID" >&2
  exit 1
}
sigkill_durable "$dir/journal-successor.json" journal successor --hold
jq -e --arg uid "$journal_uid" --arg name "$journal_name" \
  '.phase == "journal-successor" and .status == "UNKNOWN" and
   .uid == "" and .target_uid == $uid and .job_name == $name and
   .operation_id == "ccp-kind-journal-delete-successor" and
   .tombstone_durable == false and .release_attested == false' \
  "$dir/journal-successor.json" >/dev/null
journal_successor_fence="$(jq -r '.fence_token' "$dir/journal-successor.json")"
[[ "$(kubectl -n "$ns" get job "$journal_name" -o jsonpath='{.metadata.uid}')" == "$journal_successor_uid" ]] || {
  echo "successor journal process deleted the replacement Job" >&2
  exit 1
}
durable_dsn="postgres://postgres:compute-ci@127.0.0.1:1/postgres?sslmode=disable&connect_timeout=2"
if run_durable "$dir/journal-successor-unreachable.json" journal fail-closed; then
  echo "unreachable DSN during successor recovery did not fail" >&2
  exit 1
fi
durable_dsn="$binding_dsn"
[[ "$(kubectl -n "$ns" get job "$journal_name" -o jsonpath='{.metadata.uid}')" == "$journal_successor_uid" ]] || {
  echo "unreachable DSN deleted the same-name successor" >&2
  exit 1
}
[[ "$(psql_at "SELECT status FROM public.compute_operations WHERE tenant_id='tenant-a' AND compute_operation_id='ccp-kind-journal-delete-successor'")" == "UNKNOWN" ]] || {
  echo "unreachable DSN resolved the successor DELETE" >&2
  exit 1
}
if ! run_durable "$dir/journal-recover-successor.json" journal recover-successor; then
  cat "$dir/journal-recover-successor.json.err" >&2
  exit 1
fi
jq -e --arg uid "$journal_uid" --argjson previous "$journal_successor_fence" \
  '.phase == "journal-recover-successor" and .status == "MANUAL_REVIEW" and
   .reason_code == "JOB_UID_REPLACED" and .observation == "CONFLICT" and
   .uid == "" and .target_uid == $uid and .fence_token > $previous and
   .observation_rows == 1 and .outbox_events == 2 and .release_attested == false' \
  "$dir/journal-recover-successor.json" >/dev/null
journal_successor_row="$(psql_at "SELECT status || '|' || coalesce(manual_review_code,'') || '|' || coalesce(target_resource_uid,'') || '|' || (provider_resource_uid IS NULL)::text FROM public.compute_operations WHERE tenant_id='tenant-a' AND compute_operation_id='ccp-kind-journal-delete-successor'")"
[[ "$journal_successor_row" == "MANUAL_REVIEW|JOB_UID_REPLACED|${journal_uid}|true" ]] || {
  echo "successor recovery did not stay MANUAL_REVIEW for the original UID: ${journal_successor_row:-<empty>}" >&2
  exit 1
}
[[ "$(kubectl -n "$ns" get job "$journal_name" -o jsonpath='{.metadata.uid}')" == "$journal_successor_uid" &&
  "$(kubectl -n "$ns" get job "$journal_name" -o jsonpath='{.metadata.creationTimestamp}')" == "$journal_successor_created" &&
  -z "$(kubectl -n "$ns" get job "$journal_name" -o jsonpath='{.metadata.deletionTimestamp}')" &&
  "$(kubectl -n "$ns" get job "$journal_name" -o jsonpath='{.spec.suspend}')" == "true" ]] || {
  echo "JOB_UID_REPLACED recovery deleted, unsuspended, or replaced the successor" >&2
  exit 1
}
[[ "$(kubectl -n "$ns" get job "$pinned_name" -o jsonpath='{.metadata.uid}')" == "$successor_uid" ]] || {
  echo "successor recovery removed a different UID" >&2
  exit 1
}
echo "journal_delete_uid=$journal_uid journal_successor_uid=$journal_successor_uid"

# Admission versus placement. The queued writer and the least-privilege
# binding login submit one Job. A NoSchedule taint the Job does not tolerate
# keeps its Pod unscheduled after Kueue reserves quota. CCP records that as
# NOT_READY. The UID-fenced DELETE then Inspects absence and leaves a
# same-name successor in place. This is not Allocation RELEASED.
binding_role_dsn="postgres://ccp_kueue_job_binding:kueue-binding-ci-only@127.0.0.1:${pg_port}/postgres?sslmode=disable"
run_admission() {
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
    "$dir/kueue-provider-smoke" "$@" >"$outfile" 2>"$outfile.err"
  local code=$?
  set -e
  return "$code"
}
[[ "$(psql_at "SELECT rolsuper::text || ':' || rolname FROM pg_roles WHERE rolname='ccp_kueue_job_binding'")" == "false:ccp_kueue_job_binding" ]] || {
  echo "ccp_kueue_job_binding is not the restricted login" >&2
  exit 1
}
mapfile -t smoke_nodes < <(kubectl get nodes -o jsonpath='{range .items[*]}{.metadata.name}{"\n"}{end}')
[[ "${#smoke_nodes[@]}" -ge 1 ]] || { echo "no disposable node to taint" >&2; exit 1; }
for n in "${smoke_nodes[@]}"; do
  kubectl taint node "$n" ccp-kueue-smoke/unplaceable=true:NoSchedule
done
echo "admission: queued Job with least-privilege binding, Kueue admission, unscheduled Pod"
if ! run_admission "$dir/admission-create.json" admission create; then
  cat "$dir/admission-create.json.err" >&2 || true
  exit 1
fi
admission_job_uid="$(jq -r '.job_uid' "$dir/admission-create.json")"
admission_job_name="$(jq -r '.job_name' "$dir/admission-create.json")"
admission_pod_uid="$(jq -r '.pod_uid' "$dir/admission-create.json")"
admission_workload_uid="$(jq -r '.workload_uid' "$dir/admission-create.json")"
admission_workload_name="$(jq -r '.workload_name' "$dir/admission-create.json")"
[[ -n "$admission_job_uid" && -n "$admission_job_name" && -n "$admission_pod_uid" && -n "$admission_workload_uid" &&
  "$admission_job_uid" != "$admission_pod_uid" && "$admission_job_uid" != "$admission_workload_uid" &&
  "$admission_pod_uid" != "$admission_workload_uid" ]] || {
  echo "admission observation did not keep separate Job, Pod, and Workload UIDs" >&2
  exit 1
}
jq -e --arg ns "$ns" --arg job "$admission_job_uid" --arg pod "$admission_pod_uid" --arg workload "$admission_workload_uid" --arg wname "$admission_workload_name" '
  .phase == "create" and .projection == "NOT_READY" and .stage == "KUEUE_ADMITTED" and
  .result == "RESOURCE_PRESENT" and .quota_reserved == true and .admitted == true and
  .cluster_queue == "ccp-smoke" and (.pod_set_assignments | type == "string" and length > 0) and
  .workload_namespace == $ns and .workload_name == $wname and .workload_uid == $workload and
  (.workload_resource_version | type == "string" and length > 0) and
  .job_uid == $job and (.job_resource_version | type == "string" and length > 0) and
  .pod_uid == $pod and (.pod_resource_version | type == "string" and length > 0) and
  .pod_scheduled == false and .node_bound == false and .node_name == "" and
  .absence_observed == false and .allocated == false and .active == false and
  .ready == false and .costable == false and .released == false and .release_attested == false and
  .operation_id == "ccp-kind-admission-unplaced"' "$dir/admission-create.json" >/dev/null
wait_for 'admitted unplaced workload still quota-reserved' workload_admitted "$admission_job_uid"
wait_for 'clusterqueue still reserves the admitted unplaced job' quota_reserved_in_clusterqueue
pod_unplaceable() {
  pods_json "$1" | jq -e --arg uid "$admission_pod_uid" '
    [.items[] | select(.metadata.uid == $uid and
      (.metadata.resourceVersion | type == "string" and length > 0) and
      ((.spec.nodeName // "") == "") and
      any(.status.conditions[]?; .type == "PodScheduled" and .status == "False" and .reason == "Unschedulable"))] | length == 1' >/dev/null
}
wait_for 'pod exists and scheduler reports it unschedulable' pod_unplaceable "$admission_job_uid"
kubectl -n "$ns" get job "$admission_job_name" -o json | jq -e --arg uid "$admission_job_uid" '
  .metadata.uid == $uid and .metadata.labels["kueue.x-k8s.io/queue-name"] == "ccp-smoke"' >/dev/null
live_workload="$(workloads_json | jq -ce --arg id "$admission_job_uid" --arg uid "$admission_workload_uid" --arg name "$admission_workload_name" --arg ns "$ns" '
  [.items[] | select(any(.metadata.ownerReferences[]?; .kind == "Job" and .uid == $id))]
  | if length == 1 then .[0] else empty end
  | select(.metadata.uid == $uid and .metadata.name == $name and .metadata.namespace == $ns
      and (.metadata.resourceVersion | type == "string" and length > 0))')"
[[ -n "$live_workload" ]] || { echo "live workload identity did not match the admission observation" >&2; exit 1; }
for n in "${smoke_nodes[@]}"; do
  kubectl get node "$n" -o json | jq -e 'any(.spec.taints[]?; .key == "ccp-kueue-smoke/unplaceable" and .effect == "NoSchedule")' >/dev/null
done
echo "admission_vs_placement=$(jq -c '{projection,stage,workload_namespace,workload_name,workload_uid,workload_resource_version,job_uid,job_resource_version,pod_uid,pod_resource_version,quota_reserved,admitted,node_bound,node_name,allocated,active,ready,costable,released}' "$dir/admission-create.json")"
echo "admission_clusterqueue=$(kubectl get clusterqueue ccp-smoke -o json | jq -c '{uid:.metadata.uid,rv:.metadata.resourceVersion,reserving:.status.reservingWorkloads,admitted:.status.admittedWorkloads}')"
[[ "$(psql_at "SELECT job_uid FROM public.kueue_job_create_pins WHERE namespace='ccp-kueue-smoke' AND operation_id='ccp-kind-admission-unplaced'")" == "$admission_job_uid" ]] || {
  echo "least-privilege binding pin was not the admitted Job UID" >&2
  exit 1
}
kubectl -n "$ns" get job "$admission_job_name" -o json | jq '
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
  }' > "$dir/admission-successor-template.json"

echo "admission: UID-fenced DELETE, then a fresh process Inspects absence"
if ! run_admission "$dir/admission-delete.json" admission delete; then
  cat "$dir/admission-delete.json.err" >&2 || true
  exit 1
fi
jq -e --arg uid "$admission_job_uid" --arg name "$admission_job_name" '
  .phase == "delete" and .projection == "NOT_READY" and .job_uid == $uid and .job_name == $name and
  .target_uid == $uid and .absence_observed == false and .released == false and .ready == false and
  .release_attested == false and .operation_id == "ccp-kind-admission-unplaced-delete"' \
  "$dir/admission-delete.json" >/dev/null
if ! run_admission "$dir/admission-inspect-delete.json" admission inspect delete; then
  cat "$dir/admission-inspect-delete.json.err" >&2 || true
  exit 1
fi
jq -e --arg uid "$admission_job_uid" '
  .phase == "inspect-delete" and .projection == "NOT_READY" and .result == "TARGET_ABSENT" and
  .target_uid == $uid and .job_uid == "" and .absence_observed == true and
  .allocated == false and .active == false and .ready == false and .costable == false and
  .released == false and .release_attested == false' "$dir/admission-inspect-delete.json" >/dev/null
admission_job_absent() {
  kubectl -n "$ns" get jobs -o json | jq -e --arg uid "$admission_job_uid" 'all(.items[]; .metadata.uid != $uid)' >/dev/null
}
wait_for 'exact admitted Job UID absent after Inspect' admission_job_absent
if kubectl -n "$ns" get job "$admission_job_name" >/dev/null 2>&1; then
  echo "Inspect-first absence left the original Job name in place" >&2
  exit 1
fi

echo "admission: same-name successor stays in place"
kubectl apply -f "$dir/admission-successor-template.json" >/dev/null
admission_successor_uid="$(kubectl -n "$ns" get job "$admission_job_name" -o jsonpath='{.metadata.uid}')"
admission_successor_created="$(kubectl -n "$ns" get job "$admission_job_name" -o jsonpath='{.metadata.creationTimestamp}')"
[[ -n "$admission_successor_uid" && "$admission_successor_uid" != "$admission_job_uid" && -n "$admission_successor_created" ]] || {
  echo "admission successor was not a new UID" >&2
  exit 1
}
if ! run_admission "$dir/admission-inspect-create.json" admission inspect create; then
  cat "$dir/admission-inspect-create.json.err" >&2 || true
  exit 1
fi
jq -e '
  .phase == "inspect-create" and .projection == "NOT_READY" and .result == "CONFLICT" and
  .reason_code == "JOB_UID_REPLACED" and .job_uid == "" and .absence_observed == false and
  .ready == false and .released == false and .release_attested == false' \
  "$dir/admission-inspect-create.json" >/dev/null
[[ "$(kubectl -n "$ns" get job "$admission_job_name" -o jsonpath='{.metadata.uid}')" == "$admission_successor_uid" &&
  "$(kubectl -n "$ns" get job "$admission_job_name" -o jsonpath='{.metadata.creationTimestamp}')" == "$admission_successor_created" &&
  -z "$(kubectl -n "$ns" get job "$admission_job_name" -o jsonpath='{.metadata.deletionTimestamp}')" ]] || {
  echo "JOB_UID_REPLACED process deleted or replaced the admission successor" >&2
  exit 1
}
echo "admission_successor_uid=$admission_successor_uid admission_deleted_uid=$admission_job_uid"
for n in "${smoke_nodes[@]}"; do
  kubectl taint node "$n" 'ccp-kueue-smoke/unplaceable=true:NoSchedule-' >/dev/null
done
# Relist plus Kueue controller restart. Separate file so a preemption slice can
# rebase without sharing its script or projection types.
export CCP_RELIST_DIR="$dir"
export CCP_RELIST_NS="$ns"
export CCP_RELIST_PG_PORT="$pg_port"
export CCP_RELIST_PG_NAME="$pg_name"
export CCP_RELIST_API_HOST="$api_host"
export CCP_RELIST_API_PORT="$api_port"
bash scripts/kueue_relist_kind_smoke.sh
echo "PASS: disposable CPU-only Kueue quota-denial/QuotaReserved/Admitted, exact Workload/Pod/Job UID+RV and node binding, teardown observation, Postgres pin/tombstone SIGKILL recovery, real kind + Postgres ComputeOperation UNKNOWN -> fenced Inspect-first CREATE SUCCEEDED and DELETE TARGET_ABSENT after process SIGKILL, and admitted-but-unplaced queued Job projected NOT_READY through the ccp_kueue_job_binding login. TARGET_ABSENT is not Allocation RELEASED, Kueue quota release, or device release. This is NOT GPU/DRA device release, a production quota-release attestation, Provider retirement, Allocation RELEASED or cost finality."
# Independent official DRA example-driver lane. This creates a simulated GPU
# ResourceSlice/Claim/Pod *separate* from the queued CCP Job to keep authority
# and attribution claims explicit; never promote Allocation READY/RELEASED.
bash scripts/dra_example_kind_smoke.sh
# Kueue withinClusterQueue preemption. Separate file; reuses run_admission.
. scripts/kueue_preemption_kind_smoke.sh

