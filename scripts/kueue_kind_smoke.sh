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
image="kindest/node:v1.34.0@sha256:7416a61b42b1662ca6ca89f02028ac133a309a2a30ba309614e8ec94d976dc5a"
created=false
cleanup() {
  status=$?
  if [[ "$status" -ne 0 ]] && [[ "$created" == true ]]; then
    kubectl --request-timeout=10s -n "$ns" get jobs,workloads,pods -o wide || true
    kubectl --request-timeout=10s -n "$ns" logs ccp-provider-lifecycle --tail=50 || true
    kubectl --request-timeout=10s -n kueue-system logs deploy/kueue-controller-manager --tail=80 || true
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
workload_admitted() {
  workloads_json | jq -e --arg id "$1" '[.items[] | select(any(.metadata.ownerReferences[]?; .kind == "Job" and .uid == $id)) | any(.status.conditions[]?; .type == "Admitted" and .status == "True")] == [true]' >/dev/null
}
pod_scheduled() {
  pods_json "$1" | jq -e '[.items[] | select(.spec.nodeName != "" and any(.status.conditions[]?; .type == "PodScheduled" and .status == "True"))] | length == 1' >/dev/null
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
if workload_admitted "$uid"; then echo "zero quota incorrectly admitted" >&2; exit 1; fi
[[ "$(pods_json "$uid" | jq '.items | length')" == 0 ]] || { echo "Pod created before admission" >&2; exit 1; }
observer queued "$uid" UNPROVEN

# Enable admission while no node matches, to prove admission != placement.
kubectl patch clusterqueue ccp-smoke --type=json -p='[{"op":"replace","path":"/spec/resourceGroups/0/flavors/0/resources/0/nominalQuota","value":"2"},{"op":"replace","path":"/spec/resourceGroups/0/flavors/0/resources/1/nominalQuota","value":"2Gi"}]'
wait_for 'Kueue admitted Job' workload_admitted "$uid"
if pod_scheduled "$uid"; then echo "Pod scheduled before node opt-in" >&2; exit 1; fi
observer admitted "$uid" KUEUE_ADMITTED

node="$(kubectl get nodes -o jsonpath='{.items[0].metadata.name}')"
kubectl label node "$node" ccp-kueue-smoke/eligible=true --overwrite
wait_for 'Pod placed on node' pod_scheduled "$uid"
observer placed "$uid" POD_PLACED
old_pod_uid="$(pods_json "$uid" | jq -r '.items[0].metadata.uid')"
old_workload_uid="$(workloads_json | jq -r --arg id "$uid" '.items[] | select(any(.metadata.ownerReferences[]?; .kind == "Job" and .uid == $id)) | .metadata.uid')"

# Only disposable test teardown: NOT an Allocation RELEASED proof.
kubectl -n "$ns" delete job "$job" --cascade=background --wait=true
wait_for 'old Pod UID absent' old_pod_absent
wait_for 'old Kueue Workload UID absent' old_workload_absent
wait_for 'queue reservation released' quota_released

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
# against the same real API. It is NOT a broker/PG worker restart test.
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
echo "PASS: real Kueue queue/admission/placement, quota cleanup, stale UID rejection and real adapter CREATE/DELETE. NO GPU/DRA readiness or release attestation."
