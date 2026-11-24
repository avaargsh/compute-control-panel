#!/usr/bin/env bash
# Actual PDJobSetAdapter Kubernetes POST/GET acceptance. Staging-only identity;
# one disposable kind cluster; no driver or physical GPU availability claims.
set -Eeuo pipefail
[[ "${CCP_DISPOSABLE_KIND_SMOKE:-}" == "1" ]] || exit 2
[[ "$(kubectl config current-context)" == "kind-ccp-kueue-smoke" ]] || exit 2
for tool in kubectl jq; do command -v "$tool" >/dev/null || exit 2; done
ns=ccp-pd-paired-smoke
name=ccp-pd-provider-writer-smoke
intent="$(printf 'c%.0s' {1..64})"
[[ "$(kubectl -n "$ns" get sa ccp-pd-staging -o jsonpath='{.metadata.name}')" == ccp-pd-staging ]] || exit 2
[[ "$(kubectl auth can-i create jobsets.jobset.x-k8s.io -n "$ns" --as="system:serviceaccount:$ns:ccp-pd-staging")" == yes ]] || exit 2
for verb in patch update delete; do
  [[ "$(kubectl auth can-i "$verb" jobsets.jobset.x-k8s.io -n "$ns" --as="system:serviceaccount:$ns:ccp-pd-staging")" == no ]] || exit 2
done

# A real in-cluster ServiceAccount TLS/token is exercised by Go. No kubectl
# impersonation substitutes for the actual POST. The image was built from
# exactly this CI checkout and loaded into the disposable Kind node.
kubectl create -f - <<YAML
apiVersion: v1
kind: Pod
metadata: {name: $name, namespace: $ns}
spec:
  serviceAccountName: ccp-pd-staging
  restartPolicy: Never
  containers:
  - name: writer
    image: ccp-kueue-observe:smoke
    imagePullPolicy: Never
    command: ["/pd-jobset-provider-smoke"]
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
phase=''
for _ in $(seq 1 90); do
  phase="$(kubectl -n "$ns" get pod "$name" -o jsonpath='{.status.phase}' 2>/dev/null || true)"
  [[ "$phase" == Succeeded ]] && break
  if [[ "$phase" == Failed ]]; then
    kubectl -n "$ns" logs "$name" >&2 || true
    # Print only the disposable JobSet's server-defaulted metadata and spec,
    # never ServiceAccount tokens, Kubeconfig or unrelated cluster objects.
    kubectl -n "$ns" get jobsets.jobset.x-k8s.io -o json |
      jq -c --arg intent "$intent" '[.items[] |
        select(.metadata.annotations["compute-control-panel.io/pd-intent-id"]==$intent) |
        {metadata:{name:.metadata.name,uid:.metadata.uid,annotations:.metadata.annotations,labels:.metadata.labels},spec}]' >&2 || true
    echo "FAIL: real PD JobSet writer failed" >&2
    exit 1
  fi
  sleep 2
done
[[ "$phase" == Succeeded ]] || { echo "FAIL: timed out waiting for PD JobSet writer" >&2; exit 1; }
result="$(kubectl -n "$ns" logs "$name")"
jq -e '
  .mode=="DISPOSABLE_REAL_JOBSET_API" and
  (.uid|type=="string" and length>0) and
  .provider_create_pinned==true and .same_uid_on_adapter_reconstruction==true and .durable_uid_pin_verified==false and
  .unbound_state=="OPERATION_IN_PROGRESS" and
  .lost_ack_requires_inspect==true and .one_gpu_per_replica==true and
  .physical_gpu_verified==false and .driver_prepared==false and
  .allocation_ready==false and .released==false and .cost_final==false
' <<< "$result" >/dev/null || { echo "FAIL: unverified writer output: $result" >&2; exit 1; }
uid="$(jq -er '.uid' <<< "$result")"

# Must be exactly one JobSet for this intent, one pinned UID and both phases;
# no privilege escalation, mutation or extra executable command in its Pods.
read_actual() {
  kubectl -n "$ns" get jobsets.jobset.x-k8s.io -o json |
    jq -cer --arg intent "$intent" '
      [.items[] | select(.metadata.annotations["compute-control-panel.io/pd-intent-id"]==$intent)] |
      if length==1 then .[0] else empty end'
}
actual="$(read_actual)" || { echo "FAIL: real writer did not create one JobSet" >&2; exit 1; }
jq -e --arg uid "$uid" --arg intent "$intent" '
  .metadata.uid==$uid and
  .metadata.annotations["compute-control-panel.io/pd-intent-id"]==$intent and
  .metadata.annotations["kueue.x-k8s.io/admission-gated-by"]=="compute-control-panel.io/pd-activation" and
  .spec.suspend==true and
  ([.spec.replicatedJobs[] | {name,replicas}] | sort_by(.name)) ==
    [{name:"decode",replicas:1},{name:"prefill",replicas:1}] and
  all(.spec.replicatedJobs[];
    (.template.spec.template.spec.containers|length)==1 and
    .template.spec.template.spec.containers[0].image=="registry.k8s.io/pause:3.10")
' <<< "$actual" >/dev/null || {
  echo "FAIL: real created JobSet is not exact inert two-phase design" >&2; exit 1
}
if ! kubectl -n "$ns" get pods -o json | jq -e --arg js "$(jq -er '.metadata.name' <<< "$actual")" '
  [.items[] | select(.metadata.labels["jobset.sigs.k8s.io/jobset-name"]==$js)] | length==0
' >/dev/null; then
  echo "FAIL: staging JobSet created an executable Pod" >&2
  exit 1
fi

# A missing durable UID pin cannot be fabricated from JobSet annotations.
echo "real_pd_jobset_writer=$result"
echo "PASS: actual staged PDJobSetAdapter POST+GET and exact-UID replay with unbound lost ACK UNKNOWN; in-memory pins only; process recovery and physical GPU readiness remain unproven"
