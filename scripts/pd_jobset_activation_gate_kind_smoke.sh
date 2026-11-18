#!/usr/bin/env bash
# Controller-level NEGATIVE/positive check for the exact PD JobSet
# admission-gated-by annotation used by PDJobSetAdapter. This script is
# executed ONLY after pd_jobset_dra_kind_smoke.sh in disposable kind.
# The feature gate must block quota reservation despite free capacity.
# It does not attest physical GPUs, Pod execution, or cost finality.
set -Eeuo pipefail
[[ "${CCP_DISPOSABLE_KIND_SMOKE:-}" == "1" ]] || { echo "disposable opt-in required" >&2; exit 2; }
[[ "$(kubectl config current-context)" == "kind-ccp-kueue-smoke" ]] || exit 2
for tool in kubectl jq; do command -v "$tool" >/dev/null || exit 2; done

ns=ccp-pd-paired-smoke
q=ccp-pd-paired-smoke
gate=compute-control-panel.io/pd-activation
name=pd-gated
resource=example.com/gpu

# Preserve namespace and cluster-queue identities across every mutation.
baseline_uid="$(kubectl -n "$ns" get jobset pd-pair -o jsonpath='{.metadata.uid}')"
queue_uid="$(kubectl get clusterqueue "$q" -o jsonpath='{.metadata.uid}')"
[[ -n "$baseline_uid" && -n "$queue_uid" ]] || exit 1
kubectl -n "$ns" get workloads.kueue.x-k8s.io -o json |
  jq -e --arg uid "$baseline_uid" --arg q "$q" '
    [.items[] | select(any(.metadata.ownerReferences[]?;
      .kind=="JobSet" and .uid==$uid and .controller==true))
      | select(.status.admission.clusterQueue==$q)] | length==1' >/dev/null || {
  echo "FAIL: baseline paired JobSet lost Kueue admission" >&2; exit 1
}

# Two additional GPUs are available WHILE the baseline two-phase JobSet
# still owns its reservation. The gate must block admission, not scarcity.
kubectl patch clusterqueue "$q" --type=json -p="[
  {\"op\":\"test\",\"path\":\"/metadata/uid\",\"value\":\"$queue_uid\"},
  {\"op\":\"test\",\"path\":\"/spec/resourceGroups/0/flavors/0/resources/2/name\",\"value\":\"$resource\"},
  {\"op\":\"test\",\"path\":\"/spec/resourceGroups/0/flavors/0/resources/2/nominalQuota\",\"value\":\"2\"},
  {\"op\":\"replace\",\"path\":\"/spec/resourceGroups/0/flavors/0/resources/2/nominalQuota\",\"value\":\"4\"}
]"
[[ "$(kubectl get clusterqueue "$q" -o jsonpath='{.metadata.uid}')" == "$queue_uid" ]] || exit 1

# Enforce separate Kubernetes identities rather than relying on annotation
# alone. Staging can CREATE/GET JobSets but cannot PATCH, UPDATE or DELETE;
# activation is limited to GET/PATCH of one exact JobSet name. A production
# rollout still needs an independent permit verifier/admission controller
# because RBAC cannot restrict which annotation/spec field a PATCH changes.
stage_actor="system:serviceaccount:$ns:ccp-pd-staging"
activation_actor="system:serviceaccount:$ns:ccp-pd-activator"
kubectl create -f - <<YAML
apiVersion: v1
kind: ServiceAccount
metadata: {name: ccp-pd-staging, namespace: $ns}
---
apiVersion: v1
kind: ServiceAccount
metadata: {name: ccp-pd-activator, namespace: $ns}
---
apiVersion: rbac.authorization.k8s.io/v1
kind: Role
metadata: {name: ccp-pd-staging, namespace: $ns}
rules:
- apiGroups: [jobset.x-k8s.io]
  resources: [jobsets]
  verbs: [get, create]
---
apiVersion: rbac.authorization.k8s.io/v1
kind: RoleBinding
metadata: {name: ccp-pd-staging, namespace: $ns}
roleRef: {apiGroup: rbac.authorization.k8s.io, kind: Role, name: ccp-pd-staging}
subjects:
- kind: ServiceAccount
  name: ccp-pd-staging
  namespace: $ns
---
apiVersion: rbac.authorization.k8s.io/v1
kind: Role
metadata: {name: ccp-pd-activator, namespace: $ns}
rules:
- apiGroups: [jobset.x-k8s.io]
  resources: [jobsets]
  resourceNames: [$name]
  verbs: [get, patch]
---
apiVersion: rbac.authorization.k8s.io/v1
kind: RoleBinding
metadata: {name: ccp-pd-activator, namespace: $ns}
roleRef: {apiGroup: rbac.authorization.k8s.io, kind: Role, name: ccp-pd-activator}
subjects:
- kind: ServiceAccount
  name: ccp-pd-activator
  namespace: $ns
YAML

for verb in get create; do
  [[ "$(kubectl auth can-i "$verb" jobsets.jobset.x-k8s.io -n "$ns" --as="$stage_actor")" == "yes" ]] || {
    echo "FAIL: staging identity cannot $verb JobSets" >&2; exit 1
  }
done
for verb in update patch delete; do
  [[ "$(kubectl auth can-i "$verb" jobsets.jobset.x-k8s.io -n "$ns" --as="$stage_actor")" == "no" ]] || {
    echo "FAIL: staging identity has forbidden $verb authority" >&2; exit 1
  }
done
for kind in pods resourceclaims.resource.k8s.io; do
  [[ "$(kubectl auth can-i create "$kind" -n "$ns" --as="$stage_actor")" == "no" ]] || {
    echo "FAIL: staging identity can create $kind directly" >&2; exit 1
  }
done
for verb in get patch; do
  [[ "$(kubectl auth can-i "$verb" "jobsets.jobset.x-k8s.io/$name" -n "$ns" --as="$activation_actor")" == "yes" ]] || {
    echo "FAIL: scoped activation identity cannot $verb its JobSet" >&2; exit 1
  }
done
for verb in create update delete; do
  [[ "$(kubectl auth can-i "$verb" "jobsets.jobset.x-k8s.io/$name" -n "$ns" --as="$activation_actor")" == "no" ]] || {
    echo "FAIL: activation identity has forbidden $verb authority" >&2; exit 1
  }
done
[[ "$(kubectl auth can-i patch jobsets.jobset.x-k8s.io/pd-pair -n "$ns" --as="$activation_actor")" == "no" ]] || {
  echo "FAIL: activation identity can patch another JobSet" >&2; exit 1
}

# The second JobSet intentionally copies the production writer's critical
# gate, whole two-phase pod-set shape and inert, non-executable container.
cat <<YAML | kubectl create --as="$stage_actor" -f -
apiVersion: jobset.x-k8s.io/v1alpha2
kind: JobSet
metadata:
  name: $name
  namespace: $ns
  labels:
    kueue.x-k8s.io/queue-name: $q
  annotations:
    kueue.x-k8s.io/admission-gated-by: "$gate"
spec:
  suspend: true
  replicatedJobs:
  - name: prefill
    replicas: 1
    template:
      spec:
        parallelism: 1
        completions: 1
        backoffLimit: 0
        template:
          spec:
            restartPolicy: Never
            nodeSelector: {ccp-dra-quota-test/unplaced: "true"}
            containers:
            - name: consumer
              image: registry.k8s.io/pause:3.10
              resources:
                requests: {cpu: 100m, memory: 32Mi}
                claims: [{name: gpu}]
            resourceClaims:
            - {name: gpu, resourceClaimTemplateName: pd-gpu}
  - name: decode
    replicas: 1
    template:
      spec:
        parallelism: 1
        completions: 1
        backoffLimit: 0
        template:
          spec:
            restartPolicy: Never
            nodeSelector: {ccp-dra-quota-test/unplaced: "true"}
            containers:
            - name: consumer
              image: registry.k8s.io/pause:3.10
              resources:
                requests: {cpu: 100m, memory: 32Mi}
                claims: [{name: gpu}]
            resourceClaims:
            - {name: gpu, resourceClaimTemplateName: pd-gpu}
YAML

gate_uid="$(kubectl -n "$ns" get jobset "$name" -o jsonpath='{.metadata.uid}')"
[[ -n "$gate_uid" ]] || exit 1
read_gate_workload() {
  kubectl -n "$ns" get workloads.kueue.x-k8s.io -o json |
    jq -cer --arg uid "$gate_uid" '
      [.items[] | select(any(.metadata.ownerReferences[]?;
        .apiVersion=="jobset.x-k8s.io/v1alpha2" and .kind=="JobSet" and
        .uid==$uid and .controller==true))] |
      if length==1 then .[0] else empty end'
}

# The quota remains sufficient for another 2-device Workload. Even if a
# Workload object is not created while gated, it must NOT reserve any quota,
# unsuspend the JobSet, or cause child Pods.
gated_workload_uid=""
for _ in $(seq 1 20); do
  if wl="$(read_gate_workload)"; then
    gated_workload_uid="$(jq -er '.metadata.uid' <<< "$wl")"
    if jq -e '(.status.admission != null) or
       any(.status.conditions[]?; .type=="QuotaReserved" and .status=="True")' <<< "$wl" >/dev/null; then
      echo "FAIL: activation-gated JobSet reserved quota before approval" >&2
      exit 1
    fi
  fi
  if [[ "$(kubectl -n "$ns" get jobset "$name" -o jsonpath='{.spec.suspend}')" != "true" ]]; then
    echo "FAIL: activation-gated JobSet was unsuspended" >&2; exit 1
  fi
  if ! kubectl -n "$ns" get pods -o json |
    jq -e --arg name "$name" '
      [.items[] | select(.metadata.labels["jobset.sigs.k8s.io/jobset-name"]==$name)] | length==0' >/dev/null; then
    echo "FAIL: activation-gated JobSet created a Pod" >&2; exit 1
  fi
  sleep 1
done
echo "activation_gate_blocked=$(jq -cn --arg id "$gate_uid" --arg wl "$gated_workload_uid" '{jobset_uid:$id,workload_uid:$wl,quota_available:2,reserved:false,jobset_suspended:true,physical_gpu_verified:false}')"

# Only the independent activation authority can remove this gate in
# production. In this disposable test the privileged fixture removes the
# exact annotation with UID and old-value JSON preconditions.
patch="$(jq -nc --arg uid "$gate_uid" --arg gate "$gate" '[
  {op:"test",path:"/metadata/uid",value:$uid},
  {op:"test",path:"/metadata/annotations/kueue.x-k8s.io~1admission-gated-by",value:$gate},
  {op:"remove",path:"/metadata/annotations/kueue.x-k8s.io~1admission-gated-by"}]')"
# An unauthorized staging actor must fail at the API authorization boundary
# without mutating the live JobSet, even with an otherwise valid UID patch.
if rejected="$(kubectl -n "$ns" patch jobset "$name" --as="$stage_actor" --type=json -p="$patch" 2>&1)"; then
  echo "FAIL: staging identity removed the activation gate" >&2; exit 1
fi
if ! grep -qi 'forbidden' <<< "$rejected"; then
  echo "FAIL: staging PATCH failed for a reason other than RBAC: $rejected" >&2; exit 1
fi
if ! kubectl -n "$ns" get jobset "$name" -o json | jq -e --arg gate "$gate" '.metadata.annotations["kueue.x-k8s.io/admission-gated-by"] == $gate' >/dev/null; then
  echo "FAIL: denied staging PATCH still removed the gate" >&2; exit 1
fi
# This fixture impersonates the narrowly scoped activation identity. It does
# NOT impersonate a production policy/permit verifier, which remains a gate.
kubectl -n "$ns" patch jobset "$name" --as="$activation_actor" --type=json -p="$patch"
[[ "$(kubectl -n "$ns" get jobset "$name" -o jsonpath='{.metadata.uid}')" == "$gate_uid" ]] || exit 1

released_workload=""
for _ in $(seq 1 90); do
  if wl="$(read_gate_workload)" && jq -e --arg q "$q" --arg r "$resource" '
    .status.admission.clusterQueue==$q and
    (.status.admission.podSetAssignments |
      map({name,count,charge:(.resourceUsage[$r]|tostring)}) | sort_by(.name)) ==
      [{"name":"decode","count":1,"charge":"1"},{"name":"prefill","count":1,"charge":"1"}] and
    any(.status.conditions[]?; .type=="QuotaReserved" and .status=="True") and
    any(.status.conditions[]?; .type=="Admitted" and .status=="True")' <<< "$wl" >/dev/null; then
    released_workload="$wl"
    break
  fi
  sleep 2
done
[[ -n "$released_workload" ]] || { echo "FAIL: approved JobSet never admitted both phases" >&2; exit 1; }
if [[ -n "$gated_workload_uid" && "$(jq -er '.metadata.uid' <<< "$released_workload")" != "$gated_workload_uid" ]]; then
  echo "FAIL: Workload UID replaced while removing activation gate" >&2; exit 1
fi
echo "activation_gate_released=$(jq -cn --arg js "$gate_uid" --argjson wl "$released_workload" '{
  jobset_uid:$js,workload_uid:$wl.metadata.uid,admitted:true,
  assignments:$wl.status.admission.podSetAssignments,
  physical_gpu_verified:false,allocation_ready:false,cost_final:false
}')"
echo "PASS: full paired GPU quota available yet blocked by activation gate; same JobSet allowed only after exact gate removal"
