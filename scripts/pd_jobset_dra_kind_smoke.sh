#!/usr/bin/env bash
# Requires an existing disposable kind integration environment with JobSet
# v1alpha2, Kueue JobSet integration, DRA and DeviceClass mapping enabled.
# No physical GPU, driver preparation, or release evidence is asserted.
set -Eeuo pipefail
[[ "${CCP_DISPOSABLE_KIND_SMOKE:-}" == "1" ]] || { echo "disposable opt-in required" >&2; exit 2; }
[[ "$(kubectl config current-context)" == "kind-ccp-kueue-smoke" ]] || exit 2
for tool in kubectl jq; do command -v "$tool" >/dev/null || exit 2; done
kubectl get crd jobsets.jobset.x-k8s.io >/dev/null
kubectl get deviceclass gpu.example.com >/dev/null
kubectl get resourceflavor ccp-smoke >/dev/null

ns=ccp-pd-paired-smoke
q=ccp-pd-paired-smoke
resource=example.com/gpu
# create, not apply: never take over a pre-existing namespace or quota.
kubectl create -f - <<YAML
apiVersion: v1
kind: Namespace
metadata:
  name: $ns
  labels: {ccp-pd-paired-smoke: "true"}
---
apiVersion: kueue.x-k8s.io/v1beta1
kind: ClusterQueue
metadata: {name: $q}
spec:
  namespaceSelector:
    matchLabels: {ccp-pd-paired-smoke: "true"}
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
metadata: {name: $q, namespace: $ns}
spec: {clusterQueue: $q}
---
apiVersion: resource.k8s.io/v1
kind: ResourceClaimTemplate
metadata: {name: pd-gpu, namespace: $ns}
spec:
  spec:
    devices:
      requests:
      - name: gpu
        exactly: {deviceClassName: gpu.example.com, count: 1}
YAML

# One JobSet, two replicatedJobs, one per-Pod DRA claim each. Both
# requests must share one Kueue Workload; no two-job independent admission.
# The JobSet Deployment may be Available before the admission webhook has
# bound its listener. A CREATE may be retried only if a *specific* webhook
# transport failure proves admission never ran; never retry a stale, forbidden,
# schema-invalid, duplicate, or ambiguous CREATE outcome.
jobset_yaml="$(mktemp)"
jobset_err="$(mktemp)"
trap 'rm -f "$jobset_yaml" "$jobset_err"' EXIT
{
cat <<YAML
apiVersion: jobset.x-k8s.io/v1alpha2
kind: JobSet
metadata:
  name: pd-pair
  namespace: $ns
  labels:
    kueue.x-k8s.io/queue-name: $q
spec:
  suspend: true
  replicatedJobs:
YAML
for phase in prefill decode; do
cat <<YAML
  - name: $phase
    replicas: 1
    template:
      spec:
        parallelism: 1
        completions: 1
        backoffLimit: 0
        template:
          spec:
            restartPolicy: Never
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
              resourceClaimTemplateName: pd-gpu
YAML
done
} > "$jobset_yaml"
jobset_created=false
for attempt in $(seq 1 30); do
  if kubectl create -f "$jobset_yaml" 2>"$jobset_err"; then
    jobset_created=true
    break
  fi
  cat "$jobset_err" >&2
  # This exact failure is a webhook connection/readiness race, not a denial
  # or an uncertain HTTP 5xx from a webhook that may have created an object.
  if ! grep -Eq 'failed calling webhook "(mjobset|vjobset)\.kb\.io".*(connect: connection refused|no endpoints available)' "$jobset_err"; then
    echo "FAIL: JobSet CREATE was not a retryable webhook-readiness error" >&2
    exit 1
  fi
  # Even on a transport error, never issue another POST if a same-name
  # object might exist. Treat a failed or unauthorized GET as ambiguous.
  if existing="$(kubectl -n "$ns" get jobset pd-pair 2>&1)"; then
    echo "FAIL: JobSet already exists after failed CREATE; refusing duplicate" >&2
    exit 1
  fi
  if ! grep -Eqi 'NotFound|not found' <<< "$existing"; then
    echo "FAIL: could not prove JobSet absent after failed CREATE: $existing" >&2
    exit 1
  fi
  echo "JobSet webhook not listening; retrying verified-unapplied CREATE ($attempt/30)" >&2
  sleep 2
done
[[ "$jobset_created" == true ]] || { echo "FAIL: JobSet webhook never became ready" >&2; exit 1; }
uid="$(kubectl -n "$ns" get jobset pd-pair -o jsonpath='{.metadata.uid}')"
cq_uid="$(kubectl get clusterqueue "$q" -o jsonpath='{.metadata.uid}')"
[[ -n "$uid" && -n "$cq_uid" ]] || exit 1

read_wl() {
  kubectl -n "$ns" get workloads.kueue.x-k8s.io -o json | jq -cer --arg uid "$uid" '
    [.items[] | select(any(.metadata.ownerReferences[]?;
      .kind == "JobSet" and .apiVersion == "jobset.x-k8s.io/v1alpha2" and .uid == $uid and .controller == true))] |
    if length == 1 then .[0] else empty end'
}
wl=""
for _ in $(seq 1 90); do
  if wl="$(read_wl)" && jq -e --arg r "$resource" '
    (.spec.podSets | map({name,count}) | sort_by(.name)) ==
      [{"name":"decode","count":1},{"name":"prefill","count":1}] and
    .status.admission == null and
    any(.status.conditions[]?; .type == "QuotaReserved" and .status == "False" and
      ((.message // "") | contains($r)))' <<< "$wl" >/dev/null; then break; fi
  wl=""
  sleep 2
done
[[ -n "$wl" ]] || { echo "FAIL: paired request did not deny at one-device quota" >&2; exit 1; }
wl_uid="$(jq -er '.metadata.uid' <<< "$wl")"
# Denial must not start child Jobs/Pods or allocate Claims. This checks
# controller behavior, not merely the text of a Kueue condition.
if ! kubectl -n "$ns" get pods -o json | jq -e '.items | length == 0' >/dev/null; then
  echo "FAIL: denied paired JobSet created Pods" >&2; exit 1
fi
if ! kubectl -n "$ns" get resourceclaims.resource.k8s.io -o json | jq -e '.items | length == 0' >/dev/null; then
  echo "FAIL: denied paired JobSet created ResourceClaims" >&2; exit 1
fi
echo "paired_denial=$(jq -cn --arg uid "$uid" --arg wl "$wl_uid" '{jobset_uid:$uid,workload_uid:$wl,quota:1,admitted:false}')"

# Increase quota on this disposable, UID-pinned fixture only.
[[ "$(kubectl get clusterqueue "$q" -o jsonpath='{.metadata.uid}')" == "$cq_uid" ]] || exit 1
kubectl patch clusterqueue "$q" --type=json -p="[
  {\"op\":\"test\",\"path\":\"/metadata/uid\",\"value\":\"$cq_uid\"},
  {\"op\":\"test\",\"path\":\"/spec/resourceGroups/0/flavors/0/resources/2/name\",\"value\":\"$resource\"},
  {\"op\":\"replace\",\"path\":\"/spec/resourceGroups/0/flavors/0/resources/2/nominalQuota\",\"value\":\"2\"}
]"
wl=""
for _ in $(seq 1 90); do
  if wl="$(read_wl)" && jq -e --arg uid "$wl_uid" --arg q "$q" --arg r "$resource" '
    .metadata.uid == $uid and .status.admission.clusterQueue == $q and
    (.status.admission.podSetAssignments | map({name,count,charge:(.resourceUsage[$r]|tostring)}) | sort_by(.name)) ==
      [{"name":"decode","count":1,"charge":"1"},{"name":"prefill","count":1,"charge":"1"}] and
    any(.status.conditions[]?; .type == "QuotaReserved" and .status == "True") and
    any(.status.conditions[]?; .type == "Admitted" and .status == "True")' <<< "$wl" >/dev/null; then break; fi
  wl=""
  sleep 2
done
[[ -n "$wl" ]] || { echo "FAIL: paired request did not admit at two-device quota" >&2; exit 1; }
[[ "$(kubectl -n "$ns" get jobset pd-pair -o jsonpath='{.metadata.uid}')" == "$uid" ]] || exit 1
echo "paired_admission=$(jq -cn --arg js "$uid" --arg wl "$wl_uid" --argjson evidence "$wl" '{
  jobset_uid:$js,workload_uid:$wl,assignments:$evidence.status.admission.podSetAssignments,
  physical_gpu_verified:false,allocation_ready:false,release_verified:false,cost_final:false
}')"
echo "PASS: same paired workload denied at one-device quota and admitted with both phase reservations at two-device quota"
echo "Fixture retained for inspection; dispose of the dedicated kind cluster after collecting evidence."
