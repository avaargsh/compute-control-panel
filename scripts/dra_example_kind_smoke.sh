#!/usr/bin/env bash
# Independent DRA simulated-device smoke inside the existing disposable kind.
# Proves real Kubernetes DRA API allocation + kubelet example-driver injection.
# Does NOT prove physical GPU, Kueue quota for THIS Pod, or CCP RELEASED.
set -Eeuo pipefail

[[ "${CCP_DISPOSABLE_KIND_SMOKE:-}" == "1" ]] || { echo "disposable opt-in required" >&2; exit 2; }
[[ "$(kubectl config current-context)" == "kind-ccp-kueue-smoke" ]] || {
  echo "refusing to use a non-disposable Kubernetes context" >&2; exit 2
}
for tool in kubectl jq; do command -v "$tool" >/dev/null || exit 2; done

ns="ccp-dra-smoke"
claim="ccp-example-gpu"
pod="ccp-dra-consumer"
# v0.2.0 resolved by real kind/Kubelet imageID at PR head 67ee16dc.
# Digest pins the exact simulated driver artifact; this is NOT a real GPU driver.
driver_image="registry.k8s.io/dra-example-driver/dra-example-driver@sha256:6fd370402b8f54e0b57b251414f1692a9252f1e6edaf1fc3305b8ba66bf12e99"
failure() {
  rc=$?
  echo "simulated DRA smoke failed (status=$rc)" >&2
  kubectl -n "$ns" get pods,resourceclaims -o wide >&2 || true
  kubectl -n "$ns" describe pod "$pod" >&2 || true
  kubectl -n "$ns" logs daemonset/dra-example-driver-kubeletplugin --tail=80 >&2 || true
  kubectl get resourceslices -o wide >&2 || true
  return "$rc"
}
trap failure ERR
kubectl create namespace "$ns"

# Official Kubernetes DRA tutorial example-driver v0.2.0, scoped to kind only.
# Names/permissions follow the upstream tutorial; it simulates, not drives, GPUs.
kubectl apply --server-side -f - <<YAML
apiVersion: resource.k8s.io/v1
kind: DeviceClass
metadata:
  name: gpu.example.com
spec:
  selectors:
  - cel:
      expression: "device.driver == 'gpu.example.com'"
---
apiVersion: v1
kind: ServiceAccount
metadata:
  name: dra-example-driver-service-account
  namespace: $ns
---
apiVersion: rbac.authorization.k8s.io/v1
kind: ClusterRole
metadata:
  name: ccp-dra-example-driver
rules:
- apiGroups: ["resource.k8s.io"]
  resources: ["resourceclaims"]
  verbs: ["get"]
- apiGroups: [""]
  resources: ["nodes"]
  verbs: ["get"]
- apiGroups: ["resource.k8s.io"]
  resources: ["resourceslices"]
  verbs: ["get", "list", "watch", "create", "update", "patch", "delete"]
---
apiVersion: rbac.authorization.k8s.io/v1
kind: ClusterRoleBinding
metadata:
  name: ccp-dra-example-driver
subjects:
- kind: ServiceAccount
  name: dra-example-driver-service-account
  namespace: $ns
roleRef:
  kind: ClusterRole
  name: ccp-dra-example-driver
  apiGroup: rbac.authorization.k8s.io
---
apiVersion: scheduling.k8s.io/v1
kind: PriorityClass
metadata:
  name: ccp-dra-driver-high-priority
value: 1000000
globalDefault: false
description: "Disposable kind simulated DRA driver only"
---
apiVersion: apps/v1
kind: DaemonSet
metadata:
  name: dra-example-driver-kubeletplugin
  namespace: $ns
spec:
  selector:
    matchLabels: {app.kubernetes.io/name: dra-example-driver}
  template:
    metadata:
      labels: {app.kubernetes.io/name: dra-example-driver}
    spec:
      priorityClassName: ccp-dra-driver-high-priority
      serviceAccountName: dra-example-driver-service-account
      tolerations:
      - operator: Exists
      containers:
      - name: plugin
        image: $driver_image
        imagePullPolicy: IfNotPresent
        command: ["dra-example-kubeletplugin"]
        securityContext: {privileged: true}
        env:
        - {name: CDI_ROOT, value: /var/run/cdi}
        - {name: KUBELET_REGISTRAR_DIRECTORY_PATH, value: /var/lib/kubelet/plugins_registry}
        - {name: KUBELET_PLUGINS_DIRECTORY_PATH, value: /var/lib/kubelet/plugins}
        - name: NODE_NAME
          valueFrom:
            fieldRef: {fieldPath: spec.nodeName}
        - name: NAMESPACE
          valueFrom:
            fieldRef: {fieldPath: metadata.namespace}
        - {name: NUM_DEVICES, value: "3"}
        - {name: HEALTHCHECK_PORT, value: "51515"}
        volumeMounts:
        - {name: plugins-registry, mountPath: /var/lib/kubelet/plugins_registry}
        - {name: plugins, mountPath: /var/lib/kubelet/plugins}
        - {name: cdi, mountPath: /var/run/cdi}
      volumes:
      - name: plugins-registry
        hostPath: {path: /var/lib/kubelet/plugins_registry}
      - name: plugins
        hostPath: {path: /var/lib/kubelet/plugins}
      - name: cdi
        hostPath: {path: /var/run/cdi}
YAML

kubectl -n "$ns" rollout status daemonset/dra-example-driver-kubeletplugin --timeout=240s
# Record the *actual* runtime image digest, not only the mutable v0.2.0 tag.
driver_image_id="$(kubectl -n "$ns" get pods -l app.kubernetes.io/name=dra-example-driver -o json |
  jq -er '[.items[].status.containerStatuses[]? | select(.name == "plugin") | .imageID] |
    if length == 1 then .[0] else empty end')"
[[ "$driver_image_id" == "$driver_image" ]] || { echo "driver runtime image differs from pinned digest: $driver_image_id" >&2; exit 1; }
echo "simulated_dra_driver_image_id=$driver_image_id"
slice_json=""
for _ in $(seq 1 90); do
  slice_json="$(kubectl get resourceslices -o json | jq -ce '
    [.items[] | select(.spec.driver == "gpu.example.com" and
      (.metadata.uid | type == "string" and length > 0) and
      (.metadata.resourceVersion | type == "string" and length > 0) and
      (.spec.devices | type == "array" and length > 0))] |
    if length == 1 then .[0] else empty end' || true)"
  [[ -n "$slice_json" ]] && break
  sleep 2
done
[[ -n "$slice_json" ]] || { echo "no unique real ResourceSlice from example driver" >&2; exit 1; }
slice_uid="$(printf '%s\n' "$slice_json" | jq -r .metadata.uid)"
echo "simulated_dra_resource_slice=$(printf '%s\n' "$slice_json" | jq -c '{uid:.metadata.uid,rv:.metadata.resourceVersion,driver:.spec.driver,pool:.spec.pool.name,node:.spec.nodeName,deviceNames:[.spec.devices[].name]}')"

kubectl apply --server-side -f - <<YAML
apiVersion: resource.k8s.io/v1
kind: ResourceClaim
metadata:
  name: $claim
  namespace: $ns
spec:
  devices:
    requests:
    - name: gpu
      exactly:
        deviceClassName: gpu.example.com
        count: 1
---
apiVersion: v1
kind: Pod
metadata:
  name: $pod
  namespace: $ns
spec:
  restartPolicy: Never
  containers:
  - name: consumer
    image: ubuntu:24.04
    command: ["/bin/sh", "-c"]
    args: ["env | sort; sleep 600"]
    resources:
      claims:
      - name: gpu
  resourceClaims:
  - name: gpu
    resourceClaimName: $claim
YAML
kubectl -n "$ns" wait --for=condition=Ready "pod/$pod" --timeout=240s
pod_json="$(kubectl -n "$ns" get pod "$pod" -o json)"
pod_uid="$(printf '%s\n' "$pod_json" | jq -er '.metadata.uid')"
printf '%s\n' "$pod_json" | jq -e '
  (.metadata.resourceVersion | type == "string" and length > 0) and
  (.spec.nodeName | type == "string" and length > 0) and
  any(.status.conditions[]?; .type == "PodScheduled" and .status == "True")' >/dev/null

claim_json="$(kubectl -n "$ns" get resourceclaim "$claim" -o json)"
printf '%s\n' "$claim_json" | jq -e --arg uid "$pod_uid" '
  (.metadata.uid | type == "string" and length > 0) and
  (.metadata.resourceVersion | type == "string" and length > 0) and
  (.status.allocation.devices.results | type == "array" and length == 1) and
  any(.status.allocation.devices.results[]; .driver == "gpu.example.com" and
      (.pool | type == "string" and length > 0) and
      (.device | type == "string" and length > 0)) and
  any(.status.reservedFor[]?; .uid == $uid)' >/dev/null
claim_uid="$(printf '%s\n' "$claim_json" | jq -er .metadata.uid)"
device_id="$(printf '%s\n' "$claim_json" | jq -er '.status.allocation.devices.results[0].device')"
claim_pool="$(printf '%s\n' "$claim_json" | jq -er '.status.allocation.devices.results[0].pool')"
# Re-list the source API after allocation. A Claim result is valid only if its
# exact driver/pool/device still belongs to the earlier UID-pinned ResourceSlice.
fresh_slice="$(kubectl get resourceslices -o json | jq -ce \
  --arg uid "$slice_uid" --arg pool "$claim_pool" --arg device "$device_id" '
  [.items[] | select(.metadata.uid == $uid and
    .spec.driver == "gpu.example.com" and .spec.pool.name == $pool and
    any(.spec.devices[]?; .name == $device))] |
  if length == 1 then .[0] else empty end')"
[[ -n "$fresh_slice" ]] || { echo "DRA Claim device disagrees with pinned ResourceSlice" >&2; exit 1; }
container_log="$(kubectl -n "$ns" logs "$pod" -c consumer)"
printf '%s\n' "$container_log" | grep -Eq '^GPU_DEVICE_[0-9]+=' || {
  echo "DRA allocation exists but simulated kubelet/CDI device injection is unproven" >&2; exit 1
}
echo "simulated_dra_assigned=$(jq -cn --arg pod "$pod_uid" --arg claim "$claim_uid" --arg slice "$slice_uid" --arg device "$device_id" --arg image "$driver_image_id" '{podUID:$pod,claimUID:$claim,resourceSliceUID:$slice,deviceID:$device,driver:"gpu.example.com",driverImageID:$image,physicalGPU:false,kueueQuotaLinked:false,allocationReady:false}')"

# Deleting the Pod/Claim is fixture teardown; API absence does NOT prove the
# driver independently reported physical deallocation or authorize RELEASED.
kubectl -n "$ns" delete pod "$pod" --wait=true
kubectl -n "$ns" get pods -o json | jq -e --arg uid "$pod_uid" 'all(.items[]; .metadata.uid != $uid)' >/dev/null
kubectl -n "$ns" delete resourceclaim "$claim" --wait=true
kubectl -n "$ns" get resourceclaims -o json | jq -e --arg uid "$claim_uid" 'all(.items[]; .metadata.uid != $uid)' >/dev/null
echo 'simulated_dra_cleanup={"podUIDAbsent":true,"claimUIDAbsent":true,"deviceReleaseAttested":false,"allocationReleased":false}'
echo "PASS: real DRA ResourceSlice + ResourceClaim + kubelet mock-device preparation; NO physical GPU/Kueue quota or Provider three-way release finality"
