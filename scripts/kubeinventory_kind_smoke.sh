#!/usr/bin/env bash
# Real Kubernetes Node inventory acceptance in a disposable kind cluster.
# L2 proof only: no GPU device, DRA claim or physical allocation evidence.
set -Eeuo pipefail

if [[ "${CCP_DISPOSABLE_KIND_SMOKE:-}" != "1" ]]; then
  echo "CCP_DISPOSABLE_KIND_SMOKE=1 is required" >&2
  exit 2
fi
for tool in kind kubectl go git base64; do
  command -v "$tool" >/dev/null || { echo "required tool unavailable: $tool" >&2; exit 2; }
done

source_commit="$(git rev-parse --verify HEAD)"
[[ "$source_commit" =~ ^[a-f0-9]{40}$ ]] || { echo "invalid source HEAD" >&2; exit 2; }
printf 'CCP inventory L2 source SHA: %s\n' "$source_commit"

dir="$(mktemp -d)"
chmod 0700 "$dir"
export KUBECONFIG="$dir/kubeconfig"
name="ccp-inventory-smoke"
namespace="ccp-inventory-smoke"
image="kindest/node:v1.34.0@sha256:7416a61b42b1662ca6ca89f02028ac133a309a2a30ba309614e8ec94d976dc5a"
created=false

cleanup() {
  status=$?
  trap - EXIT
  if [[ "$created" == true ]]; then
    kind delete cluster --name "$name" >/dev/null 2>&1 || true
  fi
  rm -rf "$dir"
  exit "$status"
}
trap cleanup EXIT

# The integration suite must never run against an existing user's cluster.
if kind get clusters | grep -Fxq "$name"; then
  echo "refusing to reuse an existing kind cluster" >&2
  exit 2
fi
created=true
kind create cluster --name "$name" --image "$image" --wait 180s
[[ "$(kubectl config current-context)" == "kind-$name" ]] || {
  echo "unexpected Kubernetes context" >&2; exit 2;
}

kubectl create namespace "$namespace"
kubectl apply -f - <<YAML
apiVersion: v1
kind: ServiceAccount
metadata:
  name: inventory-allowed
  namespace: $namespace
---
apiVersion: v1
kind: ServiceAccount
metadata:
  name: inventory-denied
  namespace: $namespace
---
apiVersion: rbac.authorization.k8s.io/v1
kind: ClusterRole
metadata:
  name: ccp-inventory-smoke-node-list
rules:
- apiGroups: [""]
  resources: ["nodes"]
  verbs: ["list"]
---
apiVersion: rbac.authorization.k8s.io/v1
kind: ClusterRoleBinding
metadata:
  name: ccp-inventory-smoke-node-list
roleRef:
  apiGroup: rbac.authorization.k8s.io
  kind: ClusterRole
  name: ccp-inventory-smoke-node-list
subjects:
- kind: ServiceAccount
  name: inventory-allowed
  namespace: $namespace
YAML

allow="system:serviceaccount:$namespace:inventory-allowed"
deny="system:serviceaccount:$namespace:inventory-denied"

# Inspect the *real* authorizer. List is the only resource privilege granted.
[[ "$(kubectl auth can-i list nodes --as="$allow")" == "yes" ]] || {
  echo "positive list grant missing" >&2; exit 1;
}
[[ "$(kubectl auth can-i list nodes --as="$deny")" == "no" ]] || {
  echo "negative list grant unexpectedly present" >&2; exit 1;
}
for verb in get watch create update patch delete; do
  [[ "$(kubectl auth can-i "$verb" nodes --as="$allow")" == "no" ]] || {
    echo "inventory-allowed unexpectedly has $verb node permission" >&2; exit 1;
  }
done
for verb in create update patch delete; do
  [[ "$(kubectl auth can-i "$verb" pods --as="$allow" -n "$namespace")" == "no" ]] || {
    echo "inventory-allowed unexpectedly has $verb pod permission" >&2; exit 1;
  }
done

# TokenRequest credentials are short-lived. Never display or commit their bytes.
umask 077
kubectl -n "$namespace" create token inventory-allowed --duration=10m > "$dir/allow.token"
kubectl -n "$namespace" create token inventory-denied --duration=10m > "$dir/deny.token"
kubectl config view --raw --minify -o jsonpath='{.clusters[0].cluster.certificate-authority-data}' | base64 --decode > "$dir/ca.pem"
server="$(kubectl config view --raw --minify -o jsonpath='{.clusters[0].cluster.server}')"
[[ "$server" == https://* && -s "$dir/ca.pem" && -s "$dir/allow.token" && -s "$dir/deny.token" ]] || {
  echo "missing HTTPS server, CA or requested SA token" >&2; exit 1;
}

# Resolve the real kind kubelet Lease name, never synthesize a Lease.
node_name="$(kubectl get nodes -o jsonpath='{.items[0].metadata.name}')"
[[ -n "$node_name" ]] || { echo "no kind Node name available" >&2; exit 1; }
lease_present=false
for attempt in {1..30}; do
  if kubectl -n kube-node-lease get lease "$node_name" >/dev/null 2>&1; then
    lease_present=true
    break
  fi
  sleep 1
done
[[ "$lease_present" == true ]] || { echo "kind kubelet Lease not available" >&2; exit 1; }

# Verify the baseline 403 with the REAL Token before installing the Role.
# A simulated SubjectAccessReview is not a substitute for the API response.
CCP_KIND_INVENTORY_SMOKE=1 \
CCP_KIND_LEASE_PREGRANT=1 \
CCP_KIND_LEASE_NODE_NAME="$node_name" \
CCP_KIND_API_SERVER="$server" \
CCP_KIND_CA_FILE="$dir/ca.pem" \
CCP_KIND_ALLOWED_TOKEN_FILE="$dir/allow.token" \
GOTOOLCHAIN=local \
  go test -count=1 -timeout=2m -run 'TestKindLeaseBeforeGrantIsForbidden' -v ./internal/kubeinventory

# Only this disposable kind cluster receives a Lease GET Role/RoleBinding.
kubectl apply -f - <<YAML
apiVersion: rbac.authorization.k8s.io/v1
kind: Role
metadata:
  name: ccp-inventory-smoke-lease-get
  namespace: kube-node-lease
rules:
- apiGroups: ["coordination.k8s.io"]
  resources: ["leases"]
  resourceNames: ["$node_name"]
  verbs: ["get"]
---
apiVersion: rbac.authorization.k8s.io/v1
kind: RoleBinding
metadata:
  name: ccp-inventory-smoke-lease-get
  namespace: kube-node-lease
roleRef:
  apiGroup: rbac.authorization.k8s.io
  kind: Role
  name: ccp-inventory-smoke-lease-get
subjects:
- kind: ServiceAccount
  name: inventory-allowed
  namespace: $namespace
YAML

# After binding, the real-token Go test verifies the exact-name GET succeeds,
# while other-name and separately denied ServiceAccount GETs return 403.
# Keep these authorizer assertions for the non-GET verbs as defense in depth.
for verb in list watch create update patch delete; do
  [[ "$(kubectl auth can-i "$verb" "$lease_resource" --as="$allow" -n kube-node-lease)" == "no" ]] || {
    echo "Lease $verb permission exceeds read-only named GET" >&2; exit 1
  }
done

CCP_KIND_INVENTORY_SMOKE=1 \
CCP_KIND_LEASE_NODE_NAME="$node_name" \
CCP_KIND_API_SERVER="$server" \
CCP_KIND_CA_FILE="$dir/ca.pem" \
CCP_KIND_ALLOWED_TOKEN_FILE="$dir/allow.token" \
CCP_KIND_DENIED_TOKEN_FILE="$dir/deny.token" \
GOTOOLCHAIN=local \
  go test -count=1 -timeout=3m -run 'TestKind(InventoryLeastPrivilege|LeaseGetLeastPrivilege)' -v ./internal/kubeinventory

echo "L2 real Node inventory and exact Node Lease source acceptance for $source_commit"
echo "No device plugin, GPU allocation or L3 hardware evidence tested"
