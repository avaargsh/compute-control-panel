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

CCP_KIND_INVENTORY_SMOKE=1 \
CCP_KIND_API_SERVER="$server" \
CCP_KIND_CA_FILE="$dir/ca.pem" \
CCP_KIND_ALLOWED_TOKEN_FILE="$dir/allow.token" \
CCP_KIND_DENIED_TOKEN_FILE="$dir/deny.token" \
GOTOOLCHAIN=local \
  go test -count=1 -timeout=3m -run '^TestKindInventoryLeastPrivilege$' -v ./internal/kubeinventory

echo "L2 Kubernetes node inventory read and denial accepted for $source_commit"
echo "No GPU device plugin, device allocation or L3 hardware evidence was tested"
