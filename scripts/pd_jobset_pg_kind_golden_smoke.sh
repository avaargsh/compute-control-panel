#!/usr/bin/env bash
# Real cross-service (PG16 + Kueue/JobSet in disposable Kind) acceptance.
# This deliberately exercises a fixture-only previous Allocation release.
# It does not prove physical GPU allocation/release or execute a PD workload.
set -Eeuo pipefail
[[ "${CCP_DISPOSABLE_KIND_SMOKE:-}" == 1 ]] || exit 2
[[ "$(kubectl config current-context)" == kind-ccp-kueue-smoke ]] || exit 2
for tool in kubectl jq go; do command -v "$tool" >/dev/null || exit 2; done
for key in CCP_PD_KIND_ADMIN_DSN CCP_PD_KIND_BINDING_DSN CCP_PD_KIND_API_URL CCP_PD_KIND_CA_PATH; do
  [[ -n "${!key:-}" ]] || { echo "missing $key (disposable fixture)" >&2; exit 2; }
done
ns=ccp-pd-paired-smoke
actor="system:serviceaccount:$ns:ccp-pd-staging"
[[ "$(kubectl auth can-i create jobsets.jobset.x-k8s.io -n "$ns" --as="$actor")" == yes ]] || exit 2
[[ "$(kubectl auth can-i get jobsets.jobset.x-k8s.io -n "$ns" --as="$actor")" == yes ]] || exit 2
for verb in patch update delete; do
  [[ "$(kubectl auth can-i "$verb" jobsets.jobset.x-k8s.io -n "$ns" --as="$actor")" == no ]] || {
    echo "FAIL: staging ServiceAccount can $verb JobSets" >&2; exit 1;
  }
done

tmp="$(mktemp -d)"
trap 'rm -rf "$tmp"' EXIT
(umask 077; kubectl -n "$ns" create token ccp-pd-staging --duration=15m >"$tmp/token")
[[ -s "$tmp/token" && -s "$CCP_PD_KIND_CA_PATH" ]] || exit 1
receipt="$tmp/receipt.json"
go test -c -o "$tmp/pd-stage-process.test" ./internal/pdstaging

# The test reuses a signed synthetic PD fixture, canonical PG
# Promotion/Operation journal, actual K8s API with only staging permissions,
# restricted and catalog-attested PostgreSQL binding login, and Inspect-first.
DATABASE_URL="$CCP_PD_KIND_ADMIN_DSN" \
CCP_PD_KIND_BINDING_DSN="$CCP_PD_KIND_BINDING_DSN" \
CCP_PD_KIND_API_URL="$CCP_PD_KIND_API_URL" \
CCP_PD_KIND_CA_PATH="$CCP_PD_KIND_CA_PATH" \
CCP_PD_KIND_TOKEN_PATH="$tmp/token" \
CCP_PD_KIND_NAMESPACE="$ns" \
CCP_PD_KIND_RECEIPT_PATH="$receipt" \
CCP_PD_KIND_STAGE_BINARY="$tmp/pd-stage-process.test" \
CCP_DISPOSABLE_KIND_SMOKE=1 \
  go test -count=1 -timeout 90s -run '^TestPDPostgresKubeJobSetGolden$' -v ./internal/store/postgres

[[ -s "$receipt" ]] || { echo "FAIL: cross-service Go test skipped/no evidence" >&2; exit 1; }
jq -e '
  (.intent_id|type=="string" and length==64) and
  (.uid|type=="string" and length>0) and
  .operation_status=="SUCCEEDED" and .allocation_phase=="PENDING" and
  .lost_ack_unknown==true and .actual_sigkill==true and .canonical_stage==true and .inspect_first==true and
  .physical_gpu_verified==false and .driver_prepared==false and .cost_final==false
' "$receipt" >/dev/null || { echo "FAIL: untrusted cross-service receipt" >&2; exit 1; }

intent="$(jq -er '.intent_id' "$receipt")"
uid="$(jq -er '.uid' "$receipt")"
actual="$(kubectl -n "$ns" get jobsets.jobset.x-k8s.io -o json |
  jq -cer --arg intent "$intent" '
    [.items[] | select(.metadata.annotations["compute-control-panel.io/pd-intent-id"]==$intent)] |
    if length==1 then .[0] else empty end')" || {
  echo "FAIL: expected exactly one UID-pinned real JobSet" >&2; exit 1;
}
jq -e --arg uid "$uid" --arg intent "$intent" '
  .metadata.uid==$uid and
  .metadata.annotations["compute-control-panel.io/pd-intent-id"]==$intent and
  .metadata.annotations["kueue.x-k8s.io/admission-gated-by"]=="compute-control-panel.io/pd-activation" and
  .spec.suspend==true and
  ([.spec.replicatedJobs[] | {name,replicas}] | sort_by(.name)) ==
    [{"name":"decode","replicas":1},{"name":"prefill","replicas":1}] and
  all(.spec.replicatedJobs[];
    .template.spec.template.spec.containers|length==1) and
  all(.spec.replicatedJobs[];
    .template.spec.template.spec.containers[0].image=="registry.k8s.io/pause:3.10") and
  all(.spec.replicatedJobs[];
    .template.spec.template.spec.resourceClaims[0].resourceClaimTemplateName=="pd-gpu")
' <<< "$actual" >/dev/null || {
  echo "FAIL: signed PD staged JobSet has unsafe or changed shape" >&2; exit 1;
}
jsname="$(jq -er '.metadata.name' <<< "$actual")"
kubectl -n "$ns" get pods -o json |
  jq -e --arg js "$jsname" '[.items[] |
    select(.metadata.labels["jobset.sigs.k8s.io/jobset-name"]==$js)] | length==0' >/dev/null || {
  echo "FAIL: gated JobSet created runnable Pods before approval" >&2; exit 1;
}
echo "pd_pg_kube_golden=$(jq -c . "$receipt")"
echo "PASS: one signed PD Intent, PostgreSQL lease, real JobSet API/UID pin, actual SIGKILL, expired lease UNKNOWN, Inspect-first and Operation-only recovery; GPU READY/RELEASED/FINAL unproven"
