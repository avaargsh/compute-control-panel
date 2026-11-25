# CCP Live Console — local HTTPS read-only preview

**Status:** opt-in **draft** integration in [PR #183](https://github.com/avaargsh/compute-control-panel/pull/183), pending independent security review and production packaging. This is **not** the unauthenticated [synthetic Demo](DEMO-CONSOLE-QUICKSTART.md), not the default CCP service, and not approved for internet/LAN exposure or production use. No React UI is served by this command.

The separate `cmd/ccp-console` executable exposes only a **single-administrator** HTTPS API on explicit `127.0.0.1:<port>`. It reads Kubernetes Node `status.capacity` / `status.allocatable` through a scoped read-only ServiceAccount token and reports GPU quantities with **UNKNOWN** physical availability. It cannot allocate or release devices.

## Requirements and boundaries

- Go matching `go.mod` (CI pins Go **1.27.4**), `openssl`, and `htpasswd` for creating an interactive bcrypt verifier. `curl` is used to test the API; `jq` is optional for extracting the logout CSRF token.
- An already reachable **HTTPS** Kubernetes API endpoint; its verified CA PEM; a **dedicated, time-bounded, rotatable ServiceAccount bearer-token file** that can only **list core/v1 Nodes**, not workloads or Secrets; and administrator-selected cluster, tenant and project IDs. Do not use cluster-admin credentials or a kubeconfig's client private key as the reader token.
- A private workspace for the bcrypt verifier, TLS key, and session cookies (`umask 077`). Never add these files to Git, pass the password on a command line, or log them.
- This preview requires direct TLS to the local server. No reverse proxy, port forwarding, shared host, public load balancer, ingress, wildcard listener, `X-Forwarded-Proto` trust, or persistent/HA browser sessions.

## 1. Create local-only development credentials

From the repository root:

```bash
umask 077
work="$(mktemp -d)"
# htpasswd prompts for the password: do NOT use -b or put it in shell history.
htpasswd -n -B -C 12 admin | cut -d: -f2- > "$work/admin.bcrypt"
test -s "$work/admin.bcrypt"
# Development self-signed TLS certificate with a literal IP SAN.
openssl req -x509 -newkey rsa:3072 -sha256 -nodes -days 1 \
  -keyout "$work/console.key" -out "$work/console.crt" \
  -subj "/CN=CCP loopback-only development" \
  -addext "subjectAltName=IP:127.0.0.1" \
  -addext "extendedKeyUsage=serverAuth" \
  -addext "keyUsage=digitalSignature"
chmod 600 "$work/admin.bcrypt" "$work/console.key" "$work/console.crt"
```

The certificate is intended **only for disposable localhost testing**. It does not prove enterprise/browser trust. Validate the exact IP SAN, expiry and EKU; do not disable certificate verification with `curl -k`.

## 2. Supply a least-privilege Kubernetes reader

An administrator should provision a dedicated ServiceAccount and **list-only** Node RBAC on the disposable cluster, then securely provide its CA, API origin, and token. An example RBAC manifest to review and apply in a non-production cluster:

```yaml
apiVersion: v1
kind: ServiceAccount
metadata:
  name: ccp-console-reader
  namespace: default
---
apiVersion: rbac.authorization.k8s.io/v1
kind: ClusterRole
metadata:
  name: ccp-console-node-list
rules:
  - apiGroups: [""]
    resources: ["nodes"]
    verbs: ["list"]
---
apiVersion: rbac.authorization.k8s.io/v1
kind: ClusterRoleBinding
metadata:
  name: ccp-console-node-list
roleRef:
  apiGroup: rbac.authorization.k8s.io
  kind: ClusterRole
  name: ccp-console-node-list
subjects:
  - kind: ServiceAccount
    name: ccp-console-reader
    namespace: default
```

Obtain a short-lived token using the cluster administrator's approved procedure (for example `kubectl create token ccp-console-reader --duration=1h` in the selected namespace); store it with mode `0600`, rotate before expiry and revoke promptly. **Do not** assume this minimal example is sufficient for enterprise IAM, issuer/audience, network or credential-rotation policy. The Kubernetes CA must verify the exact `-kube-api` HTTPS origin.

## 3. Start the opt-in HTTPS Console

Replace only the example cluster endpoint and scoped identifiers with actual administrator-approved values:

```bash
KUBE_API="https://127.0.0.1:6443"           # actual verified Kubernetes API origin
KUBE_CA="/secure/path/kubernetes-ca.pem"     # PEM, not a client key
KUBE_TOKEN_FILE="/secure/path/node-reader.token"
go run ./cmd/ccp-console \
  -addr "127.0.0.1:8443" \
  -tls-cert "$work/console.crt" \
  -tls-key "$work/console.key" \
  -admin-user admin \
  -admin-bcrypt-file "$work/admin.bcrypt" \
  -tenant "tenant-a" -project "project-a" -cluster "cluster-a" \
  -kube-api "$KUBE_API" -kube-ca "$KUBE_CA" \
  -kube-token-file "$KUBE_TOKEN_FILE" \
  -gpu-resources "nvidia.com/gpu"
```

The API refuses non-loopback bind, missing/invalid certificate or key, unexpected certificate IP SAN, wrong Origin, invalid bcrypt hash, and mismatched tenant/project inventory scope. It does not start a browser UI. The `-gpu-resources` flag accepts explicit comma-separated extended-resource names. Ensure the local API port is available.

## 4. Verify anonymous denial, authenticated read, and logout

In a **second terminal**, set `work` to the private directory created above:

```bash
origin="https://127.0.0.1:8443"
# Expect HTTP 401. The upstream Kubernetes reader must not be called.
curl -sS --cacert "$work/console.crt" -o /dev/null -w '%{http_code}\n' \
  "$origin/api/v1/observations"
```

Privately create `$work/login.json` containing exactly `{"username":"admin","password":"<the interactively provisioned password>"}` with mode `0600`. Do not paste real credentials into terminal history or send them to a public endpoint. Then:

```bash
curl -sS --cacert "$work/console.crt" -c "$work/cookies" \
  -H "Content-Type: application/json" -H "Origin: $origin" \
  --data-binary "@$work/login.json" "$origin/api/v1/console/login"
# Expect authenticated:true and a __Host-ccp_session Secure/HttpOnly cookie.
curl -sS --cacert "$work/console.crt" -b "$work/cookies" \
  "$origin/api/v1/console/session"
curl -sS --cacert "$work/console.crt" -b "$work/cookies" \
  "$origin/api/v1/observations"
```

Expect HTTP 200 with `"data_mode":"live"` for an authorized session, and observations restricted to the configured project. Node allocatable is **not** unallocated/free GPU capacity, and there is no physical device UID proof. An invalid ServiceAccount, unreachable API, wrong CA, or forbidden Node LIST must fail closed rather than fall back to synthetic data.

To explicitly revoke the session, read `csrf_token` from the session JSON (using `jq -r .csrf_token` if available), then:

```bash
curl -sS --cacert "$work/console.crt" -b "$work/cookies" \
  -H "Origin: $origin" -H "X-CCP-CSRF: <csrf_token_from_session>" \
  -X POST "$origin/api/v1/console/logout"
# Subsequent GET /api/v1/observations using the old cookie must return HTTP 401.
```

Do not log or share `csrf_token`, `login.json`, the cookie jar or any private key.

## 5. Repeatable test checks and cleanup

```bash
go test -count=1 ./cmd/ccp-console ./internal/consoleauth ./internal/kubeinventory
```

Source-level tests verify TLS 1.2 rejection, TLS 1.3 handshake, invalid TLS leaf, loopback-only listen, real scoped Kubernetes read, login cookie, CSRF logout and revocation. **These tests do not establish a real cluster installation or independent security acceptance**; exact-head GitHub Go/race verification and a separate authorized reviewer are required before merge.

Stop the Console process, revoke the disposable ServiceAccount token per cluster policy, and securely delete the private workspace. If a TLS certificate is untrusted, fix the certificate/CA chain; never work around it by disabling verification. If observations return `UNKNOWN`, inspect source provenance and Node condition rather than assuming device availability.

Refs: [Console session security boundary](../security/CONSOLE-SESSION-V0.md), [synthetic Demo quickstart](DEMO-CONSOLE-QUICKSTART.md), [Issue #143](https://github.com/avaargsh/compute-control-panel/issues/143).