# ADR candidate — v0.1 HTTPS administrator browser session and observation read boundary

**Status:** Proposed, security review required, deliberately not mounted in the default CCP service. This is a narrow single-admin development building block for [Issue #143](https://github.com/avaargsh/compute-control-panel/issues/143), not an enterprise authentication system or an approved production release.

## Problem

The existing SPIFFE/mTLS `WorkloadAuthenticator` is for **service identity**, not a browser login mechanism. The v0.1 Console must be able to call the scoped `GET /api/v1/observations` reader without passing client-supplied `project_id`, Kubernetes credentials, or a forged SPIFFE identity. Anonymous or unscoped read access must fail closed.

## Decision in code

`internal/consoleauth.NewHandler(Config, httpapi.ObservationReader)` composes a **separate, opt-in** browser-only HTTP handler for:

| Route | Method | Security / behavior |
|---|---|---|
| `/api/v1/console/login` | POST | exact HTTPS Origin, JSON-only, bounded body, single configured bcrypt admin, global failed-attempt rate limit, random session token |
| `/api/v1/console/session` | GET | authenticated Secure HttpOnly cookie, returns authorized `project_id` and a session-scoped CSRF token |
| `/api/v1/console/logout` | POST | exact HTTPS Origin, valid session + constant-time CSRF header, revokes session immediately |
| `/api/v1/observations` | GET | composes the already scoped `httpapi.NewObservationReadHandler`; tenant/project are derived exclusively from session configuration and checked again against returned rows |

All routes require a direct HTTPS request (`r.TLS != nil`) and the expected Host. This first step does **not** trust forwarded headers, reverse proxies or browser-provided project IDs.

- Cookie: `__Host-ccp_session`, Secure, HttpOnly, SameSite=Strict, Path=/, no Domain.
- Session token: cryptographically random 256 bits. Only SHA-256 of the token is stored in memory; CSRF token is random and checked constant-time for logout.
- Session TTL: explicitly configured between 5 minutes and 8 hours, no persistence; restart revokes all sessions.
- Password: trusted pre-provisioned bcrypt verifier (cost >= default 10), no plaintext in source, no default username/password, no browser self-registration.
- Rate limit: up to 5 invalid logins per 60 seconds **globally in this process**. Real deployment must use an audited, multi-instance failure-rate policy and secure provisioned credentials.
- Upper bound: 32 active sessions; session limit fails closed rather than dropping arbitrary sessions.
- The `Config` values must be set by trusted server setup (not HTTP request, environment pass-through, browser localStorage, or an untrusted operator).
- A project-scoped `httpapi.ObservationReader` is required at construction; no wildcard-read default.
- Exact HTTP response status/permission behavior is covered by `internal/consoleauth/handler_test.go`.

## Known limitations / explicit non-goals

**Not mounted**: `cmd/compute-control-panel/main.go` still exposes its existing health/meta surface. There is no new public bind, no automatic Console startup, no TLS certificate management, no browser page behind this handler, and no exposure of a private Kubernetes token.

**Not enterprise production auth**: fixed one-admin/one-project scope, process-local revocation/session storage and login rate limits. No OIDC, MFA, SSO, HA sessions, cross-instance logout or dynamic IAM revocation. Security review, session storage and audit logging are mandatory before enterprise deployment.

**Not inference or physical allocation**: no ModelService/llm-d control writes, no GPU allocation or released evidence, and no cost FINAL.

**Do not put this handler behind a TLS-terminating reverse proxy as-is**: it intentionally rejects missing direct TLS. A proxy-aware deployment would require an audited trusted-proxy certificate and request authenticity contract, never blind `X-Forwarded-Proto` trust.

## Acceptance gates

1. Valid HTTPS login → strict session cookie → authenticated read returns only configured project → CSRF logout revokes it.
2. Missing TLS, foreign Origin/Host, cross-site Fetch Metadata, invalid credentials, malformed/oversized JSON, invalid/cross-project reader records and expired cookies fail closed.
3. Replayed cookies after logout fail; a later process restart has no stored sessions.
4. Exact-head `gofmt`, Go 1.27.4 `go vet`, `go test -race ./...` and PostgreSQL migration tests all pass.
5. Independent security review confirms no default-service route is activated and no real credentials are logged.
6. Next isolated PR: reviewed HTTPS server/certificate provisioning, Console UI login screen and real Kubernetes read integration, then Helm installer.

**Never interpret session tests passing as approval to expose CCP publicly.**
