# SPIFFE Workload Identity Provisioning

## Trust boundary

The Workload API accepts a service identity only from a TLS client certificate that Go's TLS server has already verified against the configured client CA bundle. Configure the listener with `tls.Config.ClientAuth = tls.RequireAndVerifyClientCert` and a narrowly scoped `ClientCAs` pool. The `SPIFFEWorkloadAuthenticator` then requires the verified leaf certificate to contain exactly one SPIFFE URI SAN in the configured trust domain.

The SPIFFE ID is a lookup key, not a source of Tenant or Project claims. The PostgreSQL identity directory is authoritative for:

- Tenant ID
- Service name
- Active Project grants
- Identity/grant effective windows and revocation state

The resolver reads the service identity and Project grants from a single read-only Repeatable Read snapshot. Missing, expired, not-yet-active or revoked identities fail authentication. Revoked/expired Project grants are omitted. The handler subsequently checks Project grants for each Workload request and invokes the service policy / Guard authorizer.

## Schema

Migration `0004_workload_service_identity_directory.sql` creates:

- `compute_service_identities`: unique SPIFFE ID, Tenant ID, Service name, ACTIVE/REVOKED status, `not_before` and optional `expires_at`.
- `compute_service_project_grants`: per-SPIFFE Project grant, constrained to the same Tenant as its identity, with active/revoked and effective-time fields.
- `compute_identity_directory_audit`: insert/update/delete snapshots for both tables, including database `session_user` and timestamp. Triggers write audit records in the same transaction as every identity/grant change. Ordinary PUBLIC roles are denied update/delete/truncate on the audit table.

The `granted_by` field is an operator-provided change reference (for example, a ticket or provisioning workflow ID); `changed_by` is the database session identity captured by the trigger. Neither field should be represented as a cryptographic human identity unless the provisioning system supplies and verifies that identity.

## Provision a service identity

Only the controlled identity-provisioning workflow/database administration role should write these tables. The Workload HTTP API has no identity or Project grant write endpoint.

```sql
BEGIN;

INSERT INTO compute_service_identities (
    spiffe_id, tenant_id, service_name, status, not_before, expires_at
) VALUES (
    'spiffe://cluster.example/ns/agent-runtime/sa/runtime',
    'tenant-acme',
    'agent-runtime',
    'ACTIVE',
    clock_timestamp(),
    NULL
);

INSERT INTO compute_service_project_grants (
    spiffe_id, tenant_id, project_id, status, granted_by
) VALUES
    ('spiffe://cluster.example/ns/agent-runtime/sa/runtime',
     'tenant-acme', 'project-inference', 'ACTIVE', 'IAM-CHANGE-1042'),
    ('spiffe://cluster.example/ns/agent-runtime/sa/runtime',
     'tenant-acme', 'project-training', 'ACTIVE', 'IAM-CHANGE-1042');

COMMIT;
```

Use one stable SPIFFE URI per workload service identity. Certificate rotation that retains the same URI continues to resolve to the same database identity. A trust-domain or workload identity migration should provision a second SPIFFE identity with equivalent approved bindings, verify it, then revoke the previous row.

## Revoke identity or Project grant

Revoke by changing state; do not delete records. The update trigger records before/after state in the audit ledger.

```sql
BEGIN;

UPDATE compute_service_project_grants
SET status = 'REVOKED', updated_at = clock_timestamp()
WHERE spiffe_id = 'spiffe://cluster.example/ns/agent-runtime/sa/runtime'
  AND tenant_id = 'tenant-acme'
  AND project_id = 'project-training';

UPDATE compute_service_identities
SET status = 'REVOKED', updated_at = clock_timestamp()
WHERE spiffe_id = 'spiffe://cluster.example/ns/agent-runtime/sa/runtime'
  AND tenant_id = 'tenant-acme';

COMMIT;
```

An identity revocation makes the resolver reject new authenticated requests. Project grant revocation removes that Project from subsequent resolved principals. Existing HTTP requests already authenticated before a change continue under their captured request principal; keep request lifetimes bounded and use short-lived credentials for sensitive workloads.

## Audit verification

```sql
SELECT event_id, entity_type, action, spiffe_id, tenant_id, project_id,
       changed_by, changed_at, before_state, after_state
FROM compute_identity_directory_audit
WHERE spiffe_id = 'spiffe://cluster.example/ns/agent-runtime/sa/runtime'
ORDER BY event_id;
```

The audit ledger is protected from ordinary PUBLIC updates/deletes. Database owners/superusers remain privileged; export audit records to the organization's protected audit archive for independent retention and tamper detection.

## Rollout gates

The adapter and database resolver are implemented and tested, but they are not enabled by the default service main. Production exposure still requires:

1. Deployment-managed client CA bundle and verified mTLS listener configuration.
2. Provisioned identity directory rows and an approved grant/revocation workflow.
3. A concrete Guard/Project service authorizer and authenticated Provider selector.
4. Tenant RLS / least-privilege DB roles for Workload, Allocation and Operation access.
5. Real Provider Inspect authentication and Kubernetes/Kueue/DRA evidence before claiming Allocation readiness.
