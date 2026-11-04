-- Trusted SPIFFE workload identity directory.
-- Rows are provisioned by an administrative identity workflow; the public
-- Workload API has no endpoint capable of creating or changing these records.

CREATE TABLE compute_service_identities (
    spiffe_id TEXT PRIMARY KEY
        CHECK (spiffe_id ~ '^spiffe://[^/?#]+/.+$'),
    tenant_id TEXT NOT NULL,
    service_name TEXT NOT NULL CHECK (btrim(service_name) <> ''),
    status TEXT NOT NULL DEFAULT 'ACTIVE'
        CHECK (status IN ('ACTIVE', 'REVOKED')),
    not_before TIMESTAMPTZ NOT NULL DEFAULT now(),
    expires_at TIMESTAMPTZ,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (spiffe_id, tenant_id),
    CHECK (expires_at IS NULL OR expires_at > not_before)
);

CREATE INDEX compute_service_identities_active
    ON compute_service_identities (tenant_id, service_name, spiffe_id)
    WHERE status = 'ACTIVE';

CREATE TABLE compute_service_project_grants (
    spiffe_id TEXT NOT NULL,
    tenant_id TEXT NOT NULL,
    project_id TEXT NOT NULL CHECK (btrim(project_id) <> ''),
    status TEXT NOT NULL DEFAULT 'ACTIVE'
        CHECK (status IN ('ACTIVE', 'REVOKED')),
    not_before TIMESTAMPTZ NOT NULL DEFAULT now(),
    expires_at TIMESTAMPTZ,
    granted_by TEXT NOT NULL CHECK (btrim(granted_by) <> ''),
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (spiffe_id, project_id),
    FOREIGN KEY (spiffe_id, tenant_id)
        REFERENCES compute_service_identities (spiffe_id, tenant_id),
    CHECK (expires_at IS NULL OR expires_at > not_before)
);

CREATE INDEX compute_service_project_grants_active
    ON compute_service_project_grants (tenant_id, project_id, spiffe_id)
    WHERE status = 'ACTIVE';
