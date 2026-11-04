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


-- Append-only audit ledger. Trigger writes are part of the same transaction as
-- each identity/grant change; before/after snapshots preserve revocation history.
CREATE TABLE compute_identity_directory_audit (
    event_id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    entity_type TEXT NOT NULL CHECK (entity_type IN ('SERVICE_IDENTITY', 'PROJECT_GRANT')),
    action TEXT NOT NULL CHECK (action IN ('INSERT', 'UPDATE', 'DELETE')),
    spiffe_id TEXT NOT NULL,
    tenant_id TEXT NOT NULL,
    project_id TEXT,
    before_state JSONB,
    after_state JSONB,
    changed_by TEXT NOT NULL,
    changed_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    CHECK (
        (action='INSERT' AND before_state IS NULL AND after_state IS NOT NULL)
        OR (action='UPDATE' AND before_state IS NOT NULL AND after_state IS NOT NULL)
        OR (action='DELETE' AND before_state IS NOT NULL AND after_state IS NULL)
    ),
    CHECK (
        (entity_type='SERVICE_IDENTITY' AND project_id IS NULL)
        OR (entity_type='PROJECT_GRANT' AND project_id IS NOT NULL)
    )
);

CREATE INDEX compute_identity_directory_audit_subject
    ON compute_identity_directory_audit (spiffe_id, event_id DESC);

CREATE OR REPLACE FUNCTION compute_audit_identity_directory_change()
RETURNS trigger
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog, public
AS $$
DECLARE
    row_state JSONB;
    before_value JSONB;
    after_value JSONB;
    entity_name TEXT;
BEGIN
    IF TG_OP='DELETE' THEN
        row_state := to_jsonb(OLD);
        before_value := row_state;
        after_value := NULL;
    ELSIF TG_OP='UPDATE' THEN
        row_state := to_jsonb(NEW);
        before_value := to_jsonb(OLD);
        after_value := row_state;
    ELSE
        row_state := to_jsonb(NEW);
        before_value := NULL;
        after_value := row_state;
    END IF;

    IF TG_TABLE_NAME='compute_service_identities' THEN
        entity_name := 'SERVICE_IDENTITY';
    ELSE
        entity_name := 'PROJECT_GRANT';
    END IF;

    INSERT INTO public.compute_identity_directory_audit (
        entity_type, action, spiffe_id, tenant_id, project_id,
        before_state, after_state, changed_by
    ) VALUES (
        entity_name, TG_OP, row_state->>'spiffe_id', row_state->>'tenant_id',
        row_state->>'project_id', before_value, after_value, session_user
    );

    IF TG_OP='DELETE' THEN
        RETURN OLD;
    END IF;
    RETURN NEW;
END;
$$;

CREATE TRIGGER compute_service_identities_audit
AFTER INSERT OR UPDATE OR DELETE ON compute_service_identities
FOR EACH ROW EXECUTE FUNCTION compute_audit_identity_directory_change();

CREATE TRIGGER compute_service_project_grants_audit
AFTER INSERT OR UPDATE OR DELETE ON compute_service_project_grants
FOR EACH ROW EXECUTE FUNCTION compute_audit_identity_directory_change();

REVOKE UPDATE, DELETE, TRUNCATE ON compute_identity_directory_audit FROM PUBLIC;
