-- Provider registry slice A: durable registration and monotonic drain.
-- This is an administrative, non-public bootstrap boundary. It does NOT
-- attest Provider health, Kubernetes allocation, quota release or device UID.
-- READY/DEGRADED/RETIRED transitions remain unavailable until authenticated
-- inspection and final resource/liability reconciliation are implemented.

CREATE TABLE compute_providers (
    provider_id TEXT PRIMARY KEY
        CHECK (length(provider_id) BETWEEN 1 AND 256 AND btrim(provider_id)=provider_id),
    cluster_scope TEXT NOT NULL
        CHECK (length(cluster_scope) BETWEEN 1 AND 256 AND btrim(cluster_scope)=cluster_scope),
    adapter_source TEXT NOT NULL
        CHECK (length(adapter_source) BETWEEN 1 AND 256 AND btrim(adapter_source)=adapter_source),
    adapter_version TEXT NOT NULL
        CHECK (length(adapter_version) BETWEEN 1 AND 256 AND btrim(adapter_version)=adapter_version),
    capability_digest TEXT NOT NULL CHECK (capability_digest ~ '^sha256:[a-f0-9]{64}$'),
    state TEXT NOT NULL DEFAULT 'REGISTERED'
        CHECK (state IN ('REGISTERED','DRAINING')),
    version BIGINT NOT NULL DEFAULT 1 CHECK (version > 0),
    last_reason_code TEXT NOT NULL DEFAULT 'REGISTERED'
        CHECK (last_reason_code ~ '^[A-Z][A-Z0-9_]{0,127}$'),
    registered_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    CHECK ((state='REGISTERED' AND version=1 AND last_reason_code='REGISTERED')
        OR (state='DRAINING' AND version=2 AND last_reason_code<>'REGISTERED'))
);

CREATE TABLE compute_provider_lifecycle_audit (
    event_id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    provider_id TEXT NOT NULL REFERENCES compute_providers(provider_id),
    version BIGINT NOT NULL CHECK (version > 0),
    previous_state TEXT,
    state TEXT NOT NULL CHECK (state IN ('REGISTERED','DRAINING')),
    reason_code TEXT NOT NULL CHECK (reason_code ~ '^[A-Z][A-Z0-9_]{0,127}$'),
    actor_db_role TEXT NOT NULL,
    recorded_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    UNIQUE(provider_id, version),
    CHECK ((version=1 AND previous_state IS NULL AND state='REGISTERED')
        OR (version=2 AND previous_state='REGISTERED' AND state='DRAINING'))
);
CREATE INDEX compute_provider_lifecycle_audit_provider
    ON compute_provider_lifecycle_audit(provider_id, event_id DESC);

CREATE OR REPLACE FUNCTION compute_provider_lifecycle_guard()
RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    IF TG_OP='DELETE' THEN
        RAISE EXCEPTION 'Provider rows cannot be deleted; use verified lifecycle retirement';
    END IF;
    IF TG_OP='TRUNCATE' THEN
        RAISE EXCEPTION 'Provider registry cannot be truncated';
    END IF;
    IF TG_OP='UPDATE' THEN
        IF OLD.state <> 'REGISTERED' OR NEW.state <> 'DRAINING'
           OR NEW.version <> OLD.version+1
           OR NEW.provider_id <> OLD.provider_id
           OR NEW.cluster_scope <> OLD.cluster_scope
           OR NEW.adapter_source <> OLD.adapter_source
           OR NEW.adapter_version <> OLD.adapter_version
           OR NEW.capability_digest <> OLD.capability_digest
           OR NEW.registered_at <> OLD.registered_at
           OR NEW.last_reason_code='REGISTERED' THEN
            RAISE EXCEPTION 'unsupported Provider mutation or lifecycle transition';
        END IF;
    END IF;
    RETURN NEW;
END;
$$;

CREATE TRIGGER compute_provider_lifecycle_guard_row
BEFORE UPDATE OR DELETE ON compute_providers
FOR EACH ROW EXECUTE FUNCTION compute_provider_lifecycle_guard();
CREATE TRIGGER compute_provider_lifecycle_guard_truncate
BEFORE TRUNCATE ON compute_providers
FOR EACH STATEMENT EXECUTE FUNCTION compute_provider_lifecycle_guard();

CREATE OR REPLACE FUNCTION compute_provider_lifecycle_audit_append()
RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    INSERT INTO compute_provider_lifecycle_audit
        (provider_id, version, previous_state, state, reason_code, actor_db_role)
    VALUES (
        NEW.provider_id, NEW.version,
        CASE WHEN TG_OP='INSERT' THEN NULL ELSE OLD.state END,
        NEW.state, NEW.last_reason_code, session_user
    );
    RETURN NEW;
END;
$$;

CREATE TRIGGER compute_provider_lifecycle_audit_append
AFTER INSERT OR UPDATE ON compute_providers
FOR EACH ROW EXECUTE FUNCTION compute_provider_lifecycle_audit_append();

CREATE OR REPLACE FUNCTION compute_provider_audit_immutable_guard()
RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    RAISE EXCEPTION 'Provider lifecycle audit is append-only';
END;
$$;
CREATE TRIGGER compute_provider_audit_no_mutation
BEFORE UPDATE OR DELETE ON compute_provider_lifecycle_audit
FOR EACH ROW EXECUTE FUNCTION compute_provider_audit_immutable_guard();
CREATE TRIGGER compute_provider_audit_no_truncate
BEFORE TRUNCATE ON compute_provider_lifecycle_audit
FOR EACH STATEMENT EXECUTE FUNCTION compute_provider_audit_immutable_guard();

REVOKE UPDATE, DELETE, TRUNCATE ON compute_provider_lifecycle_audit FROM PUBLIC;
