-- Remote Cluster Agent identity, pull-command journal and completion fencing.
-- Cluster Agents have no inbound connection and never connect directly to
-- Compute PostgreSQL. The control plane remains the sole ComputeOperation owner.

ALTER TABLE public.compute_providers
    ADD CONSTRAINT compute_providers_id_scope_unique UNIQUE (provider_id, cluster_scope);

CREATE TABLE public.compute_cluster_agents (
    agent_id TEXT PRIMARY KEY
        CHECK (length(agent_id) BETWEEN 1 AND 128 AND btrim(agent_id)=agent_id),
    cluster_scope TEXT NOT NULL UNIQUE
        CHECK (length(cluster_scope) BETWEEN 1 AND 256 AND btrim(cluster_scope)=cluster_scope),
    spiffe_id TEXT NOT NULL UNIQUE
        CHECK (spiffe_id ~ '^spiffe://[^/?#]+/.+$'),
    state TEXT NOT NULL DEFAULT 'REGISTERED'
        CHECK (state IN ('REGISTERED','CONNECTED','SUSPECT','DRAINING','RETIRED')),
    version BIGINT NOT NULL DEFAULT 1 CHECK (version > 0),
    lease_epoch BIGINT NOT NULL DEFAULT 0 CHECK (lease_epoch >= 0),
    instance_id TEXT,
    agent_version TEXT,
    observed_capabilities JSONB NOT NULL DEFAULT '{}'::jsonb
        CHECK (jsonb_typeof(observed_capabilities)='object'),
    capability_digest TEXT CHECK (
        capability_digest IS NULL OR capability_digest ~ '^sha256:[a-f0-9]{64}$'
    ),
    last_seen_at TIMESTAMPTZ,
    lease_expires_at TIMESTAMPTZ,
    last_reason_code TEXT NOT NULL DEFAULT 'REGISTERED'
        CHECK (last_reason_code ~ '^[A-Z][A-Z0-9_]{0,127}$'),
    registered_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    CHECK ((instance_id IS NULL) = (lease_expires_at IS NULL)),
    CHECK (instance_id IS NULL OR (length(instance_id) BETWEEN 1 AND 128 AND btrim(instance_id)=instance_id)),
    UNIQUE (agent_id, cluster_scope)
);

CREATE TABLE public.compute_cluster_agent_provider_bindings (
    provider_id TEXT PRIMARY KEY,
    cluster_scope TEXT NOT NULL,
    agent_id TEXT NOT NULL,
    created_by TEXT NOT NULL CHECK (btrim(created_by) <> ''),
    created_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    UNIQUE (agent_id, provider_id),
    FOREIGN KEY (provider_id, cluster_scope)
        REFERENCES public.compute_providers (provider_id, cluster_scope),
    FOREIGN KEY (agent_id, cluster_scope)
        REFERENCES public.compute_cluster_agents (agent_id, cluster_scope)
);

CREATE TABLE public.compute_cluster_agent_commands (
    command_id TEXT PRIMARY KEY,
    schema_version TEXT NOT NULL DEFAULT 'compute.cluster-agent/v1'
        CHECK (schema_version='compute.cluster-agent/v1'),
    agent_id TEXT NOT NULL,
    cluster_scope TEXT NOT NULL,
    provider_id TEXT NOT NULL,
    operation_id TEXT NOT NULL,
    tenant_id TEXT NOT NULL,
    workload_id TEXT NOT NULL,
    desired_generation BIGINT NOT NULL CHECK (desired_generation > 0),
    command_action TEXT NOT NULL CHECK (command_action IN ('APPLY','INSPECT')),
    request_digest TEXT NOT NULL CHECK (request_digest ~ '^sha256:[a-f0-9]{64}$'),
    request JSONB NOT NULL CHECK (jsonb_typeof(request)='object'),
    state TEXT NOT NULL DEFAULT 'PENDING'
        CHECK (state IN ('PENDING','LEASED','SUCCEEDED','FAILED','UNKNOWN')),
    attempts INTEGER NOT NULL DEFAULT 0 CHECK (attempts >= 0),
    command_fence BIGINT NOT NULL DEFAULT 0 CHECK (command_fence >= 0),
    lease_owner_instance_id TEXT,
    lease_epoch BIGINT,
    lease_expires_at TIMESTAMPTZ,
    outcome TEXT CHECK (outcome IN ('SUCCEEDED','FAILED','UNKNOWN')),
    result JSONB CHECK (result IS NULL OR jsonb_typeof(result)='object'),
    error_code TEXT CHECK (error_code IS NULL OR error_code ~ '^[A-Z][A-Z0-9_]{0,127}$'),
    created_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    UNIQUE (agent_id, operation_id, command_action),
    FOREIGN KEY (agent_id, cluster_scope)
        REFERENCES public.compute_cluster_agents (agent_id, cluster_scope),
    FOREIGN KEY (provider_id, cluster_scope)
        REFERENCES public.compute_providers (provider_id, cluster_scope),
    FOREIGN KEY (operation_id, workload_id, tenant_id, provider_id, desired_generation)
        REFERENCES public.compute_operations (compute_operation_id, workload_id, tenant_id, provider_id, desired_generation),
    CHECK (
        (state='LEASED' AND lease_owner_instance_id IS NOT NULL
            AND lease_epoch IS NOT NULL AND lease_epoch > 0
            AND lease_expires_at IS NOT NULL AND outcome IS NULL)
        OR
        (state<>'LEASED' AND lease_owner_instance_id IS NULL
            AND lease_epoch IS NULL AND lease_expires_at IS NULL)
    ),
    CHECK (
        (state IN ('PENDING','LEASED') AND outcome IS NULL AND result IS NULL AND error_code IS NULL)
        OR (state='SUCCEEDED' AND outcome='SUCCEEDED' AND result IS NOT NULL AND error_code IS NULL)
        OR (state='FAILED' AND outcome='FAILED' AND result IS NULL AND error_code IS NOT NULL)
        OR (state='UNKNOWN' AND outcome='UNKNOWN' AND result IS NULL AND error_code IS NOT NULL)
    )
);

CREATE INDEX compute_cluster_agents_live
    ON public.compute_cluster_agents (state, lease_expires_at, agent_id);
CREATE INDEX compute_cluster_agent_commands_claim
    ON public.compute_cluster_agent_commands (agent_id, state, created_at, command_id)
    WHERE state IN ('PENDING','LEASED');

CREATE TABLE public.compute_cluster_agent_lifecycle_audit (
    event_id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    agent_id TEXT NOT NULL REFERENCES public.compute_cluster_agents(agent_id),
    version BIGINT NOT NULL CHECK (version > 0),
    lease_epoch BIGINT NOT NULL CHECK (lease_epoch >= 0),
    previous_state TEXT,
    state TEXT NOT NULL,
    action TEXT NOT NULL CHECK (action IN ('REGISTERED','SESSION_CONNECTED','STATE_TRANSITIONED')),
    reason_code TEXT NOT NULL CHECK (reason_code ~ '^[A-Z][A-Z0-9_]{0,127}$'),
    instance_id TEXT,
    changed_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    UNIQUE (agent_id, version, lease_epoch)
);

CREATE OR REPLACE FUNCTION public.compute_cluster_agent_guard()
RETURNS trigger
LANGUAGE plpgsql
SET search_path = pg_catalog, pg_temp
AS $agent_guard$
DECLARE
    allowed BOOLEAN := FALSE;
BEGIN
    IF TG_OP='INSERT' THEN
        IF NEW.state <> 'REGISTERED' OR NEW.version <> 1 OR NEW.lease_epoch <> 0
           OR NEW.instance_id IS NOT NULL OR NEW.lease_expires_at IS NOT NULL THEN
            RAISE EXCEPTION 'Cluster Agent must start pre-registered and disconnected'
                USING ERRCODE = '23514';
        END IF;
        RETURN NEW;
    END IF;
    IF NEW.agent_id IS DISTINCT FROM OLD.agent_id
       OR NEW.cluster_scope IS DISTINCT FROM OLD.cluster_scope
       OR NEW.spiffe_id IS DISTINCT FROM OLD.spiffe_id
       OR NEW.registered_at IS DISTINCT FROM OLD.registered_at
       OR NEW.version < OLD.version
       OR NEW.lease_epoch < OLD.lease_epoch
       OR NEW.version > OLD.version + 1
       OR NEW.lease_epoch > OLD.lease_epoch + 1 THEN
        RAISE EXCEPTION 'Cluster Agent identity, version or epoch is not monotonic'
            USING ERRCODE = '23514';
    END IF;
    IF NEW.state IS DISTINCT FROM OLD.state OR NEW.instance_id IS DISTINCT FROM OLD.instance_id THEN
        IF NEW.version <> OLD.version + 1 THEN
            RAISE EXCEPTION 'Cluster Agent lifecycle change requires a version increment'
                USING ERRCODE = '23514';
        END IF;
        IF NEW.instance_id IS DISTINCT FROM OLD.instance_id THEN
            IF NEW.lease_epoch <> OLD.lease_epoch + 1
               OR NEW.state NOT IN ('CONNECTED','DRAINING') THEN
                RAISE EXCEPTION 'new Cluster Agent instance requires a new lease epoch'
                    USING ERRCODE = '23514';
            END IF;
        ELSE
            IF NEW.lease_epoch <> OLD.lease_epoch THEN
                RAISE EXCEPTION 'Cluster Agent state transition cannot change lease epoch'
                    USING ERRCODE = '23514';
            END IF;
            allowed :=
                (OLD.state='REGISTERED' AND NEW.state='CONNECTED')
                OR (OLD.state='CONNECTED' AND NEW.state IN ('SUSPECT','DRAINING'))
                OR (OLD.state='SUSPECT' AND NEW.state IN ('CONNECTED','DRAINING'))
                OR (OLD.state='DRAINING' AND NEW.state='RETIRED');
            IF NOT allowed THEN
                RAISE EXCEPTION 'invalid Cluster Agent lifecycle transition'
                    USING ERRCODE = '23514';
            END IF;
        END IF;
    ELSIF NEW.version <> OLD.version OR NEW.lease_epoch <> OLD.lease_epoch THEN
        RAISE EXCEPTION 'Cluster Agent version only changes on lifecycle or session transition'
            USING ERRCODE = '23514';
    END IF;
    RETURN NEW;
END;
$agent_guard$;

CREATE TRIGGER compute_cluster_agents_guard
BEFORE INSERT OR UPDATE ON public.compute_cluster_agents
FOR EACH ROW EXECUTE FUNCTION public.compute_cluster_agent_guard();

CREATE OR REPLACE FUNCTION public.compute_cluster_agent_audit_append()
RETURNS trigger
LANGUAGE plpgsql
SET search_path = pg_catalog, pg_temp
AS $agent_audit$
DECLARE
    action_value TEXT;
BEGIN
    IF TG_OP='INSERT' THEN
        INSERT INTO public.compute_cluster_agent_lifecycle_audit (
            agent_id, version, lease_epoch, previous_state, state,
            action, reason_code, instance_id
        ) VALUES (
            NEW.agent_id, NEW.version, NEW.lease_epoch, NULL,
            NEW.state, 'REGISTERED', NEW.last_reason_code, NEW.instance_id
        );
        RETURN NEW;
    END IF;
    IF NEW.state IS DISTINCT FROM OLD.state OR NEW.instance_id IS DISTINCT FROM OLD.instance_id THEN
        IF NEW.instance_id IS DISTINCT FROM OLD.instance_id THEN
            action_value := 'SESSION_CONNECTED';
        ELSE
            action_value := 'STATE_TRANSITIONED';
        END IF;
        INSERT INTO public.compute_cluster_agent_lifecycle_audit (
            agent_id, version, lease_epoch, previous_state, state,
            action, reason_code, instance_id
        ) VALUES (
            NEW.agent_id, NEW.version, NEW.lease_epoch, OLD.state,
            NEW.state, action_value, NEW.last_reason_code, NEW.instance_id
        );
    END IF;
    RETURN NEW;
END;
$agent_audit$;

CREATE TRIGGER compute_cluster_agents_audit
AFTER INSERT OR UPDATE ON public.compute_cluster_agents
FOR EACH ROW EXECUTE FUNCTION public.compute_cluster_agent_audit_append();

CREATE OR REPLACE FUNCTION public.compute_cluster_agent_audit_immutable()
RETURNS trigger
LANGUAGE plpgsql
SET search_path = pg_catalog, pg_temp
AS $audit_immutable$
BEGIN
    RAISE EXCEPTION 'Cluster Agent lifecycle audit is append-only'
        USING ERRCODE = '55000';
END;
$audit_immutable$;

CREATE TRIGGER compute_cluster_agent_audit_no_update_delete
BEFORE UPDATE OR DELETE ON public.compute_cluster_agent_lifecycle_audit
FOR EACH ROW EXECUTE FUNCTION public.compute_cluster_agent_audit_immutable();
CREATE TRIGGER compute_cluster_agent_audit_no_truncate
BEFORE TRUNCATE ON public.compute_cluster_agent_lifecycle_audit
FOR EACH STATEMENT EXECUTE FUNCTION public.compute_cluster_agent_audit_immutable();

CREATE OR REPLACE FUNCTION public.compute_cluster_agent_command_guard()
RETURNS trigger
LANGUAGE plpgsql
SET search_path = pg_catalog, pg_temp
AS $command_guard$
BEGIN
    IF TG_OP='DELETE' THEN
        RAISE EXCEPTION 'Cluster Agent command history cannot be deleted'
            USING ERRCODE = '55000';
    END IF;
    IF NEW.command_id IS DISTINCT FROM OLD.command_id
       OR NEW.agent_id IS DISTINCT FROM OLD.agent_id
       OR NEW.cluster_scope IS DISTINCT FROM OLD.cluster_scope
       OR NEW.provider_id IS DISTINCT FROM OLD.provider_id
       OR NEW.operation_id IS DISTINCT FROM OLD.operation_id
       OR NEW.tenant_id IS DISTINCT FROM OLD.tenant_id
       OR NEW.workload_id IS DISTINCT FROM OLD.workload_id
       OR NEW.desired_generation IS DISTINCT FROM OLD.desired_generation
       OR NEW.command_action IS DISTINCT FROM OLD.command_action
       OR NEW.request_digest IS DISTINCT FROM OLD.request_digest
       OR NEW.request IS DISTINCT FROM OLD.request
       OR NEW.created_at IS DISTINCT FROM OLD.created_at THEN
        RAISE EXCEPTION 'Cluster Agent command intent is immutable'
            USING ERRCODE = '55000';
    END IF;
    IF OLD.state IN ('SUCCEEDED','FAILED','UNKNOWN') THEN
        RAISE EXCEPTION 'Cluster Agent command outcome is terminal'
            USING ERRCODE = '55000';
    END IF;
    IF OLD.state='PENDING' AND NEW.state='LEASED' THEN
        IF NEW.command_fence <> OLD.command_fence + 1
           OR NEW.attempts <> OLD.attempts + 1 THEN
            RAISE EXCEPTION 'Cluster Agent command claim requires a higher fence'
                USING ERRCODE = '23514';
        END IF;
    ELSIF OLD.state='LEASED' AND NEW.state='PENDING' THEN
        IF OLD.command_action <> 'INSPECT' OR NEW.command_fence <> OLD.command_fence
           OR NEW.attempts <> OLD.attempts THEN
            RAISE EXCEPTION 'only expired read-only INSPECT may be returned to pending'
                USING ERRCODE = '23514';
        END IF;
    ELSIF OLD.state='LEASED' AND NEW.state IN ('SUCCEEDED','FAILED','UNKNOWN') THEN
        IF NEW.command_fence <> OLD.command_fence OR NEW.attempts <> OLD.attempts THEN
            RAISE EXCEPTION 'Cluster Agent completion cannot change the claim fence'
                USING ERRCODE = '23514';
        END IF;
    ELSE
        RAISE EXCEPTION 'invalid Cluster Agent command transition'
            USING ERRCODE = '23514';
    END IF;
    RETURN NEW;
END;
$command_guard$;

CREATE TRIGGER compute_cluster_agent_commands_guard
BEFORE UPDATE OR DELETE ON public.compute_cluster_agent_commands
FOR EACH ROW EXECUTE FUNCTION public.compute_cluster_agent_command_guard();

CREATE OR REPLACE FUNCTION public.compute_cluster_agent_command_no_truncate()
RETURNS trigger
LANGUAGE plpgsql
SET search_path = pg_catalog, pg_temp
AS $command_no_truncate$
BEGIN
    RAISE EXCEPTION 'Cluster Agent command history cannot be truncated'
        USING ERRCODE = '55000';
END;
$command_no_truncate$;
CREATE TRIGGER compute_cluster_agent_commands_no_truncate
BEFORE TRUNCATE ON public.compute_cluster_agent_commands
FOR EACH STATEMENT EXECUTE FUNCTION public.compute_cluster_agent_command_no_truncate();

CREATE TRIGGER compute_cluster_agent_provider_bindings_no_update
BEFORE UPDATE OR DELETE ON public.compute_cluster_agent_provider_bindings
FOR EACH ROW EXECUTE FUNCTION public.compute_cluster_agent_audit_immutable();
CREATE TRIGGER compute_cluster_agent_provider_bindings_no_truncate
BEFORE TRUNCATE ON public.compute_cluster_agent_provider_bindings
FOR EACH STATEMENT EXECUTE FUNCTION public.compute_cluster_agent_command_no_truncate();

COMMENT ON TABLE public.compute_cluster_agents IS
    'Pre-provisioned remote Kubernetes Cluster Agent identity. Heartbeat lease is not Provider readiness or GPU capacity evidence.';
COMMENT ON TABLE public.compute_cluster_agent_commands IS
    'Durable typed APPLY/INSPECT pull commands. Expired APPLY is UNKNOWN and cannot be automatically re-dispatched.';
