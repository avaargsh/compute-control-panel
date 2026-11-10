-- Versioned Project, ComputePool and Accelerator aggregates.
-- Resource identity, aggregate version, immutable spec revision and external
-- observations are separate facts. This migration does not create a scheduler
-- or make Provider/Kubernetes/Kueue inventory authoritative inside Compute CP.

CREATE TABLE public.compute_projects (
    tenant_id TEXT NOT NULL CHECK (btrim(tenant_id) <> ''),
    project_id TEXT NOT NULL CHECK (length(project_id) BETWEEN 1 AND 256 AND btrim(project_id)=project_id),
    state TEXT NOT NULL CHECK (state IN ('ACTIVE','SUSPENDED','ARCHIVED')),
    current_revision BIGINT NOT NULL CHECK (current_revision > 0),
    version BIGINT NOT NULL CHECK (version > 0),
    created_by TEXT NOT NULL CHECK (btrim(created_by) <> ''),
    updated_by TEXT NOT NULL CHECK (btrim(updated_by) <> ''),
    last_action TEXT NOT NULL CHECK (last_action IN ('CREATED','REVISION_PUBLISHED','STATE_TRANSITIONED')),
    last_reason_code TEXT NOT NULL CHECK (last_reason_code ~ '^[A-Z][A-Z0-9_]{0,127}$'),
    created_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    PRIMARY KEY (tenant_id, project_id),
    UNIQUE (tenant_id, project_id, current_revision)
);

CREATE TABLE public.compute_project_revisions (
    tenant_id TEXT NOT NULL,
    project_id TEXT NOT NULL,
    revision BIGINT NOT NULL CHECK (revision > 0),
    spec JSONB NOT NULL CHECK (
        jsonb_typeof(spec)='object'
        AND spec->>'schema_version'='1'
        AND jsonb_typeof(spec->'pool_refs')='array'
    ),
    spec_digest TEXT NOT NULL CHECK (
        spec_digest ~ '^sha256:[a-f0-9]{64}$'
        OR spec_digest ~ '^legacy:[A-Za-z0-9._:-]+$'
    ),
    created_by TEXT NOT NULL CHECK (btrim(created_by) <> ''),
    created_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    PRIMARY KEY (tenant_id, project_id, revision),
    FOREIGN KEY (tenant_id, project_id)
        REFERENCES public.compute_projects (tenant_id, project_id)
        DEFERRABLE INITIALLY DEFERRED
);

ALTER TABLE public.compute_projects
    ADD CONSTRAINT compute_projects_current_revision_fk
    FOREIGN KEY (tenant_id, project_id, current_revision)
    REFERENCES public.compute_project_revisions (tenant_id, project_id, revision)
    DEFERRABLE INITIALLY DEFERRED;

CREATE INDEX compute_projects_tenant_state
    ON public.compute_projects (tenant_id, state, updated_at DESC, project_id);

CREATE TABLE public.compute_pools (
    tenant_id TEXT NOT NULL CHECK (btrim(tenant_id) <> ''),
    pool_id TEXT NOT NULL CHECK (length(pool_id) BETWEEN 1 AND 256 AND btrim(pool_id)=pool_id),
    state TEXT NOT NULL CHECK (state IN ('DRAFT','ACTIVE','DRAINING','DISABLED','RETIRED')),
    current_revision BIGINT NOT NULL CHECK (current_revision > 0),
    version BIGINT NOT NULL CHECK (version > 0),
    created_by TEXT NOT NULL CHECK (btrim(created_by) <> ''),
    updated_by TEXT NOT NULL CHECK (btrim(updated_by) <> ''),
    last_action TEXT NOT NULL CHECK (last_action IN ('CREATED','REVISION_PUBLISHED','STATE_TRANSITIONED')),
    last_reason_code TEXT NOT NULL CHECK (last_reason_code ~ '^[A-Z][A-Z0-9_]{0,127}$'),
    created_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    PRIMARY KEY (tenant_id, pool_id),
    UNIQUE (tenant_id, pool_id, current_revision)
);

CREATE TABLE public.compute_pool_revisions (
    tenant_id TEXT NOT NULL,
    pool_id TEXT NOT NULL,
    revision BIGINT NOT NULL CHECK (revision > 0),
    spec JSONB NOT NULL CHECK (
        jsonb_typeof(spec)='object'
        AND spec->>'schema_version'='1'
        AND jsonb_typeof(spec->'provider_refs')='array'
        AND jsonb_typeof(spec->'accelerator_refs')='array'
    ),
    spec_digest TEXT NOT NULL CHECK (
        spec_digest ~ '^sha256:[a-f0-9]{64}$'
        OR spec_digest ~ '^legacy:[A-Za-z0-9._:-]+$'
    ),
    created_by TEXT NOT NULL CHECK (btrim(created_by) <> ''),
    created_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    PRIMARY KEY (tenant_id, pool_id, revision),
    FOREIGN KEY (tenant_id, pool_id)
        REFERENCES public.compute_pools (tenant_id, pool_id)
        DEFERRABLE INITIALLY DEFERRED
);

ALTER TABLE public.compute_pools
    ADD CONSTRAINT compute_pools_current_revision_fk
    FOREIGN KEY (tenant_id, pool_id, current_revision)
    REFERENCES public.compute_pool_revisions (tenant_id, pool_id, revision)
    DEFERRABLE INITIALLY DEFERRED;

CREATE INDEX compute_pools_tenant_state
    ON public.compute_pools (tenant_id, state, updated_at DESC, pool_id);

CREATE TABLE public.compute_accelerators (
    tenant_id TEXT NOT NULL CHECK (btrim(tenant_id) <> ''),
    accelerator_id TEXT NOT NULL CHECK (length(accelerator_id) BETWEEN 1 AND 256 AND btrim(accelerator_id)=accelerator_id),
    state TEXT NOT NULL CHECK (state IN ('DRAFT','ACTIVE','DEPRECATED','RETIRED')),
    current_revision BIGINT NOT NULL CHECK (current_revision > 0),
    version BIGINT NOT NULL CHECK (version > 0),
    created_by TEXT NOT NULL CHECK (btrim(created_by) <> ''),
    updated_by TEXT NOT NULL CHECK (btrim(updated_by) <> ''),
    last_action TEXT NOT NULL CHECK (last_action IN ('CREATED','REVISION_PUBLISHED','STATE_TRANSITIONED')),
    last_reason_code TEXT NOT NULL CHECK (last_reason_code ~ '^[A-Z][A-Z0-9_]{0,127}$'),
    created_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    PRIMARY KEY (tenant_id, accelerator_id),
    UNIQUE (tenant_id, accelerator_id, current_revision)
);

CREATE TABLE public.compute_accelerator_revisions (
    tenant_id TEXT NOT NULL,
    accelerator_id TEXT NOT NULL,
    revision BIGINT NOT NULL CHECK (revision > 0),
    spec JSONB NOT NULL CHECK (
        jsonb_typeof(spec)='object'
        AND spec->>'schema_version'='1'
        AND length(btrim(spec->>'vendor')) > 0
        AND length(btrim(spec->>'model')) > 0
        AND jsonb_typeof(spec->'capabilities')='object'
    ),
    spec_digest TEXT NOT NULL CHECK (
        spec_digest ~ '^sha256:[a-f0-9]{64}$'
        OR spec_digest ~ '^legacy:[A-Za-z0-9._:-]+$'
    ),
    created_by TEXT NOT NULL CHECK (btrim(created_by) <> ''),
    created_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    PRIMARY KEY (tenant_id, accelerator_id, revision),
    FOREIGN KEY (tenant_id, accelerator_id)
        REFERENCES public.compute_accelerators (tenant_id, accelerator_id)
        DEFERRABLE INITIALLY DEFERRED
);

ALTER TABLE public.compute_accelerators
    ADD CONSTRAINT compute_accelerators_current_revision_fk
    FOREIGN KEY (tenant_id, accelerator_id, current_revision)
    REFERENCES public.compute_accelerator_revisions (tenant_id, accelerator_id, revision)
    DEFERRABLE INITIALLY DEFERRED;

CREATE INDEX compute_accelerators_tenant_state
    ON public.compute_accelerators (tenant_id, state, updated_at DESC, accelerator_id);

-- Revision references bind a Project revision to exact Pool revisions and a
-- Pool revision to exact Accelerator revisions. Updating a referenced resource
-- never silently changes an existing policy graph.
CREATE TABLE public.compute_project_pool_revision_refs (
    tenant_id TEXT NOT NULL,
    project_id TEXT NOT NULL,
    project_revision BIGINT NOT NULL,
    pool_id TEXT NOT NULL,
    pool_revision BIGINT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    PRIMARY KEY (tenant_id, project_id, project_revision, pool_id),
    FOREIGN KEY (tenant_id, project_id, project_revision)
        REFERENCES public.compute_project_revisions (tenant_id, project_id, revision),
    FOREIGN KEY (tenant_id, pool_id, pool_revision)
        REFERENCES public.compute_pool_revisions (tenant_id, pool_id, revision)
);
CREATE INDEX compute_project_pool_refs_pool
    ON public.compute_project_pool_revision_refs (tenant_id, pool_id, pool_revision);

CREATE TABLE public.compute_pool_accelerator_revision_refs (
    tenant_id TEXT NOT NULL,
    pool_id TEXT NOT NULL,
    pool_revision BIGINT NOT NULL,
    accelerator_id TEXT NOT NULL,
    accelerator_revision BIGINT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    PRIMARY KEY (tenant_id, pool_id, pool_revision, accelerator_id),
    FOREIGN KEY (tenant_id, pool_id, pool_revision)
        REFERENCES public.compute_pool_revisions (tenant_id, pool_id, revision),
    FOREIGN KEY (tenant_id, accelerator_id, accelerator_revision)
        REFERENCES public.compute_accelerator_revisions (tenant_id, accelerator_id, revision)
);
CREATE INDEX compute_pool_accelerator_refs_accelerator
    ON public.compute_pool_accelerator_revision_refs (tenant_id, accelerator_id, accelerator_revision);

-- A revision's resolved edges are immutable too. The referenced revision
-- rows remain independently addressable even after current pointers advance.
CREATE OR REPLACE FUNCTION public.compute_versioned_resource_ref_immutable()
RETURNS trigger
LANGUAGE plpgsql
SET search_path = pg_catalog, pg_temp
AS $ref_immutable$
BEGIN
    RAISE EXCEPTION 'versioned resource revision references are immutable'
        USING ERRCODE = '55000';
END;
$ref_immutable$;
CREATE TRIGGER compute_project_pool_revision_refs_immutable
BEFORE UPDATE OR DELETE ON public.compute_project_pool_revision_refs
FOR EACH ROW EXECUTE FUNCTION public.compute_versioned_resource_ref_immutable();
CREATE TRIGGER compute_pool_accelerator_revision_refs_immutable
BEFORE UPDATE OR DELETE ON public.compute_pool_accelerator_revision_refs
FOR EACH ROW EXECUTE FUNCTION public.compute_versioned_resource_ref_immutable();

CREATE TABLE public.compute_resource_lifecycle_audit (
    event_id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    tenant_id TEXT NOT NULL,
    resource_kind TEXT NOT NULL CHECK (resource_kind IN ('PROJECT','COMPUTE_POOL','ACCELERATOR')),
    resource_id TEXT NOT NULL,
    aggregate_version BIGINT NOT NULL CHECK (aggregate_version > 0),
    revision BIGINT NOT NULL CHECK (revision > 0),
    action TEXT NOT NULL CHECK (action IN ('CREATED','REVISION_PUBLISHED','STATE_TRANSITIONED')),
    previous_state TEXT,
    state TEXT NOT NULL,
    reason_code TEXT NOT NULL CHECK (reason_code ~ '^[A-Z][A-Z0-9_]{0,127}$'),
    actor TEXT NOT NULL CHECK (btrim(actor) <> ''),
    spec_digest TEXT NOT NULL,
    changed_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    UNIQUE (tenant_id, resource_kind, resource_id, aggregate_version)
);
CREATE INDEX compute_resource_lifecycle_audit_subject
    ON public.compute_resource_lifecycle_audit (tenant_id, resource_kind, resource_id, event_id DESC);

CREATE OR REPLACE FUNCTION public.compute_versioned_resource_revision_immutable()
RETURNS trigger
LANGUAGE plpgsql
SET search_path = pg_catalog, pg_temp
AS $immutable$
BEGIN
    RAISE EXCEPTION 'versioned resource revisions are immutable'
        USING ERRCODE = '55000';
END;
$immutable$;

CREATE TRIGGER compute_project_revisions_immutable
BEFORE UPDATE OR DELETE ON public.compute_project_revisions
FOR EACH ROW EXECUTE FUNCTION public.compute_versioned_resource_revision_immutable();
CREATE TRIGGER compute_pool_revisions_immutable
BEFORE UPDATE OR DELETE ON public.compute_pool_revisions
FOR EACH ROW EXECUTE FUNCTION public.compute_versioned_resource_revision_immutable();
CREATE TRIGGER compute_accelerator_revisions_immutable
BEFORE UPDATE OR DELETE ON public.compute_accelerator_revisions
FOR EACH ROW EXECUTE FUNCTION public.compute_versioned_resource_revision_immutable();

CREATE OR REPLACE FUNCTION public.compute_versioned_resource_guard()
RETURNS trigger
LANGUAGE plpgsql
SET search_path = pg_catalog, pg_temp
AS $guard$
DECLARE
    allowed_transition BOOLEAN := FALSE;
BEGIN
    IF TG_OP = 'INSERT' THEN
        IF NEW.version <> 1 OR NEW.current_revision <> 1 OR NEW.last_action <> 'CREATED' THEN
            RAISE EXCEPTION 'versioned resource must start at version 1 / revision 1'
                USING ERRCODE = '23514';
        END IF;
        IF (TG_TABLE_NAME='compute_projects' AND NEW.state <> 'ACTIVE')
           OR (TG_TABLE_NAME IN ('compute_pools','compute_accelerators') AND NEW.state <> 'DRAFT') THEN
            RAISE EXCEPTION 'invalid initial versioned resource state'
                USING ERRCODE = '23514';
        END IF;
        RETURN NEW;
    END IF;

    IF NEW.tenant_id IS DISTINCT FROM OLD.tenant_id
       OR NEW.version <> OLD.version + 1
       OR NEW.created_by IS DISTINCT FROM OLD.created_by
       OR NEW.created_at IS DISTINCT FROM OLD.created_at
       OR btrim(NEW.updated_by) = ''
       OR NEW.last_reason_code !~ '^[A-Z][A-Z0-9_]{0,127}$' THEN
        RAISE EXCEPTION 'versioned resource identity and version must be monotonic'
            USING ERRCODE = '23514';
    END IF;

    IF TG_TABLE_NAME='compute_projects' AND NEW.project_id IS DISTINCT FROM OLD.project_id
       OR TG_TABLE_NAME='compute_pools' AND NEW.pool_id IS DISTINCT FROM OLD.pool_id
       OR TG_TABLE_NAME='compute_accelerators' AND NEW.accelerator_id IS DISTINCT FROM OLD.accelerator_id THEN
        RAISE EXCEPTION 'versioned resource identity is immutable'
            USING ERRCODE = '23514';
    END IF;

    IF NEW.current_revision = OLD.current_revision + 1
       AND NEW.state = OLD.state
       AND NEW.last_action = 'REVISION_PUBLISHED' THEN
        RETURN NEW;
    ELSIF NEW.current_revision = OLD.current_revision
       AND NEW.state IS DISTINCT FROM OLD.state
       AND NEW.last_action = 'STATE_TRANSITIONED' THEN
        IF TG_TABLE_NAME='compute_projects' THEN
            allowed_transition :=
                (OLD.state='ACTIVE' AND NEW.state IN ('SUSPENDED','ARCHIVED'))
                OR (OLD.state='SUSPENDED' AND NEW.state IN ('ACTIVE','ARCHIVED'));
        ELSIF TG_TABLE_NAME='compute_pools' THEN
            allowed_transition :=
                (OLD.state='DRAFT' AND NEW.state IN ('ACTIVE','RETIRED'))
                OR (OLD.state='ACTIVE' AND NEW.state IN ('DRAINING','DISABLED'))
                OR (OLD.state='DRAINING' AND NEW.state IN ('ACTIVE','DISABLED','RETIRED'))
                OR (OLD.state='DISABLED' AND NEW.state IN ('ACTIVE','RETIRED'));
        ELSIF TG_TABLE_NAME='compute_accelerators' THEN
            allowed_transition :=
                (OLD.state='DRAFT' AND NEW.state IN ('ACTIVE','RETIRED'))
                OR (OLD.state='ACTIVE' AND NEW.state IN ('DEPRECATED','RETIRED'))
                OR (OLD.state='DEPRECATED' AND NEW.state IN ('ACTIVE','RETIRED'));
        END IF;
        IF allowed_transition THEN
            RETURN NEW;
        END IF;
    END IF;

    RAISE EXCEPTION 'invalid versioned resource mutation'
        USING ERRCODE = '23514';
END;
$guard$;

CREATE TRIGGER compute_projects_version_guard
BEFORE INSERT OR UPDATE ON public.compute_projects
FOR EACH ROW EXECUTE FUNCTION public.compute_versioned_resource_guard();
CREATE TRIGGER compute_pools_version_guard
BEFORE INSERT OR UPDATE ON public.compute_pools
FOR EACH ROW EXECUTE FUNCTION public.compute_versioned_resource_guard();
CREATE TRIGGER compute_accelerators_version_guard
BEFORE INSERT OR UPDATE ON public.compute_accelerators
FOR EACH ROW EXECUTE FUNCTION public.compute_versioned_resource_guard();

CREATE OR REPLACE FUNCTION public.compute_versioned_resource_audit_append()
RETURNS trigger
LANGUAGE plpgsql
SET search_path = pg_catalog, pg_temp
AS $audit$
DECLARE
    kind TEXT;
    resource_id_value TEXT;
    digest_value TEXT;
    revision_table TEXT;
    id_column TEXT;
    previous_state_value TEXT;
BEGIN
    CASE TG_TABLE_NAME
        WHEN 'compute_projects' THEN
            kind := 'PROJECT'; resource_id_value := NEW.project_id;
            revision_table := 'compute_project_revisions'; id_column := 'project_id';
        WHEN 'compute_pools' THEN
            kind := 'COMPUTE_POOL'; resource_id_value := NEW.pool_id;
            revision_table := 'compute_pool_revisions'; id_column := 'pool_id';
        WHEN 'compute_accelerators' THEN
            kind := 'ACCELERATOR'; resource_id_value := NEW.accelerator_id;
            revision_table := 'compute_accelerator_revisions'; id_column := 'accelerator_id';
        ELSE
            RAISE EXCEPTION 'unsupported versioned resource audit source'
                USING ERRCODE = '42501';
    END CASE;

    IF TG_OP='INSERT' THEN
        previous_state_value := NULL;
    ELSE
        previous_state_value := OLD.state;
    END IF;

    EXECUTE pg_catalog.format(
        'SELECT spec_digest FROM public.%I WHERE tenant_id=$1 AND %I=$2 AND revision=$3',
        revision_table, id_column
    ) INTO digest_value USING NEW.tenant_id, resource_id_value, NEW.current_revision;

    IF digest_value IS NULL THEN
        RAISE EXCEPTION 'current versioned resource revision is missing'
            USING ERRCODE = '23503';
    END IF;

    INSERT INTO public.compute_resource_lifecycle_audit (
        tenant_id, resource_kind, resource_id, aggregate_version, revision,
        action, previous_state, state, reason_code, actor, spec_digest
    ) VALUES (
        NEW.tenant_id, kind, resource_id_value, NEW.version, NEW.current_revision,
        NEW.last_action, previous_state_value, NEW.state, NEW.last_reason_code,
        NEW.updated_by, digest_value
    );
    RETURN NEW;
END;
$audit$;

CREATE TRIGGER compute_projects_lifecycle_audit
AFTER INSERT OR UPDATE ON public.compute_projects
FOR EACH ROW EXECUTE FUNCTION public.compute_versioned_resource_audit_append();
CREATE TRIGGER compute_pools_lifecycle_audit
AFTER INSERT OR UPDATE ON public.compute_pools
FOR EACH ROW EXECUTE FUNCTION public.compute_versioned_resource_audit_append();
CREATE TRIGGER compute_accelerators_lifecycle_audit
AFTER INSERT OR UPDATE ON public.compute_accelerators
FOR EACH ROW EXECUTE FUNCTION public.compute_versioned_resource_audit_append();

CREATE OR REPLACE FUNCTION public.compute_versioned_resource_audit_immutable()
RETURNS trigger
LANGUAGE plpgsql
SET search_path = pg_catalog, pg_temp
AS $audit_immutable$
BEGIN
    RAISE EXCEPTION 'versioned resource lifecycle audit is append-only'
        USING ERRCODE = '55000';
END;
$audit_immutable$;

CREATE TRIGGER compute_resource_lifecycle_audit_no_update_delete
BEFORE UPDATE OR DELETE ON public.compute_resource_lifecycle_audit
FOR EACH ROW EXECUTE FUNCTION public.compute_versioned_resource_audit_immutable();
CREATE TRIGGER compute_resource_lifecycle_audit_no_truncate
BEFORE TRUNCATE ON public.compute_resource_lifecycle_audit
FOR EACH STATEMENT EXECUTE FUNCTION public.compute_versioned_resource_audit_immutable();

-- Preserve all Project IDs already referenced by Workloads or trusted service
-- grants. Imported placeholders are explicitly marked and can be revised
-- through the normal CAS lifecycle before they influence new scheduling.
WITH legacy_projects AS (
    SELECT tenant_id, project_id FROM public.compute_workloads
    UNION
    SELECT tenant_id, project_id FROM public.compute_service_project_grants
),
specs AS (
    SELECT tenant_id, project_id,
           jsonb_build_object(
               'schema_version', 1,
               'name', project_id,
               'description', 'Imported from a pre-versioned Project reference',
               'labels', jsonb_build_object('legacy_import','true'),
               'pool_refs', jsonb_build_array()
           ) AS spec
    FROM legacy_projects
)
INSERT INTO public.compute_project_revisions (
    tenant_id, project_id, revision, spec, spec_digest, created_by
)
SELECT tenant_id, project_id, 1, spec, 'legacy:0014:project', 'migration:0014'
FROM specs;

INSERT INTO public.compute_projects (
    tenant_id, project_id, state, current_revision, version,
    created_by, updated_by, last_action, last_reason_code
)
SELECT tenant_id, project_id, 'ACTIVE', 1, 1,
       'migration:0014', 'migration:0014', 'CREATED', 'LEGACY_IMPORT'
FROM (
    SELECT tenant_id, project_id FROM public.compute_workloads
    UNION
    SELECT tenant_id, project_id FROM public.compute_service_project_grants
) legacy_projects;
