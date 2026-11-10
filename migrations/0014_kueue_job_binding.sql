-- Kueue Job CREATE pins and DELETE tombstones for KueueJobBindingStore.
-- Keyed by namespace + tenant_id + operation_id. This is the acknowledged
-- Job UID and the UID-fenced delete intent. It is not
-- compute_operations.provider_resource_uid, not Allocation RELEASED, and not
-- Kueue quota release. A pin or tombstone UID cannot be rewritten: UPDATE of
-- the identity columns is rejected, and DELETE/TRUNCATE is rejected so the
-- row cannot be removed and inserted again. Tombstone acknowledgement can
-- move from false to true and cannot move back. Versions 0001..0013 are unchanged.

CREATE TABLE public.kueue_job_create_pins (
    namespace TEXT NOT NULL
        CHECK (
            pg_catalog.char_length(namespace) BETWEEN 1 AND 63
            AND namespace ~ '^[a-z0-9]([-a-z0-9]*[a-z0-9])?$'
        ),
    tenant_id TEXT NOT NULL,
    operation_id TEXT NOT NULL,
    job_name TEXT NOT NULL
        CHECK (
            pg_catalog.char_length(job_name) BETWEEN 1 AND 63
            AND job_name ~ '^[a-z0-9]([-a-z0-9]*[a-z0-9])?$'
        ),
    job_uid TEXT NOT NULL
        CHECK (
            pg_catalog.char_length(job_uid) > 0
            AND job_uid = pg_catalog.btrim(job_uid)
            AND pg_catalog.strpos(job_uid, '/') = 0
            AND pg_catalog.strpos(job_uid, E'\\') = 0
            AND pg_catalog.strpos(job_uid, ' ') = 0
            AND pg_catalog.strpos(job_uid, E'\t') = 0
            AND pg_catalog.strpos(job_uid, E'\n') = 0
            AND pg_catalog.strpos(job_uid, E'\r') = 0
        ),
    workload_id TEXT NOT NULL,
    provider_id TEXT NOT NULL,
    idempotency_key TEXT NOT NULL,
    request_digest TEXT NOT NULL,
    desired_generation BIGINT NOT NULL CHECK (desired_generation >= 0),
    operation_kind TEXT NOT NULL,
    target_resource_id TEXT NOT NULL,
    target_resource_uid TEXT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT pg_catalog.clock_timestamp(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT pg_catalog.clock_timestamp(),
    CONSTRAINT kueue_job_create_pins_pkey PRIMARY KEY (namespace, tenant_id, operation_id)
);

CREATE UNIQUE INDEX kueue_job_create_pins_namespace_job
    ON public.kueue_job_create_pins (namespace, job_name);

COMMENT ON TABLE public.kueue_job_create_pins IS
    'Acknowledged Kubernetes Job UID for one CREATE operation. Not compute_operations.provider_resource_uid, not Allocation RELEASED, and not Kueue quota release.';

CREATE TABLE public.kueue_job_delete_tombstones (
    namespace TEXT NOT NULL
        CHECK (
            pg_catalog.char_length(namespace) BETWEEN 1 AND 63
            AND namespace ~ '^[a-z0-9]([-a-z0-9]*[a-z0-9])?$'
        ),
    tenant_id TEXT NOT NULL,
    operation_id TEXT NOT NULL,
    job_name TEXT NOT NULL
        CHECK (
            pg_catalog.char_length(job_name) BETWEEN 1 AND 63
            AND job_name ~ '^[a-z0-9]([-a-z0-9]*[a-z0-9])?$'
        ),
    job_uid TEXT NOT NULL
        CHECK (
            pg_catalog.char_length(job_uid) > 0
            AND job_uid = pg_catalog.btrim(job_uid)
            AND pg_catalog.strpos(job_uid, '/') = 0
            AND pg_catalog.strpos(job_uid, E'\\') = 0
            AND pg_catalog.strpos(job_uid, ' ') = 0
            AND pg_catalog.strpos(job_uid, E'\t') = 0
            AND pg_catalog.strpos(job_uid, E'\n') = 0
            AND pg_catalog.strpos(job_uid, E'\r') = 0
        ),
    workload_id TEXT NOT NULL,
    provider_id TEXT NOT NULL,
    idempotency_key TEXT NOT NULL,
    request_digest TEXT NOT NULL,
    desired_generation BIGINT NOT NULL CHECK (desired_generation >= 0),
    operation_kind TEXT NOT NULL,
    target_resource_id TEXT NOT NULL,
    target_resource_uid TEXT NOT NULL,
    acknowledged BOOLEAN NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT pg_catalog.clock_timestamp(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT pg_catalog.clock_timestamp(),
    CONSTRAINT kueue_job_delete_tombstones_pkey PRIMARY KEY (namespace, tenant_id, operation_id)
);

COMMENT ON TABLE public.kueue_job_delete_tombstones IS
    'UID-fenced DELETE intent. Acknowledged means the API accepted that UID or then reported it gone. Not Allocation RELEASED and not Kueue quota release.';

CREATE FUNCTION public.kueue_job_create_pin_guard()
RETURNS trigger
LANGUAGE plpgsql
SET search_path = pg_catalog, pg_temp
AS $kueue_job_create_pin_guard$
BEGIN
    IF TG_OP = 'DELETE' THEN
        RAISE EXCEPTION 'kueue job create pin cannot be deleted'
            USING ERRCODE = '55000';
    END IF;
    IF TG_OP = 'TRUNCATE' THEN
        RAISE EXCEPTION 'kueue job create pin cannot be truncated'
            USING ERRCODE = '55000';
    END IF;
    IF TG_OP = 'UPDATE' THEN
        RAISE EXCEPTION 'kueue job create pin cannot be rewritten'
            USING ERRCODE = '55000';
    END IF;
    RETURN NEW;
END;
$kueue_job_create_pin_guard$;

CREATE TRIGGER kueue_job_create_pin_no_rewrite
BEFORE UPDATE OR DELETE ON public.kueue_job_create_pins
FOR EACH ROW EXECUTE FUNCTION public.kueue_job_create_pin_guard();

CREATE TRIGGER kueue_job_create_pin_no_truncate
BEFORE TRUNCATE ON public.kueue_job_create_pins
FOR EACH STATEMENT EXECUTE FUNCTION public.kueue_job_create_pin_guard();

CREATE FUNCTION public.kueue_job_delete_tombstone_guard()
RETURNS trigger
LANGUAGE plpgsql
SET search_path = pg_catalog, pg_temp
AS $kueue_job_delete_tombstone_guard$
BEGIN
    IF TG_OP = 'DELETE' THEN
        RAISE EXCEPTION 'kueue job delete tombstone cannot be deleted'
            USING ERRCODE = '55000';
    END IF;
    IF TG_OP = 'TRUNCATE' THEN
        RAISE EXCEPTION 'kueue job delete tombstone cannot be truncated'
            USING ERRCODE = '55000';
    END IF;
    IF TG_OP = 'UPDATE' THEN
        IF NEW.namespace IS DISTINCT FROM OLD.namespace
           OR NEW.tenant_id IS DISTINCT FROM OLD.tenant_id
           OR NEW.operation_id IS DISTINCT FROM OLD.operation_id
           OR NEW.job_name IS DISTINCT FROM OLD.job_name
           OR NEW.job_uid IS DISTINCT FROM OLD.job_uid
           OR NEW.workload_id IS DISTINCT FROM OLD.workload_id
           OR NEW.provider_id IS DISTINCT FROM OLD.provider_id
           OR NEW.idempotency_key IS DISTINCT FROM OLD.idempotency_key
           OR NEW.request_digest IS DISTINCT FROM OLD.request_digest
           OR NEW.desired_generation IS DISTINCT FROM OLD.desired_generation
           OR NEW.operation_kind IS DISTINCT FROM OLD.operation_kind
           OR NEW.target_resource_id IS DISTINCT FROM OLD.target_resource_id
           OR NEW.target_resource_uid IS DISTINCT FROM OLD.target_resource_uid
           OR NEW.created_at IS DISTINCT FROM OLD.created_at
        THEN
            RAISE EXCEPTION 'kueue job delete tombstone cannot be rewritten'
                USING ERRCODE = '55000';
        END IF;
        IF OLD.acknowledged AND NOT NEW.acknowledged THEN
            RAISE EXCEPTION 'kueue job delete tombstone acknowledgement cannot be cleared'
                USING ERRCODE = '55000';
        END IF;
    END IF;
    RETURN NEW;
END;
$kueue_job_delete_tombstone_guard$;

CREATE TRIGGER kueue_job_delete_tombstone_no_rewrite
BEFORE UPDATE OR DELETE ON public.kueue_job_delete_tombstones
FOR EACH ROW EXECUTE FUNCTION public.kueue_job_delete_tombstone_guard();

CREATE TRIGGER kueue_job_delete_tombstone_no_truncate
BEFORE TRUNCATE ON public.kueue_job_delete_tombstones
FOR EACH STATEMENT EXECUTE FUNCTION public.kueue_job_delete_tombstone_guard();

ALTER TABLE public.kueue_job_create_pins ENABLE ALWAYS TRIGGER kueue_job_create_pin_no_rewrite;
ALTER TABLE public.kueue_job_create_pins ENABLE ALWAYS TRIGGER kueue_job_create_pin_no_truncate;
ALTER TABLE public.kueue_job_delete_tombstones ENABLE ALWAYS TRIGGER kueue_job_delete_tombstone_no_rewrite;
ALTER TABLE public.kueue_job_delete_tombstones ENABLE ALWAYS TRIGGER kueue_job_delete_tombstone_no_truncate;
