-- Immutable pre-delete snapshot of the exact Kueue/DRA allocation lineage.
-- This records structural API observations while the objects still exist. It
-- does not authenticate the collector, prove NodeUnprepare execution, attest
-- physical GPU deallocation, authorize Allocation RELEASED, or finalize cost.
-- Snapshot insertion and DELETE tombstone creation serialize on the CREATE pin
-- row in PostgresKueueJobBindingStore. Historical migrations 0001..0024 remain
-- byte-for-byte unchanged.

CREATE TABLE public.kueue_job_pre_delete_dra_snapshots (
    namespace TEXT NOT NULL
        CHECK (pg_catalog.char_length(namespace) BETWEEN 1 AND 63
          AND namespace ~ '^[a-z0-9]([-a-z0-9]*[a-z0-9])?$'),
    tenant_id TEXT NOT NULL CHECK (tenant_id <> '' AND tenant_id = pg_catalog.btrim(tenant_id)),
    delete_operation_id TEXT NOT NULL CHECK (delete_operation_id <> '' AND delete_operation_id = pg_catalog.btrim(delete_operation_id)),
    create_operation_id TEXT NOT NULL CHECK (create_operation_id <> '' AND create_operation_id = pg_catalog.btrim(create_operation_id)),
    workload_id TEXT NOT NULL CHECK (workload_id <> '' AND workload_id = pg_catalog.btrim(workload_id)),
    provider_id TEXT NOT NULL CHECK (provider_id <> '' AND provider_id = pg_catalog.btrim(provider_id)),
    create_generation BIGINT NOT NULL CHECK (create_generation > 0),
    delete_generation BIGINT NOT NULL CHECK (delete_generation > 0),

    job_name TEXT NOT NULL
        CHECK (pg_catalog.char_length(job_name) BETWEEN 1 AND 63
          AND job_name ~ '^[a-z0-9]([-a-z0-9]*[a-z0-9])?$'),
    job_uid TEXT NOT NULL
        CHECK (job_uid <> '' AND job_uid = pg_catalog.btrim(job_uid)
          AND pg_catalog.strpos(job_uid, '/') = 0
          AND pg_catalog.strpos(job_uid, E'\\') = 0
          AND pg_catalog.strpos(job_uid, ' ') = 0),
    job_resource_version TEXT NOT NULL
        CHECK (job_resource_version <> '' AND job_resource_version = pg_catalog.btrim(job_resource_version)),

    kueue_workload_name TEXT NOT NULL
        CHECK (pg_catalog.char_length(kueue_workload_name) BETWEEN 1 AND 63
          AND kueue_workload_name ~ '^[a-z0-9]([-a-z0-9]*[a-z0-9])?$'),
    kueue_workload_uid TEXT NOT NULL
        CHECK (kueue_workload_uid <> '' AND kueue_workload_uid = pg_catalog.btrim(kueue_workload_uid)
          AND pg_catalog.strpos(kueue_workload_uid, '/') = 0
          AND pg_catalog.strpos(kueue_workload_uid, E'\\') = 0
          AND pg_catalog.strpos(kueue_workload_uid, ' ') = 0),
    kueue_workload_resource_version TEXT NOT NULL
        CHECK (kueue_workload_resource_version <> '' AND kueue_workload_resource_version = pg_catalog.btrim(kueue_workload_resource_version)),

    pod_name TEXT NOT NULL
        CHECK (pg_catalog.char_length(pod_name) BETWEEN 1 AND 63
          AND pod_name ~ '^[a-z0-9]([-a-z0-9]*[a-z0-9])?$'),
    pod_uid TEXT NOT NULL
        CHECK (pod_uid <> '' AND pod_uid = pg_catalog.btrim(pod_uid)
          AND pg_catalog.strpos(pod_uid, '/') = 0
          AND pg_catalog.strpos(pod_uid, E'\\') = 0
          AND pg_catalog.strpos(pod_uid, ' ') = 0),
    pod_resource_version TEXT NOT NULL
        CHECK (pod_resource_version <> '' AND pod_resource_version = pg_catalog.btrim(pod_resource_version)),
    node_name TEXT NOT NULL CHECK (node_name <> '' AND node_name = pg_catalog.btrim(node_name)),

    claim_name TEXT NOT NULL
        CHECK (pg_catalog.char_length(claim_name) BETWEEN 1 AND 63
          AND claim_name ~ '^[a-z0-9]([-a-z0-9]*[a-z0-9])?$'),
    claim_uid TEXT NOT NULL
        CHECK (claim_uid <> '' AND claim_uid = pg_catalog.btrim(claim_uid)
          AND pg_catalog.strpos(claim_uid, '/') = 0
          AND pg_catalog.strpos(claim_uid, E'\\') = 0
          AND pg_catalog.strpos(claim_uid, ' ') = 0),
    claim_resource_version TEXT NOT NULL
        CHECK (claim_resource_version <> '' AND claim_resource_version = pg_catalog.btrim(claim_resource_version)),
    claim_template_name TEXT NOT NULL
        CHECK (pg_catalog.char_length(claim_template_name) BETWEEN 1 AND 63
          AND claim_template_name ~ '^[a-z0-9]([-a-z0-9]*[a-z0-9])?$'),
    pod_claim_alias TEXT NOT NULL
        CHECK (pg_catalog.char_length(pod_claim_alias) BETWEEN 1 AND 63
          AND pod_claim_alias ~ '^[a-z0-9]([-a-z0-9]*[a-z0-9])?$'),
    consumer_container TEXT NOT NULL
        CHECK (pg_catalog.char_length(consumer_container) BETWEEN 1 AND 63
          AND consumer_container ~ '^[a-z0-9]([-a-z0-9]*[a-z0-9])?$'),

    devices JSONB NOT NULL
        CHECK (CASE WHEN pg_catalog.jsonb_typeof(devices) = 'array'
          THEN pg_catalog.jsonb_array_length(devices) BETWEEN 1 AND 128
          ELSE FALSE END),
    source TEXT NOT NULL CHECK (source <> '' AND source = pg_catalog.btrim(source)),
    adapter_version TEXT NOT NULL CHECK (adapter_version <> '' AND adapter_version = pg_catalog.btrim(adapter_version)),
    observed_at TIMESTAMPTZ NOT NULL,
    snapshot_digest TEXT NOT NULL CHECK (snapshot_digest ~ '^sha256:[0-9a-f]{64}$'),
    recorded_at TIMESTAMPTZ NOT NULL DEFAULT pg_catalog.clock_timestamp(),

    CONSTRAINT kueue_job_pre_delete_dra_snapshots_pkey
      PRIMARY KEY (namespace, tenant_id, delete_operation_id),
    CONSTRAINT kueue_job_pre_delete_dra_snapshots_one_delete_per_create
      UNIQUE (namespace, tenant_id, create_operation_id),
    CONSTRAINT kueue_job_pre_delete_dra_snapshots_create_pin_fkey
      FOREIGN KEY (namespace, tenant_id, create_operation_id)
      REFERENCES public.kueue_job_create_pins (namespace, tenant_id, operation_id)
);

COMMENT ON TABLE public.kueue_job_pre_delete_dra_snapshots IS
    'Immutable structural Kueue/DRA allocation snapshot captured before DELETE intent. Not authenticated source evidence, physical GPU release, Allocation RELEASED or cost FINAL.';

-- The DELETE tombstone insert guard locks the same CREATE pin row as
-- snapshot capture. It rejects a tombstone whose namespace/Job name resolves
-- to a different tenant or successor UID, including direct SQL callers.
CREATE FUNCTION public.kueue_job_delete_tombstone_insert_guard()
RETURNS trigger
LANGUAGE plpgsql
SET search_path = pg_catalog, pg_temp
AS $kueue_job_delete_tombstone_insert_guard$
DECLARE
    pinned_tenant_id TEXT;
    pinned_job_uid TEXT;
    pinned_workload_id TEXT;
    pinned_provider_id TEXT;
BEGIN
    IF NEW.operation_kind <> 'DELETE' OR
       NEW.target_resource_id <> NEW.namespace || '/' || NEW.job_name OR
       NEW.target_resource_uid <> NEW.job_uid THEN
        RAISE EXCEPTION 'DELETE tombstone target does not match its Job identity'
            USING ERRCODE = '23514';
    END IF;
    SELECT tenant_id, job_uid, workload_id, provider_id
      INTO pinned_tenant_id, pinned_job_uid, pinned_workload_id, pinned_provider_id
      FROM public.kueue_job_create_pins
     WHERE namespace = NEW.namespace AND job_name = NEW.job_name
     FOR UPDATE;
    IF FOUND AND (
        pinned_tenant_id <> NEW.tenant_id OR
        pinned_job_uid <> NEW.job_uid OR
        pinned_workload_id <> NEW.workload_id OR
        pinned_provider_id <> NEW.provider_id
    ) THEN
        RAISE EXCEPTION 'DELETE tombstone does not match the namespace CREATE pin'
            USING ERRCODE = '23514';
    END IF;
    RETURN NEW;
END;
$kueue_job_delete_tombstone_insert_guard$;

CREATE TRIGGER kueue_job_delete_tombstone_before_insert
BEFORE INSERT ON public.kueue_job_delete_tombstones
FOR EACH ROW EXECUTE FUNCTION public.kueue_job_delete_tombstone_insert_guard();

ALTER TABLE public.kueue_job_delete_tombstones
    ENABLE ALWAYS TRIGGER kueue_job_delete_tombstone_before_insert;

-- The snapshot insert guard independently enforces CREATE-pin identity and
-- the absence of DELETE intent. The row lock makes this race-safe even for
-- direct SQL inserts that bypass the Go store methods.
CREATE FUNCTION public.kueue_job_pre_delete_dra_snapshot_insert_guard()
RETURNS trigger
LANGUAGE plpgsql
SET search_path = pg_catalog, pg_temp
AS $kueue_job_pre_delete_dra_snapshot_insert_guard$
DECLARE
    pinned_job_name TEXT;
    pinned_job_uid TEXT;
    pinned_workload_id TEXT;
    pinned_provider_id TEXT;
    pinned_generation BIGINT;
BEGIN
    SELECT job_name, job_uid, workload_id, provider_id, desired_generation
      INTO pinned_job_name, pinned_job_uid, pinned_workload_id, pinned_provider_id, pinned_generation
      FROM public.kueue_job_create_pins
     WHERE namespace = NEW.namespace
       AND tenant_id = NEW.tenant_id
       AND operation_id = NEW.create_operation_id
     FOR UPDATE;
    IF NOT FOUND THEN
        RAISE EXCEPTION 'pre-delete DRA snapshot has no matching CREATE pin'
            USING ERRCODE = '23503';
    END IF;
    IF pinned_job_name <> NEW.job_name OR pinned_job_uid <> NEW.job_uid OR
       pinned_workload_id <> NEW.workload_id OR pinned_provider_id <> NEW.provider_id OR
       pinned_generation <> NEW.create_generation THEN
        RAISE EXCEPTION 'pre-delete DRA snapshot does not match its CREATE pin'
            USING ERRCODE = '23514';
    END IF;
    IF EXISTS (
        SELECT 1 FROM public.kueue_job_delete_tombstones
         WHERE namespace = NEW.namespace
           AND ((tenant_id = NEW.tenant_id AND operation_id = NEW.delete_operation_id)
                OR (job_name = NEW.job_name AND job_uid = NEW.job_uid))
    ) THEN
        RAISE EXCEPTION 'pre-delete DRA snapshot must precede DELETE intent'
            USING ERRCODE = '55000';
    END IF;
    RETURN NEW;
END;
$kueue_job_pre_delete_dra_snapshot_insert_guard$;

CREATE TRIGGER kueue_job_pre_delete_dra_snapshot_before_insert
BEFORE INSERT ON public.kueue_job_pre_delete_dra_snapshots
FOR EACH ROW EXECUTE FUNCTION public.kueue_job_pre_delete_dra_snapshot_insert_guard();

ALTER TABLE public.kueue_job_pre_delete_dra_snapshots
    ENABLE ALWAYS TRIGGER kueue_job_pre_delete_dra_snapshot_before_insert;

CREATE FUNCTION public.kueue_job_pre_delete_dra_snapshot_guard()
RETURNS trigger
LANGUAGE plpgsql
SET search_path = pg_catalog, pg_temp
AS $kueue_job_pre_delete_dra_snapshot_guard$
BEGIN
    RAISE EXCEPTION 'pre-delete DRA allocation snapshot is immutable'
        USING ERRCODE = '55000';
END;
$kueue_job_pre_delete_dra_snapshot_guard$;

CREATE TRIGGER kueue_job_pre_delete_dra_snapshot_no_update_delete
BEFORE UPDATE OR DELETE ON public.kueue_job_pre_delete_dra_snapshots
FOR EACH ROW EXECUTE FUNCTION public.kueue_job_pre_delete_dra_snapshot_guard();

CREATE TRIGGER kueue_job_pre_delete_dra_snapshot_no_truncate
BEFORE TRUNCATE ON public.kueue_job_pre_delete_dra_snapshots
FOR EACH STATEMENT EXECUTE FUNCTION public.kueue_job_pre_delete_dra_snapshot_guard();

ALTER TABLE public.kueue_job_pre_delete_dra_snapshots
    ENABLE ALWAYS TRIGGER kueue_job_pre_delete_dra_snapshot_no_update_delete;
ALTER TABLE public.kueue_job_pre_delete_dra_snapshots
    ENABLE ALWAYS TRIGGER kueue_job_pre_delete_dra_snapshot_no_truncate;

REVOKE ALL ON TABLE public.kueue_job_pre_delete_dra_snapshots FROM PUBLIC;
REVOKE ALL ON FUNCTION public.kueue_job_pre_delete_dra_snapshot_guard() FROM PUBLIC;
REVOKE ALL ON FUNCTION public.kueue_job_delete_tombstone_insert_guard() FROM PUBLIC;
REVOKE ALL ON FUNCTION public.kueue_job_pre_delete_dra_snapshot_insert_guard() FROM PUBLIC;

GRANT USAGE ON SCHEMA public TO ccp_kueue_job_binding;
GRANT SELECT, INSERT ON public.kueue_job_pre_delete_dra_snapshots TO ccp_kueue_job_binding;
GRANT EXECUTE ON FUNCTION public.kueue_job_pre_delete_dra_snapshot_guard() TO ccp_kueue_job_binding;
GRANT EXECUTE ON FUNCTION public.kueue_job_delete_tombstone_insert_guard() TO ccp_kueue_job_binding;
GRANT EXECUTE ON FUNCTION public.kueue_job_pre_delete_dra_snapshot_insert_guard() TO ccp_kueue_job_binding;
