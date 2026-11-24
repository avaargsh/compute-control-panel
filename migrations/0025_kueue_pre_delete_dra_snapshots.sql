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
        CHECK (pg_catalog.jsonb_typeof(devices) = 'array'
          AND pg_catalog.jsonb_array_length(devices) BETWEEN 1 AND 128),
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

GRANT USAGE ON SCHEMA public TO ccp_kueue_job_binding;
GRANT SELECT, INSERT ON public.kueue_job_pre_delete_dra_snapshots TO ccp_kueue_job_binding;
GRANT EXECUTE ON FUNCTION public.kueue_job_pre_delete_dra_snapshot_guard() TO ccp_kueue_job_binding;
