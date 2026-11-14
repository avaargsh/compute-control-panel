-- Evidence-only, append-only custom DRA NodeUnprepare receipt replay fence.
-- This is NOT a Kubernetes-signed RPC, source-key registry, physical GPU
-- deallocation attestation, Allocation RELEASED, or billing FINAL.
-- No application login is granted access; a trusted ingest writer and source
-- authority must be provisioned independently before production wiring.

CREATE TABLE public.dra_unprepare_receipt_journal (
    cluster_id TEXT NOT NULL CHECK (cluster_id <> '' AND cluster_id = pg_catalog.btrim(cluster_id)),
    source_id TEXT NOT NULL CHECK (source_id <> '' AND source_id = pg_catalog.btrim(source_id)),
    event_id TEXT NOT NULL CHECK (event_id <> '' AND event_id = pg_catalog.btrim(event_id)),
    driver TEXT NOT NULL CHECK (driver <> '' AND driver = pg_catalog.btrim(driver)),
    tenant_id TEXT NOT NULL CHECK (tenant_id <> '' AND tenant_id = pg_catalog.btrim(tenant_id)),
    provider_id TEXT NOT NULL CHECK (provider_id <> '' AND provider_id = pg_catalog.btrim(provider_id)),
    allocation_id TEXT NOT NULL CHECK (allocation_id <> '' AND allocation_id = pg_catalog.btrim(allocation_id)),
    claim_uid TEXT NOT NULL CHECK (claim_uid <> '' AND claim_uid = pg_catalog.btrim(claim_uid)),
    pod_uid TEXT NOT NULL CHECK (pod_uid <> '' AND pod_uid = pg_catalog.btrim(pod_uid)),
    allocated_claim_resource_version TEXT NOT NULL
        CHECK (allocated_claim_resource_version <> '' AND allocated_claim_resource_version = pg_catalog.btrim(allocated_claim_resource_version)),
    device_digest TEXT NOT NULL CHECK (device_digest ~ '^sha256:[0-9a-f]{64}$'),
    record_digest TEXT NOT NULL CHECK (record_digest ~ '^sha256:[0-9a-f]{64}$'),
    record_payload JSONB NOT NULL CHECK (pg_catalog.jsonb_typeof(record_payload) = 'object'),
    completed_at TIMESTAMPTZ NOT NULL,
    recorded_at TIMESTAMPTZ NOT NULL DEFAULT pg_catalog.clock_timestamp(),
    CONSTRAINT dra_unprepare_receipt_journal_pkey PRIMARY KEY (cluster_id, source_id, event_id),
    -- Across source IDs and event IDs, one exact previously allocated device
    -- epoch may have only one successful signed custom Unprepare assertion.
    CONSTRAINT dra_unprepare_receipt_one_device_epoch UNIQUE (
        cluster_id, driver, claim_uid, pod_uid, allocated_claim_resource_version, device_digest
    )
);

COMMENT ON TABLE public.dra_unprepare_receipt_journal IS
    'Immutable, source-UNATTESTED signed DRA receipt deduplication. Never physical GPU RELEASED or finance FINAL.';

REVOKE ALL ON TABLE public.dra_unprepare_receipt_journal FROM PUBLIC;

CREATE FUNCTION public.dra_unprepare_receipt_journal_guard()
RETURNS trigger
LANGUAGE plpgsql
SET search_path = pg_catalog, pg_temp
AS $dra_unprepare_receipt_journal_guard$
BEGIN
    RAISE EXCEPTION 'DRA unprepare receipt journal is immutable'
        USING ERRCODE = '55000';
END;
$dra_unprepare_receipt_journal_guard$;

CREATE TRIGGER dra_unprepare_receipt_no_update_delete
BEFORE UPDATE OR DELETE ON public.dra_unprepare_receipt_journal
FOR EACH ROW EXECUTE FUNCTION public.dra_unprepare_receipt_journal_guard();

CREATE TRIGGER dra_unprepare_receipt_no_truncate
BEFORE TRUNCATE ON public.dra_unprepare_receipt_journal
FOR EACH STATEMENT EXECUTE FUNCTION public.dra_unprepare_receipt_journal_guard();

ALTER TABLE public.dra_unprepare_receipt_journal
    ENABLE ALWAYS TRIGGER dra_unprepare_receipt_no_update_delete;
ALTER TABLE public.dra_unprepare_receipt_journal
    ENABLE ALWAYS TRIGGER dra_unprepare_receipt_no_truncate;

REVOKE ALL ON FUNCTION public.dra_unprepare_receipt_journal_guard() FROM PUBLIC;
