-- Independently administered, append-only public-key source attestation.
-- The database owner must provision SOURCE keys out of band from a trusted
-- driver/Provider registration. This migration creates NO keys and no new
-- login, mints NO credentials, and connects to NO real DRA driver.
-- A key+scope row proves only that a DBA registered that signer, not that
-- NodeUnprepareResources actually executed or physical GPU release occurred.
-- Historical migrations 0001..0017 are immutable.

CREATE TABLE public.dra_unprepare_source_keys (
    cluster_id TEXT NOT NULL CHECK (cluster_id <> '' AND cluster_id = pg_catalog.btrim(cluster_id)),
    source_id TEXT NOT NULL CHECK (source_id <> '' AND source_id = pg_catalog.btrim(source_id)),
    key_id TEXT NOT NULL CHECK (key_id <> '' AND key_id = pg_catalog.btrim(key_id)),
    tenant_id TEXT NOT NULL CHECK (tenant_id <> '' AND tenant_id = pg_catalog.btrim(tenant_id)),
    provider_id TEXT NOT NULL CHECK (provider_id <> '' AND provider_id = pg_catalog.btrim(provider_id)),
    driver TEXT NOT NULL CHECK (driver <> '' AND driver = pg_catalog.btrim(driver)),
    public_key BYTEA NOT NULL CHECK (pg_catalog.octet_length(public_key) = 32),
    valid_from TIMESTAMPTZ NOT NULL,
    valid_until TIMESTAMPTZ NOT NULL,
    maximum_age_seconds INTEGER NOT NULL CHECK (maximum_age_seconds BETWEEN 1 AND 3600),
    future_skew_seconds INTEGER NOT NULL CHECK (future_skew_seconds BETWEEN 0 AND 60),
    CONSTRAINT dra_unprepare_source_keys_window CHECK (valid_until > valid_from),
    CONSTRAINT dra_unprepare_source_keys_pkey PRIMARY KEY (cluster_id, source_id, key_id)
);

CREATE TABLE public.dra_unprepare_source_revocations (
    cluster_id TEXT NOT NULL,
    source_id TEXT NOT NULL,
    key_id TEXT NOT NULL,
    reason TEXT NOT NULL CHECK (reason <> '' AND reason = pg_catalog.btrim(reason)),
    revoked_at TIMESTAMPTZ NOT NULL DEFAULT pg_catalog.clock_timestamp(),
    CONSTRAINT dra_unprepare_source_revocations_pkey PRIMARY KEY (cluster_id, source_id, key_id),
    CONSTRAINT dra_unprepare_source_revocations_key_fkey
      FOREIGN KEY (cluster_id, source_id, key_id)
      REFERENCES public.dra_unprepare_source_keys (cluster_id, source_id, key_id)
);

-- The journal's pre-existing SECURITY INVOKER guard is fail-closed for all
-- mutating DML. NEVER use a trigger disabled in replica mode.
CREATE TRIGGER dra_unprepare_keys_no_update_delete
BEFORE UPDATE OR DELETE ON public.dra_unprepare_source_keys
FOR EACH ROW EXECUTE FUNCTION public.dra_unprepare_receipt_journal_guard();
CREATE TRIGGER dra_unprepare_keys_no_truncate
BEFORE TRUNCATE ON public.dra_unprepare_source_keys
FOR EACH STATEMENT EXECUTE FUNCTION public.dra_unprepare_receipt_journal_guard();
CREATE TRIGGER dra_unprepare_revocations_no_update_delete
BEFORE UPDATE OR DELETE ON public.dra_unprepare_source_revocations
FOR EACH ROW EXECUTE FUNCTION public.dra_unprepare_receipt_journal_guard();
CREATE TRIGGER dra_unprepare_revocations_no_truncate
BEFORE TRUNCATE ON public.dra_unprepare_source_revocations
FOR EACH STATEMENT EXECUTE FUNCTION public.dra_unprepare_receipt_journal_guard();

ALTER TABLE public.dra_unprepare_source_keys
    ENABLE ALWAYS TRIGGER dra_unprepare_keys_no_update_delete;
ALTER TABLE public.dra_unprepare_source_keys
    ENABLE ALWAYS TRIGGER dra_unprepare_keys_no_truncate;
ALTER TABLE public.dra_unprepare_source_revocations
    ENABLE ALWAYS TRIGGER dra_unprepare_revocations_no_update_delete;
ALTER TABLE public.dra_unprepare_source_revocations
    ENABLE ALWAYS TRIGGER dra_unprepare_revocations_no_truncate;

REVOKE ALL ON TABLE public.dra_unprepare_source_keys FROM PUBLIC;
REVOKE ALL ON TABLE public.dra_unprepare_source_revocations FROM PUBLIC;

-- Ingest can READ independent trust and revocation evidence, but cannot
-- INSERT/UPDATE/DELETE/TRUNCATE or administer either table. The journal
-- login was preflighted and provisioned independently by migration 0017.
GRANT SELECT ON public.dra_unprepare_source_keys TO ccp_dra_receipt_ingest;
GRANT SELECT ON public.dra_unprepare_source_revocations TO ccp_dra_receipt_ingest;

COMMENT ON TABLE public.dra_unprepare_source_keys IS
    'DBA-provisioned, immutable source public keys scoped to tenant/provider/driver and time. Not driver RPC attestation.';
COMMENT ON TABLE public.dra_unprepare_source_revocations IS
    'Append-only DBA revocation. Receipt ingest rejects revoked sources, including already-journaled replays.';
