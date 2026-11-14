-- Least-privilege dedicated LOGIN for the custom DRA Unprepare receipt journal.
-- Provision ccp_dra_receipt_ingest independently BEFORE applying this migration.
-- PostgreSQL roles are cluster-wide; this migration must not create, reuse,
-- rename, or silently repair an unrelated role, and must not mint a password.
-- It grants only USAGE(public) and SELECT/INSERT on the immutable receipt table.
-- No Provider, Allocation, operation journal, Kueue, E1 evidence, policy or
-- ledger access is authorized. This is NOT driver-source authentication,
-- an actual NodeUnprepareResources result, GPU release or cost FINAL.
-- Versions 0001..0016 are immutable and are not edited by this change.

DO $dra_receipt_ingest_preflight$
DECLARE
    ingest_oid pg_catalog.oid;
BEGIN
    SELECT oid INTO ingest_oid FROM pg_catalog.pg_roles
    WHERE rolname = 'ccp_dra_receipt_ingest'
      AND rolcanlogin AND NOT rolsuper AND NOT rolinherit
      AND NOT rolcreatedb AND NOT rolcreaterole
      AND NOT rolreplication AND NOT rolbypassrls;
    IF NOT FOUND THEN
        RAISE EXCEPTION
            'ccp_dra_receipt_ingest must be independently provisioned as a restricted NOINHERIT LOGIN role';
    END IF;

    IF EXISTS (SELECT 1 FROM pg_catalog.pg_auth_members WHERE member = ingest_oid)
       OR EXISTS (SELECT 1 FROM pg_catalog.pg_database WHERE datdba = ingest_oid)
       OR EXISTS (SELECT 1 FROM pg_catalog.pg_namespace WHERE nspowner = ingest_oid)
       OR EXISTS (SELECT 1 FROM pg_catalog.pg_class WHERE relowner = ingest_oid)
       OR EXISTS (SELECT 1 FROM pg_catalog.pg_proc WHERE proowner = ingest_oid)
       OR pg_catalog.has_database_privilege(ingest_oid, pg_catalog.current_database(), 'CREATE')
       OR pg_catalog.has_schema_privilege(ingest_oid, 'public', 'CREATE')
       OR pg_catalog.has_parameter_privilege(ingest_oid, 'session_replication_role', 'SET')
       OR pg_catalog.has_parameter_privilege(ingest_oid, 'session_replication_role', 'ALTER SYSTEM') THEN
        RAISE EXCEPTION
            'ccp_dra_receipt_ingest has unrelated ownership, membership, DDL or replication privileges';
    END IF;

    -- Refuse direct object grants that predate migration 0017. A second
    -- direct application after completion is not supported; the checksummed
    -- migration runner skips an already-applied migration.
    IF EXISTS (
        SELECT 1 FROM pg_catalog.pg_class c
        CROSS JOIN LATERAL pg_catalog.aclexplode(c.relacl) a
        WHERE a.grantee = ingest_oid
    ) OR EXISTS (
        SELECT 1 FROM pg_catalog.pg_attribute a
        CROSS JOIN LATERAL pg_catalog.aclexplode(a.attacl) x
        WHERE x.grantee = ingest_oid
    ) OR EXISTS (
        SELECT 1 FROM pg_catalog.pg_namespace n
        CROSS JOIN LATERAL pg_catalog.aclexplode(n.nspacl) a
        WHERE a.grantee = ingest_oid
          AND (n.nspname <> 'public' OR a.privilege_type <> 'USAGE' OR a.is_grantable)
    ) OR EXISTS (
        SELECT 1 FROM pg_catalog.pg_proc p
        CROSS JOIN LATERAL pg_catalog.aclexplode(p.proacl) a
        WHERE a.grantee = ingest_oid
    ) THEN
        RAISE EXCEPTION
            'ccp_dra_receipt_ingest has unexpected grants; review before applying receipt journal migration';
    END IF;

    -- Existing PUBLIC grants are inherited even by NOINHERIT roles. Reject
    -- any effective public-relation DML outside the receipt table. Startup
    -- and each individual write reattest the *current* effective privileges.
    IF EXISTS (
        SELECT 1 FROM pg_catalog.pg_class c
        JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
        WHERE n.nspname = 'public'
          AND c.relkind IN ('r', 'p', 'v', 'm', 'f')
          AND c.relname <> 'dra_unprepare_receipt_journal'
          AND (
            pg_catalog.has_table_privilege(ingest_oid, c.oid, 'SELECT')
            OR pg_catalog.has_table_privilege(ingest_oid, c.oid, 'INSERT')
            OR pg_catalog.has_table_privilege(ingest_oid, c.oid, 'UPDATE')
            OR pg_catalog.has_table_privilege(ingest_oid, c.oid, 'DELETE')
            OR pg_catalog.has_table_privilege(ingest_oid, c.oid, 'TRUNCATE')
            OR pg_catalog.has_table_privilege(ingest_oid, c.oid, 'REFERENCES')
            OR pg_catalog.has_table_privilege(ingest_oid, c.oid, 'TRIGGER')
          )
    ) THEN
        RAISE EXCEPTION
            'ccp_dra_receipt_ingest inherits unexpected public relation privileges';
    END IF;
END;
$dra_receipt_ingest_preflight$;

GRANT USAGE ON SCHEMA public TO ccp_dra_receipt_ingest;
GRANT SELECT, INSERT ON public.dra_unprepare_receipt_journal TO ccp_dra_receipt_ingest;

COMMENT ON TABLE public.dra_unprepare_receipt_journal IS
    'Immutable evidence-only custom DRA Unprepare journal: canonical restricted ingest login granted SELECT/INSERT only in migration 0017; NOT physical release or cost FINAL.';
