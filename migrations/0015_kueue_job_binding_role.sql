-- Least-privilege login for the Kueue job binding store.
-- Provision ccp_kueue_job_binding independently before this migration.
-- Roles are cluster-wide: this migration must not create or reuse one blindly.
-- The runtime login needs only the DML PostgresKueueJobBindingStore executes:
-- SELECT and INSERT on both binding tables, UPDATE of updated_at on the pin
-- table, and UPDATE of acknowledged and updated_at on the tombstone table.
-- PostgreSQL 16 requires UPDATE on one column for SELECT ... FOR UPDATE.
-- The pin guard rejects every UPDATE, so updated_at cannot be rewritten.
-- The login has no DDL, and no privileges on
-- Provider, Allocation, Workload, Operation, or Evidence authority tables.
-- Versions 0001..0014 are unchanged.
DO $kueue_binding_role_preflight$
DECLARE
    binding_oid pg_catalog.oid;
BEGIN
    SELECT oid INTO binding_oid FROM pg_catalog.pg_roles
    WHERE rolname = 'ccp_kueue_job_binding'
      AND rolcanlogin AND NOT rolsuper AND NOT rolinherit
      AND NOT rolcreatedb AND NOT rolcreaterole
      AND NOT rolreplication AND NOT rolbypassrls;
    IF NOT FOUND THEN
        RAISE EXCEPTION 'ccp_kueue_job_binding must be independently provisioned as a restricted LOGIN role';
    END IF;

    IF EXISTS (SELECT 1 FROM pg_catalog.pg_auth_members WHERE member = binding_oid)
       OR EXISTS (SELECT 1 FROM pg_catalog.pg_database WHERE datdba = binding_oid)
       OR EXISTS (SELECT 1 FROM pg_catalog.pg_namespace WHERE nspowner = binding_oid)
       OR EXISTS (SELECT 1 FROM pg_catalog.pg_class WHERE relowner = binding_oid)
       OR EXISTS (SELECT 1 FROM pg_catalog.pg_proc WHERE proowner = binding_oid)
       OR pg_catalog.has_database_privilege(binding_oid, pg_catalog.current_database(), 'CREATE')
       OR pg_catalog.has_schema_privilege(binding_oid, 'public', 'CREATE') THEN
        RAISE EXCEPTION 'ccp_kueue_job_binding has unrelated ownership, membership, or CREATE privileges';
    END IF;

    -- Refuse grants that were applied out of band. A direct re-run after this
    -- migration has committed will also stop here; the checksummed runner
    -- applies the file once.
    IF EXISTS (
        SELECT 1 FROM pg_catalog.pg_class c
        CROSS JOIN LATERAL pg_catalog.aclexplode(c.relacl) a
        WHERE a.grantee = binding_oid
    ) OR EXISTS (
        SELECT 1 FROM pg_catalog.pg_attribute col
        CROSS JOIN LATERAL pg_catalog.aclexplode(col.attacl) a
        WHERE a.grantee = binding_oid
    ) OR EXISTS (
        SELECT 1 FROM pg_catalog.pg_namespace n
        CROSS JOIN LATERAL pg_catalog.aclexplode(n.nspacl) a
        WHERE a.grantee = binding_oid
          AND (n.nspname <> 'public' OR a.privilege_type <> 'USAGE' OR a.is_grantable)
    ) OR EXISTS (
        SELECT 1 FROM pg_catalog.pg_proc p
        CROSS JOIN LATERAL pg_catalog.aclexplode(p.proacl) a
        WHERE a.grantee = binding_oid
    ) THEN
        RAISE EXCEPTION 'ccp_kueue_job_binding has unexpected grants; review them before migration';
    END IF;
END;
$kueue_binding_role_preflight$;

GRANT USAGE ON SCHEMA public TO ccp_kueue_job_binding;
GRANT SELECT, INSERT ON public.kueue_job_create_pins TO ccp_kueue_job_binding;
-- Lock column only. kueue_job_create_pin_guard rejects every UPDATE.
GRANT UPDATE (updated_at) ON public.kueue_job_create_pins TO ccp_kueue_job_binding;
GRANT SELECT, INSERT ON public.kueue_job_delete_tombstones TO ccp_kueue_job_binding;
GRANT UPDATE (acknowledged, updated_at) ON public.kueue_job_delete_tombstones TO ccp_kueue_job_binding;
-- Trigger execution checks EXECUTE of the invoking role. PUBLIC also has
-- EXECUTE today; this grant keeps the guards reachable if that is revoked.
GRANT EXECUTE ON FUNCTION public.kueue_job_create_pin_guard() TO ccp_kueue_job_binding;
GRANT EXECUTE ON FUNCTION public.kueue_job_delete_tombstone_guard() TO ccp_kueue_job_binding;
