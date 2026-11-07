-- Provider audit insertion must not require direct audit INSERT on a writer.
-- Provision ccp_provider_audit_owner independently before this migration.
-- Roles are cluster-wide: database migrations must not create/reuse them blindly.
-- All historical migrations 0001..0012 remain unchanged.
DO $audit_owner_preflight$
DECLARE
    audit_owner_oid pg_catalog.oid;
    audit_table_owner pg_catalog.oid;
BEGIN
    SELECT oid INTO audit_owner_oid FROM pg_catalog.pg_roles
    WHERE rolname='ccp_provider_audit_owner'
      AND NOT rolcanlogin AND NOT rolsuper AND NOT rolinherit
      AND NOT rolcreatedb AND NOT rolcreaterole
      AND NOT rolreplication AND NOT rolbypassrls;
    IF NOT FOUND THEN
        RAISE EXCEPTION 'Provider audit owner must be independently provisioned with restricted NOLOGIN attributes';
    END IF;
    SELECT relowner INTO audit_table_owner FROM pg_catalog.pg_class
    WHERE oid='public.compute_provider_lifecycle_audit'::pg_catalog.regclass;

    IF EXISTS (SELECT 1 FROM pg_catalog.pg_auth_members WHERE member=audit_owner_oid)
       OR EXISTS (SELECT 1 FROM pg_catalog.pg_database WHERE datdba=audit_owner_oid)
       OR EXISTS (SELECT 1 FROM pg_catalog.pg_namespace WHERE nspowner=audit_owner_oid)
       OR EXISTS (SELECT 1 FROM pg_catalog.pg_class WHERE relowner=audit_owner_oid)
       OR EXISTS (SELECT 1 FROM pg_catalog.pg_proc
                  WHERE proowner=audit_owner_oid
                    AND oid<>'public.compute_provider_lifecycle_audit_append()'::pg_catalog.regprocedure)
       OR pg_catalog.has_database_privilege(audit_owner_oid, pg_catalog.current_database(), 'CREATE')
       OR pg_catalog.has_schema_privilege(audit_owner_oid, 'public', 'CREATE') THEN
        RAISE EXCEPTION 'Provider audit owner has unrelated ownership, membership or CREATE privileges';
    END IF;

    -- Only the trusted migrator/table owner and superusers may reach this role.
    IF EXISTS (
        SELECT 1 FROM pg_catalog.pg_roles r
        WHERE r.oid NOT IN (audit_owner_oid, audit_table_owner,
                           (SELECT oid FROM pg_catalog.pg_roles WHERE rolname=current_user))
          AND NOT r.rolsuper
          -- MEMBER includes an ADMIN-only grant (SET=FALSE, INHERIT=FALSE).
          -- Such a role could delegate SET privileges and bypass the audit owner boundary.
          AND pg_catalog.pg_has_role(r.oid, audit_owner_oid, 'MEMBER')
    ) THEN
        RAISE EXCEPTION 'Provider audit owner is reachable by an untrusted role';
    END IF;

    IF EXISTS (
        SELECT 1 FROM pg_catalog.pg_class c
        CROSS JOIN LATERAL pg_catalog.aclexplode(c.relacl) a
        WHERE a.grantee=audit_owner_oid
          AND (c.oid<>'public.compute_provider_lifecycle_audit'::pg_catalog.regclass
               OR a.privilege_type<>'INSERT' OR a.is_grantable)
    ) OR EXISTS (
        SELECT 1 FROM pg_catalog.pg_attribute col
        CROSS JOIN LATERAL pg_catalog.aclexplode(col.attacl) a
        WHERE a.grantee=audit_owner_oid
    ) OR EXISTS (
        SELECT 1 FROM pg_catalog.pg_namespace n
        CROSS JOIN LATERAL pg_catalog.aclexplode(n.nspacl) a
        WHERE a.grantee=audit_owner_oid
          AND (n.nspname<>'public' OR a.privilege_type<>'USAGE' OR a.is_grantable)
    ) THEN
        RAISE EXCEPTION 'Provider audit owner has unrelated relation or schema grants';
    END IF;

    -- Refuse the old direct-INSERT workaround rather than silently changing
    -- independently administered named-role grants. Column grants count too.
    IF EXISTS (
        SELECT 1 FROM pg_catalog.pg_class c
        CROSS JOIN LATERAL pg_catalog.aclexplode(c.relacl) a
        WHERE c.oid='public.compute_provider_lifecycle_audit'::pg_catalog.regclass
          AND a.privilege_type='INSERT'
          AND a.grantee NOT IN (audit_table_owner, audit_owner_oid)
    ) OR EXISTS (
        SELECT 1 FROM pg_catalog.pg_attribute col
        CROSS JOIN LATERAL pg_catalog.aclexplode(col.attacl) a
        WHERE col.attrelid='public.compute_provider_lifecycle_audit'::pg_catalog.regclass
          AND a.privilege_type='INSERT'
          AND a.grantee NOT IN (audit_table_owner, audit_owner_oid)
    ) THEN
        RAISE EXCEPTION 'Provider audit has unexpected direct INSERT grants; review and revoke them before migration';
    END IF;
END;
$audit_owner_preflight$;

GRANT USAGE ON SCHEMA public TO ccp_provider_audit_owner;
GRANT INSERT ON public.compute_provider_lifecycle_audit TO ccp_provider_audit_owner;

CREATE OR REPLACE FUNCTION public.compute_provider_lifecycle_audit_append()
RETURNS trigger
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog, pg_temp
AS $provider_audit$
BEGIN
    -- A trigger attached before EXECUTE was revoked also needs this check.
    IF TG_RELID <> 'public.compute_providers'::pg_catalog.regclass
       OR TG_WHEN <> 'AFTER' OR TG_LEVEL <> 'ROW'
       OR TG_OP NOT IN ('INSERT', 'UPDATE') THEN
        RAISE EXCEPTION 'Provider audit trigger requires the authoritative Provider registry'
            USING ERRCODE = '42501';
    END IF;
    INSERT INTO public.compute_provider_lifecycle_audit
        (provider_id, version, previous_state, state, reason_code, actor_db_role)
    VALUES (
        NEW.provider_id, NEW.version,
        CASE WHEN TG_OP='INSERT' THEN NULL ELSE OLD.state END,
        NEW.state, NEW.last_reason_code, session_user
    );
    RETURN NEW;
END;
$provider_audit$;

REVOKE ALL ON FUNCTION public.compute_provider_lifecycle_audit_append() FROM PUBLIC;
-- PostgreSQL requires schema CREATE on the new owner for ownership transfer.
-- Keep that capability inside this transaction, never on the runtime role.
GRANT CREATE ON SCHEMA public TO ccp_provider_audit_owner;
ALTER FUNCTION public.compute_provider_lifecycle_audit_append() OWNER TO ccp_provider_audit_owner;
REVOKE CREATE ON SCHEMA public FROM ccp_provider_audit_owner;

ALTER TABLE public.compute_providers ENABLE ALWAYS TRIGGER compute_provider_lifecycle_guard_row;
ALTER TABLE public.compute_providers ENABLE ALWAYS TRIGGER compute_provider_lifecycle_guard_truncate;
ALTER TABLE public.compute_providers ENABLE ALWAYS TRIGGER compute_provider_lifecycle_audit_append;
ALTER TABLE public.compute_provider_lifecycle_audit ENABLE ALWAYS TRIGGER compute_provider_audit_no_mutation;
ALTER TABLE public.compute_provider_lifecycle_audit ENABLE ALWAYS TRIGGER compute_provider_audit_no_truncate;
