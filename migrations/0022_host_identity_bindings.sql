-- Host enrollment slice A (#182): immutable Provider-scoped identity.
-- Internal persistence only. No enrollment API, authenticated attestation,
-- session issuance, GPU health, allocation or production grants are added.
--
-- Provision this dedicated NOLOGIN role independently before migration:
-- ccp_host_identity_guard_owner. The role is not a runtime identity and must
-- not be granted to enrollment clients. It receives only the Provider columns
-- needed by this fixed SECURITY DEFINER trigger: SELECT on provider identity,
-- cluster and lifecycle, plus UPDATE(provider_id) required by PostgreSQL 16
-- SELECT ... FOR SHARE. The function never updates Provider state.
DO $host_identity_guard_owner_preflight$
DECLARE
    guard_owner_oid pg_catalog.oid;
    provider_table_owner pg_catalog.oid;
    current_user_oid pg_catalog.oid;
BEGIN
    SELECT oid INTO guard_owner_oid FROM pg_catalog.pg_roles
    WHERE rolname='ccp_host_identity_guard_owner'
      AND NOT rolcanlogin AND NOT rolsuper AND NOT rolinherit
      AND NOT rolcreatedb AND NOT rolcreaterole
      AND NOT rolreplication AND NOT rolbypassrls;
    IF NOT FOUND THEN
        RAISE EXCEPTION
            'HostIdentity guard owner must be independently provisioned as a restricted NOLOGIN NOINHERIT role';
    END IF;

    SELECT relowner INTO provider_table_owner
    FROM pg_catalog.pg_class
    WHERE oid='public.compute_providers'::pg_catalog.regclass;
    SELECT oid INTO current_user_oid
    FROM pg_catalog.pg_roles WHERE rolname=current_user;

    IF EXISTS (SELECT 1 FROM pg_catalog.pg_auth_members WHERE member=guard_owner_oid)
       OR EXISTS (SELECT 1 FROM pg_catalog.pg_database WHERE datdba=guard_owner_oid)
       OR EXISTS (SELECT 1 FROM pg_catalog.pg_namespace WHERE nspowner=guard_owner_oid)
       OR EXISTS (SELECT 1 FROM pg_catalog.pg_class WHERE relowner=guard_owner_oid)
       OR EXISTS (SELECT 1 FROM pg_catalog.pg_proc WHERE proowner=guard_owner_oid)
       OR EXISTS (SELECT 1 FROM pg_catalog.pg_default_acl WHERE defaclrole=guard_owner_oid)
       OR pg_catalog.has_database_privilege(guard_owner_oid, pg_catalog.current_database(), 'CREATE')
       OR pg_catalog.has_schema_privilege(guard_owner_oid, 'public', 'CREATE') THEN
        RAISE EXCEPTION
            'HostIdentity guard owner has unrelated ownership, default privileges or CREATE authority';
    END IF;

    -- Only the trusted Provider table owner, current migrator and superusers
    -- may be able to SET/MEMBER this role. Runtime enrollment identities must
    -- never inherit or assume the guard owner.
    IF EXISTS (
        SELECT 1 FROM pg_catalog.pg_roles r
        WHERE r.oid NOT IN (guard_owner_oid, provider_table_owner, current_user_oid)
          AND NOT r.rolsuper
          AND pg_catalog.pg_has_role(r.oid, guard_owner_oid, 'MEMBER')
    ) THEN
        RAISE EXCEPTION 'HostIdentity guard owner is reachable by an untrusted role';
    END IF;

    -- The role must start with no grants. The migration below provisions the
    -- exact four column privileges and one schema USAGE grant.
    IF EXISTS (
        SELECT 1 FROM pg_catalog.pg_class c
        CROSS JOIN LATERAL pg_catalog.aclexplode(c.relacl) a
        WHERE a.grantee=guard_owner_oid
    ) OR EXISTS (
        SELECT 1 FROM pg_catalog.pg_attribute col
        CROSS JOIN LATERAL pg_catalog.aclexplode(col.attacl) a
        WHERE a.grantee=guard_owner_oid
    ) OR EXISTS (
        SELECT 1 FROM pg_catalog.pg_namespace n
        CROSS JOIN LATERAL pg_catalog.aclexplode(n.nspacl) a
        WHERE a.grantee=guard_owner_oid
    ) OR EXISTS (
        SELECT 1 FROM pg_catalog.pg_proc p
        CROSS JOIN LATERAL pg_catalog.aclexplode(p.proacl) a
        WHERE a.grantee=guard_owner_oid
    ) THEN
        RAISE EXCEPTION 'HostIdentity guard owner has unexpected direct grants';
    END IF;
END;
$host_identity_guard_owner_preflight$;

GRANT USAGE ON SCHEMA public TO ccp_host_identity_guard_owner;
GRANT SELECT (provider_id, cluster_scope, state),
      UPDATE (provider_id)
    ON public.compute_providers TO ccp_host_identity_guard_owner;

CREATE TABLE public.compute_host_identity_bindings (
    host_id TEXT PRIMARY KEY
        CHECK (pg_catalog.octet_length(host_id) BETWEEN 1 AND 256
               AND pg_catalog.btrim(host_id)=host_id AND host_id !~ '[[:cntrl:]]'),
    provider_id TEXT NOT NULL REFERENCES public.compute_providers(provider_id),
    cluster_scope TEXT NOT NULL
        CHECK (pg_catalog.octet_length(cluster_scope) BETWEEN 1 AND 1024
               AND pg_catalog.btrim(cluster_scope)=cluster_scope AND cluster_scope !~ '[[:cntrl:]]'),
    node_uid TEXT NOT NULL
        CHECK (pg_catalog.octet_length(node_uid) BETWEEN 1 AND 256
               AND pg_catalog.btrim(node_uid)=node_uid AND node_uid !~ '[[:cntrl:]]'),
    subject TEXT NOT NULL
        CHECK (pg_catalog.octet_length(subject) BETWEEN 1 AND 1024
               AND pg_catalog.btrim(subject)=subject AND subject !~ '[[:cntrl:]]'),
    registered_at TIMESTAMPTZ NOT NULL DEFAULT pg_catalog.clock_timestamp(),
    UNIQUE (provider_id, cluster_scope, node_uid)
);

-- A foreign key's key-share lock alone does not conflict with a non-key
-- Provider lifecycle update. The SECURITY DEFINER owner holds only the exact
-- column privileges required to take this SHARE row lock and read lifecycle.
-- It cannot change Provider state, and callers receive no Provider privilege.
CREATE FUNCTION public.compute_host_identity_enrollment_guard() RETURNS trigger
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog, pg_temp
AS $guard$
DECLARE
    current_scope TEXT;
    current_state TEXT;
BEGIN
    -- Reject a trigger attached to any other relation or at another timing.
    IF TG_RELID <> 'public.compute_host_identity_bindings'::pg_catalog.regclass
       OR TG_WHEN <> 'BEFORE' THEN
        RAISE EXCEPTION 'HostIdentity guard requires the authoritative binding table'
            USING ERRCODE='42501';
    END IF;

    IF TG_OP = 'TRUNCATE' THEN
        RAISE EXCEPTION 'HostIdentity binding is immutable'
            USING ERRCODE='23514';
    END IF;
    IF TG_LEVEL <> 'ROW' THEN
        RAISE EXCEPTION 'HostIdentity guard requires row-level execution'
            USING ERRCODE='42501';
    END IF;
    IF TG_OP <> 'INSERT' THEN
        RAISE EXCEPTION 'HostIdentity binding is immutable'
            USING ERRCODE='23514';
    END IF;

    SELECT p.cluster_scope, p.state
      INTO current_scope, current_state
      FROM public.compute_providers AS p
     WHERE p.provider_id=NEW.provider_id
     FOR SHARE;

    IF NOT FOUND OR current_scope IS DISTINCT FROM NEW.cluster_scope
        OR current_state IS DISTINCT FROM 'REGISTERED' THEN
        RAISE EXCEPTION 'HostIdentity enrollment requires exact registered Provider scope'
            USING ERRCODE='23514';
    END IF;

    -- Receipt time comes from the database after lock acquisition, never from
    -- a caller-supplied value. This is not host liveness or GPU attestation.
    NEW.registered_at := pg_catalog.clock_timestamp();
    RETURN NEW;
END;
$guard$;

-- Create triggers while the migration owner still owns the function. Then
-- revoke PUBLIC execution and transfer it to the independently provisioned
-- NOLOGIN role; trigger invocation does not grant callers direct execution.
CREATE TRIGGER compute_host_identity_enrollment_insert
    BEFORE INSERT ON public.compute_host_identity_bindings
    FOR EACH ROW EXECUTE FUNCTION public.compute_host_identity_enrollment_guard();
CREATE TRIGGER compute_host_identity_binding_no_change
    BEFORE UPDATE OR DELETE ON public.compute_host_identity_bindings
    FOR EACH ROW EXECUTE FUNCTION public.compute_host_identity_enrollment_guard();
CREATE TRIGGER compute_host_identity_binding_no_truncate
    BEFORE TRUNCATE ON public.compute_host_identity_bindings
    FOR EACH STATEMENT EXECUTE FUNCTION public.compute_host_identity_enrollment_guard();

REVOKE ALL ON FUNCTION public.compute_host_identity_enrollment_guard() FROM PUBLIC;
-- PostgreSQL requires schema CREATE on the new owner for ownership transfer.
-- Keep that capability inside this transaction, never on the runtime role.
GRANT CREATE ON SCHEMA public TO ccp_host_identity_guard_owner;
ALTER FUNCTION public.compute_host_identity_enrollment_guard() OWNER TO ccp_host_identity_guard_owner;
REVOKE CREATE ON SCHEMA public FROM ccp_host_identity_guard_owner;

ALTER TABLE public.compute_host_identity_bindings
    ENABLE ALWAYS TRIGGER compute_host_identity_enrollment_insert;
ALTER TABLE public.compute_host_identity_bindings
    ENABLE ALWAYS TRIGGER compute_host_identity_binding_no_change;
ALTER TABLE public.compute_host_identity_bindings
    ENABLE ALWAYS TRIGGER compute_host_identity_binding_no_truncate;

REVOKE ALL ON TABLE public.compute_host_identity_bindings FROM PUBLIC;

-- CREATE TABLE / CREATE FUNCTION inherit the migrator's default ACLs.
-- Do not let a deployment's default privileges silently expose this new
-- immutable binding table or SECURITY DEFINER trigger to an application role.
DO $host_identity_object_acl_attestation$
DECLARE
    binding_oid pg_catalog.oid := 'public.compute_host_identity_bindings'::pg_catalog.regclass;
    guard_function_oid pg_catalog.oid := 'public.compute_host_identity_enrollment_guard()'::pg_catalog.regprocedure;
BEGIN
    IF EXISTS (
        SELECT 1 FROM pg_catalog.pg_class c
        CROSS JOIN LATERAL pg_catalog.aclexplode(c.relacl) a
        WHERE c.oid=binding_oid AND a.grantee <> c.relowner
    ) OR EXISTS (
        SELECT 1 FROM pg_catalog.pg_attribute col
        CROSS JOIN LATERAL pg_catalog.aclexplode(col.attacl) a
        WHERE col.attrelid=binding_oid
          AND a.grantee <> (SELECT c.relowner FROM pg_catalog.pg_class c WHERE c.oid=binding_oid)
    ) THEN
        RAISE EXCEPTION 'HostIdentity binding table has unexpected default or direct grants';
    END IF;

    IF EXISTS (
        SELECT 1 FROM pg_catalog.pg_proc p
        CROSS JOIN LATERAL pg_catalog.aclexplode(p.proacl) a
        WHERE p.oid=guard_function_oid AND a.grantee <> p.proowner
    ) THEN
        RAISE EXCEPTION 'HostIdentity SECURITY DEFINER guard has unexpected EXECUTE grants';
    END IF;
END;
$host_identity_object_acl_attestation$;

COMMENT ON TABLE public.compute_host_identity_bindings IS
    'Immutable CCP host enrollment binding; not independently attested physical host identity or GPU availability.';
COMMENT ON FUNCTION public.compute_host_identity_enrollment_guard() IS
    'Fixed-search-path SECURITY DEFINER guard owned by ccp_host_identity_guard_owner; only reads/locks Provider columns and never mutates Provider state.';
