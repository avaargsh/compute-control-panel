-- Authenticated HostSession ingress boundary (#182).
-- A verified SPIFFE workload principal must match one DBA-provisioned,
-- immutable subject -> HostIdentity binding grant. Revocation is serialized
-- with session start/heartbeat by a transaction-scoped advisory lock.
-- The host-session writer receives EXECUTE only on the authenticated wrappers;
-- the tuple-based functions from 0023 become owner-only.
--
-- This migration does not mount an HTTP listener, validate hardware
-- attestation, ingest Kubernetes Lease data, or assert physical GPU health.

DO $host_session_subject_grant_role_preflight$
DECLARE
    guard_owner_oid pg_catalog.oid;
    writer_oid pg_catalog.oid;
BEGIN
    SELECT oid INTO guard_owner_oid
      FROM pg_catalog.pg_roles
     WHERE rolname='ccp_host_session_guard_owner'
       AND NOT rolcanlogin AND NOT rolsuper AND NOT rolinherit
       AND NOT rolcreatedb AND NOT rolcreaterole
       AND NOT rolreplication AND NOT rolbypassrls;
    IF NOT FOUND THEN
        RAISE EXCEPTION 'ccp_host_session_guard_owner must remain restricted NOLOGIN NOINHERIT';
    END IF;

    SELECT oid INTO writer_oid
      FROM pg_catalog.pg_roles
     WHERE rolname='ccp_host_session_writer'
       AND rolcanlogin AND NOT rolsuper AND NOT rolinherit
       AND NOT rolcreatedb AND NOT rolcreaterole
       AND NOT rolreplication AND NOT rolbypassrls;
    IF NOT FOUND THEN
        RAISE EXCEPTION 'ccp_host_session_writer must remain restricted LOGIN NOINHERIT';
    END IF;

    IF EXISTS (SELECT 1 FROM pg_catalog.pg_auth_members WHERE member=guard_owner_oid)
       OR EXISTS (SELECT 1 FROM pg_catalog.pg_auth_members WHERE member=writer_oid)
       OR EXISTS (SELECT 1 FROM pg_catalog.pg_auth_members WHERE roleid=writer_oid) THEN
        RAISE EXCEPTION 'HostSession owner/writer roles have unexpected memberships';
    END IF;
END;
$host_session_subject_grant_role_preflight$;

-- The subject grant must reference the exact immutable HostIdentity tuple,
-- not merely a HostID or a mutable Node name.
CREATE UNIQUE INDEX compute_host_identity_binding_subject_tuple
    ON public.compute_host_identity_bindings
       (host_id, provider_id, cluster_scope, node_uid, subject);

CREATE TABLE public.compute_host_identity_subject_grants (
    subject TEXT PRIMARY KEY
        CHECK (pg_catalog.octet_length(subject) BETWEEN 1 AND 1024
               AND pg_catalog.btrim(subject)=subject
               AND subject !~ '[[:cntrl:]]'
               AND subject ~ '^spiffe://[^/]+/host/[^/]+$'),
    tenant_id TEXT NOT NULL
        CHECK (pg_catalog.octet_length(tenant_id) BETWEEN 1 AND 256
               AND pg_catalog.btrim(tenant_id)=tenant_id
               AND tenant_id !~ '[[:cntrl:]]'),
    service_name TEXT NOT NULL
        CHECK (service_name='compute-host-agent'),
    host_id TEXT NOT NULL UNIQUE,
    provider_id TEXT NOT NULL,
    cluster_scope TEXT NOT NULL,
    node_uid TEXT NOT NULL,
    not_before TIMESTAMPTZ NOT NULL DEFAULT pg_catalog.clock_timestamp(),
    expires_at TIMESTAMPTZ,
    revoked_at TIMESTAMPTZ,
    revocation_reason TEXT
        CHECK (revocation_reason IS NULL OR revocation_reason ~ '^[A-Z][A-Z0-9_]{0,127}$'),
    registered_at TIMESTAMPTZ NOT NULL DEFAULT pg_catalog.clock_timestamp(),
    FOREIGN KEY (host_id, provider_id, cluster_scope, node_uid, subject)
        REFERENCES public.compute_host_identity_bindings
            (host_id, provider_id, cluster_scope, node_uid, subject),
    CHECK (expires_at IS NULL OR not_before < expires_at),
    CHECK ((revoked_at IS NULL) = (revocation_reason IS NULL))
);

CREATE TABLE public.compute_host_identity_subject_grant_audit (
    event_id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    subject TEXT NOT NULL,
    tenant_id TEXT NOT NULL,
    service_name TEXT NOT NULL,
    host_id TEXT NOT NULL,
    provider_id TEXT NOT NULL,
    cluster_scope TEXT NOT NULL,
    node_uid TEXT NOT NULL,
    action TEXT NOT NULL CHECK (action IN ('GRANTED','REVOKED')),
    reason_code TEXT,
    changed_by TEXT NOT NULL,
    recorded_at TIMESTAMPTZ NOT NULL DEFAULT pg_catalog.clock_timestamp(),
    CHECK ((action='GRANTED' AND reason_code IS NULL)
        OR (action='REVOKED' AND reason_code IS NOT NULL))
);
CREATE INDEX compute_host_identity_subject_grant_audit_subject
    ON public.compute_host_identity_subject_grant_audit(subject,event_id DESC);

CREATE FUNCTION public.compute_host_identity_subject_grant_guard() RETURNS trigger
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog, pg_temp
AS $grant_guard$
BEGIN
    IF TG_RELID <> 'public.compute_host_identity_subject_grants'::pg_catalog.regclass
       OR TG_WHEN <> 'BEFORE' THEN
        RAISE EXCEPTION 'HostIdentity subject grant guard requires the authoritative grant table'
            USING ERRCODE='42501';
    END IF;

    IF TG_OP='TRUNCATE' THEN
        RAISE EXCEPTION 'HostIdentity subject grants are immutable; revoke instead'
            USING ERRCODE='23514';
    END IF;
    IF TG_LEVEL <> 'ROW' THEN
        RAISE EXCEPTION 'HostIdentity subject grant guard requires row-level execution'
            USING ERRCODE='42501';
    END IF;
    IF TG_OP='DELETE' THEN
        RAISE EXCEPTION 'HostIdentity subject grants cannot be deleted; revoke instead'
            USING ERRCODE='23514';
    END IF;

    IF TG_OP='INSERT' THEN
        IF NEW.revoked_at IS NOT NULL OR NEW.revocation_reason IS NOT NULL
           OR NEW.service_name IS DISTINCT FROM 'compute-host-agent' THEN
            RAISE EXCEPTION 'new HostIdentity subject grant has invalid state'
                USING ERRCODE='23514';
        END IF;
        NEW.registered_at := pg_catalog.clock_timestamp();
        RETURN NEW;
    END IF;

    IF TG_OP='UPDATE' THEN
        IF OLD.revoked_at IS NOT NULL
           OR NEW.revoked_at IS NULL
           OR NEW.revocation_reason IS NULL
           OR NEW.subject IS DISTINCT FROM OLD.subject
           OR NEW.tenant_id IS DISTINCT FROM OLD.tenant_id
           OR NEW.service_name IS DISTINCT FROM OLD.service_name
           OR NEW.host_id IS DISTINCT FROM OLD.host_id
           OR NEW.provider_id IS DISTINCT FROM OLD.provider_id
           OR NEW.cluster_scope IS DISTINCT FROM OLD.cluster_scope
           OR NEW.node_uid IS DISTINCT FROM OLD.node_uid
           OR NEW.not_before IS DISTINCT FROM OLD.not_before
           OR NEW.expires_at IS DISTINCT FROM OLD.expires_at
           OR NEW.registered_at IS DISTINCT FROM OLD.registered_at THEN
            RAISE EXCEPTION 'HostIdentity subject grant permits only one-way revocation'
                USING ERRCODE='23514';
        END IF;

        -- A live request holds the matching shared transaction lock until its
        -- authenticated DB operation commits. Revocation waits for that
        -- operation; requests arriving behind the revoker observe REVOKED.
        PERFORM pg_catalog.pg_advisory_xact_lock(
            pg_catalog.hashtextextended(OLD.subject,0)
        );
        NEW.revoked_at := pg_catalog.clock_timestamp();
        RETURN NEW;
    END IF;

    RAISE EXCEPTION 'unsupported HostIdentity subject grant operation'
        USING ERRCODE='42501';
END;
$grant_guard$;

CREATE FUNCTION public.compute_host_identity_subject_grant_audit_append() RETURNS trigger
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog, pg_temp
AS $grant_audit$
BEGIN
    INSERT INTO public.compute_host_identity_subject_grant_audit (
        subject, tenant_id, service_name, host_id, provider_id, cluster_scope,
        node_uid, action, reason_code, changed_by, recorded_at
    ) VALUES (
        NEW.subject, NEW.tenant_id, NEW.service_name, NEW.host_id, NEW.provider_id,
        NEW.cluster_scope, NEW.node_uid,
        CASE WHEN TG_OP='INSERT' THEN 'GRANTED' ELSE 'REVOKED' END,
        NEW.revocation_reason, SESSION_USER, pg_catalog.clock_timestamp()
    );
    RETURN NEW;
END;
$grant_audit$;

CREATE FUNCTION public.compute_host_identity_subject_grant_audit_immutable() RETURNS trigger
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog, pg_temp
AS $audit_guard$
BEGIN
    RAISE EXCEPTION 'HostIdentity subject grant audit is append-only'
        USING ERRCODE='23514';
END;
$audit_guard$;

CREATE TRIGGER compute_host_identity_subject_grant_guard_row
    BEFORE INSERT OR UPDATE OR DELETE ON public.compute_host_identity_subject_grants
    FOR EACH ROW EXECUTE FUNCTION public.compute_host_identity_subject_grant_guard();
CREATE TRIGGER compute_host_identity_subject_grant_guard_truncate
    BEFORE TRUNCATE ON public.compute_host_identity_subject_grants
    FOR EACH STATEMENT EXECUTE FUNCTION public.compute_host_identity_subject_grant_guard();
CREATE TRIGGER compute_host_identity_subject_grant_audit_append
    AFTER INSERT OR UPDATE ON public.compute_host_identity_subject_grants
    FOR EACH ROW EXECUTE FUNCTION public.compute_host_identity_subject_grant_audit_append();
CREATE TRIGGER compute_host_identity_subject_grant_audit_no_mutation
    BEFORE UPDATE OR DELETE ON public.compute_host_identity_subject_grant_audit
    FOR EACH ROW EXECUTE FUNCTION public.compute_host_identity_subject_grant_audit_immutable();
CREATE TRIGGER compute_host_identity_subject_grant_audit_no_truncate
    BEFORE TRUNCATE ON public.compute_host_identity_subject_grant_audit
    FOR EACH STATEMENT EXECUTE FUNCTION public.compute_host_identity_subject_grant_audit_immutable();

CREATE FUNCTION public.compute_host_session_start_authenticated(
    p_tenant_id TEXT,
    p_service_name TEXT,
    p_subject TEXT,
    p_expected_epoch BIGINT,
    p_start_request_id TEXT,
    p_ttl_seconds INTEGER
) RETURNS TABLE (
    host_id TEXT,
    epoch BIGINT,
    session_id TEXT,
    provider_id TEXT,
    cluster_scope TEXT,
    node_uid TEXT,
    subject TEXT,
    issued_at TIMESTAMPTZ,
    expires_at TIMESTAMPTZ
)
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog, pg_temp
AS $authenticated_start$
DECLARE
    v_host_id TEXT;
    v_provider_id TEXT;
    v_cluster_scope TEXT;
    v_node_uid TEXT;
    v_not_before TIMESTAMPTZ;
    v_expires_at TIMESTAMPTZ;
    v_revoked_at TIMESTAMPTZ;
    v_now TIMESTAMPTZ;
    v_result RECORD;
BEGIN
    IF p_tenant_id IS NULL OR pg_catalog.octet_length(p_tenant_id) NOT BETWEEN 1 AND 256
       OR pg_catalog.btrim(p_tenant_id) <> p_tenant_id OR p_tenant_id ~ '[[:cntrl:]]'
       OR p_service_name IS DISTINCT FROM 'compute-host-agent'
       OR p_subject IS NULL OR pg_catalog.octet_length(p_subject) NOT BETWEEN 1 AND 1024
       OR pg_catalog.btrim(p_subject) <> p_subject OR p_subject !~ '^spiffe://[^/]+/.+$' THEN
        RAISE EXCEPTION 'invalid authenticated HostSession principal'
            USING ERRCODE='28000';
    END IF;

    PERFORM pg_catalog.pg_advisory_xact_lock_shared(
        pg_catalog.hashtextextended(p_subject,0)
    );
    SELECT g.host_id, g.provider_id, g.cluster_scope, g.node_uid,
           g.not_before, g.expires_at, g.revoked_at
      INTO v_host_id, v_provider_id, v_cluster_scope, v_node_uid,
           v_not_before, v_expires_at, v_revoked_at
      FROM public.compute_host_identity_subject_grants AS g
     WHERE g.subject=p_subject AND g.tenant_id=p_tenant_id
       AND g.service_name=p_service_name;

    v_now := pg_catalog.clock_timestamp();
    IF NOT FOUND OR v_revoked_at IS NOT NULL
       OR v_not_before > v_now
       OR (v_expires_at IS NOT NULL AND v_expires_at <= v_now) THEN
        RAISE EXCEPTION 'HostIdentity subject grant is absent, expired or revoked'
            USING ERRCODE='28000';
    END IF;

    SELECT * INTO STRICT v_result FROM public.compute_host_session_start(
        v_host_id, v_provider_id, v_cluster_scope, v_node_uid, p_subject,
        p_expected_epoch, p_start_request_id, p_ttl_seconds
    );
    -- The raw CAS may wait on Provider/binding/request locks. Recheck
    -- after it returns; an exception rolls back any tentative insertion.
    v_now := pg_catalog.clock_timestamp();
    IF v_now < v_not_before
       OR (v_expires_at IS NOT NULL AND
           (v_now >= v_expires_at OR v_result.expires_at > v_expires_at)) THEN
        RAISE EXCEPTION 'HostIdentity authorization window cannot cover session'
            USING ERRCODE='28000';
    END IF;
    RETURN QUERY SELECT v_result.host_id, v_result.epoch, v_result.session_id,
        v_result.provider_id, v_result.cluster_scope, v_result.node_uid,
        v_result.subject, v_result.issued_at, v_result.expires_at;
END;
$authenticated_start$;

CREATE FUNCTION public.compute_host_session_heartbeat_authenticated(
    p_tenant_id TEXT,
    p_service_name TEXT,
    p_subject TEXT,
    p_epoch BIGINT,
    p_session_id TEXT,
    p_sequence BIGINT,
    p_digest TEXT
) RETURNS TABLE (
    disposition TEXT,
    recorded_at TIMESTAMPTZ
)
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog, pg_temp
AS $authenticated_heartbeat$
DECLARE
    v_host_id TEXT;
    v_provider_id TEXT;
    v_cluster_scope TEXT;
    v_node_uid TEXT;
    v_not_before TIMESTAMPTZ;
    v_expires_at TIMESTAMPTZ;
    v_revoked_at TIMESTAMPTZ;
    v_now TIMESTAMPTZ;
BEGIN
    IF p_tenant_id IS NULL OR pg_catalog.octet_length(p_tenant_id) NOT BETWEEN 1 AND 256
       OR pg_catalog.btrim(p_tenant_id) <> p_tenant_id OR p_tenant_id ~ '[[:cntrl:]]'
       OR p_service_name IS DISTINCT FROM 'compute-host-agent'
       OR p_subject IS NULL OR pg_catalog.octet_length(p_subject) NOT BETWEEN 1 AND 1024
       OR pg_catalog.btrim(p_subject) <> p_subject OR p_subject !~ '^spiffe://[^/]+/.+$' THEN
        RAISE EXCEPTION 'invalid authenticated HostSession principal'
            USING ERRCODE='28000';
    END IF;

    PERFORM pg_catalog.pg_advisory_xact_lock_shared(
        pg_catalog.hashtextextended(p_subject,0)
    );
    SELECT g.host_id, g.provider_id, g.cluster_scope, g.node_uid,
           g.not_before, g.expires_at, g.revoked_at
      INTO v_host_id, v_provider_id, v_cluster_scope, v_node_uid,
           v_not_before, v_expires_at, v_revoked_at
      FROM public.compute_host_identity_subject_grants AS g
     WHERE g.subject=p_subject AND g.tenant_id=p_tenant_id
       AND g.service_name=p_service_name;

    v_now := pg_catalog.clock_timestamp();
    IF NOT FOUND OR v_revoked_at IS NOT NULL
       OR v_not_before > v_now
       OR (v_expires_at IS NOT NULL AND v_expires_at <= v_now) THEN
        RAISE EXCEPTION 'HostIdentity subject grant is absent, expired or revoked'
            USING ERRCODE='28000';
    END IF;

    RETURN QUERY
    SELECT * FROM public.compute_host_session_heartbeat(
        v_host_id, v_provider_id, v_cluster_scope, v_node_uid, p_subject,
        p_epoch, p_session_id, p_sequence, p_digest
    );
    -- RETURN QUERY buffers rows; raising here rolls back the receipt too.
    v_now := pg_catalog.clock_timestamp();
    IF v_now < v_not_before
       OR (v_expires_at IS NOT NULL AND v_now >= v_expires_at) THEN
        RAISE EXCEPTION 'HostIdentity authorization expired during heartbeat'
            USING ERRCODE='28000';
    END IF;
END;
$authenticated_heartbeat$;

REVOKE ALL ON TABLE public.compute_host_identity_subject_grants FROM PUBLIC;
REVOKE ALL ON TABLE public.compute_host_identity_subject_grant_audit FROM PUBLIC;
REVOKE ALL ON SEQUENCE public.compute_host_identity_subject_grant_audit_event_id_seq FROM PUBLIC;
REVOKE ALL ON FUNCTION public.compute_host_identity_subject_grant_guard() FROM PUBLIC;
REVOKE ALL ON FUNCTION public.compute_host_identity_subject_grant_audit_append() FROM PUBLIC;
REVOKE ALL ON FUNCTION public.compute_host_identity_subject_grant_audit_immutable() FROM PUBLIC;
REVOKE ALL ON FUNCTION public.compute_host_session_start_authenticated(TEXT,TEXT,TEXT,BIGINT,TEXT,INTEGER) FROM PUBLIC;
REVOKE ALL ON FUNCTION public.compute_host_session_heartbeat_authenticated(TEXT,TEXT,TEXT,BIGINT,TEXT,BIGINT,TEXT) FROM PUBLIC;

GRANT USAGE ON SCHEMA public TO ccp_host_session_guard_owner;
GRANT SELECT (
    subject, tenant_id, service_name, host_id, provider_id, cluster_scope,
    node_uid, not_before, expires_at, revoked_at
) ON public.compute_host_identity_subject_grants TO ccp_host_session_guard_owner;
GRANT INSERT ON public.compute_host_identity_subject_grant_audit TO ccp_host_session_guard_owner;
GRANT USAGE ON SEQUENCE public.compute_host_identity_subject_grant_audit_event_id_seq
    TO ccp_host_session_guard_owner;

GRANT CREATE ON SCHEMA public TO ccp_host_session_guard_owner;
ALTER FUNCTION public.compute_host_identity_subject_grant_guard()
    OWNER TO ccp_host_session_guard_owner;
ALTER FUNCTION public.compute_host_identity_subject_grant_audit_append()
    OWNER TO ccp_host_session_guard_owner;
ALTER FUNCTION public.compute_host_identity_subject_grant_audit_immutable()
    OWNER TO ccp_host_session_guard_owner;
ALTER FUNCTION public.compute_host_session_start_authenticated(TEXT,TEXT,TEXT,BIGINT,TEXT,INTEGER)
    OWNER TO ccp_host_session_guard_owner;
ALTER FUNCTION public.compute_host_session_heartbeat_authenticated(TEXT,TEXT,TEXT,BIGINT,TEXT,BIGINT,TEXT)
    OWNER TO ccp_host_session_guard_owner;

SET ROLE ccp_host_session_guard_owner;
REVOKE EXECUTE ON FUNCTION public.compute_host_session_start(TEXT,TEXT,TEXT,TEXT,TEXT,BIGINT,TEXT,INTEGER)
    FROM ccp_host_session_writer;
REVOKE EXECUTE ON FUNCTION public.compute_host_session_heartbeat(TEXT,TEXT,TEXT,TEXT,TEXT,BIGINT,TEXT,BIGINT,TEXT)
    FROM ccp_host_session_writer;
GRANT EXECUTE ON FUNCTION public.compute_host_session_start_authenticated(TEXT,TEXT,TEXT,BIGINT,TEXT,INTEGER)
    TO ccp_host_session_writer;
GRANT EXECUTE ON FUNCTION public.compute_host_session_heartbeat_authenticated(TEXT,TEXT,TEXT,BIGINT,TEXT,BIGINT,TEXT)
    TO ccp_host_session_writer;
RESET ROLE;

REVOKE CREATE ON SCHEMA public FROM ccp_host_session_guard_owner;

ALTER TABLE public.compute_host_identity_subject_grants
    ENABLE ALWAYS TRIGGER compute_host_identity_subject_grant_guard_row;
ALTER TABLE public.compute_host_identity_subject_grants
    ENABLE ALWAYS TRIGGER compute_host_identity_subject_grant_guard_truncate;
ALTER TABLE public.compute_host_identity_subject_grants
    ENABLE ALWAYS TRIGGER compute_host_identity_subject_grant_audit_append;
ALTER TABLE public.compute_host_identity_subject_grant_audit
    ENABLE ALWAYS TRIGGER compute_host_identity_subject_grant_audit_no_mutation;
ALTER TABLE public.compute_host_identity_subject_grant_audit
    ENABLE ALWAYS TRIGGER compute_host_identity_subject_grant_audit_no_truncate;

COMMENT ON TABLE public.compute_host_identity_subject_grants IS
    'DBA-provisioned exact SPIFFE subject -> immutable HostIdentity binding grant; revocation is one-way and serialized with authenticated HostSession operations.';
COMMENT ON TABLE public.compute_host_identity_subject_grant_audit IS
    'Append-only audit of HostIdentity subject grant creation and revocation; changed_by is the database session_user, not an asserted human identity.';
COMMENT ON FUNCTION public.compute_host_session_start_authenticated(TEXT,TEXT,TEXT,BIGINT,TEXT,INTEGER) IS
    'Authenticated HostSession start wrapper. Resolves active tenant/service/subject grant and serializes grant revocation before calling the internal epoch CAS.';
COMMENT ON FUNCTION public.compute_host_session_heartbeat_authenticated(TEXT,TEXT,TEXT,BIGINT,TEXT,BIGINT,TEXT) IS
    'Authenticated HostSession heartbeat wrapper. Resolves active tenant/service/subject grant and serializes grant revocation before recording the heartbeat.';

DO $host_session_subject_grant_acl_attestation$
DECLARE
    guard_owner_oid pg_catalog.oid;
    writer_oid pg_catalog.oid;
    grants_oid pg_catalog.oid := 'public.compute_host_identity_subject_grants'::pg_catalog.regclass;
    audit_oid pg_catalog.oid := 'public.compute_host_identity_subject_grant_audit'::pg_catalog.regclass;
BEGIN
    SELECT oid INTO guard_owner_oid FROM pg_catalog.pg_roles WHERE rolname='ccp_host_session_guard_owner';
    SELECT oid INTO writer_oid FROM pg_catalog.pg_roles WHERE rolname='ccp_host_session_writer';

    IF EXISTS (
        SELECT 1 FROM pg_catalog.pg_class c
        CROSS JOIN LATERAL pg_catalog.aclexplode(c.relacl) a
        WHERE c.oid IN (grants_oid,audit_oid)
          AND a.grantee NOT IN (c.relowner,guard_owner_oid)
    ) OR EXISTS (
        SELECT 1 FROM pg_catalog.pg_attribute col
        CROSS JOIN LATERAL pg_catalog.aclexplode(col.attacl) a
        WHERE col.attrelid IN (grants_oid,audit_oid)
          AND a.grantee NOT IN (
              (SELECT c.relowner FROM pg_catalog.pg_class c WHERE c.oid=col.attrelid),
              guard_owner_oid
          )
    ) THEN
        RAISE EXCEPTION 'HostIdentity subject grant tables have unexpected direct/default grants';
    END IF;

    IF pg_catalog.has_table_privilege(writer_oid,grants_oid,'SELECT')
       OR pg_catalog.has_table_privilege(writer_oid,grants_oid,'INSERT')
       OR pg_catalog.has_table_privilege(writer_oid,grants_oid,'UPDATE')
       OR pg_catalog.has_table_privilege(writer_oid,grants_oid,'DELETE')
       OR pg_catalog.has_table_privilege(writer_oid,grants_oid,'TRUNCATE')
       OR pg_catalog.has_table_privilege(writer_oid,audit_oid,'SELECT')
       OR pg_catalog.has_table_privilege(writer_oid,audit_oid,'INSERT')
       OR pg_catalog.has_table_privilege(writer_oid,audit_oid,'UPDATE')
       OR pg_catalog.has_table_privilege(writer_oid,audit_oid,'DELETE')
       OR pg_catalog.has_table_privilege(writer_oid,audit_oid,'TRUNCATE') THEN
        RAISE EXCEPTION 'HostSession writer has direct HostIdentity subject grant table privileges';
    END IF;

    IF pg_catalog.has_function_privilege(writer_oid,
          'public.compute_host_session_start(text,text,text,text,text,bigint,text,integer)'::pg_catalog.regprocedure,'EXECUTE')
       OR pg_catalog.has_function_privilege(writer_oid,
          'public.compute_host_session_heartbeat(text,text,text,text,text,bigint,text,bigint,text)'::pg_catalog.regprocedure,'EXECUTE')
       OR NOT pg_catalog.has_function_privilege(writer_oid,
          'public.compute_host_session_start_authenticated(text,bigint,text,integer)'::pg_catalog.regprocedure,'EXECUTE')
       OR NOT pg_catalog.has_function_privilege(writer_oid,
          'public.compute_host_session_heartbeat_authenticated(text,bigint,text,bigint,text)'::pg_catalog.regprocedure,'EXECUTE') THEN
        RAISE EXCEPTION 'HostSession writer must execute authenticated wrappers only';
    END IF;

    IF pg_catalog.has_table_privilege(guard_owner_oid,grants_oid,'UPDATE')
       OR pg_catalog.has_table_privilege(guard_owner_oid,grants_oid,'DELETE')
       OR pg_catalog.has_table_privilege(guard_owner_oid,grants_oid,'TRUNCATE') THEN
        RAISE EXCEPTION 'HostSession guard owner received grant mutation authority';
    END IF;
END;
$host_session_subject_grant_acl_attestation$;
