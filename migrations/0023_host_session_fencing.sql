-- Durable HostSession epoch and heartbeat replay boundary (#182).
-- This migration adds an unmounted persistence API. It does not authenticate
-- a machine, provision certificates, mount an HTTP listener, or prove GPU health.
--
-- Provision these roles independently before applying migrations:
--   ccp_host_session_guard_owner  NOLOGIN NOINHERIT, owns only session tables/functions
--   ccp_host_session_writer       LOGIN NOINHERIT, receives EXECUTE only
-- The writer's caller must authenticate the SPIFFE/machine principal and check
-- revocation before invoking either function. A shared DB login is not host auth.
DO $host_session_role_preflight$
DECLARE
    guard_owner_oid pg_catalog.oid;
    writer_oid pg_catalog.oid;
    current_user_oid pg_catalog.oid;
BEGIN
    SELECT oid INTO guard_owner_oid
    FROM pg_catalog.pg_roles
    WHERE rolname='ccp_host_session_guard_owner'
      AND NOT rolcanlogin AND NOT rolsuper AND NOT rolinherit
      AND NOT rolcreatedb AND NOT rolcreaterole
      AND NOT rolreplication AND NOT rolbypassrls;
    IF NOT FOUND THEN
        RAISE EXCEPTION 'ccp_host_session_guard_owner must be independently provisioned as restricted NOLOGIN NOINHERIT';
    END IF;

    SELECT oid INTO writer_oid
    FROM pg_catalog.pg_roles
    WHERE rolname='ccp_host_session_writer'
      AND rolcanlogin AND NOT rolsuper AND NOT rolinherit
      AND NOT rolcreatedb AND NOT rolcreaterole
      AND NOT rolreplication AND NOT rolbypassrls;
    IF NOT FOUND THEN
        RAISE EXCEPTION 'ccp_host_session_writer must be independently provisioned as restricted LOGIN NOINHERIT';
    END IF;

    SELECT oid INTO current_user_oid FROM pg_catalog.pg_roles WHERE rolname=current_user;

    -- A non-superuser migration role may temporarily be a member of the
    -- dedicated NOLOGIN owner to perform ALTER OWNER. No other role may
    -- assume or inherit either restricted role; the temporary membership is
    -- revoked before commit.
    IF EXISTS (
        SELECT 1 FROM pg_catalog.pg_auth_members
        WHERE (roleid=guard_owner_oid AND member <> current_user_oid)
           OR member=guard_owner_oid
           OR roleid=writer_oid
           OR member=writer_oid
    ) OR EXISTS (
        SELECT 1 FROM pg_catalog.pg_auth_members
        WHERE roleid=guard_owner_oid AND member=current_user_oid AND NOT admin_option
    ) OR EXISTS (SELECT 1 FROM pg_catalog.pg_database WHERE datdba IN (guard_owner_oid, writer_oid))
       OR EXISTS (SELECT 1 FROM pg_catalog.pg_namespace WHERE nspowner IN (guard_owner_oid, writer_oid))
       OR EXISTS (SELECT 1 FROM pg_catalog.pg_class WHERE relowner IN (guard_owner_oid, writer_oid))
       OR EXISTS (SELECT 1 FROM pg_catalog.pg_proc WHERE proowner IN (guard_owner_oid, writer_oid))
       OR EXISTS (SELECT 1 FROM pg_catalog.pg_default_acl WHERE defaclrole IN (guard_owner_oid, writer_oid))
       OR pg_catalog.has_database_privilege(guard_owner_oid, pg_catalog.current_database(), 'CREATE')
       OR pg_catalog.has_schema_privilege(guard_owner_oid, 'public', 'CREATE')
       OR pg_catalog.has_database_privilege(writer_oid, pg_catalog.current_database(), 'CREATE')
       OR pg_catalog.has_schema_privilege(writer_oid, 'public', 'CREATE') THEN
        RAISE EXCEPTION 'HostSession roles have unrelated ownership, membership, default ACL or CREATE authority';
    END IF;

    IF EXISTS (
        SELECT 1 FROM pg_catalog.pg_class c
        CROSS JOIN LATERAL pg_catalog.aclexplode(c.relacl) a
        WHERE a.grantee IN (guard_owner_oid, writer_oid)
    ) OR EXISTS (
        SELECT 1 FROM pg_catalog.pg_attribute col
        CROSS JOIN LATERAL pg_catalog.aclexplode(col.attacl) a
        WHERE a.grantee IN (guard_owner_oid, writer_oid)
    ) OR EXISTS (
        SELECT 1 FROM pg_catalog.pg_namespace n
        CROSS JOIN LATERAL pg_catalog.aclexplode(n.nspacl) a
        WHERE a.grantee IN (guard_owner_oid, writer_oid)
    ) OR EXISTS (
        SELECT 1 FROM pg_catalog.pg_proc p
        CROSS JOIN LATERAL pg_catalog.aclexplode(p.proacl) a
        WHERE a.grantee IN (guard_owner_oid, writer_oid)
    ) THEN
        RAISE EXCEPTION 'HostSession roles have unexpected direct grants before migration';
    END IF;
END;
$host_session_role_preflight$;

GRANT USAGE ON SCHEMA public TO ccp_host_session_guard_owner;
GRANT SELECT (provider_id, cluster_scope, state),
      UPDATE (provider_id)
    ON public.compute_providers TO ccp_host_session_guard_owner;
GRANT SELECT (host_id, provider_id, cluster_scope, node_uid, subject),
      UPDATE (host_id)
    ON public.compute_host_identity_bindings TO ccp_host_session_guard_owner;

CREATE TABLE public.compute_host_sessions (
    host_id TEXT NOT NULL
        REFERENCES public.compute_host_identity_bindings(host_id),
    epoch BIGINT NOT NULL CHECK (epoch > 0),
    session_id TEXT NOT NULL UNIQUE
        CHECK (pg_catalog.octet_length(session_id) BETWEEN 1 AND 256
               AND pg_catalog.btrim(session_id)=session_id
               AND session_id !~ '[[:cntrl:]]'),
    provider_id TEXT NOT NULL REFERENCES public.compute_providers(provider_id),
    cluster_scope TEXT NOT NULL
        CHECK (pg_catalog.octet_length(cluster_scope) BETWEEN 1 AND 1024
               AND pg_catalog.btrim(cluster_scope)=cluster_scope
               AND cluster_scope !~ '[[:cntrl:]]'),
    node_uid TEXT NOT NULL
        CHECK (pg_catalog.octet_length(node_uid) BETWEEN 1 AND 256
               AND pg_catalog.btrim(node_uid)=node_uid
               AND node_uid !~ '[[:cntrl:]]'),
    subject TEXT NOT NULL
        CHECK (pg_catalog.octet_length(subject) BETWEEN 1 AND 1024
               AND pg_catalog.btrim(subject)=subject
               AND subject !~ '[[:cntrl:]]'),
    issued_at TIMESTAMPTZ NOT NULL,
    expires_at TIMESTAMPTZ NOT NULL,
    PRIMARY KEY (host_id, epoch),
    CHECK (issued_at < expires_at)
);

CREATE TABLE public.compute_host_session_heartbeats (
    host_id TEXT NOT NULL,
    epoch BIGINT NOT NULL,
    sequence BIGINT NOT NULL CHECK (sequence > 0),
    digest TEXT NOT NULL CHECK (digest ~ '^sha256:[a-f0-9]{64}$'),
    recorded_at TIMESTAMPTZ NOT NULL,
    PRIMARY KEY (host_id, epoch, sequence),
    FOREIGN KEY (host_id, epoch)
        REFERENCES public.compute_host_sessions(host_id, epoch)
);
CREATE INDEX compute_host_session_heartbeats_latest
    ON public.compute_host_session_heartbeats(host_id, epoch, sequence DESC);

CREATE FUNCTION public.compute_host_session_immutable_guard() RETURNS trigger
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog, pg_temp
AS $guard$
BEGIN
    IF TG_RELID NOT IN (
        'public.compute_host_sessions'::pg_catalog.regclass,
        'public.compute_host_session_heartbeats'::pg_catalog.regclass
    ) THEN
        RAISE EXCEPTION 'HostSession immutable guard invoked for an unexpected relation'
            USING ERRCODE='42501';
    END IF;
    IF TG_OP='TRUNCATE' THEN
        RAISE EXCEPTION 'HostSession history is immutable'
            USING ERRCODE='23514';
    END IF;
    RAISE EXCEPTION 'HostSession history is append-only'
        USING ERRCODE='23514';
END;
$guard$;

CREATE TRIGGER compute_host_sessions_no_update_delete
    BEFORE UPDATE OR DELETE ON public.compute_host_sessions
    FOR EACH ROW EXECUTE FUNCTION public.compute_host_session_immutable_guard();
CREATE TRIGGER compute_host_sessions_no_truncate
    BEFORE TRUNCATE ON public.compute_host_sessions
    FOR EACH STATEMENT EXECUTE FUNCTION public.compute_host_session_immutable_guard();
CREATE TRIGGER compute_host_session_heartbeats_no_update_delete
    BEFORE UPDATE OR DELETE ON public.compute_host_session_heartbeats
    FOR EACH ROW EXECUTE FUNCTION public.compute_host_session_immutable_guard();
CREATE TRIGGER compute_host_session_heartbeats_no_truncate
    BEFORE TRUNCATE ON public.compute_host_session_heartbeats
    FOR EACH STATEMENT EXECUTE FUNCTION public.compute_host_session_immutable_guard();

-- Session start is serialized by the Provider row and immutable binding row.
-- Database time is sampled only after those locks have been acquired.
-- Session IDs are generated inside PostgreSQL, never accepted from the caller.
CREATE FUNCTION public.compute_host_session_start(
    p_host_id TEXT,
    p_provider_id TEXT,
    p_cluster_scope TEXT,
    p_node_uid TEXT,
    p_subject TEXT,
    p_expected_epoch BIGINT,
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
AS $start$
DECLARE
    v_provider_scope TEXT;
    v_provider_state TEXT;
    v_binding_provider TEXT;
    v_binding_scope TEXT;
    v_binding_node_uid TEXT;
    v_binding_subject TEXT;
    v_previous_epoch BIGINT := 0;
    v_previous_provider TEXT;
    v_previous_scope TEXT;
    v_previous_node_uid TEXT;
    v_previous_subject TEXT;
    v_previous_expires_at TIMESTAMPTZ;
    v_has_previous BOOLEAN := false;
    v_now TIMESTAMPTZ;
BEGIN
    IF p_host_id IS NULL OR pg_catalog.octet_length(p_host_id) NOT BETWEEN 1 AND 256
       OR pg_catalog.btrim(p_host_id) <> p_host_id OR p_host_id ~ '[[:cntrl:]]'
       OR p_provider_id IS NULL OR pg_catalog.octet_length(p_provider_id) NOT BETWEEN 1 AND 256
       OR pg_catalog.btrim(p_provider_id) <> p_provider_id OR p_provider_id ~ '[[:cntrl:]]'
       OR p_cluster_scope IS NULL OR pg_catalog.octet_length(p_cluster_scope) NOT BETWEEN 1 AND 1024
       OR pg_catalog.btrim(p_cluster_scope) <> p_cluster_scope OR p_cluster_scope ~ '[[:cntrl:]]'
       OR p_node_uid IS NULL OR pg_catalog.octet_length(p_node_uid) NOT BETWEEN 1 AND 256
       OR pg_catalog.btrim(p_node_uid) <> p_node_uid OR p_node_uid ~ '[[:cntrl:]]'
       OR p_subject IS NULL OR pg_catalog.octet_length(p_subject) NOT BETWEEN 1 AND 1024
       OR pg_catalog.btrim(p_subject) <> p_subject OR p_subject ~ '[[:cntrl:]]'
       OR p_expected_epoch IS NULL OR p_expected_epoch < 0
       OR p_expected_epoch = 9223372036854775807
       OR p_ttl_seconds IS NULL OR p_ttl_seconds < 1 OR p_ttl_seconds > 86400 THEN
        RAISE EXCEPTION 'invalid HostSession start request' USING ERRCODE='22023';
    END IF;

    SELECT p.cluster_scope, p.state
      INTO v_provider_scope, v_provider_state
      FROM public.compute_providers AS p
     WHERE p.provider_id=p_provider_id
     FOR SHARE;
    IF NOT FOUND OR v_provider_scope IS DISTINCT FROM p_cluster_scope
       OR v_provider_state IS DISTINCT FROM 'REGISTERED' THEN
        RAISE EXCEPTION 'HostSession start requires exact REGISTERED Provider scope'
            USING ERRCODE='23514';
    END IF;

    SELECT b.provider_id, b.cluster_scope, b.node_uid, b.subject
      INTO v_binding_provider, v_binding_scope, v_binding_node_uid, v_binding_subject
      FROM public.compute_host_identity_bindings AS b
     WHERE b.host_id=p_host_id
     FOR UPDATE;
    IF NOT FOUND OR v_binding_provider IS DISTINCT FROM p_provider_id
       OR v_binding_scope IS DISTINCT FROM p_cluster_scope
       OR v_binding_node_uid IS DISTINCT FROM p_node_uid
       OR v_binding_subject IS DISTINCT FROM p_subject THEN
        RAISE EXCEPTION 'HostSession start does not match the immutable HostIdentity binding'
            USING ERRCODE='23514';
    END IF;

    SELECT s.epoch, s.provider_id, s.cluster_scope, s.node_uid, s.subject, s.expires_at
      INTO v_previous_epoch, v_previous_provider, v_previous_scope,
           v_previous_node_uid, v_previous_subject, v_previous_expires_at
      FROM public.compute_host_sessions AS s
     WHERE s.host_id=p_host_id
     ORDER BY s.epoch DESC
     LIMIT 1
     FOR UPDATE;
    v_has_previous := FOUND;
    IF NOT v_has_previous THEN
        v_previous_epoch := 0;
    END IF;

    v_now := pg_catalog.clock_timestamp();
    IF v_has_previous THEN
        IF p_expected_epoch <> v_previous_epoch
           OR v_previous_provider IS DISTINCT FROM p_provider_id
           OR v_previous_scope IS DISTINCT FROM p_cluster_scope
           OR v_previous_node_uid IS DISTINCT FROM p_node_uid
           OR v_previous_subject IS DISTINCT FROM p_subject
           OR v_now < v_previous_expires_at THEN
            RAISE EXCEPTION 'HostSession expected epoch is stale or predecessor is still active'
                USING ERRCODE='23514';
        END IF;
    ELSIF p_expected_epoch <> 0 THEN
        RAISE EXCEPTION 'HostSession history is absent for a nonzero expected epoch'
            USING ERRCODE='23514';
    END IF;

    RETURN QUERY
    INSERT INTO public.compute_host_sessions AS inserted (
        host_id, epoch, session_id, provider_id, cluster_scope, node_uid,
        subject, issued_at, expires_at
    ) VALUES (
        p_host_id, v_previous_epoch + 1,
        'hs_' || pg_catalog.gen_random_uuid()::TEXT,
        p_provider_id, p_cluster_scope, p_node_uid, p_subject,
        v_now, v_now + pg_catalog.make_interval(secs => p_ttl_seconds::DOUBLE PRECISION)
    )
    RETURNING inserted.host_id, inserted.epoch, inserted.session_id,
              inserted.provider_id, inserted.cluster_scope, inserted.node_uid,
              inserted.subject, inserted.issued_at, inserted.expires_at;
END;
$start$;

-- Heartbeats lock the same Provider and binding before the latest session row.
-- Exact sequence/digest replay returns the original DB receipt and never
-- extends expiry. Conflicting, stale or old-epoch writes stay rejected.
CREATE FUNCTION public.compute_host_session_heartbeat(
    p_host_id TEXT,
    p_provider_id TEXT,
    p_cluster_scope TEXT,
    p_node_uid TEXT,
    p_subject TEXT,
    p_epoch BIGINT,
    p_session_id TEXT,
    p_sequence BIGINT,
    p_digest TEXT
) RETURNS TABLE (disposition TEXT, recorded_at TIMESTAMPTZ)
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog, pg_temp
AS $heartbeat$
DECLARE
    v_provider_scope TEXT;
    v_provider_state TEXT;
    v_binding_provider TEXT;
    v_binding_scope TEXT;
    v_binding_node_uid TEXT;
    v_binding_subject TEXT;
    v_session_epoch BIGINT;
    v_session_id TEXT;
    v_session_provider TEXT;
    v_session_scope TEXT;
    v_session_node_uid TEXT;
    v_session_subject TEXT;
    v_issued_at TIMESTAMPTZ;
    v_expires_at TIMESTAMPTZ;
    v_last_sequence BIGINT := 0;
    v_last_digest TEXT := '';
    v_last_recorded_at TIMESTAMPTZ;
    v_has_heartbeat BOOLEAN := false;
    v_now TIMESTAMPTZ;
    v_recorded_at TIMESTAMPTZ;
BEGIN
    IF p_host_id IS NULL OR pg_catalog.octet_length(p_host_id) NOT BETWEEN 1 AND 256
       OR pg_catalog.btrim(p_host_id) <> p_host_id OR p_host_id ~ '[[:cntrl:]]'
       OR p_provider_id IS NULL OR pg_catalog.octet_length(p_provider_id) NOT BETWEEN 1 AND 256
       OR pg_catalog.btrim(p_provider_id) <> p_provider_id OR p_provider_id ~ '[[:cntrl:]]'
       OR p_cluster_scope IS NULL OR pg_catalog.octet_length(p_cluster_scope) NOT BETWEEN 1 AND 1024
       OR pg_catalog.btrim(p_cluster_scope) <> p_cluster_scope OR p_cluster_scope ~ '[[:cntrl:]]'
       OR p_node_uid IS NULL OR pg_catalog.octet_length(p_node_uid) NOT BETWEEN 1 AND 256
       OR pg_catalog.btrim(p_node_uid) <> p_node_uid OR p_node_uid ~ '[[:cntrl:]]'
       OR p_subject IS NULL OR pg_catalog.octet_length(p_subject) NOT BETWEEN 1 AND 1024
       OR pg_catalog.btrim(p_subject) <> p_subject OR p_subject ~ '[[:cntrl:]]'
       OR p_epoch IS NULL OR p_epoch <= 0
       OR p_session_id IS NULL OR pg_catalog.octet_length(p_session_id) NOT BETWEEN 1 AND 256
       OR pg_catalog.btrim(p_session_id) <> p_session_id OR p_session_id ~ '[[:cntrl:]]'
       OR p_sequence IS NULL OR p_sequence <= 0
       OR p_digest IS NULL OR p_digest !~ '^sha256:[a-f0-9]{64}$' THEN
        RAISE EXCEPTION 'invalid HostSession heartbeat request' USING ERRCODE='22023';
    END IF;

    SELECT p.cluster_scope, p.state
      INTO v_provider_scope, v_provider_state
      FROM public.compute_providers AS p
     WHERE p.provider_id=p_provider_id
     FOR SHARE;
    IF NOT FOUND OR v_provider_scope IS DISTINCT FROM p_cluster_scope
       OR v_provider_state IS DISTINCT FROM 'REGISTERED' THEN
        RAISE EXCEPTION 'HostSession heartbeat requires exact REGISTERED Provider scope'
            USING ERRCODE='23514';
    END IF;

    SELECT b.provider_id, b.cluster_scope, b.node_uid, b.subject
      INTO v_binding_provider, v_binding_scope, v_binding_node_uid, v_binding_subject
      FROM public.compute_host_identity_bindings AS b
     WHERE b.host_id=p_host_id
     FOR UPDATE;
    IF NOT FOUND OR v_binding_provider IS DISTINCT FROM p_provider_id
       OR v_binding_scope IS DISTINCT FROM p_cluster_scope
       OR v_binding_node_uid IS DISTINCT FROM p_node_uid
       OR v_binding_subject IS DISTINCT FROM p_subject THEN
        RAISE EXCEPTION 'HostSession heartbeat does not match immutable HostIdentity binding'
            USING ERRCODE='23514';
    END IF;

    SELECT s.epoch, s.session_id, s.provider_id, s.cluster_scope,
           s.node_uid, s.subject, s.issued_at, s.expires_at
      INTO v_session_epoch, v_session_id, v_session_provider, v_session_scope,
           v_session_node_uid, v_session_subject, v_issued_at, v_expires_at
      FROM public.compute_host_sessions AS s
     WHERE s.host_id=p_host_id
     ORDER BY s.epoch DESC
     LIMIT 1
     FOR UPDATE;
    IF NOT FOUND OR v_session_epoch IS DISTINCT FROM p_epoch
       OR v_session_id IS DISTINCT FROM p_session_id
       OR v_session_provider IS DISTINCT FROM p_provider_id
       OR v_session_scope IS DISTINCT FROM p_cluster_scope
       OR v_session_node_uid IS DISTINCT FROM p_node_uid
       OR v_session_subject IS DISTINCT FROM p_subject THEN
        RAISE EXCEPTION 'HostSession heartbeat references a stale or foreign session epoch'
            USING ERRCODE='23514';
    END IF;

    v_now := pg_catalog.clock_timestamp();
    IF v_now < v_issued_at OR NOT v_now < v_expires_at THEN
        RAISE EXCEPTION 'HostSession heartbeat is outside the active session window'
            USING ERRCODE='23514';
    END IF;

    SELECT h.sequence, h.digest, h.recorded_at
      INTO v_last_sequence, v_last_digest, v_last_recorded_at
      FROM public.compute_host_session_heartbeats AS h
     WHERE h.host_id=p_host_id AND h.epoch=p_epoch
     ORDER BY h.sequence DESC
     LIMIT 1;
    v_has_heartbeat := FOUND;
    IF NOT v_has_heartbeat THEN
        v_last_sequence := 0;
        v_last_digest := '';
    END IF;

    IF v_has_heartbeat AND v_now < v_last_recorded_at THEN
        RAISE EXCEPTION 'HostSession database clock moved behind the last accepted receipt'
            USING ERRCODE='23514';
    END IF;

    IF p_sequence < v_last_sequence THEN
        RAISE EXCEPTION 'HostSession heartbeat sequence moved backwards'
            USING ERRCODE='23514';
    END IF;
    IF p_sequence = v_last_sequence THEN
        IF v_has_heartbeat AND p_digest = v_last_digest THEN
            RETURN QUERY SELECT 'REPLAY'::TEXT, v_last_recorded_at;
            RETURN;
        END IF;
        RAISE EXCEPTION 'HostSession heartbeat sequence conflicts with a prior digest'
            USING ERRCODE='23514';
    END IF;

    INSERT INTO public.compute_host_session_heartbeats
        (host_id, epoch, sequence, digest, recorded_at)
    VALUES (p_host_id, p_epoch, p_sequence, p_digest, v_now)
    RETURNING compute_host_session_heartbeats.recorded_at INTO v_recorded_at;

    RETURN QUERY SELECT 'ACCEPT'::TEXT, v_recorded_at;
END;
$heartbeat$;

COMMENT ON TABLE public.compute_host_sessions IS
    'Append-only durable HostIdentity session epochs; server-issued IDs and DB-clock expiry; not host attestation or GPU readiness.';
COMMENT ON TABLE public.compute_host_session_heartbeats IS
    'Append-only per-epoch heartbeat receipts; exact replay is idempotent and does not extend session expiry.';
COMMENT ON FUNCTION public.compute_host_session_start(TEXT,TEXT,TEXT,TEXT,TEXT,BIGINT,INTEGER) IS
    'Unmounted internal CAS entrypoint; caller must authenticate/revoke principal before invoking; locks Provider and immutable HostIdentity binding.';
COMMENT ON FUNCTION public.compute_host_session_heartbeat(TEXT,TEXT,TEXT,TEXT,TEXT,BIGINT,TEXT,BIGINT,TEXT) IS
    'Unmounted internal heartbeat entrypoint; enforces active latest epoch, DB clock, monotonic sequence and exact replay without expiry extension.';

REVOKE ALL ON TABLE public.compute_host_sessions FROM PUBLIC;
REVOKE ALL ON TABLE public.compute_host_session_heartbeats FROM PUBLIC;
REVOKE ALL ON FUNCTION public.compute_host_session_immutable_guard() FROM PUBLIC;
REVOKE ALL ON FUNCTION public.compute_host_session_start(TEXT,TEXT,TEXT,TEXT,TEXT,BIGINT,INTEGER) FROM PUBLIC;
REVOKE ALL ON FUNCTION public.compute_host_session_heartbeat(TEXT,TEXT,TEXT,TEXT,TEXT,BIGINT,TEXT,BIGINT,TEXT) FROM PUBLIC;
ALTER TABLE public.compute_host_sessions
    ENABLE ALWAYS TRIGGER compute_host_sessions_no_update_delete;
ALTER TABLE public.compute_host_sessions
    ENABLE ALWAYS TRIGGER compute_host_sessions_no_truncate;
ALTER TABLE public.compute_host_session_heartbeats
    ENABLE ALWAYS TRIGGER compute_host_session_heartbeats_no_update_delete;
ALTER TABLE public.compute_host_session_heartbeats
    ENABLE ALWAYS TRIGGER compute_host_session_heartbeats_no_truncate;

GRANT USAGE ON SCHEMA public TO ccp_host_session_writer;
GRANT EXECUTE ON FUNCTION public.compute_host_session_start(TEXT,TEXT,TEXT,TEXT,TEXT,BIGINT,INTEGER)
    TO ccp_host_session_writer;
GRANT EXECUTE ON FUNCTION public.compute_host_session_heartbeat(TEXT,TEXT,TEXT,TEXT,TEXT,BIGINT,TEXT,BIGINT,TEXT)
    TO ccp_host_session_writer;

-- The NOLOGIN owner is the only direct table owner. The runtime writer gets
-- EXECUTE only; Provider and binding reads/locks remain inside the definer.
GRANT CREATE ON SCHEMA public TO ccp_host_session_guard_owner;
ALTER TABLE public.compute_host_sessions OWNER TO ccp_host_session_guard_owner;
ALTER TABLE public.compute_host_session_heartbeats OWNER TO ccp_host_session_guard_owner;
ALTER FUNCTION public.compute_host_session_immutable_guard() OWNER TO ccp_host_session_guard_owner;
ALTER FUNCTION public.compute_host_session_start(TEXT,TEXT,TEXT,TEXT,TEXT,BIGINT,INTEGER)
    OWNER TO ccp_host_session_guard_owner;
ALTER FUNCTION public.compute_host_session_heartbeat(TEXT,TEXT,TEXT,TEXT,TEXT,BIGINT,TEXT,BIGINT,TEXT)
    OWNER TO ccp_host_session_guard_owner;

REVOKE ccp_host_session_guard_owner FROM CURRENT_USER;
REVOKE CREATE ON SCHEMA public FROM ccp_host_session_guard_owner;

DO $host_session_acl_attestation$
DECLARE
    guard_owner_oid pg_catalog.oid;
    writer_oid pg_catalog.oid;
BEGIN
    SELECT oid INTO guard_owner_oid FROM pg_catalog.pg_roles WHERE rolname='ccp_host_session_guard_owner';
    SELECT oid INTO writer_oid FROM pg_catalog.pg_roles WHERE rolname='ccp_host_session_writer';

    IF EXISTS (
        SELECT 1 FROM pg_catalog.pg_class c
        CROSS JOIN LATERAL pg_catalog.aclexplode(c.relacl) a
        WHERE c.oid IN ('public.compute_host_sessions'::pg_catalog.regclass,
                        'public.compute_host_session_heartbeats'::pg_catalog.regclass)
          AND a.grantee <> guard_owner_oid
    ) OR EXISTS (
        SELECT 1 FROM pg_catalog.pg_attribute col
        CROSS JOIN LATERAL pg_catalog.aclexplode(col.attacl) a
        WHERE col.attrelid IN ('public.compute_host_sessions'::pg_catalog.regclass,
                               'public.compute_host_session_heartbeats'::pg_catalog.regclass)
          AND a.grantee <> guard_owner_oid
    ) THEN
        RAISE EXCEPTION 'HostSession tables have unexpected direct/default grants';
    END IF;

    IF EXISTS (
        SELECT 1 FROM pg_catalog.pg_proc p
        CROSS JOIN LATERAL pg_catalog.aclexplode(p.proacl) a
        WHERE p.oid IN (
            'public.compute_host_session_immutable_guard()'::pg_catalog.regprocedure,
            'public.compute_host_session_start(text,text,text,text,text,bigint,integer)'::pg_catalog.regprocedure,
            'public.compute_host_session_heartbeat(text,text,text,text,text,bigint,text,bigint,text)'::pg_catalog.regprocedure
        )
          AND a.grantee NOT IN (
              guard_owner_oid,
              CASE WHEN p.proname IN ('compute_host_session_start','compute_host_session_heartbeat')
                   THEN writer_oid ELSE guard_owner_oid END
          )
    ) THEN
        RAISE EXCEPTION 'HostSession functions have unexpected EXECUTE grants';
    END IF;
END;
$host_session_acl_attestation$;

