-- Host enrollment slice A (#182): immutable Provider-scoped identity.
-- Internal persistence only. No enrollment API, authenticated attestation,
-- session issuance, GPU health, allocation or production grants are added.
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
-- Provider lifecycle update. Take a SHARE row lock and inspect the current
-- lifecycle and cluster in the same transaction. A Drain that wins the lock
-- cannot be followed by new enrollment; a committed prior binding is retained.
-- Invoker rights deliberately grant no Provider or enrollment privileges.
CREATE FUNCTION public.compute_host_identity_enrollment_guard() RETURNS trigger
LANGUAGE plpgsql
SET search_path = pg_catalog, pg_temp
AS $guard$
DECLARE
    current_scope TEXT;
    current_state TEXT;
BEGIN
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

CREATE TRIGGER compute_host_identity_enrollment_insert
    BEFORE INSERT ON public.compute_host_identity_bindings
    FOR EACH ROW EXECUTE FUNCTION public.compute_host_identity_enrollment_guard();
CREATE TRIGGER compute_host_identity_binding_no_change
    BEFORE UPDATE OR DELETE ON public.compute_host_identity_bindings
    FOR EACH ROW EXECUTE FUNCTION public.compute_host_identity_enrollment_guard();
CREATE TRIGGER compute_host_identity_binding_no_truncate
    BEFORE TRUNCATE ON public.compute_host_identity_bindings
    FOR EACH STATEMENT EXECUTE FUNCTION public.compute_host_identity_enrollment_guard();

ALTER TABLE public.compute_host_identity_bindings
    ENABLE ALWAYS TRIGGER compute_host_identity_enrollment_insert;
ALTER TABLE public.compute_host_identity_bindings
    ENABLE ALWAYS TRIGGER compute_host_identity_binding_no_change;
ALTER TABLE public.compute_host_identity_bindings
    ENABLE ALWAYS TRIGGER compute_host_identity_binding_no_truncate;

REVOKE ALL ON TABLE public.compute_host_identity_bindings FROM PUBLIC;
REVOKE ALL ON FUNCTION public.compute_host_identity_enrollment_guard() FROM PUBLIC;

COMMENT ON TABLE public.compute_host_identity_bindings IS
    'Immutable CCP host enrollment binding; not independently attested physical host identity or GPU availability.';
