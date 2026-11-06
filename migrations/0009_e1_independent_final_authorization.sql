-- v0009: independent, exact-row authority before any new immutable FINAL.
-- The E1 writer MUST NOT be able to provision or modify this approval ledger.
-- The issuer must separately authenticate the tenant, source observations,
-- policy version/revision, and exact derived FINAL row before writing a ticket.
-- Preserve checksummed versions 0001..0008 as already applied.
CREATE TABLE public.compute_e1_final_authorizations (
    tenant_id TEXT NOT NULL CHECK (btrim(tenant_id) <> ''),
    evidence_set_id TEXT NOT NULL CHECK (btrim(evidence_set_id) <> ''),
    provider_id TEXT NOT NULL CHECK (btrim(provider_id) <> ''),
    policy_version TEXT NOT NULL CHECK (btrim(policy_version) <> ''),
    policy_revision BIGINT NOT NULL CHECK (policy_revision > 0),
    expected_final JSONB NOT NULL CHECK (jsonb_typeof(expected_final) = 'object'),
    expires_at TIMESTAMPTZ NOT NULL,
    approved_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    revoked BOOLEAN NOT NULL DEFAULT false,
    CONSTRAINT compute_e1_final_authorizations_pkey
        PRIMARY KEY (tenant_id, evidence_set_id, policy_revision),
    CHECK (expires_at > approved_at)
);

-- Never make an issuer's privilege available through PUBLIC.
REVOKE ALL ON TABLE public.compute_e1_final_authorizations FROM PUBLIC;

-- A revocation is monotonic; the signed-off exact row, version, and source
-- cannot be overwritten. Ordinary E1 writers have no approval-table DML.
CREATE FUNCTION public.compute_protect_e1_final_authorizations()
RETURNS trigger
LANGUAGE plpgsql
SET search_path = pg_catalog, public, pg_temp
AS $approval_guard$
BEGIN
    IF TG_OP = 'UPDATE'
       AND OLD.revoked = false AND NEW.revoked = true
       AND (to_jsonb(NEW) - 'revoked') = (to_jsonb(OLD) - 'revoked') THEN
        RETURN NEW;
    END IF;
    RAISE EXCEPTION 'E1 independent FINAL authorization is immutable'
        USING ERRCODE = '23514';
END;
$approval_guard$;

CREATE TRIGGER compute_e1_final_auth_immutable
BEFORE UPDATE OR DELETE ON public.compute_e1_final_authorizations
FOR EACH ROW EXECUTE FUNCTION public.compute_protect_e1_final_authorizations();

CREATE TRIGGER compute_e1_final_auth_no_truncate
BEFORE TRUNCATE ON public.compute_e1_final_authorizations
FOR EACH STATEMENT EXECUTE FUNCTION public.compute_protect_e1_final_authorizations();

-- Deliberately SECURITY DEFINER: E1 writer has no direct read/write access
-- to issuer tickets. This function contains no dynamic SQL, pins its schema
-- and is owned by the independently provisioned migration role.
-- A row-share lock fences both approval revocation and policy revision
-- changes until the FINAL transaction commits or rolls back.
CREATE FUNCTION public.compute_require_e1_final_authorization()
RETURNS trigger
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog, public, pg_temp
AS $e1_final_auth$
DECLARE
    db_now TIMESTAMPTZ;
    authorized BOOLEAN := false;
BEGIN
    -- An immutable row already bound by a unique key can only cause an
    -- ON CONFLICT replay/conflict; no new FINAL row can be created. Preserve
    -- Go's idempotent replay/conflict behavior after ticket expiration.
    IF EXISTS (
        SELECT 1
        FROM public.compute_allocation_usage_evidence_sets prior
        WHERE prior.tenant_id = NEW.tenant_id
          AND (prior.evidence_set_id = NEW.evidence_set_id
               OR (prior.allocation_id = NEW.allocation_id
                   AND prior.allocation_generation = NEW.allocation_generation)
               OR prior.usage_interval_id = NEW.usage_interval_id)
    ) THEN
        RETURN NEW;
    END IF;

    -- A current approved policy is necessary even for a direct SQL writer.
    -- Distinguish a revoked/expired policy from a missing per-evidence ticket
    -- so the Go caller preserves ErrEvidencePolicyUntrusted on lock delays.
    IF NOT EXISTS (
        SELECT 1 FROM public.compute_evidence_policy_fences policy
        WHERE policy.tenant_id = NEW.tenant_id
          AND policy.provider_id = NEW.provider_id
          AND policy.policy_version = NEW.evidence_policy_version
          AND policy.revoked = false
          AND policy.effective_from <= clock_timestamp()
          AND policy.effective_until > clock_timestamp()
        FOR SHARE
    ) THEN
        RAISE EXCEPTION 'E1 FINAL policy is absent, revoked or expired'
            USING ERRCODE = 'E1P01';
    END IF;
    -- The clock is read AFTER waiting for the authority row-share lock.
    db_now := clock_timestamp();

    SELECT true INTO authorized
    FROM public.compute_e1_final_authorizations approval
    JOIN public.compute_evidence_policy_fences policy
      ON policy.tenant_id = approval.tenant_id
     AND policy.provider_id = approval.provider_id
     AND policy.policy_version = approval.policy_version
    WHERE approval.tenant_id = NEW.tenant_id
      AND approval.evidence_set_id = NEW.evidence_set_id
      AND approval.provider_id = NEW.provider_id
      AND approval.policy_version = NEW.evidence_policy_version
      AND approval.policy_revision = policy.revision
      AND approval.revoked = false
      AND policy.revoked = false
      AND approval.expires_at > db_now
      AND policy.effective_from <= db_now
      AND policy.effective_until > db_now
      -- This binds *every* persisted FINAL field except server recorded_at,
      -- including billing quantities, interval, GPU model, provenance JSON,
      -- source IDs, Operation, Tenant, Project, and policy version.
      AND approval.expected_final = (to_jsonb(NEW) - 'recorded_at')
    FOR SHARE OF approval, policy;

    IF COALESCE(authorized, false) IS NOT true THEN
        RAISE EXCEPTION 'E1 FINAL requires independently authorized exact evidence'
            USING ERRCODE = 'E1A01';
    END IF;
    RETURN NEW;
END;
$e1_final_auth$;

REVOKE ALL ON FUNCTION public.compute_require_e1_final_authorization() FROM PUBLIC;
REVOKE ALL ON FUNCTION public.compute_protect_e1_final_authorizations() FROM PUBLIC;

CREATE TRIGGER compute_usage_evidence_z_authorization_guard
BEFORE INSERT ON public.compute_allocation_usage_evidence_sets
FOR EACH ROW EXECUTE FUNCTION public.compute_require_e1_final_authorization();
