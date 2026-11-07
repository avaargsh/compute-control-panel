-- v0012: validate policy and independent approval deadlines AFTER row-lock waits.
-- Keep all checksummed migrations 0001..0011 unchanged. Replacing this function
-- preserves its owner, ACL and the existing ALWAYS authorization trigger.
CREATE OR REPLACE FUNCTION public.compute_require_e1_final_authorization()
RETURNS trigger
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog, public, pg_temp
AS $e1_final_auth$
DECLARE
    db_now TIMESTAMPTZ;
    locked_policy_revision BIGINT;
    policy_effective_from TIMESTAMPTZ;
    policy_effective_until TIMESTAMPTZ;
    approval_expires_at TIMESTAMPTZ;
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

    -- Preserve policy-before-approval lock ordering. Read deadlines from the
    -- locked row; time predicates evaluated before a lock wait can go stale.
    SELECT policy.revision, policy.effective_from, policy.effective_until
    INTO locked_policy_revision, policy_effective_from, policy_effective_until
    FROM public.compute_evidence_policy_fences policy
    WHERE policy.tenant_id = NEW.tenant_id
      AND policy.provider_id = NEW.provider_id
      AND policy.policy_version = NEW.evidence_policy_version
      AND policy.revoked = false
    FOR SHARE;

    IF NOT FOUND THEN
        RAISE EXCEPTION 'E1 FINAL policy is absent, revoked or expired'
            USING ERRCODE = 'E1P01';
    END IF;
    db_now := clock_timestamp();
    IF policy_effective_from > db_now OR policy_effective_until <= db_now THEN
        RAISE EXCEPTION 'E1 FINAL policy is absent, revoked or expired'
            USING ERRCODE = 'E1P01';
    END IF;

    SELECT true, approval.expires_at INTO authorized, approval_expires_at
    FROM public.compute_e1_final_authorizations approval
    WHERE approval.tenant_id = NEW.tenant_id
      AND approval.evidence_set_id = NEW.evidence_set_id
      AND approval.provider_id = NEW.provider_id
      AND approval.policy_version = NEW.evidence_policy_version
      AND approval.policy_revision = locked_policy_revision
      AND approval.revoked = false
      -- This binds *every* persisted FINAL field except server recorded_at,
      -- including billing quantities, interval, GPU model, provenance JSON,
      -- source IDs, Operation, Tenant, Project, and policy version.
      AND approval.expected_final = (to_jsonb(NEW) - 'recorded_at')
    FOR SHARE;

    -- A share lock fences revocation, but cannot stop either deadline from
    -- passing. Recheck BOTH windows using a fresh clock after the approval lock.
    db_now := clock_timestamp();
    IF policy_effective_from > db_now OR policy_effective_until <= db_now THEN
        RAISE EXCEPTION 'E1 FINAL policy is absent, revoked or expired'
            USING ERRCODE = 'E1P01';
    END IF;
    IF COALESCE(authorized, false) IS NOT true OR approval_expires_at <= db_now THEN
        RAISE EXCEPTION 'E1 FINAL requires independently authorized exact evidence'
            USING ERRCODE = 'E1A01';
    END IF;
    RETURN NEW;
END;
$e1_final_auth$;

REVOKE ALL ON FUNCTION public.compute_require_e1_final_authorization() FROM PUBLIC;
