package postgres

import (
	"context"
	"fmt"
	"strings"
)

// Security contract is pinned to the original, forward-only v0009 function
// source. A checksummed migration ledger cannot prove the live function body.
const expectedE1FinalAuth = `DECLARE
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
END;`
const expectedE1FinalMutation = `BEGIN
    IF TG_OP = 'UPDATE'
       AND OLD.revoked = false AND NEW.revoked = true
       AND (to_jsonb(NEW) - 'revoked') = (to_jsonb(OLD) - 'revoked') THEN
        RETURN NEW;
    END IF;
    RAISE EXCEPTION 'E1 independent FINAL authorization is immutable'
        USING ERRCODE = '23514';
END;`

// This is transactional catalog attestation, not a proof against a privileged
// owner who tampers with SQL objects AFTER attestation.
func verifyE1FinalAuthorizationCatalog(ctx context.Context, q evidenceWriterQueryRow) error {
	if q == nil {
		return ErrEvidencePolicyUntrusted
	}
	var ok bool
	err := q.QueryRowContext(ctx, `
		SELECT EXISTS (
			SELECT 1 FROM pg_catalog.pg_class c
			JOIN pg_catalog.pg_namespace ns ON ns.oid=c.relnamespace
			WHERE c.oid='public.compute_e1_final_authorizations'::regclass
			  AND ns.nspname='public' AND c.relkind='r' AND c.relpersistence='p'
			  -- The independent issuer/migrator may own a separate schema relation.
			  AND NOT EXISTS (
				  SELECT 1 FROM pg_catalog.aclexplode(
					  coalesce(c.relacl,pg_catalog.acldefault('r',c.relowner))) a
				  WHERE a.grantee=0
			  )
		)
	`).Scan(&ok)
	if err != nil || !ok {
		return fmt.Errorf("%w: E1 approval relation durability or PUBLIC ACL drift: %v", ErrEvidencePolicyUntrusted, err)
	}
	err = q.QueryRowContext(ctx, `
		WITH expected(name,typ) AS (
			VALUES ('tenant_id','text'::regtype),
				('evidence_set_id','text'::regtype),
				('provider_id','text'::regtype),
				('policy_version','text'::regtype),
				('policy_revision','bigint'::regtype),
				('expected_final','jsonb'::regtype),
				('expires_at','timestamptz'::regtype),
				('approved_at','timestamptz'::regtype),
				('revoked','boolean'::regtype)
		)
		SELECT count(*)=(SELECT count(*) FROM expected)
		FROM expected e
		JOIN pg_catalog.pg_attribute a
		  ON a.attrelid='public.compute_e1_final_authorizations'::regclass
		 AND a.attname=e.name AND a.atttypid=e.typ
		 AND a.attnotnull AND NOT a.attisdropped
	`).Scan(&ok)
	if err != nil || !ok {
		return fmt.Errorf("%w: E1 approval column catalog drift: %v", ErrEvidencePolicyUntrusted, err)
	}
	err = q.QueryRowContext(ctx, `
		SELECT EXISTS (
			SELECT 1 FROM pg_catalog.pg_constraint c
			WHERE c.conrelid='public.compute_e1_final_authorizations'::regclass
			  AND c.conname='compute_e1_final_authorizations_pkey'
			  AND c.contype='p' AND c.convalidated
			  AND NOT c.condeferrable AND NOT c.condeferred
			  AND (
				SELECT array_agg(a.attname::text ORDER BY k.ordinality)
				FROM unnest(c.conkey) WITH ORDINALITY k(attnum,ordinality)
				JOIN pg_catalog.pg_attribute a
				  ON a.attrelid=c.conrelid AND a.attnum=k.attnum
			  )=ARRAY['tenant_id','evidence_set_id','policy_revision']::text[]
		)
	`).Scan(&ok)
	if err != nil || !ok {
		return fmt.Errorf("%w: E1 approval PK drift: %v", ErrEvidencePolicyUntrusted, err)
	}
	for _, f := range []struct {
		name            string
		src             string
		securityDefiner bool
	}{
		{"public.compute_require_e1_final_authorization()", expectedE1FinalAuth, true},
		{"public.compute_protect_e1_final_authorizations()", expectedE1FinalMutation, false},
	} {
		var body string
		var valid bool
		err := q.QueryRowContext(ctx, `
			SELECT p.prosrc,
				(p.pronargs=0 AND p.prokind='f' AND p.prosecdef=$2
				 AND p.prolang=(SELECT oid FROM pg_catalog.pg_language WHERE lanname='plpgsql')
				 AND p.prorettype='pg_catalog.trigger'::regtype
				 AND p.provolatile='v' AND p.proparallel='u'
				 AND NOT p.proleakproof
				 AND p.proowner=(SELECT relowner FROM pg_catalog.pg_class
				                 WHERE oid='public.compute_e1_final_authorizations'::regclass)
				 AND p.proconfig=ARRAY['search_path=pg_catalog, public, pg_temp']::text[])
			FROM pg_catalog.pg_proc p WHERE p.oid=to_regprocedure($1)
		`, f.name, f.securityDefiner).Scan(&body, &valid)
		if err != nil || !valid || strings.TrimSpace(body) != strings.TrimSpace(f.src) {
			return fmt.Errorf("%w: E1 approval function drift %s: %v", ErrEvidencePolicyUntrusted, f.name, err)
		}
	}
	for _, t := range []struct {
		relation, trigger, function string
		event                       int
	}{
		{"public.compute_allocation_usage_evidence_sets", "compute_usage_evidence_z_authorization_guard", "public.compute_require_e1_final_authorization()", 7},
		{"public.compute_e1_final_authorizations", "compute_e1_final_auth_immutable", "public.compute_protect_e1_final_authorizations()", 27},
		{"public.compute_e1_final_authorizations", "compute_e1_final_auth_no_truncate", "public.compute_protect_e1_final_authorizations()", 34},
	} {
		err := q.QueryRowContext(ctx, `
			SELECT EXISTS (
				SELECT 1 FROM pg_catalog.pg_trigger g
				WHERE g.tgrelid=to_regclass($1) AND g.tgname=$2
				  AND NOT g.tgisinternal AND g.tgenabled='O'
				  AND g.tgtype=$4 AND g.tgqual IS NULL
				  AND g.tgattr::text='' AND g.tgnargs=0
				  AND NOT g.tgdeferrable AND NOT g.tginitdeferred
				  AND g.tgfoid=to_regprocedure($3)
			)
		`, t.relation, t.trigger, t.function, t.event).Scan(&ok)
		if err != nil || !ok {
			return fmt.Errorf("%w: E1 approval trigger drift %s: %v", ErrEvidencePolicyUntrusted, t.trigger, err)
		}
	}
	return nil
}
