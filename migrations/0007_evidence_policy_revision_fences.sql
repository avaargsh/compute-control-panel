-- v0007: administrator-provisioned E1 evidence policy revision fencing.
-- Additive only; do not rewrite applied Provider v0005 or E1 v0006.
-- A transaction holding SELECT ... FOR SHARE on a matching row blocks
-- concurrent administrative UPDATE/DELETE revocation until after commit.
CREATE TABLE compute_evidence_policy_fences (
    tenant_id TEXT NOT NULL CHECK (btrim(tenant_id) <> ''),
    provider_id TEXT NOT NULL CHECK (btrim(provider_id) <> ''),
    policy_version TEXT NOT NULL CHECK (btrim(policy_version) <> ''),
    policy_digest TEXT NOT NULL CHECK (policy_digest ~ '^sha256:[a-f0-9]{64}$'),
    effective_from TIMESTAMPTZ NOT NULL,
    effective_until TIMESTAMPTZ NOT NULL,
    revision BIGINT NOT NULL CHECK (revision > 0),
    revoked BOOLEAN NOT NULL DEFAULT false,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    PRIMARY KEY (tenant_id, provider_id, policy_version),
    CHECK (effective_until > effective_from)
);

-- Authority provisioning is separate from the ordinary E1 writer.
-- Production MUST use a non-owner, narrowly scoped DB application role,
-- with read-only permission on this table and no ability to create policies,
-- change/revoke rows, disable triggers or bypass permissions.
REVOKE INSERT, UPDATE, DELETE, TRUNCATE, REFERENCES, TRIGGER
ON compute_evidence_policy_fences FROM PUBLIC;

-- The policy identity is immutable and the revision changes on EVERY update.
-- Explicit revision bumps prevent silent rollback or reactivation of an
-- already-revoked policy using a stale authority snapshot.
CREATE FUNCTION compute_enforce_evidence_policy_revision()
RETURNS trigger
LANGUAGE plpgsql
AS $$
BEGIN
    IF NEW.tenant_id IS DISTINCT FROM OLD.tenant_id
       OR NEW.provider_id IS DISTINCT FROM OLD.provider_id
       OR NEW.policy_version IS DISTINCT FROM OLD.policy_version
    THEN
        RAISE EXCEPTION 'evidence policy fence identity is immutable'
            USING ERRCODE = '23514';
    END IF;
    IF OLD.revision = 9223372036854775807
       OR NEW.revision <> OLD.revision + 1
    THEN
        RAISE EXCEPTION 'evidence policy fence revision must increase by one'
            USING ERRCODE = '23514';
    END IF;
    NEW.updated_at := clock_timestamp();
    RETURN NEW;
END;
$$;

CREATE TRIGGER compute_evidence_policy_revision_guard
BEFORE UPDATE ON compute_evidence_policy_fences
FOR EACH ROW EXECUTE FUNCTION compute_enforce_evidence_policy_revision();

-- No deletion/re-creation of the same version at revision 1. Revocation and
-- any re-approval must be done by a privileged monotonic UPDATE, with
-- independent approval/audit outside this migration.
CREATE FUNCTION compute_reject_evidence_policy_removal()
RETURNS trigger
LANGUAGE plpgsql
AS $$
BEGIN
    RAISE EXCEPTION 'evidence policy fence history cannot be removed'
        USING ERRCODE = '55000';
END;
$$;

CREATE TRIGGER compute_evidence_policy_no_delete
BEFORE DELETE ON compute_evidence_policy_fences
FOR EACH ROW EXECUTE FUNCTION compute_reject_evidence_policy_removal();

CREATE TRIGGER compute_evidence_policy_no_truncate
BEFORE TRUNCATE ON compute_evidence_policy_fences
FOR EACH STATEMENT EXECUTE FUNCTION compute_reject_evidence_policy_removal();
