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
