-- v0010: fail-closed E1 FINAL authority and integrity under session_replication_role=replica.
--
-- PostgreSQL's ordinary ENABLE TRIGGER is skipped in replica mode. Use
-- ENABLE ALWAYS for FINAL authorization, source/policy integrity and immutability,
-- even for controlled administrative replication/restore sessions.
-- E1 runtime writers still must NOT have SET session_replication_role, table
-- ownership, SUPERUSER or schema DDL. No trigger can contain a hostile
-- superuser who can disable it or modify the table/catalog.
--
-- Forward-only upgrade; do not modify v0001..v0009 checksums.
ALTER TABLE public.compute_allocation_usage_evidence_sets
    ENABLE ALWAYS TRIGGER compute_usage_evidence_z_authorization_guard;

ALTER TABLE public.compute_e1_final_authorizations
    ENABLE ALWAYS TRIGGER compute_e1_final_auth_immutable;

ALTER TABLE public.compute_e1_final_authorizations
    ENABLE ALWAYS TRIGGER compute_e1_final_auth_no_truncate;

-- Existing v0006/v0007 source and policy integrity guards are equally
-- security-sensitive. Keep them in ALWAYS mode so replication sessions do not
-- bypass Provider/Allocation checks, immutability or policy revisions.
ALTER TABLE public.compute_allocation_usage_evidence_sets
    ENABLE ALWAYS TRIGGER compute_usage_evidence_scope_guard;
ALTER TABLE public.compute_allocation_usage_evidence_sets
    ENABLE ALWAYS TRIGGER compute_usage_evidence_append_only;
ALTER TABLE public.compute_allocation_usage_evidence_sets
    ENABLE ALWAYS TRIGGER compute_usage_evidence_no_truncate;

ALTER TABLE public.compute_evidence_policy_fences
    ENABLE ALWAYS TRIGGER compute_evidence_policy_revision_guard;
ALTER TABLE public.compute_evidence_policy_fences
    ENABLE ALWAYS TRIGGER compute_evidence_policy_no_delete;
ALTER TABLE public.compute_evidence_policy_fences
    ENABLE ALWAYS TRIGGER compute_evidence_policy_no_truncate;
