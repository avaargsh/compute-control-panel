-- v0008: prevent pg_temp/search_path from shadowing v0006 E1 INSERT
-- trigger Provider/Workload/Allocation authority reads.
--
-- The existing v0006 compute_validate_usage_evidence_set() body uses
-- unqualified table names. With ordinary TEMP rights, an E1 writer could
-- manufacture SUCCEEDED/BOUND rows in same-name temporary tables and make
-- a direct INSERT appear valid. Do not rewrite the checksummed v0006 bytes.
--
-- An explicitly placed pg_temp at the END prevents its implicit first
-- position for relation lookup, while preserving normal pg_catalog behavior.
-- Keep this function SECURITY INVOKER: policy and resource authority belong
-- to the permanent public tables, never a caller-controlled temp schema.
ALTER FUNCTION public.compute_validate_usage_evidence_set()
    SET search_path = pg_catalog, public, pg_temp;
