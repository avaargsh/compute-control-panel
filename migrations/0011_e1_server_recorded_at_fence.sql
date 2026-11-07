-- v0011: FINAL evidence recorded_at is database-owned, not caller-provided.
-- v0001..v0010 are immutable checksummed migrations.
-- An E1 restricted writer receives table-level INSERT for immutable FINAL,
-- which also allows explicitly supplying a caller-chosen recorded_at. The
-- v0009 exact-row approval intentionally excludes this server-owned field.
-- Never trust the DEFAULT alone: force a fresh server clock on every INSERT.
CREATE FUNCTION public.compute_stamp_e1_recorded_at()
RETURNS trigger
LANGUAGE plpgsql
SET search_path = pg_catalog, public, pg_temp
AS $e1_stamp$
BEGIN
    NEW.recorded_at := clock_timestamp();
    RETURN NEW;
END;
$e1_stamp$;

REVOKE ALL ON FUNCTION public.compute_stamp_e1_recorded_at() FROM PUBLIC;

-- Alphabetical BEFORE INSERT trigger order: existing scope_guard, this
-- y-recorded_at guard, then z-authorization guard. Authorization compares the
-- server-stamped NEW row excluding recorded_at; all other bytes stay bound.
CREATE TRIGGER compute_usage_evidence_y_recorded_at_guard
BEFORE INSERT ON public.compute_allocation_usage_evidence_sets
FOR EACH ROW EXECUTE FUNCTION public.compute_stamp_e1_recorded_at();

ALTER TABLE public.compute_allocation_usage_evidence_sets
    ENABLE ALWAYS TRIGGER compute_usage_evidence_y_recorded_at_guard;
