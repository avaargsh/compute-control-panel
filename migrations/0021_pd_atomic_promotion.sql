-- M5.8: persist the exact connection between a previously recorded PD
-- proposal and the next existing Workload/Allocation/Operation generation.
-- The writer must perform workload generation CAS, old allocation RELEASED
-- checks and both Outbox inserts in ONE transaction. No new runtime grants.
CREATE TABLE public.compute_pd_promotions (
    intent_id TEXT PRIMARY KEY REFERENCES public.compute_pd_intents(intent_id),
    workload_id TEXT NOT NULL,
    tenant_id TEXT NOT NULL,
    project_id TEXT NOT NULL,
    target_generation BIGINT NOT NULL CHECK (target_generation > 1),
    provider_id TEXT NOT NULL REFERENCES public.compute_providers(provider_id),
    pool_id TEXT NOT NULL,
    allocation_id TEXT NOT NULL UNIQUE,
    operation_id TEXT NOT NULL UNIQUE,
    workload_event_id TEXT NOT NULL UNIQUE,
    operation_event_id TEXT NOT NULL UNIQUE,
    promoted_at TIMESTAMPTZ NOT NULL DEFAULT pg_catalog.clock_timestamp(),
    FOREIGN KEY (workload_id, tenant_id) REFERENCES public.compute_workloads(workload_id, tenant_id),
    FOREIGN KEY (allocation_id, workload_id, tenant_id, provider_id, target_generation)
      REFERENCES public.compute_allocations(allocation_id, workload_id, tenant_id, provider_id, desired_generation),
    FOREIGN KEY (operation_id, workload_id, tenant_id, provider_id, target_generation)
      REFERENCES public.compute_operations(compute_operation_id, workload_id, tenant_id, provider_id, desired_generation),
    FOREIGN KEY (workload_event_id) REFERENCES public.compute_outbox_events(event_id),
    FOREIGN KEY (operation_event_id) REFERENCES public.compute_outbox_events(event_id),
    UNIQUE (workload_id, tenant_id, target_generation),
    CHECK (
      allocation_id = 'pd-allocation-' || intent_id AND
      operation_id = 'pd-operation-' || intent_id AND
      workload_event_id = 'pd-workload-' || intent_id AND
      operation_event_id = 'pd-operation-event-' || intent_id
    )
);

CREATE FUNCTION public.compute_pd_promotion_immutable() RETURNS trigger
LANGUAGE plpgsql SET search_path=pg_catalog,pg_temp AS $$
BEGIN
  RAISE EXCEPTION 'PD promotion journal is immutable' USING ERRCODE='23514';
END;
$$;
CREATE TRIGGER compute_pd_promotion_no_change
  BEFORE UPDATE OR DELETE ON public.compute_pd_promotions
  FOR EACH ROW EXECUTE FUNCTION public.compute_pd_promotion_immutable();
CREATE TRIGGER compute_pd_promotion_no_truncate
  BEFORE TRUNCATE ON public.compute_pd_promotions
  FOR EACH STATEMENT EXECUTE FUNCTION public.compute_pd_promotion_immutable();
ALTER TABLE public.compute_pd_promotions ENABLE ALWAYS TRIGGER compute_pd_promotion_no_change;
ALTER TABLE public.compute_pd_promotions ENABLE ALWAYS TRIGGER compute_pd_promotion_no_truncate;
REVOKE ALL ON TABLE public.compute_pd_promotions FROM PUBLIC;
REVOKE ALL ON FUNCTION public.compute_pd_promotion_immutable() FROM PUBLIC;
COMMENT ON TABLE public.compute_pd_promotions IS
  'Immutable, non-final PD promotion evidence. Actual admission, physical readiness, release and finance require independent verification.';
