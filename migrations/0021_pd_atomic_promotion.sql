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


-- The application checks deadlines before subsequently locking old Allocation
-- rows. A row lock can delay that transaction past source expiry. This
-- DEFERRABLE trigger rechecks the exact independently administered fence at
-- COMMIT with the database clock. A prior successful SELECT is not sufficient.
CREATE FUNCTION public.compute_pd_promotion_commit_guard() RETURNS trigger
LANGUAGE plpgsql
SET search_path = pg_catalog, pg_temp
AS $guard$
DECLARE
    checked_at TIMESTAMPTZ := pg_catalog.clock_timestamp();
BEGIN
    PERFORM 1
      FROM public.compute_pd_intents i
      JOIN public.compute_pd_intent_fences f
        ON f.workload_id=i.workload_id AND f.tenant_id=i.tenant_id
       AND f.project_id=i.project_id AND f.provider_id=i.provider_id
       AND f.target_generation=i.target_generation AND f.candidate=i.candidate
       AND f.fence_version=i.fence_version AND NOT f.revoked
      JOIN public.compute_providers p ON p.provider_id=i.provider_id
      JOIN public.compute_workloads w
        ON w.workload_id=i.workload_id AND w.tenant_id=i.tenant_id
       AND w.project_id=i.project_id AND w.owner_service=i.owner_service
       AND w.owner_resource_type=i.owner_resource_type AND w.owner_resource_id=i.owner_resource_id
      JOIN public.compute_allocations a
        ON a.allocation_id=NEW.allocation_id AND a.workload_id=i.workload_id
       AND a.tenant_id=i.tenant_id AND a.provider_id=i.provider_id
       AND a.desired_generation=i.target_generation
      JOIN public.compute_operations op
        ON op.compute_operation_id=NEW.operation_id AND op.workload_id=i.workload_id
       AND op.tenant_id=i.tenant_id AND op.provider_id=i.provider_id
       AND op.allocation_id=a.allocation_id AND op.desired_generation=i.target_generation
       AND op.operation_kind='CREATE'
      JOIN public.compute_outbox_events wo
        ON wo.event_id=NEW.workload_event_id AND wo.tenant_id=i.tenant_id
       AND wo.producer='compute-control-panel'
       AND wo.event_type='compute.pd.workload.generation-accepted.v1'
       AND wo.aggregate_type='compute_workload' AND wo.aggregate_id=i.workload_id
      JOIN public.compute_outbox_events oe
        ON oe.event_id=NEW.operation_event_id AND oe.tenant_id=i.tenant_id
       AND oe.producer='compute-control-panel'
       AND oe.event_type='compute.pd.operation.staged.v1'
       AND oe.aggregate_type='compute_operation' AND oe.aggregate_id=NEW.operation_id
     WHERE i.intent_id=NEW.intent_id AND i.workload_id=NEW.workload_id
       AND i.tenant_id=NEW.tenant_id AND i.project_id=NEW.project_id
       AND i.provider_id=NEW.provider_id AND i.target_generation=NEW.target_generation
       AND p.state='REGISTERED'
       AND w.desired_state='ACTIVE' AND w.desired_generation=NEW.target_generation
       AND w.latest_allocation_id=NEW.allocation_id
       AND a.phase='PENDING' AND op.status='PENDING'
       AND checked_at<i.valid_until AND checked_at<f.valid_until
     FOR SHARE OF i,f,p,w,a,op,wo,oe;
    IF NOT FOUND THEN
        RAISE EXCEPTION 'PD promotion authority or deadline expired at commit'
            USING ERRCODE='PD001';
    END IF;
    RETURN NEW;
END;
$guard$;

CREATE CONSTRAINT TRIGGER compute_pd_promotion_current_at_commit
    AFTER INSERT ON public.compute_pd_promotions
    DEFERRABLE INITIALLY DEFERRED
    FOR EACH ROW EXECUTE FUNCTION public.compute_pd_promotion_commit_guard();
ALTER TABLE public.compute_pd_promotions
    ENABLE ALWAYS TRIGGER compute_pd_promotion_current_at_commit;
REVOKE ALL ON FUNCTION public.compute_pd_promotion_commit_guard() FROM PUBLIC;
