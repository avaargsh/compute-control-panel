-- M5.6 non-actuating PD proposal journal. No new dispatchable Workload or
-- Allocation/Operation authority, and no grants to existing runtime roles.
-- The trusted source/authorization administrator owns fence provisioning.
CREATE TABLE public.compute_pd_intent_fences (
    workload_id TEXT NOT NULL,
    tenant_id TEXT NOT NULL,
    project_id TEXT NOT NULL,
    provider_id TEXT NOT NULL REFERENCES public.compute_providers(provider_id),
    target_generation BIGINT NOT NULL CHECK (target_generation > 1),
    candidate JSONB NOT NULL CHECK (pg_catalog.jsonb_typeof(candidate) = 'object'),
    fence_version BIGINT NOT NULL CHECK (fence_version > 0),
    valid_until TIMESTAMPTZ NOT NULL CHECK (pg_catalog.isfinite(valid_until)),
    revoked BOOLEAN NOT NULL DEFAULT FALSE,
    PRIMARY KEY (workload_id, tenant_id),
    FOREIGN KEY (workload_id, tenant_id) REFERENCES public.compute_workloads(workload_id, tenant_id),
    CHECK ((
        candidate->>'Workload' = workload_id
        AND candidate->'Scope'->>'Tenant' = tenant_id
        AND candidate->'Scope'->>'Project' = project_id
        AND candidate->'Generation' = pg_catalog.to_jsonb(target_generation)
        AND candidate->>'IntentID' ~ '^[a-f0-9]{64}$'
        AND candidate->>'GenerationKey' ~ '^[a-f0-9]{64}$'
    ) IS TRUE)
);

CREATE TABLE public.compute_pd_intents (
    intent_id TEXT PRIMARY KEY CHECK (intent_id ~ '^[a-f0-9]{64}$'),
    generation_key TEXT NOT NULL UNIQUE CHECK (generation_key ~ '^[a-f0-9]{64}$'),
    workload_id TEXT NOT NULL,
    tenant_id TEXT NOT NULL,
    project_id TEXT NOT NULL,
    owner_service TEXT NOT NULL,
    owner_resource_type TEXT NOT NULL,
    owner_resource_id TEXT NOT NULL,
    provider_id TEXT NOT NULL REFERENCES public.compute_providers(provider_id),
    target_generation BIGINT NOT NULL CHECK (target_generation > 1),
    candidate JSONB NOT NULL CHECK (pg_catalog.jsonb_typeof(candidate) = 'object'),
    fence_version BIGINT NOT NULL CHECK (fence_version > 0),
    valid_until TIMESTAMPTZ NOT NULL CHECK (pg_catalog.isfinite(valid_until)),
    recorded_at TIMESTAMPTZ NOT NULL DEFAULT pg_catalog.clock_timestamp(),
    event_id TEXT NOT NULL UNIQUE,
    UNIQUE (workload_id, tenant_id, target_generation),
    FOREIGN KEY (workload_id, tenant_id) REFERENCES public.compute_workloads(workload_id, tenant_id),
    FOREIGN KEY (event_id) REFERENCES public.compute_outbox_events(event_id) DEFERRABLE INITIALLY DEFERRED,
    CHECK (recorded_at < valid_until),
    CHECK (event_id = 'pd-intent-' || intent_id),
    CHECK ((
        candidate->>'Workload' = workload_id
        AND candidate->'Scope'->>'Tenant' = tenant_id
        AND candidate->'Scope'->>'Project' = project_id
        AND candidate->'Generation' = pg_catalog.to_jsonb(target_generation)
        AND candidate->>'IntentID' = intent_id
        AND candidate->>'GenerationKey' = generation_key
    ) IS TRUE)
);

-- Independently pinned candidate includes every source revision and digest.
-- Lock the existing Workload, current fence and Provider; do not infer READY.
-- The deferred gate rechecks authority and real database time after Outbox
-- insertion, including any lock wait. No physical state is advanced here.
CREATE FUNCTION public.compute_pd_intent_guard() RETURNS trigger
LANGUAGE plpgsql
SET search_path = pg_catalog, pg_temp
AS $$
BEGIN
    IF TG_WHEN = 'BEFORE' THEN
        NEW.recorded_at := pg_catalog.clock_timestamp();
    END IF;
    PERFORM 1
      FROM public.compute_workloads w
      JOIN public.compute_pd_intent_fences f ON f.workload_id=w.workload_id AND f.tenant_id=w.tenant_id
      JOIN public.compute_providers p ON p.provider_id=f.provider_id
     WHERE w.workload_id=NEW.workload_id AND w.tenant_id=NEW.tenant_id
       AND w.project_id=NEW.project_id AND w.owner_service=NEW.owner_service
       AND w.owner_resource_type=NEW.owner_resource_type AND w.owner_resource_id=NEW.owner_resource_id
       AND w.desired_state='ACTIVE' AND w.desired_generation=NEW.target_generation-1
       AND f.project_id=NEW.project_id AND f.provider_id=NEW.provider_id
       AND f.target_generation=NEW.target_generation AND f.candidate=NEW.candidate
       AND f.fence_version=NEW.fence_version AND NOT f.revoked
       AND f.valid_until>=NEW.valid_until AND p.state='REGISTERED'
     FOR SHARE OF w,f,p;
    IF NOT FOUND OR NEW.valid_until<=pg_catalog.clock_timestamp() THEN
        RAISE EXCEPTION 'PD intent authority or deadline is not current' USING ERRCODE='23514';
    END IF;
    IF TG_WHEN = 'AFTER' THEN
        PERFORM 1 FROM public.compute_outbox_events e
         WHERE e.event_id=NEW.event_id AND e.producer='compute-control-panel'
           AND e.event_type='compute.pd.intent.recorded.v1' AND e.schema_version=1
           AND e.tenant_id=NEW.tenant_id AND e.aggregate_type='compute_pd_intent'
           AND e.aggregate_id=NEW.intent_id AND e.aggregate_version=1
           AND e.payload=pg_catalog.jsonb_build_object('provider_id',NEW.provider_id,'candidate',NEW.candidate)
         FOR SHARE OF e;
        IF NOT FOUND OR NEW.valid_until<=pg_catalog.clock_timestamp() THEN
            RAISE EXCEPTION 'PD intent lacks exact Outbox event or deadline expired' USING ERRCODE='23514';
        END IF;
    END IF;
    RETURN NEW;
END;
$$;

CREATE TRIGGER compute_pd_intent_current
    BEFORE INSERT ON public.compute_pd_intents
    FOR EACH ROW EXECUTE FUNCTION public.compute_pd_intent_guard();
CREATE CONSTRAINT TRIGGER compute_pd_intent_commit_guard
    AFTER INSERT ON public.compute_pd_intents
    DEFERRABLE INITIALLY DEFERRED
    FOR EACH ROW EXECUTE FUNCTION public.compute_pd_intent_guard();

CREATE FUNCTION public.compute_pd_intent_immutable() RETURNS trigger
LANGUAGE plpgsql
SET search_path = pg_catalog, pg_temp
AS $$
BEGIN
    RAISE EXCEPTION 'PD intent journal is append-only' USING ERRCODE='23514';
END;
$$;
CREATE TRIGGER compute_pd_intent_immutable_row
    BEFORE UPDATE OR DELETE ON public.compute_pd_intents
    FOR EACH ROW EXECUTE FUNCTION public.compute_pd_intent_immutable();
CREATE TRIGGER compute_pd_intent_immutable_truncate
    BEFORE TRUNCATE ON public.compute_pd_intents
    FOR EACH STATEMENT EXECUTE FUNCTION public.compute_pd_intent_immutable();

ALTER TABLE public.compute_pd_intents ENABLE ALWAYS TRIGGER compute_pd_intent_current;
ALTER TABLE public.compute_pd_intents ENABLE ALWAYS TRIGGER compute_pd_intent_commit_guard;
ALTER TABLE public.compute_pd_intents ENABLE ALWAYS TRIGGER compute_pd_intent_immutable_row;
ALTER TABLE public.compute_pd_intents ENABLE ALWAYS TRIGGER compute_pd_intent_immutable_truncate;

REVOKE ALL ON TABLE public.compute_pd_intent_fences,public.compute_pd_intents FROM PUBLIC;
REVOKE ALL ON FUNCTION public.compute_pd_intent_guard(),public.compute_pd_intent_immutable() FROM PUBLIC;

COMMENT ON TABLE public.compute_pd_intent_fences IS
    'Independently administered latest exact authorized proposal/revisions; no runtime writer is provisioned.';
COMMENT ON TABLE public.compute_pd_intents IS
    'Non-actuating immutable proposal journal; does not advance Workload desired_generation or authorize provider dispatch.';
