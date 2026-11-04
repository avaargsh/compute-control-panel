-- Version 0005: persist only complete, costable Allocation usage evidence.
-- One immutable FINAL evidence set is allowed per Allocation generation in v1.
-- Raw provider response bodies and credentials must never be placed in these
-- payloads; evidence payloads contain normalized identity/provenance references.

CREATE TABLE compute_allocation_usage_evidence_sets (
    evidence_set_id TEXT NOT NULL CHECK (btrim(evidence_set_id) <> ''),
    schema_version TEXT NOT NULL
        CHECK (schema_version = 'compute.economics.allocation-usage-evidence.v1'),
    tenant_id TEXT NOT NULL,
    project_id TEXT NOT NULL,
    workload_id TEXT NOT NULL,
    allocation_id TEXT NOT NULL,
    allocation_generation BIGINT NOT NULL CHECK (allocation_generation > 0),
    provider_id TEXT NOT NULL,
    compute_operation_id TEXT NOT NULL,
    provider_resource_id TEXT NOT NULL CHECK (btrim(provider_resource_id) <> ''),
    provider_resource_uid TEXT NOT NULL CHECK (btrim(provider_resource_uid) <> ''),
    pool_id TEXT NOT NULL,
    profile_id TEXT NOT NULL,
    gpu_model TEXT NOT NULL,
    capability_profile_id TEXT NOT NULL,
    evidence_policy_version TEXT NOT NULL,
    metering_policy_version TEXT NOT NULL,
    evidence_digest TEXT NOT NULL
        CHECK (evidence_digest ~ '^sha256:[a-f0-9]{64}$'),
    usage_interval_id TEXT NOT NULL,
    usage_start TIMESTAMPTZ NOT NULL,
    usage_end TIMESTAMPTZ NOT NULL,
    gpu_count BIGINT NOT NULL CHECK (gpu_count > 0),
    usage_unit TEXT NOT NULL CHECK (usage_unit = 'GPU_DEVICE_COUNT'),
    attribution_method TEXT NOT NULL CHECK (attribution_method = 'DEVICE_BINDING'),
    quality TEXT NOT NULL CHECK (quality = 'DERIVED'),
    source_coverage NUMERIC NOT NULL CHECK (source_coverage = 1),
    source_event_ids JSONB NOT NULL
        CHECK (jsonb_typeof(source_event_ids) = 'array' AND jsonb_array_length(source_event_ids) > 0),
    evidence_payload JSONB NOT NULL CHECK (jsonb_typeof(evidence_payload) = 'object'),
    usage_interval_payload JSONB NOT NULL CHECK (jsonb_typeof(usage_interval_payload) = 'object'),
    recorded_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),

    PRIMARY KEY (tenant_id, evidence_set_id),
    UNIQUE (tenant_id, allocation_id, allocation_generation),
    UNIQUE (tenant_id, usage_interval_id),

    FOREIGN KEY (workload_id, tenant_id)
        REFERENCES compute_workloads (workload_id, tenant_id),
    FOREIGN KEY (allocation_id, workload_id, tenant_id, provider_id, allocation_generation)
        REFERENCES compute_allocations (
            allocation_id, workload_id, tenant_id, provider_id, desired_generation
        ),
    FOREIGN KEY (compute_operation_id, workload_id, tenant_id, provider_id, allocation_generation)
        REFERENCES compute_operations (
            compute_operation_id, workload_id, tenant_id, provider_id, desired_generation
        ),

    CHECK (usage_end > usage_start),
    CHECK (usage_interval_id = 'usage_' || substring(evidence_digest FROM 8))
);

CREATE INDEX compute_usage_evidence_workload_time
    ON compute_allocation_usage_evidence_sets (tenant_id, workload_id, usage_start DESC);
CREATE INDEX compute_usage_evidence_provider_time
    ON compute_allocation_usage_evidence_sets (tenant_id, provider_id, profile_id, usage_start DESC);

-- A normalized payload is still checked against authoritative database rows.
-- This prevents callers from associating a valid evidence digest with a
-- different Project or a Provider resource that the accepted Operation never
-- created. The application performs the same check for a useful error.
CREATE OR REPLACE FUNCTION compute_validate_usage_evidence_set()
RETURNS trigger
LANGUAGE plpgsql
AS $$
BEGIN
    IF NOT EXISTS (
        SELECT 1
        FROM compute_operations o
        JOIN compute_workloads w
          ON w.workload_id = o.workload_id AND w.tenant_id = o.tenant_id
        JOIN compute_allocations a
          ON a.allocation_id = o.allocation_id
         AND a.workload_id = o.workload_id
         AND a.tenant_id = o.tenant_id
         AND a.provider_id = o.provider_id
         AND a.desired_generation = o.desired_generation
        WHERE o.compute_operation_id = NEW.compute_operation_id
          AND o.tenant_id = NEW.tenant_id
          AND o.workload_id = NEW.workload_id
          AND o.allocation_id = NEW.allocation_id
          AND o.provider_id = NEW.provider_id
          AND o.desired_generation = NEW.allocation_generation
          AND o.operation_kind IN ('CREATE', 'UPDATE')
          AND o.status = 'SUCCEEDED'
          AND a.phase IN ('BOUND', 'RELEASING', 'RELEASED')
          AND o.provider_resource_id = NEW.provider_resource_id
          AND o.provider_resource_uid = NEW.provider_resource_uid
          AND w.project_id = NEW.project_id
    ) THEN
        RAISE EXCEPTION 'usage evidence does not match a successful Provider operation and Workload scope'
            USING ERRCODE = '23514';
    END IF;
    RETURN NEW;
END;
$$;

CREATE TRIGGER compute_usage_evidence_scope_guard
BEFORE INSERT ON compute_allocation_usage_evidence_sets
FOR EACH ROW EXECUTE FUNCTION compute_validate_usage_evidence_set();

CREATE OR REPLACE FUNCTION compute_reject_usage_evidence_mutation()
RETURNS trigger
LANGUAGE plpgsql
AS $$
BEGIN
    RAISE EXCEPTION 'compute_allocation_usage_evidence_sets are append-only'
        USING ERRCODE = '55000';
END;
$$;

CREATE TRIGGER compute_usage_evidence_append_only
BEFORE UPDATE OR DELETE ON compute_allocation_usage_evidence_sets
FOR EACH ROW EXECUTE FUNCTION compute_reject_usage_evidence_mutation();

REVOKE UPDATE, DELETE, TRUNCATE ON compute_allocation_usage_evidence_sets FROM PUBLIC;
