-- Compute Control Plane initial durable domain schema.
-- PostgreSQL 16+. Apply with the dedicated migration role.
-- Application writes must use tenant-scoped transactions and version/fence CAS.

CREATE TABLE compute_workloads (
    workload_id TEXT PRIMARY KEY,
    tenant_id TEXT NOT NULL,
    project_id TEXT NOT NULL,
    owner_service TEXT NOT NULL,
    owner_resource_type TEXT NOT NULL,
    owner_resource_id TEXT NOT NULL,
    compute_profile_id TEXT,
    compute_profile_revision TEXT,
    normalized_request JSONB NOT NULL CHECK (jsonb_typeof(normalized_request) = 'object'),
    request_digest TEXT NOT NULL CHECK (request_digest ~ '^sha256:[a-f0-9]{64}$'),
    idempotency_key TEXT NOT NULL,
    desired_generation BIGINT NOT NULL DEFAULT 1 CHECK (desired_generation > 0),
    version BIGINT NOT NULL DEFAULT 1 CHECK (version > 0),
    desired_state TEXT NOT NULL CHECK (desired_state IN ('ACTIVE', 'DELETE_REQUESTED')),
    observed_generation BIGINT NOT NULL DEFAULT 0 CHECK (observed_generation >= 0),
    observed_phase TEXT NOT NULL CHECK (observed_phase IN (
        'REQUESTED', 'ACCEPTED', 'PROVISIONING', 'ALLOCATED', 'RELEASING',
        'RELEASED', 'REJECTED', 'FAILED_CONFIRMED'
    )),
    latest_allocation_id TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (tenant_id, idempotency_key),
    CHECK ((compute_profile_id IS NULL) = (compute_profile_revision IS NULL))
);

CREATE INDEX compute_workloads_tenant_project_created
    ON compute_workloads (tenant_id, project_id, created_at DESC, workload_id);
CREATE INDEX compute_workloads_reconcile
    ON compute_workloads (observed_phase, updated_at, workload_id)
    WHERE observed_phase IN ('ACCEPTED', 'PROVISIONING', 'RELEASING');

CREATE TABLE compute_allocations (
    allocation_id TEXT PRIMARY KEY,
    tenant_id TEXT NOT NULL,
    workload_id TEXT NOT NULL REFERENCES compute_workloads(workload_id),
    provider_id TEXT NOT NULL,
    pool_id TEXT NOT NULL,
    desired_generation BIGINT NOT NULL CHECK (desired_generation > 0),
    phase TEXT NOT NULL CHECK (phase IN (
        'PENDING', 'BOUND', 'RELEASING', 'RELEASED', 'REJECTED', 'FAILED_CONFIRMED'
    )),
    provider_resource_refs JSONB NOT NULL DEFAULT '[]'::jsonb
        CHECK (jsonb_typeof(provider_resource_refs) = 'array'),
    provider_resource_digest TEXT CHECK (
        provider_resource_digest IS NULL OR provider_resource_digest ~ '^sha256:[a-f0-9]{64}$'
    ),
    observed_at TIMESTAMPTZ,
    version BIGINT NOT NULL DEFAULT 1 CHECK (version > 0),
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (workload_id, desired_generation),
    UNIQUE (allocation_id, tenant_id)
);

CREATE UNIQUE INDEX compute_allocations_one_active_per_workload
    ON compute_allocations (workload_id)
    WHERE phase IN ('PENDING', 'BOUND', 'RELEASING');
CREATE INDEX compute_allocations_tenant_workload
    ON compute_allocations (tenant_id, workload_id, created_at DESC);

CREATE TABLE compute_operations (
    compute_operation_id TEXT PRIMARY KEY,
    tenant_id TEXT NOT NULL,
    workload_id TEXT NOT NULL REFERENCES compute_workloads(workload_id),
    allocation_id TEXT,
    provider_id TEXT NOT NULL,
    operation_kind TEXT NOT NULL CHECK (operation_kind IN ('CREATE', 'UPDATE', 'DELETE')),
    desired_generation BIGINT NOT NULL CHECK (desired_generation > 0),
    provider_request_digest TEXT NOT NULL CHECK (provider_request_digest ~ '^sha256:[a-f0-9]{64}$'),
    idempotency_key TEXT NOT NULL,
    version BIGINT NOT NULL DEFAULT 1 CHECK (version > 0),
    fence_token BIGINT NOT NULL DEFAULT 0 CHECK (fence_token >= 0),
    status TEXT NOT NULL CHECK (status IN (
        'PENDING', 'LEASED', 'IN_FLIGHT', 'UNKNOWN', 'RECONCILING',
        'SUCCEEDED', 'FAILED', 'MANUAL_REVIEW', 'SUPERSEDED'
    )),
    lease_owner TEXT,
    lease_expires_at TIMESTAMPTZ,
    target_resource_id TEXT,
    target_resource_uid TEXT,
    provider_resource_id TEXT,
    provider_resource_uid TEXT,
    error_code TEXT,
    manual_review_code TEXT,
    cleanup_required BOOLEAN NOT NULL DEFAULT FALSE,
    reconcile_required BOOLEAN NOT NULL DEFAULT FALSE,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (tenant_id, idempotency_key),
    UNIQUE (workload_id, desired_generation, operation_kind, provider_request_digest),
    CHECK ((lease_owner IS NULL) = (lease_expires_at IS NULL)),
    CHECK (
        (status IN ('LEASED', 'IN_FLIGHT', 'RECONCILING')
          AND lease_owner IS NOT NULL AND lease_expires_at IS NOT NULL AND fence_token > 0)
        OR
        (status NOT IN ('LEASED', 'IN_FLIGHT', 'RECONCILING')
          AND lease_owner IS NULL AND lease_expires_at IS NULL)
    ),
    CHECK (
        (status IN ('PENDING', 'LEASED', 'IN_FLIGHT', 'UNKNOWN', 'RECONCILING')
          AND provider_resource_id IS NULL AND provider_resource_uid IS NULL
          AND error_code IS NULL AND manual_review_code IS NULL
          AND cleanup_required = FALSE AND reconcile_required = FALSE)
        OR
        (status = 'SUCCEEDED' AND error_code IS NULL AND manual_review_code IS NULL
          AND cleanup_required = FALSE AND reconcile_required = FALSE
          AND ((operation_kind = 'DELETE' AND provider_resource_id IS NULL AND provider_resource_uid IS NULL)
            OR (operation_kind IN ('CREATE', 'UPDATE')
              AND provider_resource_id IS NOT NULL AND provider_resource_uid IS NOT NULL)))
        OR
        (status = 'FAILED' AND error_code IS NOT NULL AND manual_review_code IS NULL
          AND provider_resource_id IS NULL AND provider_resource_uid IS NULL
          AND cleanup_required = FALSE AND reconcile_required = FALSE)
        OR
        (status = 'MANUAL_REVIEW' AND manual_review_code IS NOT NULL AND error_code IS NULL
          AND provider_resource_id IS NULL AND provider_resource_uid IS NULL
          AND cleanup_required = FALSE AND reconcile_required = FALSE)
        OR
        (status = 'SUPERSEDED' AND reconcile_required = TRUE
          AND error_code IS NULL AND manual_review_code IS NULL
          AND (NOT cleanup_required OR (provider_resource_id IS NOT NULL AND provider_resource_uid IS NOT NULL)))
    ),
    CHECK (
        (operation_kind IN ('UPDATE', 'DELETE') AND target_resource_id IS NOT NULL AND target_resource_uid IS NOT NULL)
        OR
        (operation_kind = 'CREATE' AND target_resource_id IS NULL AND target_resource_uid IS NULL)
    )
);

CREATE INDEX compute_operations_reconcile
    ON compute_operations (status, updated_at, compute_operation_id)
    WHERE status IN ('PENDING', 'UNKNOWN', 'RECONCILING');
CREATE INDEX compute_operations_expired_lease
    ON compute_operations (lease_expires_at, compute_operation_id)
    WHERE status IN ('LEASED', 'IN_FLIGHT', 'RECONCILING');

CREATE TABLE compute_resource_observations (
    observation_id TEXT PRIMARY KEY,
    tenant_id TEXT NOT NULL,
    workload_id TEXT NOT NULL REFERENCES compute_workloads(workload_id),
    allocation_id TEXT,
    compute_operation_id TEXT REFERENCES compute_operations(compute_operation_id),
    provider_id TEXT NOT NULL,
    desired_generation BIGINT NOT NULL CHECK (desired_generation > 0),
    resource_kind TEXT NOT NULL,
    provider_resource_id TEXT,
    provider_resource_uid TEXT,
    provider_resource_version TEXT,
    result TEXT NOT NULL CHECK (result IN (
        'NOT_FOUND', 'PENDING', 'ADMITTED', 'SCHEDULED', 'RUNNING',
        'READY', 'DEGRADED', 'DELETING', 'TERMINAL', 'UNKNOWN'
    )),
    source TEXT NOT NULL,
    adapter_version TEXT NOT NULL,
    observed_at TIMESTAMPTZ NOT NULL,
    source_timestamp TIMESTAMPTZ,
    response_digest TEXT CHECK (response_digest IS NULL OR response_digest ~ '^sha256:[a-f0-9]{64}$'),
    evidence_ref TEXT,
    freshness_expires_at TIMESTAMPTZ,
    details JSONB NOT NULL DEFAULT '{}'::jsonb CHECK (jsonb_typeof(details) = 'object')
);
CREATE INDEX compute_resource_observations_latest
    ON compute_resource_observations (tenant_id, workload_id, provider_id, observed_at DESC);

CREATE TABLE compute_outbox_events (
    event_id TEXT PRIMARY KEY,
    producer TEXT NOT NULL,
    event_type TEXT NOT NULL,
    schema_version INTEGER NOT NULL CHECK (schema_version > 0),
    tenant_id TEXT NOT NULL,
    aggregate_type TEXT NOT NULL,
    aggregate_id TEXT NOT NULL,
    aggregate_version BIGINT NOT NULL CHECK (aggregate_version > 0),
    occurred_at TIMESTAMPTZ NOT NULL,
    correlation_id TEXT NOT NULL,
    causation_id TEXT,
    traceparent TEXT,
    payload JSONB NOT NULL CHECK (jsonb_typeof(payload) = 'object'),
    delivery_state TEXT NOT NULL DEFAULT 'PENDING'
        CHECK (delivery_state IN ('PENDING', 'PUBLISHING', 'PUBLISHED', 'QUARANTINED')),
    attempts INTEGER NOT NULL DEFAULT 0 CHECK (attempts >= 0),
    next_attempt_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    lease_owner TEXT,
    lease_expires_at TIMESTAMPTZ,
    published_at TIMESTAMPTZ,
    last_error_code TEXT,
    UNIQUE (producer, aggregate_type, aggregate_id, aggregate_version),
    CHECK ((lease_owner IS NULL) = (lease_expires_at IS NULL)),
    CHECK ((delivery_state = 'PUBLISHING') = (lease_owner IS NOT NULL)),
    CHECK ((delivery_state = 'PUBLISHED') = (published_at IS NOT NULL))
);
CREATE INDEX compute_outbox_pending
    ON compute_outbox_events (next_attempt_at, occurred_at, event_id)
    WHERE delivery_state IN ('PENDING', 'PUBLISHING');

CREATE TABLE compute_inbox_events (
    consumer_name TEXT NOT NULL,
    event_id TEXT NOT NULL,
    producer TEXT NOT NULL,
    event_type TEXT NOT NULL,
    schema_version INTEGER NOT NULL CHECK (schema_version > 0),
    tenant_id TEXT NOT NULL,
    aggregate_type TEXT NOT NULL,
    aggregate_id TEXT NOT NULL,
    aggregate_version BIGINT NOT NULL CHECK (aggregate_version > 0),
    payload JSONB NOT NULL CHECK (jsonb_typeof(payload) = 'object'),
    state TEXT NOT NULL DEFAULT 'RECEIVED'
        CHECK (state IN ('RECEIVED', 'APPLIED', 'QUARANTINED')),
    received_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    applied_at TIMESTAMPTZ,
    error_code TEXT,
    PRIMARY KEY (consumer_name, event_id),
    UNIQUE (consumer_name, aggregate_type, aggregate_id, aggregate_version),
    CHECK ((state = 'APPLIED') = (applied_at IS NOT NULL)),
    CHECK ((state = 'QUARANTINED') = (error_code IS NOT NULL))
);

CREATE TABLE compute_inbox_cursors (
    consumer_name TEXT NOT NULL,
    aggregate_type TEXT NOT NULL,
    aggregate_id TEXT NOT NULL,
    last_applied_version BIGINT NOT NULL DEFAULT 0 CHECK (last_applied_version >= 0),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (consumer_name, aggregate_type, aggregate_id)
);
