-- PostgreSQL 16 constraint smoke tests for migration 0001.
-- Run after migrations/0001_compute_core.sql in an empty test database.

INSERT INTO compute_workloads (
    workload_id, tenant_id, project_id, owner_service, owner_resource_type,
    owner_resource_id, normalized_request, request_digest, idempotency_key,
    desired_state, observed_phase
) VALUES (
    'workload-ci-1', 'tenant-ci-1', 'project-ci-1', 'agent-runtime', 'run',
    'run-ci-1', '{}'::jsonb, 'sha256:' || repeat('a', 64), 'idem-ci-1',
    'ACTIVE', 'ACCEPTED'
);

INSERT INTO compute_allocations (
    allocation_id, tenant_id, workload_id, provider_id, pool_id,
    desired_generation, phase
) VALUES (
    'allocation-ci-1', 'tenant-ci-1', 'workload-ci-1', 'provider-ci-1', 'pool-ci-1',
    1, 'PENDING'
);

UPDATE compute_workloads
SET latest_allocation_id = 'allocation-ci-1'
WHERE workload_id = 'workload-ci-1' AND tenant_id = 'tenant-ci-1';

INSERT INTO compute_workloads (
    workload_id, tenant_id, project_id, owner_service, owner_resource_type,
    owner_resource_id, normalized_request, request_digest, idempotency_key,
    desired_state, observed_phase
) VALUES (
    'workload-ci-2', 'tenant-ci-1', 'project-ci-1', 'agent-runtime', 'run',
    'run-ci-2', '{}'::jsonb, 'sha256:' || repeat('e', 64), 'idem-ci-2',
    'ACTIVE', 'ACCEPTED'
);
INSERT INTO compute_allocations (
    allocation_id, tenant_id, workload_id, provider_id, pool_id, desired_generation, phase
) VALUES (
    'allocation-ci-2', 'tenant-ci-1', 'workload-ci-2', 'provider-ci-1', 'pool-ci-1', 1, 'PENDING'
);

DO $cross_workload$
BEGIN
    BEGIN
        UPDATE compute_workloads
        SET latest_allocation_id = 'allocation-ci-2'
        WHERE workload_id = 'workload-ci-1' AND tenant_id = 'tenant-ci-1';
        RAISE EXCEPTION 'workload accepted another workload allocation reference';
    EXCEPTION
        WHEN foreign_key_violation THEN NULL;
    END;
END $cross_workload$;

INSERT INTO compute_operations (
    compute_operation_id, tenant_id, workload_id, allocation_id, provider_id,
    operation_kind, desired_generation, provider_request_digest, idempotency_key, status
) VALUES (
    'operation-ci-1', 'tenant-ci-1', 'workload-ci-1', 'allocation-ci-1', 'provider-ci-1',
    'CREATE', 1, 'sha256:' || repeat('b', 64), 'op-idem-ci-1', 'PENDING'
);

INSERT INTO compute_resource_observations (
    observation_id, tenant_id, workload_id, allocation_id, compute_operation_id,
    provider_id, desired_generation, resource_kind, result, source, adapter_version, observed_at
) VALUES (
    'observation-ci-1', 'tenant-ci-1', 'workload-ci-1', 'allocation-ci-1', 'operation-ci-1',
    'provider-ci-1', 1, 'kubernetes.job', 'UNKNOWN', 'fake-provider', 'test-v1', now()
);

INSERT INTO compute_outbox_events (
    event_id, producer, event_type, schema_version, tenant_id, aggregate_type,
    aggregate_id, aggregate_version, occurred_at, correlation_id, payload
) VALUES (
    'event-ci-1', 'compute-control-panel', 'compute.workload.accepted.v1', 1,
    'tenant-ci-1', 'compute_workload', 'workload-ci-1', 1, now(), 'corr-ci-1', '{}'::jsonb
);

INSERT INTO compute_inbox_events (
    consumer_name, event_id, producer, event_type, schema_version, tenant_id,
    aggregate_type, aggregate_id, aggregate_version, payload
) VALUES (
    'consumer-ci-1', 'event-ci-1', 'compute-control-panel', 'compute.workload.accepted.v1', 1,
    'tenant-ci-1', 'compute_workload', 'workload-ci-1', 1, '{}'::jsonb
);

INSERT INTO compute_inbox_cursors (consumer_name, aggregate_type, aggregate_id, last_applied_version)
VALUES ('consumer-ci-1', 'compute_workload', 'workload-ci-1', 0);

-- Cross-tenant workload/allocation references must be rejected.
DO $$
BEGIN
    BEGIN
        INSERT INTO compute_allocations (
            allocation_id, tenant_id, workload_id, provider_id, pool_id, desired_generation, phase
        ) VALUES (
            'allocation-cross-tenant-ci', 'tenant-ci-other', 'workload-ci-1',
            'provider-ci-other', 'pool-ci-other', 2, 'PENDING'
        );
        RAISE EXCEPTION 'cross-tenant allocation reference unexpectedly accepted';
    EXCEPTION
        WHEN foreign_key_violation THEN NULL;
    END;
END $$;

-- A PENDING operation cannot carry a live lease; the status/lease check fences this shape.
DO $$
BEGIN
    BEGIN
        INSERT INTO compute_operations (
            compute_operation_id, tenant_id, workload_id, provider_id, operation_kind,
            desired_generation, provider_request_digest, idempotency_key, status,
            lease_owner, lease_expires_at
        ) VALUES (
            'operation-invalid-lease-ci', 'tenant-ci-1', 'workload-ci-1', 'provider-ci-1',
            'CREATE', 1, 'sha256:' || repeat('c', 64), 'op-invalid-ci', 'PENDING',
            'worker-ci-1', now() + interval '1 minute'
        );
        RAISE EXCEPTION 'invalid pending lease unexpectedly accepted';
    EXCEPTION
        WHEN check_violation THEN NULL;
    END;
END $$;

-- UPDATE success must retain exact target resource ID + UID identity.
DO $$
BEGIN
    BEGIN
        INSERT INTO compute_operations (
            compute_operation_id, tenant_id, workload_id, provider_id, operation_kind,
            desired_generation, provider_request_digest, idempotency_key, status,
            target_resource_id, target_resource_uid, provider_resource_id, provider_resource_uid
        ) VALUES (
            'operation-invalid-update-ci', 'tenant-ci-1', 'workload-ci-1', 'provider-ci-1',
            'UPDATE', 1, 'sha256:' || repeat('d', 64), 'op-invalid-update-ci', 'SUCCEEDED',
            'job-target-ci', 'uid-target-ci', 'job-foreign-ci', 'uid-foreign-ci'
        );
        RAISE EXCEPTION 'UPDATE success with mismatched target unexpectedly accepted';
    EXCEPTION
        WHEN check_violation THEN NULL;
    END;
END $$;
