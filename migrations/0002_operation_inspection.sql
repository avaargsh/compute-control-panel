-- Version 0002: preserve semantic distinction between provider operation
-- inspection outcomes and Kubernetes resource phase observations.
-- Initial 0001 result enum is a RESOURCE phase, NOT proof that an operation
-- succeeded, failed, or was not applied.
ALTER TABLE compute_resource_observations
    ADD COLUMN operation_result TEXT,
    ADD CONSTRAINT compute_resource_observations_operation_result_check CHECK (
        operation_result IS NULL OR (
            compute_operation_id IS NOT NULL
            AND resource_kind = 'provider.operation'
            AND result = 'UNKNOWN'
            AND operation_result IN (
                'RESOURCE_PRESENT', 'TARGET_ABSENT', 'OPERATION_NOT_APPLIED_FINAL',
                'OPERATION_IN_PROGRESS', 'OPERATION_FAILED_FINAL', 'CONFLICT'
            )
        )
    );

-- Inspections should be append-only evidence. No application worker should
-- mutate or delete old observations to rewrite the reconciliation history.
CREATE OR REPLACE FUNCTION compute_reject_observation_mutation()
RETURNS TRIGGER LANGUAGE plpgsql AS $$
BEGIN
    RAISE EXCEPTION 'compute_resource_observations are append-only'
        USING ERRCODE = '55000';
END;
$$;

CREATE TRIGGER compute_observations_append_only
BEFORE UPDATE OR DELETE ON compute_resource_observations
FOR EACH ROW EXECUTE FUNCTION compute_reject_observation_mutation();

CREATE INDEX compute_resource_observations_operation_history
    ON compute_resource_observations (tenant_id, compute_operation_id, observed_at DESC)
    WHERE operation_result IS NOT NULL;
