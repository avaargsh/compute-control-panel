-- Enforce that an Operation's Allocation belongs to the same Workload,
-- tenant, Provider and desired generation. NULL allocation_id remains valid
-- for historical low-level operation fixtures during the staged migration.
ALTER TABLE compute_operations
    ADD CONSTRAINT compute_operations_allocation_identity_fk
    FOREIGN KEY (allocation_id, workload_id, tenant_id, provider_id, desired_generation)
    REFERENCES compute_allocations (
        allocation_id, workload_id, tenant_id, provider_id, desired_generation
    );
