package postgres

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"

	"github.com/avaargsh/compute-control-panel/internal/operation"
)

// WorkloadDetails is a consistent, tenant-scoped read projection. It includes
// durable Allocation and ComputeOperation history, never mutable provider
// payloads or caller-supplied identifiers.
type WorkloadDetails struct {
	Workload    Workload
	Allocations []Allocation
	Operations  []operation.Operation
}

// GetWorkloadDetails reads the Workload and its immutable Allocation/Operation
// history from one read-only repeatable-read snapshot. Callers must still
// enforce Project grants and service-level read authorization before returning
// the result to an API client.
func (r *Repository) GetWorkloadDetails(ctx context.Context, tenantID, workloadID string) (WorkloadDetails, error) {
	if strings.TrimSpace(tenantID) == "" || strings.TrimSpace(workloadID) == "" {
		return WorkloadDetails{}, ErrInvalidRequest
	}
	tx, err := r.db.BeginTx(ctx, &sql.TxOptions{
		Isolation: sql.LevelRepeatableRead,
		ReadOnly:  true,
	})
	if err != nil {
		return WorkloadDetails{}, fmt.Errorf("begin Workload read snapshot: %w", err)
	}
	defer func() { _ = tx.Rollback() }()

	const selectWorkload = `SELECT workload_id, tenant_id, project_id, request_digest, idempotency_key,
		desired_generation, version, desired_state, observed_phase,
		latest_allocation_id, created_at, updated_at
		FROM compute_workloads
		WHERE tenant_id=$1 AND workload_id=$2`
	workload, err := scanWorkload(tx.QueryRowContext(ctx, selectWorkload, tenantID, workloadID))
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return WorkloadDetails{}, sql.ErrNoRows
		}
		return WorkloadDetails{}, fmt.Errorf("read tenant Workload: %w", err)
	}

	details := WorkloadDetails{
		Workload:    workload,
		Allocations: make([]Allocation, 0),
		Operations:  make([]operation.Operation, 0),
	}
	allocationRows, err := tx.QueryContext(ctx,
		`SELECT `+allocationColumns+` FROM compute_allocations
		WHERE tenant_id=$1 AND workload_id=$2
		ORDER BY desired_generation, created_at, allocation_id`,
		tenantID, workloadID,
	)
	if err != nil {
		return WorkloadDetails{}, fmt.Errorf("query Workload Allocations: %w", err)
	}
	for allocationRows.Next() {
		allocation, scanErr := scanAllocation(allocationRows)
		if scanErr != nil {
			_ = allocationRows.Close()
			return WorkloadDetails{}, fmt.Errorf("scan Workload Allocation: %w", scanErr)
		}
		details.Allocations = append(details.Allocations, allocation)
	}
	if err := allocationRows.Err(); err != nil {
		_ = allocationRows.Close()
		return WorkloadDetails{}, fmt.Errorf("iterate Workload Allocations: %w", err)
	}
	if err := allocationRows.Close(); err != nil {
		return WorkloadDetails{}, fmt.Errorf("close Workload Allocation rows: %w", err)
	}

	operationRows, err := tx.QueryContext(ctx,
		"SELECT "+operationColumns+" FROM compute_operations "+
			"WHERE tenant_id=$1 AND workload_id=$2 "+
			"ORDER BY desired_generation, created_at, compute_operation_id",
		tenantID, workloadID,
	)
	if err != nil {
		return WorkloadDetails{}, fmt.Errorf("query Workload Operations: %w", err)
	}
	for operationRows.Next() {
		item, scanErr := scanOperation(operationRows)
		if scanErr != nil {
			_ = operationRows.Close()
			return WorkloadDetails{}, fmt.Errorf("scan Workload Operation: %w", scanErr)
		}
		details.Operations = append(details.Operations, item)
	}
	if err := operationRows.Err(); err != nil {
		_ = operationRows.Close()
		return WorkloadDetails{}, fmt.Errorf("iterate Workload Operations: %w", err)
	}
	if err := operationRows.Close(); err != nil {
		return WorkloadDetails{}, fmt.Errorf("close Workload Operation rows: %w", err)
	}
	if err := tx.Commit(); err != nil {
		return WorkloadDetails{}, fmt.Errorf("commit Workload read snapshot: %w", err)
	}
	return details, nil
}
