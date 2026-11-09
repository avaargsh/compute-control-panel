package postgres

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"

	"github.com/avaargsh/compute-control-panel/internal/identity"
)

var ErrWorkloadIdentityNotFound = errors.New("trusted workload identity is not active")

// ResolveWorkloadPrincipal maps a verified SPIFFE URI SAN to its authoritative
// Tenant, Service and active Project grants. The query is read-only and uses a
// repeatable-read snapshot so revocation/grant updates cannot produce a mixed
// principal. Certificate path validation is the caller’s TLS responsibility.
func (r *Repository) ResolveWorkloadPrincipal(ctx context.Context, spiffeID string) (identity.WorkloadPrincipal, error) {
	if strings.TrimSpace(spiffeID) == "" || spiffeID != strings.TrimSpace(spiffeID) ||
		!strings.HasPrefix(spiffeID, "spiffe://") {
		return identity.WorkloadPrincipal{}, ErrWorkloadIdentityNotFound
	}
	tx, err := r.db.BeginTx(ctx, &sql.TxOptions{
		Isolation: sql.LevelRepeatableRead,
		ReadOnly:  true,
	})
	if err != nil {
		return identity.WorkloadPrincipal{}, fmt.Errorf("begin workload identity lookup: %w", err)
	}
	defer func() { _ = tx.Rollback() }()

	var principal identity.WorkloadPrincipal
	principal.Subject = spiffeID
	err = tx.QueryRowContext(ctx, `SELECT tenant_id, service_name
		FROM public.compute_service_identities
		WHERE spiffe_id=$1
		  AND status='ACTIVE'
		  AND not_before <= transaction_timestamp()
		  AND (expires_at IS NULL OR expires_at > transaction_timestamp())`,
		spiffeID,
	).Scan(&principal.TenantID, &principal.Service)
	if errors.Is(err, sql.ErrNoRows) {
		return identity.WorkloadPrincipal{}, ErrWorkloadIdentityNotFound
	}
	if err != nil {
		return identity.WorkloadPrincipal{}, fmt.Errorf("read workload identity: %w", err)
	}

	principal.ProjectIDs = make([]string, 0)
	rows, err := tx.QueryContext(ctx, `SELECT project_id
		FROM public.compute_service_project_grants
		WHERE spiffe_id=$1
		  AND tenant_id=$2
		  AND status='ACTIVE'
		  AND not_before <= transaction_timestamp()
		  AND (expires_at IS NULL OR expires_at > transaction_timestamp())
		ORDER BY project_id`,
		spiffeID, principal.TenantID,
	)
	if err != nil {
		return identity.WorkloadPrincipal{}, fmt.Errorf("query workload Project grants: %w", err)
	}
	for rows.Next() {
		var projectID string
		if err := rows.Scan(&projectID); err != nil {
			_ = rows.Close()
			return identity.WorkloadPrincipal{}, fmt.Errorf("scan workload Project grant: %w", err)
		}
		principal.ProjectIDs = append(principal.ProjectIDs, projectID)
	}
	if err := rows.Err(); err != nil {
		_ = rows.Close()
		return identity.WorkloadPrincipal{}, fmt.Errorf("iterate workload Project grants: %w", err)
	}
	if err := rows.Close(); err != nil {
		return identity.WorkloadPrincipal{}, fmt.Errorf("close workload Project grants: %w", err)
	}
	if err := tx.Commit(); err != nil {
		return identity.WorkloadPrincipal{}, fmt.Errorf("commit workload identity lookup: %w", err)
	}
	return principal, nil
}

var _ interface {
	ResolveWorkloadPrincipal(context.Context, string) (identity.WorkloadPrincipal, error)
} = (*Repository)(nil)
