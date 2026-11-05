package postgres

import (
	"context"
	"database/sql"
	"fmt"
)

// evidenceWriterQueryRow allows checking the exact session used by the E1
// evidence write transaction. A separate pool-level check is not sufficient
// because a pooled connection can have a different current_user.
type evidenceWriterQueryRow interface {
	QueryRowContext(context.Context, string, ...any) *sql.Row
}

// verifyEvidenceWriterPrivileges must execute on the transaction connection
// used for E1 FINAL. The writer needs SELECT on policy and (in PG16) the
// UPDATE privilege on at least one column to SELECT ... FOR SHARE. Grant
// UPDATE(updated_at) ONLY. The database revision trigger rejects a write
// to updated_at alone: it also requires revision+1, which this role cannot
// perform. Never grant UPDATE(revision) or table-wide UPDATE to the writer.
//
// Privilege grants can change at runtime; only a separate admin role may
// provision or revoke policy, and production must restrict role membership,
// connection pooling and DDL access independently.
func verifyEvidenceWriterPrivileges(ctx context.Context, q evidenceWriterQueryRow) error {
	if q == nil {
		return ErrEvidencePolicyUntrusted
	}
	var privileged, hasRead, hasLockColumn, canInsertEvidence bool
	err := q.QueryRowContext(ctx, `
		SELECT
		  (r.rolsuper OR r.rolbypassrls OR r.rolcreaterole OR r.rolcreatedb
		   OR session_user <> current_user
		   OR has_database_privilege(current_user, current_database(), 'CREATE')
		   OR EXISTS (
		     SELECT 1 FROM pg_roles other
		     WHERE other.rolname <> current_user
		       AND pg_has_role(current_user, other.oid, 'SET')
		   )
		   OR has_schema_privilege(current_user, 'public', 'CREATE')
		   OR has_table_privilege(current_user, 'public.compute_evidence_policy_fences', 'INSERT')
		   OR has_table_privilege(current_user, 'public.compute_evidence_policy_fences', 'DELETE')
		   OR has_table_privilege(current_user, 'public.compute_evidence_policy_fences', 'TRUNCATE')
		   OR has_table_privilege(current_user, 'public.compute_evidence_policy_fences', 'TRIGGER')
		   OR has_table_privilege(current_user, 'public.compute_evidence_policy_fences', 'REFERENCES')
		   OR COALESCE((
		     SELECT bool_or(has_column_privilege(current_user,
		                      'public.compute_evidence_policy_fences',
		                      a.attname, 'UPDATE'))
		     FROM pg_attribute a
		     WHERE a.attrelid='public.compute_evidence_policy_fences'::regclass
		       AND a.attnum>0 AND NOT a.attisdropped AND a.attname<>'updated_at'
		   ), TRUE)),
		  has_table_privilege(current_user, 'public.compute_evidence_policy_fences', 'SELECT'),
		  has_column_privilege(current_user, 'public.compute_evidence_policy_fences', 'updated_at', 'UPDATE'),
		  has_table_privilege(current_user, 'public.compute_allocation_usage_evidence_sets', 'INSERT')
		FROM pg_roles r WHERE r.rolname=current_user
	`).Scan(&privileged, &hasRead, &hasLockColumn, &canInsertEvidence)
	if err != nil {
		return fmt.Errorf("%w: cannot attest E1 writer database role: %v", ErrEvidencePolicyUntrusted, err)
	}
	if privileged || !hasRead || !hasLockColumn || !canInsertEvidence {
		return fmt.Errorf("%w: E1 writer requires a separate narrowly scoped database role", ErrEvidencePolicyUntrusted)
	}
	return nil
}
