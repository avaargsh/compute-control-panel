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
// Catalog relations are schema-qualified because an ordinary TEMP-capable
// writer can create same-name relations in pg_temp.
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
		     SELECT 1 FROM pg_catalog.pg_roles other
		     WHERE other.rolname <> current_user
		       AND pg_has_role(current_user, other.oid, 'SET')
		   )
		   OR has_schema_privilege(current_user, 'public', 'CREATE')
		   -- Ordinary (origin) trigger mode is part of the E1 write contract.
		   -- PG16 permits a separately granted SET on this parameter. Merely
		   -- inspecting the current value is insufficient: a credentialed E1
		   -- writer could switch to replica before a direct SQL FINAL INSERT.
		   -- Refuse both effective SET and ALTER SYSTEM grants (including roles
		   -- inherited from other principals), even while origin is active.
		   OR current_setting('session_replication_role') <> 'origin'
		   OR pg_catalog.has_parameter_privilege(current_user, 'session_replication_role', 'SET')
		   OR pg_catalog.has_parameter_privilege(current_user, 'session_replication_role', 'ALTER SYSTEM')
		   OR EXISTS (
		     -- v0009 promotes E1 authority/integrity triggers to ALWAYS; reverting
		     -- any one to origin-only permits replication-mode direct SQL bypass.
		     -- Applied migration checksums are not proof that required
		     -- triggers are still enabled or still target their handlers.
		     -- Check the actual catalog on the very same write transaction.
		     SELECT 1 FROM (VALUES
		       ('public.compute_evidence_policy_fences'::regclass,
		        'compute_evidence_policy_revision_guard'::name,
		        'public.compute_enforce_evidence_policy_revision()'::regprocedure, 19, '02892e13304f82ef1437ba17ae2a3c5a'),
		       ('public.compute_evidence_policy_fences'::regclass,
		        'compute_evidence_policy_no_delete'::name,
		        'public.compute_reject_evidence_policy_removal()'::regprocedure, 11, '380edb09a1ec70141294358d043252b6'),
		       ('public.compute_evidence_policy_fences'::regclass,
		        'compute_evidence_policy_no_truncate'::name,
		        'public.compute_reject_evidence_policy_removal()'::regprocedure, 34, '380edb09a1ec70141294358d043252b6'),
		       ('public.compute_allocation_usage_evidence_sets'::regclass,
		        'compute_usage_evidence_scope_guard'::name,
		        'public.compute_validate_usage_evidence_set()'::regprocedure, 7, 'cb547f395690abc9b4866fcc204fea97'),
		       ('public.compute_allocation_usage_evidence_sets'::regclass,
		        'compute_usage_evidence_append_only'::name,
		        'public.compute_reject_usage_evidence_mutation()'::regprocedure, 27, 'b232e93c435de335ccc98fe8a39b7fd1'),
		       ('public.compute_allocation_usage_evidence_sets'::regclass,
		        'compute_usage_evidence_no_truncate'::name,
		        'public.compute_reject_usage_evidence_mutation()'::regprocedure, 34, 'b232e93c435de335ccc98fe8a39b7fd1')
		     ) AS required(relation_oid, trigger_name, handler_oid, event_bits, source_md5)
		     LEFT JOIN pg_catalog.pg_trigger t
		       ON t.tgrelid=required.relation_oid AND t.tgname=required.trigger_name
		     LEFT JOIN pg_catalog.pg_proc p ON p.oid=t.tgfoid
		     LEFT JOIN pg_catalog.pg_language l ON l.oid=p.prolang
		     WHERE t.oid IS NULL
		        OR t.tgenabled <> 'A'
		        OR t.tgfoid<>required.handler_oid
		        OR t.tgtype<>required.event_bits
		        OR t.tgqual IS NOT NULL
		        OR t.tgnargs<>0
		        OR t.tgattr::text<>''
		        OR t.tgdeferrable
		        OR t.tginitdeferred
		        -- CREATE OR REPLACE FUNCTION preserves the handler OID. A
		        -- checksum of the *applied migration* does not bind the
		        -- actual pg_proc.prosrc after a privileged DDL/restore.
		        -- These four MD5 baselines are from the unchanged v0006/v0007
		        -- PL/pgSQL AS $...$ bodies, not caller-provided policy.
		        OR p.oid IS NULL
		        OR l.lanname <> 'plpgsql'
		        OR md5(p.prosrc) <> required.source_md5
		        OR p.prosecdef
		        OR CASE WHEN required.handler_oid =
		             'public.compute_validate_usage_evidence_set()'::regprocedure
		             -- v0008 pins permanent public authority ahead of
		             -- pg_temp inside the original v0006 trigger function.
		             THEN p.proconfig IS DISTINCT FROM
		               ARRAY['search_path=pg_catalog, public, pg_temp']::text[]
		             ELSE p.proconfig IS NOT NULL END
		        OR p.provolatile <> 'v'
		        OR p.proparallel <> 'u'
		        OR p.proleakproof
		        OR p.prorettype <> 'pg_catalog.trigger'::regtype
		   )
		   OR EXISTS (
		     -- A policy-only role check is insufficient: E1 also trusts the
		     -- immutable evidence row and Provider/Workload/Allocation facts.
		     -- Fail closed if the effective role can modify those objects
		     -- beyond the single lock-compatible UPDATE column. The latter
		     -- remains a separate deployment-hardening gate for mutable rows.
		     SELECT 1 FROM (VALUES
		       ('public.compute_evidence_policy_fences'::regclass, 'updated_at'::name, false),
		       ('public.compute_operations'::regclass, 'updated_at'::name, false),
		       ('public.compute_workloads'::regclass, 'updated_at'::name, false),
		       ('public.compute_allocations'::regclass, 'updated_at'::name, false),
		       ('public.compute_e1_final_authorizations'::regclass, 'approved_at'::name, false),
		       ('public.compute_allocation_usage_evidence_sets'::regclass, 'recorded_at'::name, true)
		     ) AS protected(relation_oid, lock_column, allow_insert)
		     WHERE has_table_privilege(current_user, protected.relation_oid, 'DELETE')
		        OR has_table_privilege(current_user, protected.relation_oid, 'TRUNCATE')
		        OR has_table_privilege(current_user, protected.relation_oid, 'TRIGGER')
		        OR has_table_privilege(current_user, protected.relation_oid, 'REFERENCES')
		        OR (NOT protected.allow_insert AND
		            has_table_privilege(current_user, protected.relation_oid, 'INSERT'))
		        OR EXISTS (
		          SELECT 1 FROM pg_catalog.pg_attribute a
		          WHERE a.attrelid=protected.relation_oid
		            AND a.attnum>0 AND NOT a.attisdropped
		            AND (
		              (a.attname<>protected.lock_column AND
		               has_column_privilege(current_user, protected.relation_oid, a.attname, 'UPDATE'))
		              OR (NOT protected.allow_insert AND
		                  has_column_privilege(current_user, protected.relation_oid, a.attname, 'INSERT'))
		              OR has_column_privilege(current_user, protected.relation_oid, a.attname, 'REFERENCES')
		            )
		        )
		   )
		   -- The SECURITY DEFINER issuer check needs ledger SELECT, but the
		   -- E1 writer never does. Reject table grants and column grants,
		   -- including inherited permissions from other roles.
		   OR EXISTS (
		     SELECT 1 FROM pg_catalog.pg_attribute a
		     WHERE a.attrelid='public.compute_e1_final_authorizations'::regclass
		       AND a.attnum>0 AND NOT a.attisdropped
		       AND has_column_privilege(
		         current_user, a.attrelid, a.attname, 'SELECT')
		   )
		   OR has_column_privilege(current_user, 'public.compute_e1_final_authorizations', 'approved_at', 'UPDATE')
		   OR has_table_privilege(current_user, 'public.compute_evidence_policy_fences', 'INSERT')
		   OR has_table_privilege(current_user, 'public.compute_evidence_policy_fences', 'DELETE')
		   OR has_table_privilege(current_user, 'public.compute_evidence_policy_fences', 'TRUNCATE')
		   OR has_table_privilege(current_user, 'public.compute_evidence_policy_fences', 'TRIGGER')
		   OR has_table_privilege(current_user, 'public.compute_evidence_policy_fences', 'REFERENCES')
		   OR COALESCE((
		     SELECT bool_or(has_column_privilege(current_user,
		                      'public.compute_evidence_policy_fences',
		                      a.attname, 'UPDATE'))
		     FROM pg_catalog.pg_attribute a
		     WHERE a.attrelid='public.compute_evidence_policy_fences'::regclass
		       AND a.attnum>0 AND NOT a.attisdropped AND a.attname<>'updated_at'
		   ), TRUE)),
		  has_table_privilege(current_user, 'public.compute_evidence_policy_fences', 'SELECT'),
		  has_column_privilege(current_user, 'public.compute_evidence_policy_fences', 'updated_at', 'UPDATE'),
		  has_table_privilege(current_user, 'public.compute_allocation_usage_evidence_sets', 'INSERT')
		FROM pg_catalog.pg_roles r WHERE r.rolname=current_user
	`).Scan(&privileged, &hasRead, &hasLockColumn, &canInsertEvidence)
	if err != nil {
		return fmt.Errorf("%w: cannot attest E1 writer database role: %v", ErrEvidencePolicyUntrusted, err)
	}
	if privileged || !hasRead || !hasLockColumn || !canInsertEvidence {
		return fmt.Errorf("%w: E1 writer requires a separate narrowly scoped database role", ErrEvidencePolicyUntrusted)
	}
	return verifyE1FinalAuthorizationCatalog(ctx, q)
}
