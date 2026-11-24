package postgres

import (
	"context"
	"errors"
	"testing"
	"time"
)

// This is the first durable enrollment contract, not an authorization or
// physical GPU attestation test. Only the disposable integration DB is used.
// Runtime enrollment/heartbeat writers must remain unmounted until their
// separate least-privilege, attestation and session CAS gates pass.
func TestHostIdentityBindingDatabaseContract(t *testing.T) {
	db := openIntegrationDB(t)
	repo, err := NewRepository(db)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	p := providerRegistrationFixture(t)
	if _, created, err := repo.RegisterProvider(ctx, p); err != nil || !created {
		t.Fatalf("provider fixture: created=%v err=%v", created, err)
	}
	hostID := "host-" + randomSuffix(t)
	nodeUID := "node-" + randomSuffix(t)
	subject := "spiffe://ccp.example/host/" + hostID

	const insert = `INSERT INTO public.compute_host_identity_bindings
		(host_id, provider_id, cluster_scope, node_uid, subject)
		VALUES($1,$2,$3,$4,$5)`
	// The database, not the caller, must supply the enrollment receipt time.
	var before, recorded, after time.Time
	if err := db.QueryRowContext(ctx, "SELECT pg_catalog.clock_timestamp()").Scan(&before); err != nil {
		t.Fatal(err)
	}
	forged := time.Date(2000, 1, 1, 0, 0, 0, 0, time.UTC)
	if err := db.QueryRowContext(ctx, `INSERT INTO public.compute_host_identity_bindings
		(host_id, provider_id, cluster_scope, node_uid, subject, registered_at)
		VALUES($1,$2,$3,$4,$5,$6) RETURNING registered_at`,
		hostID, p.ProviderID, p.ClusterScope, nodeUID, subject, forged).Scan(&recorded); err != nil {
		t.Fatalf("valid immutable binding must persist: %v", err)
	}
	if err := db.QueryRowContext(ctx, "SELECT pg_catalog.clock_timestamp()").Scan(&after); err != nil {
		t.Fatal(err)
	}
	if recorded.Equal(forged) || recorded.Before(before) || recorded.After(after) {
		t.Fatalf("enrollment receipt is not database-recorded: before=%v recorded=%v after=%v", before, recorded, after)
	}

	for _, tc := range []struct {
		name     string
		query    string
		args     []any
		sqlState string
	}{
		{
			name:     "duplicate native node identity",
			query:    insert,
			args:     []any{hostID + "-duplicate", p.ProviderID, p.ClusterScope, nodeUID, subject},
			sqlState: "23505",
		},
		{
			name:     "unknown provider",
			query:    insert,
			args:     []any{hostID + "-unknown", p.ProviderID + "-absent", p.ClusterScope, nodeUID + "-new", subject},
			sqlState: "23514",
		},
		{
			name:     "cluster mismatch",
			query:    insert,
			args:     []any{hostID + "-cluster", p.ProviderID, p.ClusterScope + "-foreign", nodeUID + "-new", subject},
			sqlState: "23514",
		},
		{
			name:     "missing native UID",
			query:    insert,
			args:     []any{hostID + "-no-uid", p.ProviderID, p.ClusterScope, "", subject},
			sqlState: "23514",
		},
		{
			name:     "missing subject",
			query:    insert,
			args:     []any{hostID + "-no-subject", p.ProviderID, p.ClusterScope, nodeUID + "-new", ""},
			sqlState: "23514",
		},
		{
			name:     "successor Node adoption",
			query:    "UPDATE public.compute_host_identity_bindings SET node_uid=$2 WHERE host_id=$1",
			args:     []any{hostID, nodeUID + "-successor"},
			sqlState: "23514",
		},
		{
			name:     "principal reassignment",
			query:    "UPDATE public.compute_host_identity_bindings SET subject=$2 WHERE host_id=$1",
			args:     []any{hostID, subject + "-successor"},
			sqlState: "23514",
		},
		{
			name:     "receipt time mutation",
			query:    "UPDATE public.compute_host_identity_bindings SET registered_at=$2 WHERE host_id=$1",
			args:     []any{hostID, forged},
			sqlState: "23514",
		},
		{
			name:     "deletion of enrolled identity",
			query:    "DELETE FROM public.compute_host_identity_bindings WHERE host_id=$1",
			args:     []any{hostID},
			sqlState: "23514",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			_, err := db.ExecContext(ctx, tc.query, tc.args...)
			var state interface{ SQLState() string }
			if !errors.As(err, &state) || state.SQLState() != tc.sqlState {
				t.Fatalf("want SQLSTATE %s for %s, got %v", tc.sqlState, tc.name, err)
			}
		})
	}

	if _, err := repo.BeginProviderDrain(ctx, p.ProviderID, 1, "PLANNED_MAINTENANCE"); err != nil {
		t.Fatal(err)
	}
	_, err = db.ExecContext(ctx, insert, hostID+"-after-drain", p.ProviderID, p.ClusterScope, nodeUID+"-new", subject)
	var state interface{ SQLState() string }
	if !errors.As(err, &state) || state.SQLState() != "23514" {
		t.Fatalf("draining Provider enrollment must return SQLSTATE 23514, got %v", err)
	}
	// Roll back even if the immutability guard is missing. Never destroy the
	// shared disposable database's fixture rows when a negative test fails.
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback()
	_, truncateErr := tx.ExecContext(ctx, "TRUNCATE public.compute_host_identity_bindings")
	rollbackErr := tx.Rollback()
	var truncateState interface{ SQLState() string }
	truncateSQLState := ""
	if errors.As(truncateErr, &truncateState) {
		truncateSQLState = truncateState.SQLState()
	}
	// The new subject-grant FK may reject TRUNCATE before statement triggers
	// with 0A000; without that FK the immutable guard returns 23514.
	if rollbackErr != nil || (truncateSQLState != "23514" && truncateSQLState != "0A000") {
		t.Fatalf("TRUNCATE must be rejected by the FK or immutable guard: SQLSTATE=%s truncate=%v rollback=%v",
			truncateSQLState, truncateErr, rollbackErr)
	}
	var provider, cluster, uid, persistedSubject string
	var persistedAt time.Time
	if err := db.QueryRowContext(ctx,
		"SELECT provider_id, cluster_scope, node_uid, subject, registered_at FROM public.compute_host_identity_bindings WHERE host_id=$1",
		hostID).Scan(&provider, &cluster, &uid, &persistedSubject, &persistedAt); err != nil {
		t.Fatal(err)
	}
	if provider != p.ProviderID || cluster != p.ClusterScope || uid != nodeUID || persistedSubject != subject || !persistedAt.Equal(recorded) {
		t.Fatal("rejected writes changed immutable enrollment")
	}
	var count int
	if err := db.QueryRowContext(ctx,
		"SELECT count(*) FROM public.compute_host_identity_bindings WHERE provider_id=$1",
		p.ProviderID).Scan(&count); err != nil || count != 1 {
		t.Fatalf("provider host cardinality=%d err=%v, want one", count, err)
	}
}

// A Drain that has already obtained the Provider row lock must win over a
// concurrent enrollment. A foreign key alone locks the key, not the lifecycle
// state, and is therefore insufficient to serialize these operations.
func TestHostIdentityEnrollmentWaitsForProviderDrain(t *testing.T) {
	db := openIntegrationDB(t)
	writer, _ := testHostIdentityRestrictedWriterDB(t, db)
	repo, err := NewRepository(db)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	p := providerRegistrationFixture(t)
	if _, created, err := repo.RegisterProvider(ctx, p); err != nil || !created {
		t.Fatalf("provider fixture: created=%v err=%v", created, err)
	}
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback()
	var drainPID int
	if err := tx.QueryRowContext(ctx, "SELECT pg_catalog.pg_backend_pid()").Scan(&drainPID); err != nil {
		t.Fatal(err)
	}
	result, err := tx.ExecContext(ctx, `UPDATE public.compute_providers
		SET state='DRAINING', version=2, last_reason_code='PLANNED_MAINTENANCE',
		    updated_at=pg_catalog.clock_timestamp()
		WHERE provider_id=$1 AND state='REGISTERED' AND version=1`, p.ProviderID)
	if err != nil {
		t.Fatal(err)
	}
	if count, err := result.RowsAffected(); err != nil || count != 1 {
		t.Fatalf("drain fixture rows=%d err=%v", count, err)
	}

	hostID := "host-" + randomSuffix(t)
	nodeUID := "node-" + randomSuffix(t)
	subject := "spiffe://ccp.example/host/" + hostID
	conn, err := writer.Conn(ctx)
	if err != nil {
		t.Fatal(err)
	}
	// On failure, release the blocking transaction before closing the waiting
	// connection. Cancel the statement even if the test exits via Fatal.
	defer func() {
		cancel()
		_ = tx.Rollback()
		_ = conn.Close()
	}()
	var pid int
	if err := conn.QueryRowContext(ctx, "SELECT pg_catalog.pg_backend_pid()").Scan(&pid); err != nil {
		t.Fatal(err)
	}
	done := make(chan error, 1)
	go func() {
		_, err := conn.ExecContext(ctx, `INSERT INTO public.compute_host_identity_bindings
			(host_id, provider_id, cluster_scope, node_uid, subject)
			VALUES($1,$2,$3,$4,$5)`, hostID, p.ProviderID, p.ClusterScope, nodeUID, subject)
		done <- err
	}()

	// Require actual PostgreSQL lock-wait evidence instead of assuming a
	// goroutine or a fixed sleep proves overlap.
	deadline := time.NewTimer(5 * time.Second)
	defer deadline.Stop()
	tick := time.NewTicker(20 * time.Millisecond)
	defer tick.Stop()
	waiting := false
	for !waiting {
		select {
		case err := <-done:
			t.Fatalf("enrollment completed before drain released its row lock: %v", err)
		case <-deadline.C:
			t.Fatal("enrollment did not wait on Provider lifecycle lock")
		case <-ctx.Done():
			t.Fatal(ctx.Err())
		case <-tick.C:
			if err := tx.QueryRowContext(ctx,
				"SELECT $1 = ANY(pg_catalog.pg_blocking_pids($2))", drainPID, pid).Scan(&waiting); err != nil {
				t.Fatal(err)
			}
		}
	}
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}
	select {
	case err := <-done:
		var state interface{ SQLState() string }
		if !errors.As(err, &state) || state.SQLState() != "23514" {
			t.Fatalf("enrollment must reject committed Drain with check-violation SQLSTATE 23514, got %v", err)
		}
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	var count int
	if err := db.QueryRowContext(ctx,
		"SELECT count(*) FROM public.compute_host_identity_bindings WHERE host_id=$1",
		hostID).Scan(&count); err != nil || count != 0 {
		t.Fatalf("rejected enrollment persisted: count=%d err=%v", count, err)
	}
}

// TestHostIdentityBindingRestrictedRoleCannotEnroll verifies that the
// existing DRA receipt writer has no enrollment or mutation authority.
// Do not add grants to make an unmounted enrollment path work.
func TestHostIdentityBindingRestrictedRoleCannotEnroll(t *testing.T) {
	db := openIntegrationDB(t)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback()
	if _, err := tx.ExecContext(ctx, "SET LOCAL ROLE ccp_dra_receipt_ingest"); err != nil {
		t.Fatal(err)
	}
	var canRead, canInsert, canUpdate, canDelete, canTruncate, canExecute bool
	if err := tx.QueryRowContext(ctx, `SELECT
		pg_catalog.has_table_privilege(current_user, 'public.compute_host_identity_bindings', 'SELECT'),
		pg_catalog.has_table_privilege(current_user, 'public.compute_host_identity_bindings', 'INSERT'),
		pg_catalog.has_table_privilege(current_user, 'public.compute_host_identity_bindings', 'UPDATE'),
		pg_catalog.has_table_privilege(current_user, 'public.compute_host_identity_bindings', 'DELETE'),
		pg_catalog.has_table_privilege(current_user, 'public.compute_host_identity_bindings', 'TRUNCATE'),
		pg_catalog.has_function_privilege(current_user, 'public.compute_host_identity_enrollment_guard()', 'EXECUTE')`).Scan(
		&canRead, &canInsert, &canUpdate, &canDelete, &canTruncate, &canExecute); err != nil {
		t.Fatal(err)
	}
	if canRead || canInsert || canUpdate || canDelete || canTruncate || canExecute {
		t.Fatal("DRA receipt writer has unexpected HostIdentity authority")
	}

	_, err = tx.ExecContext(ctx, `INSERT INTO public.compute_host_identity_bindings
		(host_id, provider_id, cluster_scope, node_uid, subject)
		VALUES ('denied-host', 'denied-provider', 'denied-scope', 'denied-node', 'denied-subject')`)
	var state interface{ SQLState() string }
	if !errors.As(err, &state) || state.SQLState() != "42501" {
		t.Fatalf("restricted enrollment must fail with SQLSTATE 42501, got %v", err)
	}
}
