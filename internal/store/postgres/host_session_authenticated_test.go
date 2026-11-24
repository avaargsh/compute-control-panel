package postgres

import (
	"context"
	"database/sql"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/avaargsh/compute-control-panel/internal/hostidentity"
)

func provisionHostSessionSubjectGrant(t *testing.T, db *sql.DB, binding hostidentity.Binding, tenantID string) {
	t.Helper()
	_, err := db.ExecContext(context.Background(), `
		INSERT INTO public.compute_host_identity_subject_grants
			(subject, tenant_id, service_name, host_id, provider_id, cluster_scope, node_uid)
		VALUES ($1,$2,'compute-host-agent',$3,$4,$5,$6)
	`, binding.Subject, tenantID, binding.HostID, binding.ProviderID, binding.ClusterScope, binding.NodeUID)
	if err != nil {
		t.Fatalf("provision exact host subject grant: %v", err)
	}
}

func TestHostSessionAuthenticatedStartHeartbeatAndGrantRevocation(t *testing.T) {
	admin := openIntegrationDB(t)
	_, binding := hostSessionFixture(t, admin)
	writer, err := hostSessionWriterDB(t)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	tenantID := "tenant-host-agent-" + randomSuffix(t)
	provisionHostSessionSubjectGrant(t, admin, binding, tenantID)
	repo, err := NewRepository(writer)
	if err != nil {
		t.Fatal(err)
	}
	resolved, err := repo.ResolveHostBinding(ctx, binding.Subject)
	if err != nil || resolved != binding {
		t.Fatalf("resolve active host subject grant: binding=%+v err=%v", resolved, err)
	}
	start := AuthenticatedHostSessionStartRequest{
		Subject:       binding.Subject,
		ExpectedEpoch: 0, RequestID: "authenticated-start-" + randomSuffix(t), TTL: 30 * time.Second,
	}
	first, err := repo.StartAuthenticatedHostSession(ctx, start)
	if err != nil {
		t.Fatalf("authenticated HostSession start: %v", err)
	}
	if first.Binding != binding || first.Epoch != 1 || !strings.HasPrefix(first.SessionID, "hs_") {
		t.Fatalf("authenticated start returned unexpected fence: %+v", first)
	}

	heartbeat := AuthenticatedHostHeartbeatRequest{
		Subject: binding.Subject,
		Epoch:   first.Epoch, SessionID: first.SessionID, Sequence: 1,
		Digest: "sha256:" + strings.Repeat("a", 64),
	}
	disposition, recordedAt, err := repo.RecordAuthenticatedHostHeartbeat(ctx, heartbeat)
	if err != nil || disposition != hostidentity.Accept || recordedAt.IsZero() {
		t.Fatalf("authenticated heartbeat: disposition=%s recorded_at=%v err=%v", disposition, recordedAt, err)
	}
	disposition, replayAt, err := repo.RecordAuthenticatedHostHeartbeat(ctx, heartbeat)
	if err != nil || disposition != hostidentity.Replay || !replayAt.Equal(recordedAt) {
		t.Fatalf("authenticated exact replay: disposition=%s recorded_at=%v err=%v", disposition, replayAt, err)
	}

	if _, err := repo.StartAuthenticatedHostSession(ctx, AuthenticatedHostSessionStartRequest{
		Subject:       "spiffe://ccp.example/host/unregistered-" + randomSuffix(t),
		ExpectedEpoch: 0, RequestID: "unknown-subject-" + randomSuffix(t), TTL: 30 * time.Second,
	}); !errors.Is(err, ErrHostSessionUnauthorized) {
		t.Fatalf("unregistered host subject was accepted: %v", err)
	}
	if _, err := repo.StartAuthenticatedHostSession(ctx, AuthenticatedHostSessionStartRequest{
		Subject:       "spiffe://ccp.example/ns/compute-host/sa/host-agent",
		ExpectedEpoch: 0, RequestID: "workload-subject-" + randomSuffix(t), TTL: 30 * time.Second,
	}); !errors.Is(err, ErrHostSessionInvalid) {
		t.Fatalf("workload identity was accepted: %v", err)
	}

	if _, err := admin.ExecContext(ctx, `
		UPDATE public.compute_host_identity_subject_grants
		SET revoked_at=pg_catalog.clock_timestamp(), revocation_reason='TEST_REVOKED'
		WHERE subject=$1
	`, binding.Subject); err != nil {
		t.Fatalf("revoke host subject grant: %v", err)
	}
	heartbeat.Sequence = 2
	heartbeat.Digest = "sha256:" + strings.Repeat("b", 64)
	if _, _, err := repo.RecordAuthenticatedHostHeartbeat(ctx, heartbeat); !errors.Is(err, ErrHostSessionUnauthorized) {
		t.Fatalf("revoked host subject continued heartbeating: %v", err)
	}
	if _, err := repo.StartAuthenticatedHostSession(ctx, AuthenticatedHostSessionStartRequest{
		Subject:       binding.Subject,
		ExpectedEpoch: 1, RequestID: "revoked-start-" + randomSuffix(t), TTL: 30 * time.Second,
	}); !errors.Is(err, ErrHostSessionUnauthorized) {
		t.Fatalf("revoked host subject started a successor session: %v", err)
	}
	if _, err := repo.ResolveHostBinding(ctx, binding.Subject); !errors.Is(err, ErrHostSessionUnauthorized) {
		t.Fatalf("revoked host subject resolved to an active binding: %v", err)
	}
}

func TestHostSessionSubjectGrantRevocationFencesConcurrentHeartbeat(t *testing.T) {
	admin := openIntegrationDB(t)
	_, binding := hostSessionFixture(t, admin)
	writer, err := hostSessionWriterDB(t)
	if err != nil {
		t.Fatal(err)
	}
	writer.SetMaxOpenConns(1)
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	tenantID := "tenant-host-revoke-race-" + randomSuffix(t)
	provisionHostSessionSubjectGrant(t, admin, binding, tenantID)
	repo, err := NewRepository(writer)
	if err != nil {
		t.Fatal(err)
	}
	first, err := repo.StartAuthenticatedHostSession(ctx, AuthenticatedHostSessionStartRequest{
		Subject:       binding.Subject,
		ExpectedEpoch: 0, RequestID: "revoke-race-start-" + randomSuffix(t), TTL: 30 * time.Second,
	})
	if err != nil {
		t.Fatal(err)
	}

	tx, err := admin.BeginTx(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback()
	var revokerPID int
	if err := tx.QueryRowContext(ctx, "SELECT pg_catalog.pg_backend_pid()").Scan(&revokerPID); err != nil {
		t.Fatal(err)
	}
	if _, err := tx.ExecContext(ctx, `
		UPDATE public.compute_host_identity_subject_grants
		SET revoked_at=pg_catalog.clock_timestamp(), revocation_reason='RACE_REVOKED'
		WHERE subject=$1
	`, binding.Subject); err != nil {
		t.Fatal(err)
	}

	var writerPID int
	if err := writer.QueryRowContext(ctx, "SELECT pg_catalog.pg_backend_pid()").Scan(&writerPID); err != nil {
		t.Fatal(err)
	}
	done := make(chan error, 1)
	go func() {
		_, _, err := repo.RecordAuthenticatedHostHeartbeat(ctx, AuthenticatedHostHeartbeatRequest{
			Subject: binding.Subject,
			Epoch:   first.Epoch, SessionID: first.SessionID, Sequence: 1,
			Digest: "sha256:" + strings.Repeat("c", 64),
		})
		done <- err
	}()

	deadline := time.NewTimer(5 * time.Second)
	defer deadline.Stop()
	ticker := time.NewTicker(20 * time.Millisecond)
	defer ticker.Stop()
	waiting := false
	for !waiting {
		select {
		case err := <-done:
			t.Fatalf("heartbeat completed before revocation committed: %v", err)
		case <-deadline.C:
			t.Fatal("heartbeat did not wait for the in-flight subject-grant revocation")
		case <-ctx.Done():
			t.Fatal(ctx.Err())
		case <-ticker.C:
			if err := admin.QueryRowContext(ctx, `
				SELECT $1 = ANY(pg_catalog.pg_blocking_pids($2))
			`, revokerPID, writerPID).Scan(&waiting); err != nil {
				t.Fatal(err)
			}
		}
	}
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}
	select {
	case err := <-done:
		if !errors.Is(err, ErrHostSessionUnauthorized) {
			t.Fatalf("heartbeat after committed revocation must be denied, got %v", err)
		}
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
}

func TestHostSessionSubjectGrantCannotBeSharedAcrossHostBindings(t *testing.T) {
	admin := openIntegrationDB(t)
	_, first := hostSessionFixture(t, admin)
	_, second := hostSessionFixture(t, admin)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	tenant := "tenant-host-unique-" + randomSuffix(t)
	provisionHostSessionSubjectGrant(t, admin, first, tenant)

	_, err := admin.ExecContext(ctx, `
		INSERT INTO public.compute_host_identity_subject_grants
			(subject, tenant_id, service_name, host_id, provider_id, cluster_scope, node_uid)
		VALUES ($1,$2,'compute-host-agent',$3,$4,$5,$6)
	`, first.Subject, tenant, second.HostID, second.ProviderID, second.ClusterScope, second.NodeUID)
	if err == nil {
		t.Fatal("one host certificate subject was allowed to bind a second HostID/Node UID")
	}

	_, err = admin.ExecContext(ctx, `
		INSERT INTO public.compute_host_identity_subject_grants
			(subject, tenant_id, service_name, host_id, provider_id, cluster_scope, node_uid)
		VALUES ($1,$2,'compute-host-agent',$3,$4,$5,$6)
	`, second.Subject, tenant, first.HostID, first.ProviderID, first.ClusterScope, first.NodeUID)
	if err == nil {
		t.Fatal("a second certificate subject was allowed to take an already granted HostID")
	}

	var count int
	if err := admin.QueryRowContext(ctx, `
		SELECT count(*) FROM public.compute_host_identity_subject_grants WHERE subject=$1
	`, first.Subject).Scan(&count); err != nil {
		t.Fatal(err)
	}
	if count != 1 {
		t.Fatalf("subject grant uniqueness changed after rejected cross-host insert: rows=%d", count)
	}
}

func TestHostSessionWriterCanOnlyInvokeAuthenticatedFunctions(t *testing.T) {
	_ = openIntegrationDB(t)
	writer, err := hostSessionWriterDB(t)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	var rawStart, rawHeartbeat, resolver, authenticatedStart, authenticatedHeartbeat bool
	err = writer.QueryRowContext(ctx, `
		SELECT
			pg_catalog.has_function_privilege(current_user,
				'public.compute_host_session_start(text,text,text,text,text,bigint,text,integer)', 'EXECUTE'),
			pg_catalog.has_function_privilege(current_user,
				'public.compute_host_session_heartbeat(text,text,text,text,text,bigint,text,bigint,text)', 'EXECUTE'),
			pg_catalog.has_function_privilege(current_user,
				'public.compute_host_identity_subject_resolve(text)', 'EXECUTE'),
			pg_catalog.has_function_privilege(current_user,
				'public.compute_host_session_start_authenticated(text,bigint,text,integer)', 'EXECUTE'),
			pg_catalog.has_function_privilege(current_user,
				'public.compute_host_session_heartbeat_authenticated(text,bigint,text,bigint,text)', 'EXECUTE')
	`).Scan(&rawStart, &rawHeartbeat, &resolver, &authenticatedStart, &authenticatedHeartbeat)
	if err != nil {
		t.Fatal(err)
	}
	if rawStart || rawHeartbeat || !resolver || !authenticatedStart || !authenticatedHeartbeat {
		t.Fatalf("unexpected HostSession function ACLs: raw=%t/%t resolver=%t authenticated=%t/%t",
			rawStart, rawHeartbeat, resolver, authenticatedStart, authenticatedHeartbeat)
	}

	for _, statement := range []string{
		`SELECT * FROM public.compute_host_session_start('h','p','c','n','s',0,'r',30)`,
		`SELECT * FROM public.compute_host_session_heartbeat('h','p','c','n','s',1,'hs_x',1,'sha256:` + strings.Repeat("a", 64) + `')`,
		`SELECT subject FROM public.compute_host_identity_subject_grants`,
		`INSERT INTO public.compute_host_identity_subject_grants(subject,tenant_id,service_name,host_id,provider_id,cluster_scope,node_uid)
		  VALUES('s','t','compute-host-agent','h','p','c','n')`,
		`UPDATE public.compute_host_identity_subject_grants SET revoked_at=pg_catalog.clock_timestamp(), revocation_reason='DENIED'`,
		`DELETE FROM public.compute_host_identity_subject_grants`,
		`TRUNCATE public.compute_host_identity_subject_grants`,
		`SELECT subject FROM public.compute_host_identity_subject_grant_audit`,
		`SELECT pg_catalog.nextval('public.compute_host_identity_subject_grant_audit_event_id_seq')`,
	} {
		if _, err := writer.ExecContext(ctx, statement); err == nil {
			t.Fatalf("restricted HostSession writer unexpectedly executed %q", statement)
		}
	}
}

func TestAuthenticatedHostSessionCannotOutliveGrant(t *testing.T) {
	admin := openIntegrationDB(t)
	_, binding := hostSessionFixture(t, admin)
	writer, err := hostSessionWriterDB(t)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	tenant := "tenant-host-expiry-" + randomSuffix(t)
	_, err = admin.ExecContext(ctx, `
		INSERT INTO public.compute_host_identity_subject_grants
			(subject, tenant_id, service_name, host_id, provider_id, cluster_scope, node_uid, expires_at)
		VALUES ($1,$2,'compute-host-agent',$3,$4,$5,$6,pg_catalog.clock_timestamp()+interval '60 seconds')
	`, binding.Subject, tenant, binding.HostID, binding.ProviderID, binding.ClusterScope, binding.NodeUID)
	if err != nil {
		t.Fatal(err)
	}
	repo, err := NewRepository(writer)
	if err != nil {
		t.Fatal(err)
	}
	_, err = repo.StartAuthenticatedHostSession(ctx, AuthenticatedHostSessionStartRequest{
		Subject:       binding.Subject,
		ExpectedEpoch: 0, RequestID: "too-long-" + randomSuffix(t), TTL: 120 * time.Second,
	})
	if !errors.Is(err, ErrHostSessionUnauthorized) {
		t.Fatalf("session outliving grant must be rejected, got %v", err)
	}
	var count int
	if err := admin.QueryRowContext(ctx, `
		SELECT count(*) FROM public.compute_host_sessions WHERE host_id=$1
	`, binding.HostID).Scan(&count); err != nil {
		t.Fatal(err)
	}
	if count != 0 {
		t.Fatalf("authorization rejection left %d durable sessions", count)
	}
}

func TestAuthenticatedHostSessionGrantExpiresWhileWaitingForProvider(t *testing.T) {
	admin := openIntegrationDB(t)
	_, binding := hostSessionFixture(t, admin)
	writer, err := hostSessionWriterDB(t)
	if err != nil {
		t.Fatal(err)
	}
	writer.SetMaxOpenConns(1)
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	repo, err := NewRepository(writer)
	if err != nil {
		t.Fatal(err)
	}
	tenant := "tenant-lock-expiry-" + randomSuffix(t)
	var expiresAt time.Time
	err = admin.QueryRowContext(ctx, `
		INSERT INTO public.compute_host_identity_subject_grants
			(subject, tenant_id, service_name, host_id, provider_id, cluster_scope, node_uid, expires_at)
		VALUES ($1,$2,'compute-host-agent',$3,$4,$5,$6,pg_catalog.clock_timestamp()+interval '5 seconds')
		RETURNING expires_at
	`, binding.Subject, tenant, binding.HostID, binding.ProviderID, binding.ClusterScope, binding.NodeUID).Scan(&expiresAt)
	if err != nil {
		t.Fatal(err)
	}
	tx, err := admin.BeginTx(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback()
	var blockerPID, writerPID int
	if err := tx.QueryRowContext(ctx, `
		SELECT pg_catalog.pg_backend_pid()
		FROM public.compute_providers WHERE provider_id=$1 FOR UPDATE
	`, binding.ProviderID).Scan(&blockerPID); err != nil {
		t.Fatal(err)
	}
	if err := writer.QueryRowContext(ctx, "SELECT pg_catalog.pg_backend_pid()").Scan(&writerPID); err != nil {
		t.Fatal(err)
	}
	req := AuthenticatedHostSessionStartRequest{
		Subject:       binding.Subject,
		ExpectedEpoch: 0, RequestID: "expiry-lock-" + randomSuffix(t), TTL: time.Second,
	}
	done := make(chan error, 1)
	go func() {
		_, err := repo.StartAuthenticatedHostSession(ctx, req)
		done <- err
	}()
	ticker := time.NewTicker(20 * time.Millisecond)
	defer ticker.Stop()
	waiting := false
	for !waiting {
		select {
		case err := <-done:
			t.Fatalf("start returned before reaching Provider lock: %v", err)
		case <-ctx.Done():
			t.Fatal(ctx.Err())
		case <-ticker.C:
			var active bool
			if err := admin.QueryRowContext(ctx, `
				SELECT $1 = ANY(pg_catalog.pg_blocking_pids($2)),
				       pg_catalog.clock_timestamp() < $3
			`, blockerPID, writerPID, expiresAt).Scan(&waiting, &active); err != nil {
				t.Fatal(err)
			}
			if !active {
				t.Fatal("fixture did not establish lock wait before grant expiry")
			}
		}
	}
	// The unchanged Provider remains REGISTERED. Wait using database time.
	if _, err := admin.ExecContext(ctx, `
		SELECT pg_catalog.pg_sleep(GREATEST(0, EXTRACT(EPOCH FROM ($1::timestamptz - pg_catalog.clock_timestamp())))::double precision + 0.05)
	`, expiresAt); err != nil {
		t.Fatal(err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}
	select {
	case err := <-done:
		if !errors.Is(err, ErrHostSessionUnauthorized) || hostSessionSQLState(err) != "28000" {
			t.Fatalf("expired authorization after lock wait must reject, got %v", err)
		}
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	var count int
	if err := admin.QueryRowContext(ctx, `
		SELECT count(*) FROM public.compute_host_sessions WHERE host_id=$1
	`, binding.HostID).Scan(&count); err != nil {
		t.Fatal(err)
	}
	if count != 0 {
		t.Fatalf("post-lock authorization rejection left %d durable sessions", count)
	}
}
