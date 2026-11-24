package postgres

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/avaargsh/compute-control-panel/internal/hostidentity"
)

func provisionHostSessionSubjectGrant(t *testing.T, db interface {
	ExecContext(context.Context, string, ...any) (interface{ RowsAffected() (int64, error) }, error)
}, binding hostidentity.Binding, tenantID string) {
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
	start := AuthenticatedHostSessionStartRequest{
		TenantID: tenantID, Service: "compute-host-agent", Subject: binding.Subject,
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
		TenantID: tenantID, Service: "compute-host-agent", Subject: binding.Subject,
		Epoch: first.Epoch, SessionID: first.SessionID, Sequence: 1,
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
		TenantID: tenantID + "-wrong", Service: "compute-host-agent", Subject: binding.Subject,
		ExpectedEpoch: 1, RequestID: "wrong-tenant-" + randomSuffix(t), TTL: 30 * time.Second,
	}); !errors.Is(err, ErrHostSessionUnauthorized) {
		t.Fatalf("cross-tenant host subject grant was accepted: %v", err)
	}
	if _, err := repo.StartAuthenticatedHostSession(ctx, AuthenticatedHostSessionStartRequest{
		TenantID: tenantID, Service: "agent-runtime", Subject: binding.Subject,
		ExpectedEpoch: 1, RequestID: "wrong-service-" + randomSuffix(t), TTL: 30 * time.Second,
	}); !errors.Is(err, ErrHostSessionUnauthorized) {
		t.Fatalf("non-host service identity was accepted: %v", err)
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
		TenantID: tenantID, Service: "compute-host-agent", Subject: binding.Subject,
		ExpectedEpoch: 1, RequestID: "revoked-start-" + randomSuffix(t), TTL: 30 * time.Second,
	}); !errors.Is(err, ErrHostSessionUnauthorized) {
		t.Fatalf("revoked host subject started a successor session: %v", err)
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
		TenantID: tenantID, Service: "compute-host-agent", Subject: binding.Subject,
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
			TenantID: tenantID, Service: "compute-host-agent", Subject: binding.Subject,
			Epoch: first.Epoch, SessionID: first.SessionID, Sequence: 1,
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

func TestHostSessionWriterCanOnlyInvokeAuthenticatedFunctions(t *testing.T) {
	admin := openIntegrationDB(t)
	writer, err := hostSessionWriterDB(t)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	var rawStart, rawHeartbeat, authenticatedStart, authenticatedHeartbeat bool
	err = writer.QueryRowContext(ctx, `
		SELECT
			pg_catalog.has_function_privilege(current_user,
				'public.compute_host_session_start(text,text,text,text,text,bigint,text,integer)', 'EXECUTE'),
			pg_catalog.has_function_privilege(current_user,
				'public.compute_host_session_heartbeat(text,text,text,text,text,bigint,text,bigint,text)', 'EXECUTE'),
			pg_catalog.has_function_privilege(current_user,
				'public.compute_host_session_start_authenticated(text,text,text,bigint,text,integer)', 'EXECUTE'),
			pg_catalog.has_function_privilege(current_user,
				'public.compute_host_session_heartbeat_authenticated(text,text,text,bigint,text,bigint,text)', 'EXECUTE')
	`).Scan(&rawStart, &rawHeartbeat, &authenticatedStart, &authenticatedHeartbeat)
	if err != nil {
		t.Fatal(err)
	}
	if rawStart || rawHeartbeat || !authenticatedStart || !authenticatedHeartbeat {
		t.Fatalf("unexpected HostSession function ACLs: raw=%t/%t authenticated=%t/%t",
			rawStart, rawHeartbeat, authenticatedStart, authenticatedHeartbeat)
	}

	for _, statement := range []string{
		`SELECT * FROM public.compute_host_session_start('h','p','c','n','s',0,'r',30)`,
		`SELECT * FROM public.compute_host_session_heartbeat('h','p','c','n','s',1,'hs_x',1,'sha256:` + strings.Repeat("a", 64) + `')`,
		`SELECT subject FROM public.compute_host_identity_subject_grants`,
		`INSERT INTO public.compute_host_identity_subject_grants(subject,tenant_id,service_name,host_id,provider_id,cluster_scope,node_uid)
		  VALUES('s','t','compute-host-agent','h','p','c','n')`,
	} {
		if _, err := writer.ExecContext(ctx, statement); err == nil {
			t.Fatalf("restricted HostSession writer unexpectedly executed %q", statement)
		}
	}
	_ = admin
}
