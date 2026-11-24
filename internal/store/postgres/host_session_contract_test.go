package postgres

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"net/url"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/avaargsh/compute-control-panel/internal/hostidentity"
)

func hostSessionFixture(t *testing.T, db *sql.DB) (*Repository, hostidentity.Binding) {
	t.Helper()
	repo, err := NewRepository(db)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	provider := providerRegistrationFixture(t)
	if _, created, err := repo.RegisterProvider(ctx, provider); err != nil || !created {
		t.Fatalf("register Provider: created=%t err=%v", created, err)
	}
	binding := hostidentity.Binding{
		HostID:       "host-" + randomSuffix(t),
		ProviderID:   provider.ProviderID,
		ClusterScope: provider.ClusterScope,
		NodeUID:      "node-uid-" + randomSuffix(t),
		Subject:      "spiffe://ccp.example/host/" + randomSuffix(t),
	}
	if _, err := db.ExecContext(ctx, `INSERT INTO public.compute_host_identity_bindings
		(host_id, provider_id, cluster_scope, node_uid, subject)
		VALUES($1,$2,$3,$4,$5)`, binding.HostID, binding.ProviderID,
		binding.ClusterScope, binding.NodeUID, binding.Subject); err != nil {
		t.Fatalf("insert immutable HostIdentity binding: %v", err)
	}
	return repo, binding
}

func hostSessionSQLState(err error) string {
	var state interface{ SQLState() string }
	if errors.As(err, &state) {
		return state.SQLState()
	}
	return ""
}

func TestHostSessionStartAndHeartbeatDatabaseContract(t *testing.T) {
	db := openIntegrationDB(t)
	repo, binding := hostSessionFixture(t, db)
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	var functionOwner, tableOwner string
	var securityDefiner, canSelect, canInsert, schemaUsage, rowSecurity bool
	if err := db.QueryRowContext(ctx, `
		SELECT pg_catalog.pg_get_userbyid(p.proowner),
		       pg_catalog.pg_get_userbyid(c.relowner),
		       p.prosecdef,
		       pg_catalog.has_table_privilege(p.proowner, c.oid, 'SELECT'),
		       pg_catalog.has_table_privilege(p.proowner, c.oid, 'INSERT'),
		       pg_catalog.has_schema_privilege(p.proowner, 'public', 'USAGE'),
		       c.relrowsecurity
		FROM pg_catalog.pg_proc p
		CROSS JOIN pg_catalog.pg_class c
		WHERE p.oid='public.compute_host_session_start(text,text,text,text,text,bigint,text,integer)'::pg_catalog.regprocedure
		  AND c.oid='public.compute_host_sessions'::pg_catalog.regclass
	`).Scan(&functionOwner, &tableOwner, &securityDefiner, &canSelect, &canInsert, &schemaUsage, &rowSecurity); err != nil {
		t.Fatal(err)
	}
	if functionOwner != "ccp_host_session_guard_owner" || tableOwner != functionOwner ||
		!securityDefiner || !canSelect || !canInsert || !schemaUsage || rowSecurity {
		t.Fatalf("unsafe HostSession definer catalog: function_owner=%s table_owner=%s security_definer=%t select=%t insert=%t schema_usage=%t row_security=%t",
			functionOwner, tableOwner, securityDefiner, canSelect, canInsert, schemaUsage, rowSecurity)
	}

	foreignBinding := binding
	foreignBinding.NodeUID = binding.NodeUID + "-foreign"
	if _, err := repo.StartHostSession(ctx, HostSessionStartRequest{Binding: foreignBinding, ExpectedEpoch: 0, RequestID: "foreign-start-" + randomSuffix(t), TTL: 60 * time.Second}); !errors.Is(err, ErrHostSessionRejected) ||
		hostSessionSQLState(err) != "23514" {
		t.Fatalf("foreign Node UID created a HostSession: %v", err)
	}

	var before, after time.Time
	if err := db.QueryRowContext(ctx, `SELECT pg_catalog.clock_timestamp()`).Scan(&before); err != nil {
		t.Fatal(err)
	}
	first, err := repo.StartHostSession(ctx, HostSessionStartRequest{Binding: binding, ExpectedEpoch: 0, RequestID: "start-initial-" + binding.HostID, TTL: 60 * time.Second})
	if err != nil {
		t.Fatalf("first durable session start: %v", err)
	}
	if err := db.QueryRowContext(ctx, `SELECT pg_catalog.clock_timestamp()`).Scan(&after); err != nil {
		t.Fatal(err)
	}
	if first.Binding != binding || first.Epoch != 1 || !strings.HasPrefix(first.SessionID, "hs_") ||
		first.IssuedAt.Before(before) || first.IssuedAt.After(after) ||
		!first.ExpiresAt.After(first.IssuedAt) ||
		first.LastSequence != 0 || first.LastDigest != "" || !first.LastRecordedAt.IsZero() {
		t.Fatalf("invalid database-issued first session fence: before=%v fence=%+v after=%v", before, first, after)
	}

	// A retry after a lost ACK must reuse the persisted request identity and
	// return the original session, not allocate a second epoch or ID.
	replayedStart, err := repo.StartHostSession(ctx, HostSessionStartRequest{
		Binding: binding, ExpectedEpoch: 0, RequestID: "start-initial-" + binding.HostID, TTL: 60 * time.Second,
	})
	if err != nil || replayedStart.Epoch != first.Epoch || replayedStart.SessionID != first.SessionID ||
		!replayedStart.IssuedAt.Equal(first.IssuedAt) || !replayedStart.ExpiresAt.Equal(first.ExpiresAt) {
		t.Fatalf("lost-ACK start replay did not return the original fence: first=%+v replay=%+v err=%v", first, replayedStart, err)
	}

	// A new repository instance must observe the durable active epoch. It must
	// not replace it merely because a process restarted or expectedEpoch is stale.
	restarted, err := NewRepository(db)
	if err != nil {
		t.Fatal(err)
	}

	changedPayload := HostSessionStartRequest{
		Binding: binding, ExpectedEpoch: 0, RequestID: "start-initial-" + binding.HostID, TTL: 30 * time.Second,
	}
	if _, err := restarted.StartHostSession(ctx, changedPayload); !errors.Is(err, ErrHostSessionRejected) ||
		hostSessionSQLState(err) != "23514" {
		t.Fatalf("same start request ID with changed TTL was not rejected: %v", err)
	}
	if _, err := restarted.StartHostSession(ctx, HostSessionStartRequest{Binding: binding, ExpectedEpoch: 0, RequestID: "start-restart-" + binding.HostID, TTL: 60 * time.Second}); !errors.Is(err, ErrHostSessionRejected) ||
		hostSessionSQLState(err) != "23514" {
		t.Fatalf("active epoch replacement after repository restart was not rejected: %v", err)
	}

	digest := "sha256:" + strings.Repeat("a", 64)
	heartbeat := hostidentity.Heartbeat{
		Binding: binding, Epoch: first.Epoch, SessionID: first.SessionID,
		Sequence: 1, Digest: digest,
	}
	foreignHeartbeat := heartbeat
	foreignHeartbeat.Binding.Subject = "spiffe://ccp.example/host/foreign"
	if _, _, err := repo.RecordHostHeartbeat(ctx, foreignHeartbeat); !errors.Is(err, ErrHostSessionRejected) ||
		hostSessionSQLState(err) != "23514" {
		t.Fatalf("foreign subject wrote a HostSession heartbeat: %v", err)
	}

	disposition, recordedAt, err := repo.RecordHostHeartbeat(ctx, heartbeat)
	if err != nil || disposition != hostidentity.Accept || recordedAt.IsZero() {
		t.Fatalf("first heartbeat: disposition=%s recorded_at=%v err=%v", disposition, recordedAt, err)
	}
	disposition, replayAt, err := restarted.RecordHostHeartbeat(ctx, heartbeat)
	if err != nil || disposition != hostidentity.Replay || !replayAt.Equal(recordedAt) {
		t.Fatalf("exact replay after repository restart: disposition=%s recorded_at=%v want=%v err=%v",
			disposition, replayAt, recordedAt, err)
	}

	conflict := heartbeat
	conflict.Digest = "sha256:" + strings.Repeat("b", 64)
	if _, _, err := repo.RecordHostHeartbeat(ctx, conflict); !errors.Is(err, ErrHostSessionRejected) ||
		hostSessionSQLState(err) != "23514" {
		t.Fatalf("same sequence with conflicting digest was not rejected: %v", err)
	}
	older := heartbeat
	older.Sequence = 0
	if _, _, err := repo.RecordHostHeartbeat(ctx, older); !errors.Is(err, ErrHostSessionInvalid) {
		t.Fatalf("non-positive sequence was not rejected before SQL: %v", err)
	}
	next := heartbeat
	next.Sequence = 2
	next.Digest = "sha256:" + strings.Repeat("c", 64)
	if disposition, _, err := repo.RecordHostHeartbeat(ctx, next); err != nil || disposition != hostidentity.Accept {
		t.Fatalf("strictly newer heartbeat: disposition=%s err=%v", disposition, err)
	}

	var count int
	if err := db.QueryRowContext(ctx, `SELECT count(*) FROM public.compute_host_session_heartbeats
		WHERE host_id=$1 AND epoch=$2`, binding.HostID, first.Epoch).Scan(&count); err != nil {
		t.Fatal(err)
	}
	if count != 2 {
		t.Fatalf("durable heartbeat receipts=%d, want 2 (exact replay must not append)", count)
	}
	var expiry time.Time
	if err := db.QueryRowContext(ctx, `SELECT expires_at FROM public.compute_host_sessions
		WHERE host_id=$1 AND epoch=$2`, binding.HostID, first.Epoch).Scan(&expiry); err != nil {
		t.Fatal(err)
	}
	if !expiry.Equal(first.ExpiresAt) {
		t.Fatalf("heartbeat extended or rewrote session expiry: got=%v want=%v", expiry, first.ExpiresAt)
	}
}

func TestHostSessionExpiredEpochReplacementAndProviderDrain(t *testing.T) {
	db := openIntegrationDB(t)
	repo, binding := hostSessionFixture(t, db)
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	first, err := repo.StartHostSession(ctx, HostSessionStartRequest{Binding: binding, ExpectedEpoch: 0, RequestID: "start-short-" + binding.HostID, TTL: time.Second})
	if err != nil {
		t.Fatal(err)
	}
	time.Sleep(1100 * time.Millisecond)
	expiredHeartbeat := hostidentity.Heartbeat{
		Binding: binding, Epoch: first.Epoch, SessionID: first.SessionID,
		Sequence: 1, Digest: "sha256:" + strings.Repeat("f", 64),
	}
	if _, _, err := repo.RecordHostHeartbeat(ctx, expiredHeartbeat); !errors.Is(err, ErrHostSessionRejected) ||
		hostSessionSQLState(err) != "23514" {
		t.Fatalf("expired session accepted a heartbeat: %v", err)
	}
	second, err := repo.StartHostSession(ctx, HostSessionStartRequest{Binding: binding, ExpectedEpoch: first.Epoch, RequestID: "start-successor-" + binding.HostID, TTL: 30 * time.Second})
	if err != nil {
		t.Fatalf("expired predecessor should advance one durable epoch: %v", err)
	}
	if second.Epoch != first.Epoch+1 || second.SessionID == first.SessionID {
		t.Fatalf("session epoch did not advance monotonically: first=%+v second=%+v", first, second)
	}
	if _, _, err := repo.RecordHostHeartbeat(ctx, expiredHeartbeat); !errors.Is(err, ErrHostSessionRejected) ||
		hostSessionSQLState(err) != "23514" {
		t.Fatalf("superseded epoch accepted a heartbeat: %v", err)
	}

	if _, err := repo.BeginProviderDrain(ctx, binding.ProviderID, 1, "SESSION_FENCE_TEST"); err != nil {
		t.Fatal(err)
	}
	heartbeat := hostidentity.Heartbeat{
		Binding: binding, Epoch: second.Epoch, SessionID: second.SessionID,
		Sequence: 1, Digest: "sha256:" + strings.Repeat("d", 64),
	}
	if _, _, err := repo.RecordHostHeartbeat(ctx, heartbeat); !errors.Is(err, ErrHostSessionRejected) ||
		hostSessionSQLState(err) != "23514" {
		t.Fatalf("DRAINING Provider accepted a host heartbeat: %v", err)
	}
	replayedSecond, err := repo.StartHostSession(ctx, HostSessionStartRequest{
		Binding: binding, ExpectedEpoch: first.Epoch, RequestID: "start-successor-" + binding.HostID, TTL: 30 * time.Second,
	})
	if err != nil || replayedSecond.SessionID != second.SessionID || replayedSecond.Epoch != second.Epoch {
		t.Fatalf("lost-ACK replay after Drain did not return original session: replay=%+v second=%+v err=%v",
			replayedSecond, second, err)
	}
	if _, err := repo.StartHostSession(ctx, HostSessionStartRequest{Binding: binding, ExpectedEpoch: second.Epoch, RequestID: "start-draining-" + binding.HostID, TTL: 30 * time.Second}); !errors.Is(err, ErrHostSessionRejected) ||
		hostSessionSQLState(err) != "23514" {
		t.Fatalf("DRAINING Provider accepted a new session: %v", err)
	}
}

func TestHostSessionConcurrentStartHasOneWinner(t *testing.T) {
	db := openIntegrationDB(t)
	repo, binding := hostSessionFixture(t, db)
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	const parallel = 8
	errs := make([]error, parallel)
	fences := make([]hostidentity.SessionFence, parallel)
	var wg sync.WaitGroup
	for i := range errs {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			fences[i], errs[i] = repo.StartHostSession(ctx, HostSessionStartRequest{Binding: binding, ExpectedEpoch: 0, RequestID: fmt.Sprintf("start-concurrent-%s-%d", binding.HostID, i), TTL: 30 * time.Second})
		}(i)
	}
	wg.Wait()
	winners := 0
	for i, err := range errs {
		if err == nil {
			winners++
			if fences[i].Epoch != 1 || fences[i].SessionID == "" {
				t.Fatalf("winner returned invalid fence: %+v", fences[i])
			}
			continue
		}
		if !errors.Is(err, ErrHostSessionRejected) || hostSessionSQLState(err) != "23514" {
			t.Fatalf("concurrent session start returned unexpected error: %v", err)
		}
	}
	if winners != 1 {
		t.Fatalf("concurrent first-session winners=%d, want exactly one", winners)
	}
}

func TestHostSessionConcurrentSameRequestIsIdempotent(t *testing.T) {
	db := openIntegrationDB(t)
	repo, binding := hostSessionFixture(t, db)
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	request := HostSessionStartRequest{
		Binding: binding, ExpectedEpoch: 0,
		RequestID: "start-same-" + binding.HostID, TTL: 30 * time.Second,
	}
	const parallel = 8
	errs := make([]error, parallel)
	fences := make([]hostidentity.SessionFence, parallel)
	var wg sync.WaitGroup
	for i := range errs {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			fences[i], errs[i] = repo.StartHostSession(ctx, request)
		}(i)
	}
	wg.Wait()
	for i, err := range errs {
		if err != nil {
			t.Fatalf("same idempotency request %d did not replay successfully: %v", i, err)
		}
		if fences[i].Epoch != fences[0].Epoch || fences[i].SessionID != fences[0].SessionID ||
			!fences[i].IssuedAt.Equal(fences[0].IssuedAt) || !fences[i].ExpiresAt.Equal(fences[0].ExpiresAt) {
			t.Fatalf("same idempotency key returned divergent session fences: first=%+v current=%+v", fences[0], fences[i])
		}
	}
	var count int
	if err := db.QueryRowContext(ctx, `SELECT count(*) FROM public.compute_host_sessions WHERE host_id=$1`,
		binding.HostID).Scan(&count); err != nil {
		t.Fatal(err)
	}
	if count != 1 {
		t.Fatalf("concurrent same-request session rows=%d, want 1", count)
	}
}

func TestHostSessionRestrictedWriterAndCatalogBoundary(t *testing.T) {
	admin := openIntegrationDB(t)
	writer, err := hostSessionWriterDB(t)
	if err != nil {
		t.Fatal(err)
	}
	_, binding := hostSessionFixture(t, admin)
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	var sessionUser, currentUser string
	if err := writer.QueryRowContext(ctx, `SELECT session_user, current_user`).Scan(&sessionUser, &currentUser); err != nil {
		t.Fatal(err)
	}
	if sessionUser != "ccp_host_session_writer" || currentUser != sessionUser {
		t.Fatalf("session writer identity mismatch: session_user=%q current_user=%q", sessionUser, currentUser)
	}

	var canReadProvider, canUpdateProvider, canReadBinding, canUpdateBinding bool
	var canReadSessions, canInsertSessions, canReadHeartbeats, canInsertHeartbeats bool
	err = writer.QueryRowContext(ctx, `SELECT
		pg_catalog.has_table_privilege(current_user, 'public.compute_providers', 'SELECT'),
		pg_catalog.has_table_privilege(current_user, 'public.compute_providers', 'UPDATE'),
		pg_catalog.has_table_privilege(current_user, 'public.compute_host_identity_bindings', 'SELECT'),
		pg_catalog.has_table_privilege(current_user, 'public.compute_host_identity_bindings', 'UPDATE'),
		pg_catalog.has_table_privilege(current_user, 'public.compute_host_sessions', 'SELECT'),
		pg_catalog.has_table_privilege(current_user, 'public.compute_host_sessions', 'INSERT'),
		pg_catalog.has_table_privilege(current_user, 'public.compute_host_session_heartbeats', 'SELECT'),
		pg_catalog.has_table_privilege(current_user, 'public.compute_host_session_heartbeats', 'INSERT')`).
		Scan(&canReadProvider, &canUpdateProvider, &canReadBinding, &canUpdateBinding,
			&canReadSessions, &canInsertSessions, &canReadHeartbeats, &canInsertHeartbeats)
	if err != nil {
		t.Fatal(err)
	}
	if canReadProvider || canUpdateProvider || canReadBinding || canUpdateBinding ||
		canReadSessions || canInsertSessions || canReadHeartbeats || canInsertHeartbeats {
		t.Fatalf("session writer received direct table privileges: provider=%t/%t binding=%t/%t sessions=%t/%t heartbeats=%t/%t",
			canReadProvider, canUpdateProvider, canReadBinding, canUpdateBinding,
			canReadSessions, canInsertSessions, canReadHeartbeats, canInsertHeartbeats)
	}

	writerRepo, err := NewRepository(writer)
	if err != nil {
		t.Fatal(err)
	}
	writerFirst, err := writerRepo.StartHostSession(ctx, HostSessionStartRequest{Binding: binding, ExpectedEpoch: 0, RequestID: "start-restricted-" + binding.HostID, TTL: 30 * time.Second})
	if err != nil {
		t.Fatalf("restricted writer could not invoke the session-start function: %v", err)
	}
	heartbeat := hostidentity.Heartbeat{
		Binding: binding, Epoch: writerFirst.Epoch, SessionID: writerFirst.SessionID,
		Sequence: 1, Digest: "sha256:" + strings.Repeat("e", 64),
	}
	if disposition, _, err := writerRepo.RecordHostHeartbeat(ctx, heartbeat); err != nil || disposition != hostidentity.Accept {
		t.Fatalf("restricted writer function call: disposition=%s err=%v", disposition, err)
	}

	for _, statement := range []string{
		`SELECT provider_id FROM public.compute_providers WHERE provider_id=$1`,
		`SELECT host_id FROM public.compute_host_identity_bindings WHERE host_id=$1`,
		`SELECT host_id FROM public.compute_host_sessions WHERE host_id=$1`,
		`INSERT INTO public.compute_host_sessions(host_id,epoch,session_id,provider_id,cluster_scope,node_uid,subject,issued_at,expires_at) VALUES('x',1,'x','x','x','x','x',pg_catalog.clock_timestamp(),pg_catalog.clock_timestamp()+interval '1 minute')`,
		`UPDATE public.compute_host_sessions SET subject=subject WHERE host_id=$1`,
		`DELETE FROM public.compute_host_session_heartbeats WHERE host_id=$1`,
		`TRUNCATE public.compute_host_sessions`,
		`SELECT public.compute_host_session_immutable_guard()`,
		`SET ROLE ccp_host_session_guard_owner`,
	} {
		var args []any
		if strings.Contains(statement, "$1") {
			args = []any{binding.HostID}
		}
		_, err := writer.ExecContext(ctx, statement, args...)
		if hostSessionSQLState(err) != "42501" {
			t.Fatalf("restricted writer operation should be denied with SQLSTATE 42501: statement=%q err=%v", statement, err)
		}
	}

	var ownerStart, ownerHeartbeat, startConfig, heartbeatConfig string
	var startSecurityDefiner, heartbeatSecurityDefiner bool
	if err := admin.QueryRowContext(ctx, `
		SELECT pg_catalog.pg_get_userbyid(s.proowner), s.prosecdef,
		       pg_catalog.array_to_string(s.proconfig, ','),
		       pg_catalog.pg_get_userbyid(h.proowner), h.prosecdef,
		       pg_catalog.array_to_string(h.proconfig, ',')
		FROM pg_catalog.pg_proc s
		CROSS JOIN pg_catalog.pg_proc h
		WHERE s.oid='public.compute_host_session_start(text,text,text,text,text,bigint,text,integer)'::pg_catalog.regprocedure
		  AND h.oid='public.compute_host_session_heartbeat(text,text,text,text,text,bigint,text,bigint,text)'::pg_catalog.regprocedure
	`).Scan(&ownerStart, &startSecurityDefiner, &startConfig,
		&ownerHeartbeat, &heartbeatSecurityDefiner, &heartbeatConfig); err != nil {
		t.Fatal(err)
	}
	if ownerStart != "ccp_host_session_guard_owner" || ownerHeartbeat != ownerStart ||
		!startSecurityDefiner || !heartbeatSecurityDefiner ||
		startConfig != "search_path=pg_catalog, pg_temp" || heartbeatConfig != "search_path=pg_catalog, pg_temp" {
		t.Fatalf("unsafe HostSession SECURITY DEFINER boundary: owner=%q/%q start=%t/%q heartbeat=%t/%q",
			ownerStart, ownerHeartbeat, startSecurityDefiner, startConfig, heartbeatSecurityDefiner, heartbeatConfig)
	}

	var canLogin, inherit, superuser bool
	if err := admin.QueryRowContext(ctx, `SELECT rolcanlogin, rolinherit, rolsuper
		FROM pg_catalog.pg_roles WHERE rolname='ccp_host_session_guard_owner'`).
		Scan(&canLogin, &inherit, &superuser); err != nil {
		t.Fatal(err)
	}
	if canLogin || inherit || superuser {
		t.Fatalf("unsafe session guard owner role: login=%t inherit=%t superuser=%t", canLogin, inherit, superuser)
	}

	var ownerCanSelectSessions, ownerCanInsertSessions bool
	var ownerCanSelectHeartbeats, ownerCanInsertHeartbeats bool
	if err := admin.QueryRowContext(ctx, `
		SELECT
			pg_catalog.has_table_privilege('ccp_host_session_guard_owner', 'public.compute_host_sessions', 'SELECT'),
			pg_catalog.has_table_privilege('ccp_host_session_guard_owner', 'public.compute_host_sessions', 'INSERT'),
			pg_catalog.has_table_privilege('ccp_host_session_guard_owner', 'public.compute_host_session_heartbeats', 'SELECT'),
			pg_catalog.has_table_privilege('ccp_host_session_guard_owner', 'public.compute_host_session_heartbeats', 'INSERT')
	`).Scan(&ownerCanSelectSessions, &ownerCanInsertSessions,
		&ownerCanSelectHeartbeats, &ownerCanInsertHeartbeats); err != nil {
		t.Fatal(err)
	}
	if !ownerCanSelectSessions || !ownerCanInsertSessions ||
		!ownerCanSelectHeartbeats || !ownerCanInsertHeartbeats {
		t.Fatalf("dedicated owner lost its session table privileges: sessions=%t/%t heartbeats=%t/%t",
			ownerCanSelectSessions, ownerCanInsertSessions, ownerCanSelectHeartbeats, ownerCanInsertHeartbeats)
	}

	var unexpectedGrant bool
	if err := admin.QueryRowContext(ctx, `
		SELECT EXISTS (
			SELECT 1 FROM pg_catalog.pg_class c
			CROSS JOIN LATERAL pg_catalog.aclexplode(c.relacl) a
			WHERE c.oid IN ('public.compute_host_sessions'::pg_catalog.regclass,
			                'public.compute_host_session_heartbeats'::pg_catalog.regclass)
			  AND a.grantee <> c.relowner
		) OR EXISTS (
			SELECT 1 FROM pg_catalog.pg_proc p
			CROSS JOIN LATERAL pg_catalog.aclexplode(p.proacl) a
			WHERE p.oid IN (
				'public.compute_host_session_start(text,text,text,text,text,bigint,text,integer)'::pg_catalog.regprocedure,
				'public.compute_host_session_heartbeat(text,text,text,text,text,bigint,text,bigint,text)'::pg_catalog.regprocedure
			) AND (
				a.grantee NOT IN (p.proowner, pg_catalog.to_regrole('ccp_host_session_writer'))
				OR (a.grantee=pg_catalog.to_regrole('ccp_host_session_writer') AND a.grantor <> p.proowner)
			)
		)
	`).Scan(&unexpectedGrant); err != nil {
		t.Fatal(err)
	}
	if unexpectedGrant {
		t.Fatal("HostSession objects have unexpected grants beyond the owner and dedicated writer")
	}

	var alwaysTriggers int
	if err := admin.QueryRowContext(ctx, `
		SELECT count(*)::integer
		FROM pg_catalog.pg_trigger t
		WHERE t.tgrelid IN (
			'public.compute_host_sessions'::pg_catalog.regclass,
			'public.compute_host_session_heartbeats'::pg_catalog.regclass
		)
		  AND t.tgenabled='A'
	`).Scan(&alwaysTriggers); err != nil {
		t.Fatal(err)
	}
	if alwaysTriggers != 4 {
		t.Fatalf("HostSession ENABLE ALWAYS immutable trigger count=%d, want 4", alwaysTriggers)
	}
}

func hostSessionWriterDB(t *testing.T) (*sql.DB, error) {
	t.Helper()
	parsed, err := url.Parse(os.Getenv("DATABASE_URL"))
	if err != nil || parsed.Scheme == "" || parsed.Host == "" {
		return nil, fmt.Errorf("PostgreSQL integration DSN must be a URL: %v", err)
	}
	parsed.User = url.UserPassword("ccp_host_session_writer", "host-session-ci-only")
	writer, err := sql.Open("pgx", parsed.String())
	if err != nil {
		return nil, err
	}
	writer.SetMaxOpenConns(4)
	writer.SetMaxIdleConns(2)
	t.Cleanup(func() { _ = writer.Close() })
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := writer.PingContext(ctx); err != nil {
		return nil, err
	}
	return writer, nil
}

func TestHostSessionInvalidInputsFailBeforeDatabase(t *testing.T) {
	db := openIntegrationDB(t)
	repo, err := NewRepository(db)
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	binding := hostidentity.Binding{HostID: "host-a", ProviderID: "provider-a", ClusterScope: "cluster-a", NodeUID: "node-a", Subject: "spiffe://ccp.example/host/a"}
	for _, ttl := range []time.Duration{0, -time.Second, 24*time.Hour + time.Second, 1500 * time.Millisecond} {
		if _, err := repo.StartHostSession(ctx, HostSessionStartRequest{Binding: binding, ExpectedEpoch: 0, RequestID: "invalid-ttl", TTL: ttl}); !errors.Is(err, ErrHostSessionInvalid) {
			t.Fatalf("invalid session TTL %v accepted: %v", ttl, err)
		}
	}
	if _, err := repo.StartHostSession(ctx, HostSessionStartRequest{Binding: hostidentity.Binding{}, ExpectedEpoch: 0, RequestID: "invalid-binding", TTL: time.Second}); !errors.Is(err, ErrHostSessionInvalid) {
		t.Fatalf("empty HostIdentity binding accepted: %v", err)
	}
	if _, err := repo.StartHostSession(ctx, HostSessionStartRequest{Binding: binding, ExpectedEpoch: 0, RequestID: "", TTL: time.Second}); !errors.Is(err, ErrHostSessionInvalid) {
		t.Fatalf("empty session start idempotency key accepted: %v", err)
	}
	if _, err := repo.StartHostSession(ctx, HostSessionStartRequest{Binding: binding, ExpectedEpoch: -1, RequestID: "negative-epoch", TTL: time.Second}); !errors.Is(err, ErrHostSessionInvalid) {
		t.Fatalf("negative expected epoch accepted: %v", err)
	}

	if _, _, err := repo.RecordHostHeartbeat(ctx, hostidentity.Heartbeat{Binding: binding, Epoch: 1, SessionID: "s", Sequence: 1, Digest: "bad"}); !errors.Is(err, ErrHostSessionInvalid) {
		t.Fatalf("invalid heartbeat digest accepted: %v", err)
	}
}
