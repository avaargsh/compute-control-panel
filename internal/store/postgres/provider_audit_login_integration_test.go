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

	"github.com/avaargsh/compute-control-panel/internal/pgtest"
)

// The administrator only provisions fixtures and verifies durable rows. Every
// registry call uses a separate LOGIN, so session_user cannot remain the CI
// administrator as it would with SET ROLE.
func testProviderAuditWriterDB(t *testing.T, admin *sql.DB) (*sql.DB, string) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	role := "provider_writer_" + randomSuffix(t)
	if err := pgtest.ExecCatalog(ctx, admin, fmt.Sprintf(`CREATE ROLE %s
		LOGIN PASSWORD 'provider-ci-only' NOINHERIT NOSUPERUSER
		NOCREATEDB NOCREATEROLE NOREPLICATION NOBYPASSRLS`, role)); err != nil {
		t.Fatalf("create restricted Provider login: %v", err)
	}
	t.Cleanup(func() {
		cleanup, stop := context.WithTimeout(context.Background(), 15*time.Second)
		defer stop()
		if err := pgtest.CleanupRole(cleanup, admin, role); err != nil {
			t.Errorf("drop role %s: %v", role, err)
		}
	})
	if err := pgtest.ExecCatalog(ctx, admin,
		"GRANT USAGE ON SCHEMA public TO "+role,
		"GRANT SELECT, INSERT, UPDATE ON public.compute_providers TO "+role,
	); err != nil {
		t.Fatalf("grant restricted Provider privileges: %v", err)
	}
	parsed, err := url.Parse(os.Getenv("DATABASE_URL"))
	if err != nil || parsed.Scheme == "" || parsed.Host == "" {
		t.Fatalf("PostgreSQL integration DSN must be a URL: %v", err)
	}
	parsed.User = url.UserPassword(role, "provider-ci-only")
	writer, err := sql.Open("pgx", parsed.String())
	if err != nil {
		t.Fatal(err)
	}
	writer.SetMaxOpenConns(1)
	writer.SetMaxIdleConns(1)
	// Cleanup is LIFO: close sessions (and their TEMP objects) before the role.
	t.Cleanup(func() { _ = writer.Close() })
	var sessionRole, effectiveRole string
	if err := writer.QueryRowContext(ctx, "SELECT session_user, current_user").Scan(&sessionRole, &effectiveRole); err != nil {
		t.Fatalf("connect restricted Provider login: %v", err)
	}
	if sessionRole != role || effectiveRole != role {
		t.Fatalf("expected real login %s; session_user=%s current_user=%s", role, sessionRole, effectiveRole)
	}
	var canInsertAudit, canSetOwner bool
	if err := writer.QueryRowContext(ctx, `SELECT
		has_table_privilege(current_user, 'public.compute_provider_lifecycle_audit', 'INSERT'),
		pg_has_role(current_user, 'ccp_provider_audit_owner', 'SET')`).
		Scan(&canInsertAudit, &canSetOwner); err != nil {
		t.Fatal(err)
	}
	if canInsertAudit || canSetOwner {
		t.Fatalf("Provider writer has privileged audit access: INSERT=%t SET owner=%t",
			canInsertAudit, canSetOwner)
	}
	return writer, role
}

func requireProviderPermissionDenied(t *testing.T, ctx context.Context, writer *sql.DB, query string, args ...any) {
	t.Helper()
	_, err := writer.ExecContext(ctx, query, args...)
	var sqlErr interface{ SQLState() string }
	if !errors.As(err, &sqlErr) || sqlErr.SQLState() != "42501" {
		t.Fatalf("expected permission denied (42501) for %q, got %v", query, err)
	}
}

func requireProviderAuditRows(t *testing.T, ctx context.Context, admin *sql.DB, providerID, actor string, want int) {
	t.Helper()
	rows, err := admin.QueryContext(ctx, `SELECT version, previous_state, state,
		reason_code, actor_db_role, recorded_at
		FROM public.compute_provider_lifecycle_audit WHERE provider_id=$1 ORDER BY version`, providerID)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	count := 0
	for rows.Next() {
		var version int64
		var previous sql.NullString
		var state, reason, recordedActor string
		var recordedAt time.Time
		if err := rows.Scan(&version, &previous, &state, &reason, &recordedActor, &recordedAt); err != nil {
			t.Fatal(err)
		}
		count++
		if version != int64(count) || recordedActor != actor || recordedAt.IsZero() {
			t.Fatalf("wrong audit identity/version: version=%d actor=%s recorded_at=%s", version, recordedActor, recordedAt)
		}
		switch count {
		case 1:
			if previous.Valid || state != "REGISTERED" || reason != "REGISTERED" {
				t.Fatalf("wrong registration audit: previous=%+v state=%s reason=%s", previous, state, reason)
			}
		case 2:
			if !previous.Valid || previous.String != "REGISTERED" || state != "DRAINING" || reason == "REGISTERED" {
				t.Fatalf("wrong Drain audit: previous=%+v state=%s reason=%s", previous, state, reason)
			}
		default:
			t.Fatalf("unexpected extra Provider audit version %d", version)
		}
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	if count != want {
		t.Fatalf("Provider audit count=%d, want %d", count, want)
	}
}

func TestProviderAuditRestrictedLoginRepositoryLifecycle(t *testing.T) {
	admin := openIntegrationDB(t)
	writer, actor := testProviderAuditWriterDB(t, admin)
	repo, err := NewRepository(writer)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	p := providerRegistrationFixture(t)
	registered, created, err := repo.RegisterProvider(ctx, p)
	if err != nil || !created || registered.State != ProviderRegistered || registered.Version != 1 {
		t.Fatalf("restricted login registration: %+v created=%t err=%v", registered, created, err)
	}
	got, err := repo.GetProvider(ctx, p.ProviderID)
	if err != nil || got.ProviderRegistration != p || got.State != ProviderRegistered || got.Version != 1 {
		t.Fatalf("restricted login read: %+v err=%v", got, err)
	}
	replay, created, err := repo.RegisterProvider(ctx, p)
	if err != nil || created || replay.ProviderRegistration != p || replay.Version != 1 {
		t.Fatalf("restricted login replay: %+v created=%t err=%v", replay, created, err)
	}
	conflict := p
	conflict.CapabilityDigest = "sha256:" + strings.Repeat("b", 64)
	if _, _, err := repo.RegisterProvider(ctx, conflict); !errors.Is(err, ErrProviderConflict) {
		t.Fatalf("restricted login changed immutable registration: %v", err)
	}

	// This is a structurally valid v2 event for an existing REGISTERED Provider;
	// only the privilege boundary, rather than a CHECK constraint, rejects it.
	for _, denied := range []struct {
		name  string
		query string
		args  []any
	}{
		{"forged audit INSERT", `INSERT INTO public.compute_provider_lifecycle_audit
			(provider_id, version, previous_state, state, reason_code, actor_db_role)
			VALUES ($1,2,'REGISTERED','DRAINING','FORGED','forged-login')`, []any{p.ProviderID}},
		{"audit UPDATE", "UPDATE public.compute_provider_lifecycle_audit SET actor_db_role='forged-login' WHERE provider_id=$1", []any{p.ProviderID}},
		{"audit DELETE", "DELETE FROM public.compute_provider_lifecycle_audit WHERE provider_id=$1", []any{p.ProviderID}},
		{"audit TRUNCATE", "TRUNCATE public.compute_provider_lifecycle_audit", nil},
		{"Provider TRUNCATE", "TRUNCATE public.compute_providers", nil},
		{"disable trigger", "ALTER TABLE public.compute_providers DISABLE TRIGGER compute_provider_lifecycle_audit_append", nil},
		{"drop trigger", "DROP TRIGGER compute_provider_lifecycle_audit_append ON public.compute_providers", nil},
		{"alter function", "ALTER FUNCTION public.compute_provider_lifecycle_audit_append() SECURITY INVOKER", nil},
		{"replace function", `CREATE OR REPLACE FUNCTION public.compute_provider_lifecycle_audit_append()
			RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN RETURN NEW; END; $$`, nil},
		{"call audit function", "SELECT public.compute_provider_lifecycle_audit_append()", nil},
		{"assume audit owner", "SET ROLE ccp_provider_audit_owner", nil},
		{"disable origin triggers", "SET session_replication_role='replica'", nil},
	} {
		t.Run(denied.name, func(t *testing.T) {
			requireProviderPermissionDenied(t, ctx, writer, denied.query, denied.args...)
		})
	}
	requireProviderAuditRows(t, ctx, admin, p.ProviderID, actor, 1)
	drained, err := repo.BeginProviderDrain(ctx, p.ProviderID, 1, "MAINTENANCE")
	if err != nil || drained.State != ProviderDraining || drained.Version != 2 {
		t.Fatalf("restricted login Drain: %+v err=%v", drained, err)
	}
	if _, err := repo.BeginProviderDrain(ctx, p.ProviderID, 1, "RETRY"); !errors.Is(err, ErrProviderStale) {
		t.Fatalf("stale restricted login Drain: %v", err)
	}
	if _, err := repo.BeginProviderDrain(ctx, p.ProviderID, 2, "RETRY"); !errors.Is(err, ErrProviderTransition) {
		t.Fatalf("repeated restricted login Drain: %v", err)
	}
	replay, created, err = repo.RegisterProvider(ctx, p)
	if err != nil || created || replay.State != ProviderDraining || replay.Version != 2 {
		t.Fatalf("restricted login replay resurrected Provider: %+v created=%t err=%v", replay, created, err)
	}
	requireProviderAuditRows(t, ctx, admin, p.ProviderID, actor, 2)
}

func TestProviderAuditRestrictedLoginConcurrentDrain(t *testing.T) {
	admin := openIntegrationDB(t)
	writer, actor := testProviderAuditWriterDB(t, admin)
	writer.SetMaxOpenConns(8)
	writer.SetMaxIdleConns(8)
	repo, err := NewRepository(writer)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	p := providerRegistrationFixture(t)
	if _, created, err := repo.RegisterProvider(ctx, p); err != nil || !created {
		t.Fatalf("restricted login registration: created=%t err=%v", created, err)
	}
	var wg sync.WaitGroup
	errs := make([]error, 8)
	for i := range errs {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			_, errs[i] = repo.BeginProviderDrain(ctx, p.ProviderID, 1, "DRAIN_REQUEST")
		}(i)
	}
	wg.Wait()
	winners := 0
	for _, err := range errs {
		if err == nil {
			winners++
		} else if !errors.Is(err, ErrProviderStale) {
			t.Fatalf("restricted login concurrent Drain: %v", err)
		}
	}
	if winners != 1 {
		t.Fatalf("restricted login concurrent Drain winners=%d, want 1", winners)
	}
	requireProviderAuditRows(t, ctx, admin, p.ProviderID, actor, 2)
}

func TestProviderAuditRestrictedLoginTemporaryShadows(t *testing.T) {
	admin := openIntegrationDB(t)
	writer, actor := testProviderAuditWriterDB(t, admin)
	repo, err := NewRepository(writer)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	// The one-connection pool keeps both shadows and the caller-controlled
	// search_path in the same session as every repository call.
	for _, statement := range []string{
		"CREATE TEMP TABLE compute_providers (LIKE public.compute_providers INCLUDING ALL)",
		`CREATE TEMP TABLE compute_provider_lifecycle_audit (
			provider_id text, version bigint, previous_state text, state text,
			reason_code text, actor_db_role text, recorded_at timestamptz)`,
		"SET search_path = pg_temp, public, pg_catalog",
	} {
		if _, err := writer.ExecContext(ctx, statement); err != nil {
			t.Fatalf("create caller-controlled Provider shadows: %v", err)
		}
	}
	p := providerRegistrationFixture(t)
	if _, created, err := repo.RegisterProvider(ctx, p); err != nil || !created {
		t.Fatalf("register real Provider despite shadows: created=%t err=%v", created, err)
	}
	requireProviderAuditRows(t, ctx, admin, p.ProviderID, actor, 1)
	var shadowRows int
	if err := writer.QueryRowContext(ctx, "SELECT count(*) FROM pg_temp.compute_providers").Scan(&shadowRows); err != nil {
		t.Fatal(err)
	}
	if shadowRows != 0 {
		t.Fatalf("registration wrote %d phantom TEMP Providers", shadowRows)
	}
	if _, err := writer.ExecContext(ctx, `INSERT INTO pg_temp.compute_providers
		SELECT * FROM public.compute_providers WHERE provider_id=$1`, p.ProviderID); err != nil {
		t.Fatal(err)
	}
	if _, err := writer.ExecContext(ctx, `UPDATE pg_temp.compute_providers
		SET state='DRAINING', version=2, last_reason_code='TEMP_ONLY', capability_digest=$1`,
		"sha256:"+strings.Repeat("b", 64)); err != nil {
		t.Fatal(err)
	}
	got, err := repo.GetProvider(ctx, p.ProviderID)
	if err != nil || got.ProviderRegistration != p || got.State != ProviderRegistered || got.Version != 1 {
		t.Fatalf("TEMP Provider changed authoritative read: %+v err=%v", got, err)
	}
	replay, created, err := repo.RegisterProvider(ctx, p)
	if err != nil || created || replay.State != ProviderRegistered || replay.Version != 1 {
		t.Fatalf("TEMP Provider changed authoritative replay: %+v created=%t err=%v", replay, created, err)
	}
	drained, err := repo.BeginProviderDrain(ctx, p.ProviderID, 1, "MAINTENANCE")
	if err != nil || drained.State != ProviderDraining || drained.Version != 2 {
		t.Fatalf("TEMP Provider changed authoritative Drain: %+v err=%v", drained, err)
	}
	var shadowState, shadowReason, shadowDigest string
	var shadowVersion int64
	if err := writer.QueryRowContext(ctx, `SELECT state, version, last_reason_code, capability_digest
		FROM pg_temp.compute_providers WHERE provider_id=$1`, p.ProviderID).
		Scan(&shadowState, &shadowVersion, &shadowReason, &shadowDigest); err != nil {
		t.Fatal(err)
	}
	if shadowState != "DRAINING" || shadowVersion != 2 || shadowReason != "TEMP_ONLY" || shadowDigest != "sha256:"+strings.Repeat("b", 64) {
		t.Fatalf("repository changed TEMP Provider: state=%s version=%d reason=%s digest=%s", shadowState, shadowVersion, shadowReason, shadowDigest)
	}
	if err := writer.QueryRowContext(ctx, "SELECT count(*) FROM pg_temp.compute_provider_lifecycle_audit").Scan(&shadowRows); err != nil {
		t.Fatal(err)
	}
	if shadowRows != 0 {
		t.Fatalf("audit function wrote %d phantom TEMP audit rows", shadowRows)
	}
	requireProviderAuditRows(t, ctx, admin, p.ProviderID, actor, 2)
}

func TestProviderAuditRestrictedLoginRejectsPreattachedForeignTrigger(t *testing.T) {
	admin := openIntegrationDB(t)
	writer, actor := testProviderAuditWriterDB(t, admin)
	repo, err := NewRepository(writer)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	p := providerRegistrationFixture(t)
	if _, created, err := repo.RegisterProvider(ctx, p); err != nil || !created {
		t.Fatalf("register real Provider: created=%t err=%v", created, err)
	}
	if _, err := writer.ExecContext(ctx, `CREATE TEMP TABLE compute_providers AS
		SELECT * FROM public.compute_providers WHERE provider_id=$1`, p.ProviderID); err != nil {
		t.Fatalf("create TEMP Provider trigger source: %v", err)
	}
	const attach = `CREATE TRIGGER forged_provider_audit AFTER UPDATE ON pg_temp.compute_providers
		FOR EACH ROW EXECUTE FUNCTION public.compute_provider_lifecycle_audit_append()`
	requireProviderPermissionDenied(t, ctx, writer, attach)
	// PostgreSQL checks function EXECUTE when attaching a trigger, not on each
	// invocation. Simulate a trigger attached before the forward migration
	// revoked the historical function's PUBLIC EXECUTE privilege.
	if err := pgtest.ExecCatalog(ctx, admin, "GRANT EXECUTE ON FUNCTION public.compute_provider_lifecycle_audit_append() TO "+actor); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		cleanup, stop := context.WithTimeout(context.Background(), 15*time.Second)
		defer stop()
		if err := pgtest.ExecCatalogCleanup(cleanup, admin, "REVOKE EXECUTE ON FUNCTION public.compute_provider_lifecycle_audit_append() FROM "+actor); err != nil {
			t.Errorf("revoke historical function execute: %v", err)
		}
	})
	if _, err := writer.ExecContext(ctx, attach); err != nil {
		t.Fatalf("attach historical caller-owned trigger: %v", err)
	}
	if err := pgtest.ExecCatalog(ctx, admin, "REVOKE EXECUTE ON FUNCTION public.compute_provider_lifecycle_audit_append() FROM "+actor); err != nil {
		t.Fatal(err)
	}
	requireProviderPermissionDenied(t, ctx, writer, `UPDATE pg_temp.compute_providers
		SET state='DRAINING', version=2, last_reason_code='FORGED' WHERE provider_id=$1`, p.ProviderID)
	got, err := repo.GetProvider(ctx, p.ProviderID)
	if err != nil || got.State != ProviderRegistered || got.Version != 1 {
		t.Fatalf("foreign trigger changed real Provider: %+v err=%v", got, err)
	}
	var shadowState string
	var shadowVersion int64
	if err := writer.QueryRowContext(ctx, "SELECT state, version FROM pg_temp.compute_providers WHERE provider_id=$1", p.ProviderID).
		Scan(&shadowState, &shadowVersion); err != nil {
		t.Fatal(err)
	}
	if shadowState != "REGISTERED" || shadowVersion != 1 {
		t.Fatalf("foreign trigger rejection did not roll back TEMP row: state=%s version=%d", shadowState, shadowVersion)
	}
	requireProviderAuditRows(t, ctx, admin, p.ProviderID, actor, 1)
}
