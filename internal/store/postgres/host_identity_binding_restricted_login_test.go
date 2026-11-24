package postgres

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"net/url"
	"os"
	"testing"
	"time"

	"github.com/avaargsh/compute-control-panel/internal/pgtest"
)

// A separate LOGIN proves that enrollment does not depend on the Provider
// table owner or a caller with Provider SELECT/UPDATE privileges.
func testHostIdentityRestrictedWriterDB(t *testing.T, admin *sql.DB) (*sql.DB, string) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	role := "host_identity_writer_" + randomSuffix(t)
	if err := pgtest.ExecCatalog(ctx, admin, fmt.Sprintf(`CREATE ROLE %s
		LOGIN PASSWORD 'host-identity-ci-only' NOINHERIT NOSUPERUSER
		NOCREATEDB NOCREATEROLE NOREPLICATION NOBYPASSRLS`, role)); err != nil {
		t.Fatalf("create restricted HostIdentity login: %v", err)
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
		"GRANT INSERT ON public.compute_host_identity_bindings TO "+role,
	); err != nil {
		t.Fatalf("grant minimal HostIdentity INSERT: %v", err)
	}

	parsed, err := url.Parse(os.Getenv("DATABASE_URL"))
	if err != nil || parsed.Scheme == "" || parsed.Host == "" {
		t.Fatalf("PostgreSQL integration DSN must be a URL: %v", err)
	}
	parsed.User = url.UserPassword(role, "host-identity-ci-only")
	writer, err := sql.Open("pgx", parsed.String())
	if err != nil {
		t.Fatal(err)
	}
	writer.SetMaxOpenConns(2)
	writer.SetMaxIdleConns(1)
	t.Cleanup(func() { _ = writer.Close() })

	var sessionRole, effectiveRole string
	if err := writer.QueryRowContext(ctx, "SELECT session_user, current_user").Scan(&sessionRole, &effectiveRole); err != nil {
		t.Fatalf("connect restricted HostIdentity login: %v", err)
	}
	if sessionRole != role || effectiveRole != role {
		t.Fatalf("expected distinct HostIdentity LOGIN %s; session_user=%s current_user=%s", role, sessionRole, effectiveRole)
	}
	var canSelectProvider, canUpdateProvider bool
	if err := writer.QueryRowContext(ctx, `SELECT
		pg_catalog.has_table_privilege(current_user, 'public.compute_providers', 'SELECT'),
		pg_catalog.has_table_privilege(current_user, 'public.compute_providers', 'UPDATE')`).
		Scan(&canSelectProvider, &canUpdateProvider); err != nil {
		t.Fatal(err)
	}
	if canSelectProvider || canUpdateProvider {
		t.Fatalf("restricted HostIdentity login unexpectedly has Provider access: SELECT=%t UPDATE=%t",
			canSelectProvider, canUpdateProvider)
	}
	return writer, role
}

func TestHostIdentityBindingRestrictedLoginDoesNotNeedProviderPrivileges(t *testing.T) {
	admin := openIntegrationDB(t)
	writer, actor := testHostIdentityRestrictedWriterDB(t, admin)
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	p := providerRegistrationFixture(t)
	repo, err := NewRepository(admin)
	if err != nil {
		t.Fatal(err)
	}
	if _, created, err := repo.RegisterProvider(ctx, p); err != nil || !created {
		t.Fatalf("register Provider fixture: created=%t err=%v", created, err)
	}

	hostID := "host-" + randomSuffix(t)
	nodeUID := "node-" + randomSuffix(t)
	subject := "spiffe://ccp.example/host/" + hostID
	_, err = writer.ExecContext(ctx, `INSERT INTO public.compute_host_identity_bindings
		(host_id, provider_id, cluster_scope, node_uid, subject)
		VALUES ($1,$2,$3,$4,$5)`, hostID, p.ProviderID, p.ClusterScope, nodeUID, subject)
	if err != nil {
		t.Fatalf("restricted LOGIN with only HostIdentity INSERT must enroll against a registered Provider: %v", err)
	}

	var count int
	if err := admin.QueryRowContext(ctx,
		"SELECT count(*) FROM public.compute_host_identity_bindings WHERE host_id=$1", hostID).Scan(&count); err != nil {
		t.Fatal(err)
	}
	if count != 1 {
		t.Fatalf("durable binding count=%d, want 1 (writer=%s)", count, actor)
	}

	// The login can insert only. It cannot read or mutate the Provider
	// registry, mutate/delete a binding, truncate the table, or execute the
	// privileged trigger function directly.
	for _, tc := range []struct {
		name  string
		query string
		args  []any
	}{
		{"Provider SELECT", "SELECT provider_id FROM public.compute_providers WHERE provider_id=$1", []any{p.ProviderID}},
		{"Provider UPDATE", "UPDATE public.compute_providers SET state=state WHERE provider_id=$1", []any{p.ProviderID}},
		{"binding UPDATE", "UPDATE public.compute_host_identity_bindings SET subject=subject WHERE host_id=$1", []any{hostID}},
		{"binding DELETE", "DELETE FROM public.compute_host_identity_bindings WHERE host_id=$1", []any{hostID}},
		{"binding TRUNCATE", "TRUNCATE public.compute_host_identity_bindings", nil},
		{"direct guard execution", "SELECT public.compute_host_identity_enrollment_guard()", nil},
		{"assume guard owner", "SET ROLE ccp_host_identity_guard_owner", nil},
	} {
		t.Run(tc.name, func(t *testing.T) {
			_, err := writer.ExecContext(ctx, tc.query, tc.args...)
			var state interface{ SQLState() string }
			if !errors.As(err, &state) || state.SQLState() != "42501" {
				t.Fatalf("want SQLSTATE 42501 for restricted operation %q, got %v", tc.name, err)
			}
		})
	}
}

func TestHostIdentityGuardOwnerCatalogBoundary(t *testing.T) {
	db := openIntegrationDB(t)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	var securityDefiner, publicExecute, unexpectedFunctionExecute bool
	var owner, config string
	err := db.QueryRowContext(ctx, `
		SELECT p.prosecdef,
		       pg_catalog.pg_get_userbyid(p.proowner),
		       pg_catalog.array_to_string(p.proconfig, ','),
		       EXISTS (
		         SELECT 1
		         FROM pg_catalog.aclexplode(
		           COALESCE(p.proacl, pg_catalog.acldefault('f', p.proowner))
		         ) a
		         WHERE a.grantee=0 AND a.privilege_type='EXECUTE'
		       ),
		       EXISTS (
		         SELECT 1
		         FROM pg_catalog.aclexplode(p.proacl) a
		         WHERE a.grantee <> p.proowner AND a.privilege_type='EXECUTE'
		       )
		FROM pg_catalog.pg_proc p
		WHERE p.oid='public.compute_host_identity_enrollment_guard()'::pg_catalog.regprocedure
	`).Scan(&securityDefiner, &owner, &config, &publicExecute, &unexpectedFunctionExecute)
	if err != nil {
		t.Fatal(err)
	}
	if !securityDefiner || owner != "ccp_host_identity_guard_owner" ||
		config != "search_path=pg_catalog, pg_temp" || publicExecute || unexpectedFunctionExecute {
		t.Fatalf("unsafe HostIdentity guard: security_definer=%t owner=%q search_path=%q public_execute=%t unexpected_execute=%t",
			securityDefiner, owner, config, publicExecute, unexpectedFunctionExecute)
	}

	var canLogin, inherit, superuser bool
	var selectProviderID, selectCluster, selectState bool
	var updateProviderID, updateState bool
	err = db.QueryRowContext(ctx, `
		SELECT r.rolcanlogin, r.rolinherit, r.rolsuper,
		       pg_catalog.has_column_privilege(r.oid, 'public.compute_providers', 'provider_id', 'SELECT'),
		       pg_catalog.has_column_privilege(r.oid, 'public.compute_providers', 'cluster_scope', 'SELECT'),
		       pg_catalog.has_column_privilege(r.oid, 'public.compute_providers', 'state', 'SELECT'),
		       pg_catalog.has_column_privilege(r.oid, 'public.compute_providers', 'provider_id', 'UPDATE'),
		       pg_catalog.has_column_privilege(r.oid, 'public.compute_providers', 'state', 'UPDATE')
		FROM pg_catalog.pg_roles r
		WHERE r.rolname='ccp_host_identity_guard_owner'
	`).Scan(&canLogin, &inherit, &superuser, &selectProviderID, &selectCluster,
		&selectState, &updateProviderID, &updateState)
	if err != nil {
		t.Fatal(err)
	}
	if canLogin || inherit || superuser || !selectProviderID || !selectCluster ||
		!selectState || !updateProviderID || updateState {
		t.Fatalf("unsafe HostIdentity guard owner: login=%t inherit=%t superuser=%t SELECT(provider_id,cluster,state)=%t/%t/%t UPDATE(provider_id,state)=%t/%t",
			canLogin, inherit, superuser, selectProviderID, selectCluster, selectState,
			updateProviderID, updateState)
	}

	var alwaysTriggers int
	if err := db.QueryRowContext(ctx, `
		SELECT count(*)::integer
		FROM pg_catalog.pg_trigger t
		WHERE t.tgrelid='public.compute_host_identity_bindings'::pg_catalog.regclass
		  AND t.tgfoid='public.compute_host_identity_enrollment_guard()'::pg_catalog.regprocedure
		  AND t.tgenabled='A'
	`).Scan(&alwaysTriggers); err != nil {
		t.Fatal(err)
	}
	if alwaysTriggers != 3 {
		t.Fatalf("HostIdentity ALWAYS guard trigger count=%d, want 3", alwaysTriggers)
	}

	var unexpectedBindingGrant bool
	if err := db.QueryRowContext(ctx, `
		SELECT EXISTS (
			SELECT 1
			FROM pg_catalog.pg_class c
			CROSS JOIN LATERAL pg_catalog.aclexplode(c.relacl) a
			WHERE c.oid='public.compute_host_identity_bindings'::pg_catalog.regclass
			  AND a.grantee <> c.relowner
		) OR EXISTS (
			SELECT 1
			FROM pg_catalog.pg_attribute col
			CROSS JOIN LATERAL pg_catalog.aclexplode(col.attacl) a
			WHERE col.attrelid='public.compute_host_identity_bindings'::pg_catalog.regclass
			  AND a.grantee <> (
			    SELECT c.relowner FROM pg_catalog.pg_class c
			    WHERE c.oid='public.compute_host_identity_bindings'::pg_catalog.regclass
			  )
		)
	`).Scan(&unexpectedBindingGrant); err != nil {
		t.Fatal(err)
	}
	if unexpectedBindingGrant {
		t.Fatal("HostIdentity binding table has grants beyond its owner")
	}
}
