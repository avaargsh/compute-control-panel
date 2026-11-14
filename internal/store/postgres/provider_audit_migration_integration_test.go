package postgres

import (
	"context"
	"database/sql"
	"errors"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/avaargsh/compute-control-panel/internal/migrate"
	"github.com/avaargsh/compute-control-panel/internal/pgtest"
	"github.com/jackc/pgx/v5/pgconn"
)

const providerAuditOwnerRole = "ccp_provider_audit_owner"

func providerAuditMigrations(t *testing.T) []migrate.Migration {
	t.Helper()
	migrations, err := migrate.Load(os.DirFS(filepath.Join("..", "..", "..", "migrations")))
	if err != nil {
		t.Fatalf("load Provider audit migrations: %v", err)
	}
	if len(migrations) < 13 || migrations[12].Version != 13 {
		t.Fatal("Provider audit hardening requires forward migration 0013")
	}
	return migrations[:13]
}

func requireProviderAuditFixtureAdmin(t *testing.T, db *sql.DB) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	var superuser bool
	if err := db.QueryRowContext(ctx, "SELECT rolsuper FROM pg_roles WHERE rolname=current_user").Scan(&superuser); err != nil {
		t.Fatal(err)
	}
	if !superuser {
		t.Skip("requires disposable PostgreSQL CI superuser for role and database fixtures")
	}
}

func assertProviderAuditMigrationCatalog(t *testing.T, db *sql.DB) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	var login, inherit, superuser, createDB, createRole, replication, bypassRLS bool
	if err := db.QueryRowContext(ctx, `SELECT rolcanlogin, rolinherit, rolsuper, rolcreatedb,
		rolcreaterole, rolreplication, rolbypassrls FROM pg_roles WHERE rolname=$1`, providerAuditOwnerRole).
		Scan(&login, &inherit, &superuser, &createDB, &createRole, &replication, &bypassRLS); err != nil {
		t.Fatalf("read audit owner attributes: %v", err)
	}
	if login || inherit || superuser || createDB || createRole || replication || bypassRLS {
		t.Fatalf("audit owner has unsafe attributes: login=%t inherit=%t superuser=%t createdb=%t createrole=%t replication=%t bypassrls=%t",
			login, inherit, superuser, createDB, createRole, replication, bypassRLS)
	}
	var definer, publicExecute bool
	var owner, config string
	if err := db.QueryRowContext(ctx, `SELECT p.prosecdef, pg_get_userbyid(p.proowner),
		COALESCE(array_to_string(p.proconfig, ';'), ''), EXISTS (
			SELECT 1 FROM aclexplode(COALESCE(p.proacl, acldefault('f', p.proowner))) a
			WHERE a.grantee=0 AND a.privilege_type='EXECUTE')
		FROM pg_proc p WHERE p.oid='public.compute_provider_lifecycle_audit_append()'::regprocedure`).
		Scan(&definer, &owner, &config, &publicExecute); err != nil {
		t.Fatalf("read audit function catalog: %v", err)
	}
	if !definer || owner != providerAuditOwnerRole || config != "search_path=pg_catalog, pg_temp" || publicExecute {
		t.Fatalf("unsafe audit function: definer=%t owner=%q config=%q public_execute=%t", definer, owner, config, publicExecute)
	}
	var schemaUsage, schemaCreate, auditInsert, auditOther, providerDML, roleMembership bool
	if err := db.QueryRowContext(ctx, `SELECT
		has_schema_privilege($1, 'public', 'USAGE'),
		has_schema_privilege($1, 'public', 'CREATE'),
		has_table_privilege($1, 'public.compute_provider_lifecycle_audit', 'INSERT'),
		has_table_privilege($1, 'public.compute_provider_lifecycle_audit', 'SELECT,UPDATE,DELETE,TRUNCATE,REFERENCES,TRIGGER')
			OR has_any_column_privilege($1, 'public.compute_provider_lifecycle_audit', 'SELECT,UPDATE,REFERENCES'),
		has_table_privilege($1, 'public.compute_providers', 'SELECT,INSERT,UPDATE,DELETE,TRUNCATE,REFERENCES,TRIGGER')
			OR has_any_column_privilege($1, 'public.compute_providers', 'SELECT,INSERT,UPDATE,REFERENCES'),
		EXISTS (SELECT 1 FROM pg_auth_members m JOIN pg_roles r ON r.oid=m.member WHERE r.rolname=$1)`, providerAuditOwnerRole).
		Scan(&schemaUsage, &schemaCreate, &auditInsert, &auditOther, &providerDML, &roleMembership); err != nil {
		t.Fatalf("read audit owner privileges: %v", err)
	}
	if !schemaUsage || schemaCreate || !auditInsert || auditOther || providerDML || roleMembership {
		t.Fatalf("unsafe audit owner privileges: usage=%t create=%t insert=%t other_audit=%t provider_dml=%t membership=%t",
			schemaUsage, schemaCreate, auditInsert, auditOther, providerDML, roleMembership)
	}
	var unexpectedInsert bool
	if err := db.QueryRowContext(ctx, `SELECT EXISTS (
		SELECT 1 FROM pg_class c CROSS JOIN LATERAL
			aclexplode(COALESCE(c.relacl, acldefault('r', c.relowner))) a
		WHERE c.oid='public.compute_provider_lifecycle_audit'::regclass
			AND a.privilege_type='INSERT' AND a.grantee<>c.relowner
			AND a.grantee<>(SELECT oid FROM pg_roles WHERE rolname=$1)
		UNION ALL
		SELECT 1 FROM pg_attribute att JOIN pg_class c ON c.oid=att.attrelid
			CROSS JOIN LATERAL aclexplode(att.attacl) a
		WHERE c.oid='public.compute_provider_lifecycle_audit'::regclass
			AND att.attnum>0 AND NOT att.attisdropped AND a.privilege_type='INSERT'
			AND a.grantee<>c.relowner AND a.grantee<>(SELECT oid FROM pg_roles WHERE rolname=$1)
	)`, providerAuditOwnerRole).Scan(&unexpectedInsert); err != nil {
		t.Fatalf("read audit INSERT grants: %v", err)
	}
	if unexpectedInsert {
		t.Fatal("audit table retains a non-owner table or column INSERT grant")
	}
}

func TestProviderAuditMigrationCatalog(t *testing.T) {
	assertProviderAuditMigrationCatalog(t, openIntegrationDB(t))
}

// Role changes and deliberately unsafe grants remain uncommitted. Executing
// the migration SQL directly exercises its preflight without replacing the
// checked migration ledger or persisting a cluster-wide role mutation.
func TestProviderAuditMigrationRejectsUnsafePreprovisioning(t *testing.T) {
	admin := openIntegrationDB(t)
	requireProviderAuditFixtureAdmin(t, admin)
	migration := providerAuditMigrations(t)[12]
	cases := []struct {
		name  string
		setup func(string) []string
	}{
		{
			name: "missing owner",
			setup: func(suffix string) []string {
				return []string{"ALTER ROLE " + providerAuditOwnerRole + " RENAME TO ccp_audit_absent_" + suffix}
			},
		},
		{
			name: "login owner",
			setup: func(string) []string {
				return []string{"ALTER ROLE " + providerAuditOwnerRole + " LOGIN"}
			},
		},
		{
			name: "admin-only audit owner membership",
			setup: func(suffix string) []string {
				role := "ccp_audit_admin_only_" + suffix
				return []string{
					"CREATE ROLE " + role + " NOLOGIN",
					"GRANT " + providerAuditOwnerRole + " TO " + role + " WITH INHERIT FALSE, SET FALSE, ADMIN TRUE",
				}
			},
		},
		{
			name: "direct table insert grant",
			setup: func(suffix string) []string {
				role := "ccp_audit_untrusted_" + suffix
				return []string{"CREATE ROLE " + role + " NOLOGIN", "GRANT INSERT ON public.compute_provider_lifecycle_audit TO " + role}
			},
		},
		{
			name: "direct column insert grant",
			setup: func(suffix string) []string {
				role := "ccp_audit_untrusted_" + suffix
				return []string{"CREATE ROLE " + role + " NOLOGIN", "GRANT INSERT (provider_id) ON public.compute_provider_lifecycle_audit TO " + role}
			},
		},
		{
			name: "owner foreign column grant",
			setup: func(string) []string {
				return []string{"GRANT UPDATE (state) ON public.compute_providers TO " + providerAuditOwnerRole}
			},
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
			defer cancel()
			conn, release, err := pgtest.LockCatalogConn(ctx, admin)
			if err != nil {
				t.Fatal(err)
			}
			defer release()
			tx, err := conn.BeginTx(ctx, nil)
			if err != nil {
				t.Fatal(err)
			}
			// Roll back while the catalog lock is still held so another package
			// cannot interleave role DDL with this transaction's catalog locks.
			defer func() { _ = tx.Rollback() }()
			for _, statement := range tc.setup(randomSuffix(t)) {
				if _, err := tx.ExecContext(ctx, statement); err != nil {
					t.Fatalf("prepare unsafe role fixture: %v", err)
				}
			}
			_, err = tx.ExecContext(ctx, migration.SQL)
			var pgErr *pgconn.PgError
			if !errors.As(err, &pgErr) || pgErr.Code != "P0001" || !strings.Contains(strings.ToLower(pgErr.Message), "audit") {
				t.Fatalf("expected intentional audit preflight rejection, got %v", err)
			}
		})
	}
	assertProviderAuditMigrationCatalog(t, admin)
}

// This database is owned by a real non-superuser login with no CREATEROLE.
// The audit owner is externally provisioned once in this disposable cluster;
// the migration must only perform its database-local grants and ownership swap.
func TestProviderAuditMigrationWithRestrictedMigrationLogin(t *testing.T) {
	admin := openIntegrationDB(t)
	requireProviderAuditFixtureAdmin(t, admin)
	migrations := providerAuditMigrations(t)
	suffix := randomSuffix(t)
	role := "ccp_audit_migrator_" + suffix
	database := "ccp_audit_migration_" + suffix
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	if err := pgtest.ExecCatalog(ctx, admin, "CREATE ROLE "+role+" LOGIN PASSWORD 'provider-audit-ci-only' NOINHERIT NOSUPERUSER NOCREATEDB NOCREATEROLE NOREPLICATION NOBYPASSRLS"); err != nil {
		t.Fatalf("create migration LOGIN fixture: %v", err)
	}
	t.Cleanup(func() {
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 15*time.Second)
		defer cleanupCancel()
		if _, err := admin.ExecContext(cleanupCtx, "DROP DATABASE IF EXISTS "+database+" WITH (FORCE)"); err != nil {
			t.Errorf("drop migration database fixture: %v", err)
		}
		if err := pgtest.CleanupRole(cleanupCtx, admin, role); err != nil {
			t.Errorf("drop migration LOGIN fixture: %v", err)
		}
	})
	if err := pgtest.ExecCatalog(ctx, admin, "GRANT "+providerAuditOwnerRole+" TO "+role+" WITH INHERIT FALSE, SET TRUE"); err != nil {
		t.Fatalf("provision trusted ownership-transfer membership: %v", err)
	}
	if _, err := admin.ExecContext(ctx, "CREATE DATABASE "+database+" OWNER "+role); err != nil {
		t.Fatalf("create fresh migration database: %v", err)
	}
	parsed, err := url.Parse(os.Getenv("DATABASE_URL"))
	if err != nil || parsed.Scheme == "" || parsed.Host == "" {
		t.Fatalf("PostgreSQL integration DSN must be a URL: %v", err)
	}
	parsed.User = url.UserPassword(role, "provider-audit-ci-only")
	parsed.Path = "/" + database
	parsed.RawPath = ""
	query := parsed.Query()
	query.Set("search_path", "public")
	parsed.RawQuery = query.Encode()
	migrationDB, err := sql.Open("pgx", parsed.String())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = migrationDB.Close() })
	migrationDB.SetMaxOpenConns(2)
	var currentRole, sessionRole, currentDatabase string
	var superuser, createRole bool
	if err := migrationDB.QueryRowContext(ctx, `SELECT current_user, session_user, current_database(), rolsuper, rolcreaterole
		FROM pg_roles WHERE rolname=current_user`).Scan(&currentRole, &sessionRole, &currentDatabase, &superuser, &createRole); err != nil {
		t.Fatalf("connect restricted migration LOGIN: %v", err)
	}
	if currentRole != role || sessionRole != role || currentDatabase != database || superuser || createRole {
		t.Fatalf("unexpected migration identity: current=%q session=%q database=%q superuser=%t createrole=%t",
			currentRole, sessionRole, currentDatabase, superuser, createRole)
	}
	versions, err := migrate.Apply(ctx, migrationDB, migrations)
	if err != nil {
		t.Fatalf("restricted LOGIN fresh migration: %v", err)
	}
	if len(versions) != 13 || versions[12] != 13 {
		t.Fatalf("expected migrations 0001..0013, applied %v", versions)
	}
	if versions, err = migrate.Apply(ctx, migrationDB, migrations); err != nil || len(versions) != 0 {
		t.Fatalf("restricted LOGIN migration re-entry: applied=%v err=%v", versions, err)
	}
	var applied int
	if err := migrationDB.QueryRowContext(ctx, "SELECT count(*) FROM schema_migrations WHERE version=13").Scan(&applied); err != nil {
		t.Fatal(err)
	}
	if applied != 1 {
		t.Fatalf("expected one 0013 ledger row, got %d", applied)
	}
	assertProviderAuditMigrationCatalog(t, migrationDB)
}
