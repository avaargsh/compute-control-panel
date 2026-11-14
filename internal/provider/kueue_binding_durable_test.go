package provider

import (
	"context"
	"crypto/rand"
	"database/sql"
	"encoding/hex"
	"errors"
	"net/url"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgconn"
)

func TestDurableKueueBindingConstructionFailClosed(t *testing.T) {
	_, err := NewServiceAccountKueueJobAdapter(
		"https://kubernetes.default.svc", "ca.crt", "token", "tenant-a", " \t",
	)
	if !errors.Is(err, ErrKueueBindingDSNEmpty) {
		t.Fatalf("empty DSN err=%v", err)
	}

	_, db, err := OpenDurablePostgresKueueJobBindingStore(context.Background(), "")
	if !errors.Is(err, ErrKueueBindingDSNEmpty) || db != nil {
		t.Fatalf("empty opener db=%v err=%v", db != nil, err)
	}

	unreachable := "postgres://ccp_kueue_job_binding:secret@127.0.0.1:1/postgres?sslmode=disable&connect_timeout=2"
	_, err = NewServiceAccountKueueJobAdapter(
		"https://kubernetes.default.svc", "ca.crt", "token", "tenant-a", unreachable,
	)
	if !errors.Is(err, ErrKueueBindingUnavailable) {
		t.Fatalf("unreachable DSN err=%v", err)
	}

	for _, dsn := range []string{
		"postgres://ccp_kueue_job_binding@127.0.0.1:1/postgres?sslmode=disable&search_path=pg_temp&connect_timeout=1",
		"postgres://ccp_kueue_job_binding@127.0.0.1:1/postgres?sslmode=disable&options=-csearch_path%3Dpg_temp&connect_timeout=1",
	} {
		_, err = NewServiceAccountKueueJobAdapter(
			"https://kubernetes.default.svc", "ca.crt", "token", "tenant-a", dsn,
		)
		if !errors.Is(err, ErrKueueBindingUnavailable) || !strings.Contains(err.Error(), "search_path") {
			t.Fatalf("search_path DSN err=%v", err)
		}
	}
}

func TestDurableKueueBindingRejectsMissingSchema(t *testing.T) {
	admin := openProviderPostgresDB(t)
	if !postgresCurrentUserIsSuperuser(t, admin) {
		t.Skip("requires disposable PostgreSQL superuser to create an empty database")
	}
	database := kueueTestIdent(t, "ccp_kempty_")
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	if _, err := admin.ExecContext(ctx, "CREATE DATABASE "+database); err != nil {
		t.Fatalf("create empty database: %v", err)
	}
	t.Cleanup(func() {
		cleanup, stop := context.WithTimeout(context.Background(), 15*time.Second)
		defer stop()
		if _, err := admin.ExecContext(cleanup, "DROP DATABASE "+database+" WITH (FORCE)"); err != nil {
			t.Errorf("drop empty database: %v", err)
		}
	})
	dsn := kueueBindingDSN(t, "", "", database)
	_, err := NewServiceAccountKueueJobAdapter(
		"https://kubernetes.default.svc", "ca.crt", "token", "tenant-a", dsn,
	)
	if !errors.Is(err, ErrKueueBindingSchema) {
		t.Fatalf("missing schema err=%v", err)
	}
}

func TestDurableKueueBindingRejectsOverPrivilegedRole(t *testing.T) {
	admin := openProviderPostgresDB(t)
	if !postgresCurrentUserIsSuperuser(t, admin) {
		t.Skip("requires disposable PostgreSQL superuser to provision binding roles")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	_, err := NewServiceAccountKueueJobAdapter(
		"https://kubernetes.default.svc", "ca.crt", "token", "tenant-a", os.Getenv("DATABASE_URL"),
	)
	if !errors.Is(err, ErrKueueBindingRole) || !strings.Contains(err.Error(), "superuser") {
		t.Fatalf("superuser DSN err=%v", err)
	}

	broad := kueueTestIdent(t, "ccp_kbroad_")
	createKueueBindingRole(t, admin, broad)
	grantKueueBindingPrivileges(t, admin, broad)
	if _, err := admin.ExecContext(ctx, "GRANT SELECT ON public.compute_providers TO "+broad); err != nil {
		t.Fatal(err)
	}
	_, _, err = OpenDurablePostgresKueueJobBindingStore(ctx, kueueBindingDSN(t, broad, kueueBindingTestPassword, ""))
	if !errors.Is(err, ErrKueueBindingRole) || !strings.Contains(err.Error(), "authority_table") {
		t.Fatalf("provider SELECT err=%v", err)
	}

	narrow := kueueTestIdent(t, "ccp_kmiss_")
	createKueueBindingRole(t, admin, narrow)
	if _, err := admin.ExecContext(ctx, "GRANT SELECT ON public.kueue_job_create_pins, public.kueue_job_delete_tombstones TO "+narrow); err != nil {
		t.Fatal(err)
	}
	_, _, err = OpenDurablePostgresKueueJobBindingStore(ctx, kueueBindingDSN(t, narrow, kueueBindingTestPassword, ""))
	if !errors.Is(err, ErrKueueBindingRole) || !strings.Contains(err.Error(), "missing_binding_dml") {
		t.Fatalf("missing INSERT err=%v", err)
	}
}

func TestDurableKueueBindingCanonicalRoleAndShadow(t *testing.T) {
	admin := openProviderPostgresDB(t)
	if !postgresCurrentUserIsSuperuser(t, admin) {
		t.Skip("requires disposable PostgreSQL superuser to attest the binding role")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	if _, err := admin.ExecContext(ctx, "ALTER ROLE "+kueueBindingRoleName+" PASSWORD '"+kueueBindingTestPassword+"'"); err != nil {
		t.Fatalf("binding role %s is not provisioned: %v", kueueBindingRoleName, err)
	}
	dsn := kueueBindingDSN(t, kueueBindingRoleName, kueueBindingTestPassword, "")
	store, db, err := OpenDurablePostgresKueueJobBindingStore(ctx, dsn)
	if err != nil {
		t.Fatalf("canonical binding role rejected: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })

	_, err = NewServiceAccountKueueJobAdapter(
		"https://kubernetes.default.svc", "missing-ca.crt", "missing-token", "tenant-a", dsn,
	)
	if errors.Is(err, ErrKueueBindingRole) || errors.Is(err, ErrKueueBindingSchema) || errors.Is(err, ErrKueueBindingUnavailable) || errors.Is(err, ErrKueueBindingDSNEmpty) {
		t.Fatalf("in-cluster constructor rejected the binding role before the ServiceAccount files: %v", err)
	}
	if !errors.Is(err, ErrInvalidKueueObservation) {
		t.Fatalf("constructor err=%v", err)
	}
	// The queued production constructor must accept exactly the same restricted
	// durable binding role, and only then validate ServiceAccount transport.
	_, err = NewQueuedServiceAccountKueueJobAdapter(
		"https://kubernetes.default.svc", "missing-ca.crt", "missing-token",
		"tenant-a", "ccp-smoke", dsn,
	)
	if errors.Is(err, ErrKueueBindingRole) || errors.Is(err, ErrKueueBindingSchema) ||
		errors.Is(err, ErrKueueBindingUnavailable) || errors.Is(err, ErrKueueBindingDSNEmpty) {
		t.Fatalf("queued constructor rejected the canonical durable role: %v", err)
	}
	if !errors.Is(err, ErrInvalidKueueObservation) {
		t.Fatalf("queued constructor did not reach TLS file validation: %v", err)
	}

	db.SetMaxOpenConns(1)
	db.SetMaxIdleConns(1)
	req := uniqueKueueRequest(t)
	const namespace = "tenant-a"
	suffix := strings.TrimPrefix(req.OperationID, "op-")
	jobName := "job-" + suffix
	if _, err := db.ExecContext(ctx, `CREATE TEMP TABLE kueue_job_create_pins (LIKE public.kueue_job_create_pins INCLUDING DEFAULTS)`); err != nil {
		t.Fatalf("create pg_temp pin shadow: %v", err)
	}
	if _, err := db.ExecContext(ctx, `
		INSERT INTO pg_temp.kueue_job_create_pins (
			namespace, tenant_id, operation_id, job_name, job_uid,
			workload_id, provider_id, idempotency_key, request_digest,
			desired_generation, operation_kind, target_resource_id, target_resource_uid
		) VALUES ($1,$2,$3,$4,'shadow-uid',$5,$6,$7,$8,$9,$10,$11,$12)`,
		namespace, req.TenantID, req.OperationID, jobName,
		req.WorkloadID, req.ProviderID, req.IdempotencyKey, req.RequestDigest,
		req.DesiredGeneration, string(req.Kind), req.TargetResourceID, req.TargetResourceUID,
	); err != nil {
		t.Fatalf("insert pg_temp pin shadow: %v", err)
	}
	if _, err := db.ExecContext(ctx, `SET search_path = pg_temp, public, pg_catalog`); err != nil {
		t.Fatal(err)
	}
	pin, ok, err := store.GetCreatePin(namespace, req)
	if err != nil || ok || pin.UID == "shadow-uid" {
		t.Fatalf("shadow pin adopted: %+v ok=%t err=%v", pin, ok, err)
	}
	var path string
	if err := db.QueryRowContext(ctx, `SELECT pg_catalog.current_setting('search_path')`).Scan(&path); err != nil {
		t.Fatal(err)
	}
	if path != kueueBindingSearchPath {
		t.Fatalf("search_path=%q", path)
	}
	if err := store.PutCreatePin(namespace, req, jobName, "uid-"+suffix); err != nil {
		t.Fatal(err)
	}
	var publicUID, tempUID string
	var tempCount int
	if err := db.QueryRowContext(ctx, `
		SELECT job_uid FROM public.kueue_job_create_pins
		WHERE namespace=$1 AND tenant_id=$2 AND operation_id=$3`,
		namespace, req.TenantID, req.OperationID).Scan(&publicUID); err != nil {
		t.Fatal(err)
	}
	if err := db.QueryRowContext(ctx, `
		SELECT count(*)::int, COALESCE(max(job_uid), '') FROM pg_temp.kueue_job_create_pins
		WHERE namespace=$1 AND tenant_id=$2 AND operation_id=$3`,
		namespace, req.TenantID, req.OperationID).Scan(&tempCount, &tempUID); err != nil {
		t.Fatal(err)
	}
	if publicUID != "uid-"+suffix || tempCount != 1 || tempUID != "shadow-uid" {
		t.Fatalf("pin crossed schemas: public=%s temp=%s count=%d", publicUID, tempUID, tempCount)
	}
	_, err = db.ExecContext(ctx, `
		UPDATE public.kueue_job_create_pins
		SET updated_at = pg_catalog.clock_timestamp()
		WHERE namespace=$1 AND tenant_id=$2 AND operation_id=$3`,
		namespace, req.TenantID, req.OperationID)
	if postgresSQLState(err) != "55000" {
		t.Fatalf("pin lock column was writable: %s %v", postgresSQLState(err), err)
	}
	_, err = db.ExecContext(ctx, `
		UPDATE public.kueue_job_create_pins SET job_uid='rewritten'
		WHERE namespace=$1 AND tenant_id=$2 AND operation_id=$3`,
		namespace, req.TenantID, req.OperationID)
	if postgresSQLState(err) != "42501" {
		t.Fatalf("pin identity column was not denied: %s %v", postgresSQLState(err), err)
	}
}

// Test retries only the transient PostgreSQL catalog tuple-update collision
// seen when package-level race tests concurrently GRANT on shared ACL catalogs.
func TestRetryKueueCatalogGrantOnlyForTupleUpdate(t *testing.T) {
    conflict := &pgconn.PgError{Code: "XX000", Message: "tuple concurrently updated"}
    unrelated := &pgconn.PgError{Code: "XX000", Message: "corrupted catalog"}
    cases := []struct {
        name string
        failures []error
        attempts int
        wantErr error
    }{
        {"one transient conflict", []error{conflict, nil}, 2, nil},
        {"two transient conflicts", []error{conflict, conflict, nil}, 3, nil},
        {"different internal failure", []error{unrelated, nil}, 1, unrelated},
        {"non-pg failure", []error{errors.New("denied"), nil}, 1, nil},
    }
    for _, tc := range cases {
        t.Run(tc.name, func(t *testing.T) {
            calls := 0
            err := retryKueueCatalogGrant(context.Background(), func(context.Context) error {
                result := tc.failures[calls]
                calls++
                return result
            })
            if calls != tc.attempts {
                t.Fatalf("calls=%d want=%d", calls, tc.attempts)
            }
            if tc.name == "non-pg failure" {
                if err == nil || err.Error() != "denied" {
                    t.Fatalf("unrelated error lost: %v", err)
                }
            } else if !errors.Is(err, tc.wantErr) {
                t.Fatalf("err=%v want=%v", err, tc.wantErr)
            }
        })
    }
}

const kueueBindingTestPassword = "kueue-binding-test"

func postgresCurrentUserIsSuperuser(t *testing.T, db *sql.DB) bool {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	var superuser bool
	if err := db.QueryRowContext(ctx, `SELECT rolsuper FROM pg_catalog.pg_roles WHERE rolname = current_user`).Scan(&superuser); err != nil {
		t.Fatal(err)
	}
	return superuser
}

func kueueTestIdent(t *testing.T, prefix string) string {
	t.Helper()
	var buf [8]byte
	if _, err := rand.Read(buf[:]); err != nil {
		t.Fatal(err)
	}
	name := prefix + hex.EncodeToString(buf[:])
	for _, r := range name {
		if (r < 'a' || r > 'z') && (r < '0' || r > '9') && r != '_' {
			t.Fatalf("unsafe identifier %q", name)
		}
	}
	return name
}

func createKueueBindingRole(t *testing.T, admin *sql.DB, role string) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	if _, err := admin.ExecContext(ctx, `CREATE ROLE `+role+` LOGIN PASSWORD '`+kueueBindingTestPassword+`' NOINHERIT NOSUPERUSER NOCREATEDB NOCREATEROLE NOREPLICATION NOBYPASSRLS`); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		cleanup, stop := context.WithTimeout(context.Background(), 15*time.Second)
		defer stop()
		_, _ = admin.ExecContext(cleanup, "DROP OWNED BY "+role)
		if _, err := admin.ExecContext(cleanup, "DROP ROLE "+role); err != nil {
			t.Errorf("drop role %s: %v", role, err)
		}
	})
}

func grantKueueBindingPrivileges(t *testing.T, admin *sql.DB, role string) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	for _, statement := range []string{
		"GRANT USAGE ON SCHEMA public TO " + role,
		"GRANT SELECT, INSERT ON public.kueue_job_create_pins TO " + role,
		"GRANT UPDATE (updated_at) ON public.kueue_job_create_pins TO " + role,
		"GRANT SELECT, INSERT ON public.kueue_job_delete_tombstones TO " + role,
		"GRANT UPDATE (acknowledged, updated_at) ON public.kueue_job_delete_tombstones TO " + role,
		"GRANT EXECUTE ON FUNCTION public.kueue_job_create_pin_guard() TO " + role,
		"GRANT EXECUTE ON FUNCTION public.kueue_job_delete_tombstone_guard() TO " + role,
	} {
		if _, err := admin.ExecContext(ctx, statement); err != nil {
			t.Fatalf("%s: %v", statement, err)
		}
	}
}

func kueueBindingDSN(t *testing.T, user, password, database string) string {
	t.Helper()
	parsed, err := url.Parse(os.Getenv("DATABASE_URL"))
	if err != nil || parsed.Scheme == "" || parsed.Host == "" {
		t.Fatalf("DATABASE_URL: %v", err)
	}
	if user != "" {
		parsed.User = url.UserPassword(user, password)
	}
	if database != "" {
		parsed.Path = "/" + database
		parsed.RawPath = ""
	}
	query := parsed.Query()
	query.Del("search_path")
	parsed.RawQuery = query.Encode()
	return parsed.String()
}
