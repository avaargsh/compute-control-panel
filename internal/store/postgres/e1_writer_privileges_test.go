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
)

// Test connections use a separate non-owner login. The migration/superuser
// connection is retained ONLY for fixture provisioning and verification;
// it is never supplied as the E1 evidence-writing repository's database.
func testEvidenceWriterDB(t *testing.T, admin *sql.DB) *sql.DB {
	t.Helper()
	role := "e1_writer_" + randomSuffix(t)
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	if _, err := admin.ExecContext(ctx, fmt.Sprintf("CREATE ROLE %s LOGIN PASSWORD 'e1-ci-only'", role)); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		// Close all writer sessions before removing grants and the test role.
		_, _ = admin.ExecContext(context.Background(), fmt.Sprintf("DROP OWNED BY %s", role))
		_, _ = admin.ExecContext(context.Background(), fmt.Sprintf("DROP ROLE %s", role))
	})
	for _, grant := range []string{
		"GRANT USAGE ON SCHEMA public TO %s",
		"GRANT SELECT ON ALL TABLES IN SCHEMA public TO %s",
		"GRANT INSERT ON compute_allocation_usage_evidence_sets TO %s",
		// E1 immutable replay uses SELECT .. FOR SHARE. PostgreSQL
		// requires one UPDATE-able column, but its append-only trigger
		// denies all actual UPDATE attempts.
		"GRANT UPDATE (recorded_at) ON compute_allocation_usage_evidence_sets TO %s",
		"GRANT UPDATE (updated_at) ON compute_evidence_policy_fences TO %s",
		// The existing Provider operation/Workload safety SELECT .. FOR
		// SHARE clauses also require a minimal UPDATE-column privilege.
		// These are distinct from policy-administration capabilities.
		"GRANT UPDATE (updated_at) ON compute_operations TO %s",
		"GRANT UPDATE (updated_at) ON compute_workloads TO %s",
		"GRANT UPDATE (updated_at) ON compute_allocations TO %s",
	} {
		if _, err := admin.ExecContext(ctx, fmt.Sprintf(grant, role)); err != nil {
			t.Fatal(err)
		}
	}
	parsed, err := url.Parse(os.Getenv("DATABASE_URL"))
	if err != nil || parsed.Scheme == "" || parsed.Host == "" {
		t.Fatalf("PostgreSQL integration DSN must be an URL: %v", err)
	}
	parsed.User = url.UserPassword(role, "e1-ci-only")
	writer, err := sql.Open("pgx", parsed.String())
	if err != nil {
		t.Fatal(err)
	}
	writer.SetMaxOpenConns(16)
	writer.SetMaxIdleConns(4)
	if err := writer.PingContext(ctx); err != nil {
		_ = writer.Close()
		t.Fatalf("connect E1 restricted writer: %v", err)
	}
	// LIFO cleanup: close writer sessions before removing role/grants.
	t.Cleanup(func() { _ = writer.Close() })
	return writer
}

func TestE1WriterRoleAttestationRejectsDatabaseOwner(t *testing.T) {
	admin := openIntegrationDB(t)
	// Migration/table-owner identity is forbidden for E1 FINAL writes.
	if err := verifyEvidenceWriterPrivileges(context.Background(), admin); !errors.Is(err, ErrEvidencePolicyUntrusted) {
		t.Fatalf("E1 policy owner/superuser accepted as writer: %v", err)
	}
}

func TestE1WriterRoleCanReadLockButCannotRewritePolicy(t *testing.T) {
	admin := openIntegrationDB(t)
	writer := testEvidenceWriterDB(t, admin)
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	if err := verifyEvidenceWriterPrivileges(ctx, writer); err != nil {
		t.Fatalf("restricted E1 writer rejected: %v", err)
	}
	tenant := "tenant-writer-role-" + randomSuffix(t)
	_, err := admin.ExecContext(ctx, `
		INSERT INTO compute_evidence_policy_fences
		(tenant_id,provider_id,policy_version,policy_digest,
		 effective_from,effective_until,revision,revoked)
		VALUES ($1,'provider-writer-role','version-1',
		        'sha256:' || repeat('a',64),
		        clock_timestamp()-interval '1 hour',
		        clock_timestamp()+interval '1 hour',1,false)
	`, tenant)
	if err != nil {
		t.Fatalf("provision test-only policy row: %v", err)
	}
	// Row-share locks need UPDATE on at least one column in PG16.
	// UPDATE(updated_at) is narrowly granted but cannot succeed alone
	// because the policy revision trigger requires an admin-only bump.
	for _, query := range []string{
		"UPDATE compute_evidence_policy_fences SET updated_at=clock_timestamp() WHERE tenant_id=$1",
		"UPDATE compute_evidence_policy_fences SET revision=revision+1 WHERE tenant_id=$1",
		"DELETE FROM compute_evidence_policy_fences WHERE tenant_id=$1",
		"TRUNCATE compute_evidence_policy_fences",
		"INSERT INTO compute_evidence_policy_fences (tenant_id) VALUES ('forged')",
	} {
		tx, err := writer.BeginTx(ctx, nil)
		if err != nil {
			t.Fatal(err)
		}
		if query == "TRUNCATE compute_evidence_policy_fences" ||
			query == "INSERT INTO compute_evidence_policy_fences (tenant_id) VALUES ('forged')" {
			_, err = tx.ExecContext(ctx, query)
		} else {
			_, err = tx.ExecContext(ctx, query, tenant)
		}
		_ = tx.Rollback()
		if err == nil {
			t.Fatalf("E1 writer can mutate trusted policy registry: %s", query)
		}
	}
}


func TestE1WriterRoleRejectsSetRolePolicyAdministratorMembership(t *testing.T) {
	admin := openIntegrationDB(t)
	writer := testEvidenceWriterDB(t, admin)
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	var login string
	if err := writer.QueryRowContext(ctx, "SELECT current_user").Scan(&login); err != nil {
		t.Fatal(err)
	}
	grantRole := "e1_policy_admin_" + randomSuffix(t)
	if _, err := admin.ExecContext(ctx, fmt.Sprintf("CREATE ROLE %s NOLOGIN", grantRole)); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		_, _ = admin.ExecContext(context.Background(), fmt.Sprintf("REVOKE %s FROM %s", grantRole, login))
		_, _ = admin.ExecContext(context.Background(), fmt.Sprintf("DROP OWNED BY %s", grantRole))
		_, _ = admin.ExecContext(context.Background(), fmt.Sprintf("DROP ROLE %s", grantRole))
	})
	_, err := admin.ExecContext(ctx, fmt.Sprintf("GRANT UPDATE(revision) ON compute_evidence_policy_fences TO %s", grantRole))
	if err != nil {
		t.Fatal(err)
	}
	// PostgreSQL 16 SET ROLE membership does not have to inherit privileges.
	// A user who can switch to a policy administrator is not an E1 writer.
	_, err = admin.ExecContext(ctx, fmt.Sprintf("GRANT %s TO %s WITH INHERIT FALSE, SET TRUE", grantRole, login))
	if err != nil {
		t.Fatal(err)
	}
	if err := verifyEvidenceWriterPrivileges(ctx, writer); !errors.Is(err, ErrEvidencePolicyUntrusted) {
		t.Fatalf("E1 writer can SET ROLE into privileged policy administrator: %v", err)
	}
}
