package postgres

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/avaargsh/compute-control-panel/internal/pgtest"
)

// TEMP is an ordinary writer capability. Same-name temporary catalogs must
// never conceal a non-inherited SET ROLE path to the policy administrator.
func TestE1WriterRejectsSetRoleMembershipDespiteTemporaryRolesCatalog(t *testing.T) {
	admin := openIntegrationDB(t)
	writer := testEvidenceWriterDB(t, admin)
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	conn, err := writer.Conn(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	if err := verifyEvidenceWriterPrivileges(ctx, conn); err != nil {
		t.Fatalf("baseline restricted E1 writer rejected: %v", err)
	}
	var login string
	if err := conn.QueryRowContext(ctx, "SELECT current_user").Scan(&login); err != nil {
		t.Fatal(err)
	}
	if _, err := conn.ExecContext(ctx, `
		CREATE TEMP TABLE pg_roles AS
		SELECT * FROM pg_catalog.pg_roles WHERE rolname=current_user
	`); err != nil {
		t.Fatalf("create temporary roles catalog: %v", err)
	}
	if err := verifyEvidenceWriterPrivileges(ctx, conn); err != nil {
		t.Fatalf("safe writer with unrelated temporary catalog rejected: %v", err)
	}

	adminRole := "e1_shadow_policy_admin_" + randomSuffix(t)
	if err := pgtest.ExecCatalog(ctx, admin, fmt.Sprintf("CREATE ROLE %s NOLOGIN", adminRole)); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		cleanup, stop := context.WithTimeout(context.Background(), 15*time.Second)
		defer stop()
		if err := pgtest.CleanupRole(cleanup, admin, adminRole, fmt.Sprintf("REVOKE %s FROM %s", adminRole, login)); err != nil {
			t.Errorf("drop role %s: %v", adminRole, err)
		}
	})
	if err := pgtest.ExecCatalog(ctx, admin,
		fmt.Sprintf("GRANT UPDATE(revision) ON public.compute_evidence_policy_fences TO %s", adminRole),
		fmt.Sprintf("GRANT %s TO %s WITH INHERIT FALSE, SET TRUE", adminRole, login),
	); err != nil {
		t.Fatal(err)
	}
	var maySetRole, inheritedUpdate bool
	if err := conn.QueryRowContext(ctx, `
		SELECT pg_catalog.pg_has_role(current_user, $1, 'SET'),
		       pg_catalog.has_column_privilege(current_user,
		         'public.compute_evidence_policy_fences', 'revision', 'UPDATE')
	`, adminRole).Scan(&maySetRole, &inheritedUpdate); err != nil {
		t.Fatal(err)
	}
	if !maySetRole || inheritedUpdate {
		t.Fatalf("expected SET ROLE without inherited policy UPDATE: SET=%t UPDATE=%t", maySetRole, inheritedUpdate)
	}
	if err := verifyEvidenceWriterPrivileges(ctx, conn); !errors.Is(err, ErrEvidencePolicyUntrusted) {
		t.Fatalf("temporary pg_roles concealed policy administrator membership: %v", err)
	}
}

// Keep real policy attributes in the shadow so its absence cannot trigger
// the policy check's fail-closed empty-result branch. Only the forbidden
// Provider status column is hidden from an unqualified pg_attribute read.
func TestE1WriterRejectsStatusGrantDespiteTemporaryAttributesCatalog(t *testing.T) {
	admin := openIntegrationDB(t)
	writer := testEvidenceWriterDB(t, admin)
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	conn, err := writer.Conn(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	if err := verifyEvidenceWriterPrivileges(ctx, conn); err != nil {
		t.Fatalf("baseline restricted E1 writer rejected: %v", err)
	}
	var login string
	if err := conn.QueryRowContext(ctx, "SELECT current_user").Scan(&login); err != nil {
		t.Fatal(err)
	}
	if _, err := conn.ExecContext(ctx, `
		CREATE TEMP TABLE pg_attribute AS
		SELECT attrelid,attnum,attisdropped,attname FROM pg_catalog.pg_attribute
		WHERE NOT (attrelid='public.compute_operations'::pg_catalog.regclass
		           AND attname='status')
	`); err != nil {
		t.Fatalf("create temporary attributes catalog: %v", err)
	}
	if err := verifyEvidenceWriterPrivileges(ctx, conn); err != nil {
		t.Fatalf("safe writer with unrelated temporary catalog rejected: %v", err)
	}
	if err := pgtest.ExecCatalog(ctx, admin, fmt.Sprintf(
		"GRANT UPDATE(status) ON public.compute_operations TO %s", login)); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		cleanup, stop := context.WithTimeout(context.Background(), 15*time.Second)
		defer stop()
		if err := pgtest.ExecCatalogCleanup(cleanup, admin, fmt.Sprintf(
			"REVOKE UPDATE(status) ON public.compute_operations FROM %s", login)); err != nil {
			t.Errorf("revoke status update: %v", err)
		}
	})
	var canUpdateStatus bool
	if err := conn.QueryRowContext(ctx, `
		SELECT pg_catalog.has_column_privilege(current_user,
		  'public.compute_operations', 'status', 'UPDATE')
	`).Scan(&canUpdateStatus); err != nil {
		t.Fatal(err)
	}
	if !canUpdateStatus {
		t.Fatal("test writer did not receive forbidden Provider status UPDATE")
	}
	if err := verifyEvidenceWriterPrivileges(ctx, conn); !errors.Is(err, ErrEvidencePolicyUntrusted) {
		t.Fatalf("temporary pg_attribute concealed Provider status UPDATE grant: %v", err)
	}
}
