package postgres

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/avaargsh/compute-control-panel/internal/pgtest"
)

// E1 FINAL must fail closed when its database login has unexpected DML or
// trigger capabilities on authority-bearing relations, even if its policy
// registry privileges remain restricted. The test alters only a disposable
// role created for this test, not an application migration or production IAM.
func TestE1WriterRoleRejectsUnexpectedControlAndEvidenceGrants(t *testing.T) {
	admin := openIntegrationDB(t)
	writer := testEvidenceWriterDB(t, admin)
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	var role string
	if err := writer.QueryRowContext(ctx, "SELECT current_user").Scan(&role); err != nil {
		t.Fatal(err)
	}
	if err := verifyEvidenceWriterPrivileges(ctx, writer); err != nil {
		t.Fatalf("baseline restricted E1 writer must pass: %v", err)
	}

	tests := []struct {
		name      string
		privilege string
		relation  string
	}{
		{"Provider operation status", "UPDATE(status)", "public.compute_operations"},
		{"Allocation lifecycle", "UPDATE(phase)", "public.compute_allocations"},
		{"Workload Project", "UPDATE(project_id)", "public.compute_workloads"},
		{"evidence digest", "UPDATE(evidence_digest)", "public.compute_allocation_usage_evidence_sets"},
		{"Provider operation insertion", "INSERT", "public.compute_operations"},
		{"Workload deletion", "DELETE", "public.compute_workloads"},
		{"Allocation truncation", "TRUNCATE", "public.compute_allocations"},
		{"evidence deletion", "DELETE", "public.compute_allocation_usage_evidence_sets"},
		{"evidence trigger control", "TRIGGER", "public.compute_allocation_usage_evidence_sets"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			grant := fmt.Sprintf("GRANT %s ON %s TO %s", tc.privilege, tc.relation, role)
			if err := pgtest.ExecCatalog(ctx, admin, grant); err != nil {
				t.Fatalf("install unexpected test-only privilege %s: %v", grant, err)
			}
			t.Cleanup(func() {
				revoke := fmt.Sprintf("REVOKE %s ON %s FROM %s", tc.privilege, tc.relation, role)
				cleanup, stop := context.WithTimeout(context.Background(), 15*time.Second)
				defer stop()
				if err := pgtest.ExecCatalogCleanup(cleanup, admin, revoke); err != nil {
					t.Errorf("revoke test-only privilege: %v", err)
				}
			})

			if err := verifyEvidenceWriterPrivileges(ctx, writer); !errors.Is(err, ErrEvidencePolicyUntrusted) {
				t.Fatalf("E1 writer accepted unexpected %s on %s: %v", tc.privilege, tc.relation, err)
			}
		})
	}
}
