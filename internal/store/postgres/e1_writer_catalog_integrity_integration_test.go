package postgres

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/avaargsh/compute-control-panel/internal/pgtest"
)

// A migration checksum does not prove that a required trigger remained
// enabled after a privileged administrator change or database restore.
// Test the actual catalog at the restricted writer's E1 attestation boundary.
func TestE1WriterRejectsDisabledIntegrityTriggers(t *testing.T) {
	admin := openIntegrationDB(t)
	writer := testEvidenceWriterDB(t, admin)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	if err := verifyEvidenceWriterPrivileges(ctx, writer); err != nil {
		t.Fatalf("baseline E1 writer attestation: %v", err)
	}

	tests := []struct {
		relation string
		trigger  string
	}{
		{"public.compute_evidence_policy_fences", "compute_evidence_policy_revision_guard"},
		{"public.compute_evidence_policy_fences", "compute_evidence_policy_no_delete"},
		{"public.compute_evidence_policy_fences", "compute_evidence_policy_no_truncate"},
		{"public.compute_allocation_usage_evidence_sets", "compute_usage_evidence_scope_guard"},
		{"public.compute_allocation_usage_evidence_sets", "compute_usage_evidence_append_only"},
		{"public.compute_allocation_usage_evidence_sets", "compute_usage_evidence_no_truncate"},
	}
	for _, tc := range tests {
		t.Run(tc.trigger, func(t *testing.T) {
			disable := fmt.Sprintf("ALTER TABLE %s DISABLE TRIGGER %s", tc.relation, tc.trigger)
			enable := fmt.Sprintf("ALTER TABLE %s ENABLE ALWAYS TRIGGER %s", tc.relation, tc.trigger)
			if _, err := admin.ExecContext(ctx, disable); err != nil {
				t.Fatalf("disable test-only trigger: %v", err)
			}
			t.Cleanup(func() {
				if _, err := admin.ExecContext(context.Background(), enable); err != nil {
					t.Errorf("restore integrity trigger after test: %v", err)
				}
			})
			if err := verifyEvidenceWriterPrivileges(ctx, writer); !errors.Is(err, ErrEvidencePolicyUntrusted) {
				t.Fatalf("E1 writer accepted disabled integrity trigger %s: %v", tc.trigger, err)
			}
		})
	}
}

// The application writer must not be able to disable the same catalog
// protections, even when it has a narrow UPDATE grant for row locks.
func TestE1WriterCannotDisableIntegrityTriggers(t *testing.T) {
	admin := openIntegrationDB(t)
	writer := testEvidenceWriterDB(t, admin)
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	for _, query := range []string{
		"ALTER TABLE public.compute_evidence_policy_fences DISABLE TRIGGER compute_evidence_policy_revision_guard",
		"ALTER TABLE public.compute_allocation_usage_evidence_sets DISABLE TRIGGER compute_usage_evidence_append_only",
	} {
		if _, err := writer.ExecContext(ctx, query); err == nil {
			t.Fatalf("restricted E1 writer disabled integrity trigger: %s", query)
		}
	}
}

// PG16 permits a superuser to GRANT SET on session_replication_role without
// granting the writer table ownership. Replica mode skips ordinary triggers,
// so the E1 attestation must reject that *transaction's* effective mode.
func TestE1WriterRejectsReplicaSessionMode(t *testing.T) {
	admin := openIntegrationDB(t)
	writer := testEvidenceWriterDB(t, admin)
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	var login string
	if err := writer.QueryRowContext(ctx, "SELECT current_user").Scan(&login); err != nil {
		t.Fatal(err)
	}
	if err := pgtest.ExecCatalog(ctx, admin,
		fmt.Sprintf("GRANT SET ON PARAMETER session_replication_role TO %s", login)); err != nil {
		t.Fatalf("grant test-only parameter SET: %v", err)
	}
	t.Cleanup(func() {
		cleanup, stop := context.WithTimeout(context.Background(), 15*time.Second)
		defer stop()
		if err := pgtest.ExecCatalogCleanup(cleanup, admin,
			fmt.Sprintf("REVOKE SET ON PARAMETER session_replication_role FROM %s", login)); err != nil {
			t.Errorf("revoke test-only parameter SET: %v", err)
		}
	})
	tx, err := writer.BeginTx(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback()
	if _, err := tx.ExecContext(ctx, "SET LOCAL session_replication_role = replica"); err != nil {
		t.Fatalf("set replica session with granted permission: %v", err)
	}
	if err := verifyEvidenceWriterPrivileges(ctx, tx); !errors.Is(err, ErrEvidencePolicyUntrusted) {
		t.Fatalf("E1 attestation accepted replica session that can skip integrity triggers: %v", err)
	}
}

func TestE1WriterRejectsOriginOnlyIntegrityTriggerDowngrade(t *testing.T) {
	admin := openIntegrationDB(t)
	writer := testEvidenceWriterDB(t, admin)
	ctx, cancel := context.WithTimeout(context.Background(), 25*time.Second)
	defer cancel()
	if err := verifyEvidenceWriterPrivileges(ctx, writer); err != nil {
		t.Fatalf("baseline ALWAYS integrity triggers rejected: %v", err)
	}
	const downgrade = "ALTER TABLE public.compute_allocation_usage_evidence_sets ENABLE TRIGGER compute_usage_evidence_scope_guard"
	const restore = "ALTER TABLE public.compute_allocation_usage_evidence_sets ENABLE ALWAYS TRIGGER compute_usage_evidence_scope_guard"
	if _, err := admin.ExecContext(ctx, downgrade); err != nil {
		t.Fatalf("downgrade trigger to origin only: %v", err)
	}
	t.Cleanup(func() {
		if _, err := admin.ExecContext(context.Background(), restore); err != nil {
			t.Errorf("restore ALWAYS evidence scope guard: %v", err)
		}
	})
	if err := verifyEvidenceWriterPrivileges(ctx, writer); !errors.Is(err, ErrEvidencePolicyUntrusted) {
		t.Fatalf("E1 writer accepted origin-only integrity trigger: %v", err)
	}
}
