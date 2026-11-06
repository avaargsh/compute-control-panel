package postgres

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"
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
			enable := fmt.Sprintf("ALTER TABLE %s ENABLE TRIGGER %s", tc.relation, tc.trigger)
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
