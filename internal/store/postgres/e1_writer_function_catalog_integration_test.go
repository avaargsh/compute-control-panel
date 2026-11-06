package postgres

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"
)

// CREATE OR REPLACE FUNCTION preserves the trigger function OID: checking
// only pg_trigger.tgfoid cannot prove that an immutable guard still rejects
// mutation. A privileged migration/restore can drift independently of the
// checksummed migration ledger.
func TestE1WriterRejectsReplacedIntegrityFunctionBodies(t *testing.T) {
	admin := openIntegrationDB(t)
	writer := testEvidenceWriterDB(t, admin)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	if err := verifyEvidenceWriterPrivileges(ctx, writer); err != nil {
		t.Fatalf("baseline attestation: %v", err)
	}
	for _, fn := range []string{
		"compute_validate_usage_evidence_set",
		"compute_reject_usage_evidence_mutation",
		"compute_enforce_evidence_policy_revision",
		"compute_reject_evidence_policy_removal",
	} {
		t.Run(fn, func(t *testing.T) {
			qualified := "public." + fn + "()"
			var oldDefinition string
			if err := admin.QueryRowContext(ctx,
				"SELECT pg_get_functiondef($1::regprocedure)", qualified).Scan(&oldDefinition); err != nil {
				t.Fatal(err)
			}
			// Must restore even when an assertion fails: other PostgreSQL
			// integration tests share the migration-created function.
			t.Cleanup(func() {
				if _, err := admin.ExecContext(context.Background(), oldDefinition); err != nil {
					t.Errorf("restore original function %s: %v", fn, err)
				}
			})
			replace := fmt.Sprintf(
				"CREATE OR REPLACE FUNCTION public.%s() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN RETURN NEW; END; $$", fn)
			if _, err := admin.ExecContext(ctx, replace); err != nil {
				t.Fatalf("replace function body without changing OID: %v", err)
			}
			if err := verifyEvidenceWriterPrivileges(ctx, writer); !errors.Is(err, ErrEvidencePolicyUntrusted) {
				t.Fatalf("E1 accepted rewritten trigger function %s: %v", fn, err)
			}
		})
	}
}

func TestE1WriterRejectsUnsafeIntegrityFunctionAttributes(t *testing.T) {
	admin := openIntegrationDB(t)
	writer := testEvidenceWriterDB(t, admin)
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	fn := "public.compute_validate_usage_evidence_set()"
	tests := []struct {
		name    string
		change  string
		restore string
	}{
		{"security-definer", "SECURITY DEFINER", "SECURITY INVOKER"},
		{"untrusted-function-search-path", "SET search_path = pg_temp, public", "SET search_path = pg_catalog, public, pg_temp"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if _, err := admin.ExecContext(ctx, "ALTER FUNCTION "+fn+" "+tc.change); err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() {
				if _, err := admin.ExecContext(context.Background(), "ALTER FUNCTION "+fn+" "+tc.restore); err != nil {
					t.Errorf("restore integrity function attribute: %v", err)
				}
			})
			if err := verifyEvidenceWriterPrivileges(ctx, writer); !errors.Is(err, ErrEvidencePolicyUntrusted) {
				t.Fatalf("E1 accepted unsafe function attribute %s: %v", tc.name, err)
			}
		})
	}
}
