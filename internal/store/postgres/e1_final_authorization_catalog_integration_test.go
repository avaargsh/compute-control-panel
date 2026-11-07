package postgres

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"
)

// The E1 runtime writer may read ordinary policy projections, but must never
// become the independently authenticated issuer for exact FINAL approvals.
// These privileges must be checked on the same transaction login as the write.
func TestE1WriterRejectsIndependentAuthorizationIssuerGrants(t *testing.T) {
	admin := openIntegrationDB(t)
	writer := testEvidenceWriterDB(t, admin)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	var login string
	if err := writer.QueryRowContext(ctx, "SELECT current_user").Scan(&login); err != nil {
		t.Fatal(err)
	}
	if err := verifyEvidenceWriterPrivileges(ctx, writer); err != nil {
		t.Fatalf("baseline restricted writer rejected: %v", err)
	}
	for _, grant := range []string{
		"INSERT", "DELETE", "TRUNCATE", "TRIGGER", "REFERENCES",
		"UPDATE(revoked)", "UPDATE(approved_at)",
	} {
		t.Run(grant, func(t *testing.T) {
			statement := fmt.Sprintf("GRANT %s ON public.compute_e1_final_authorizations TO %s", grant, login)
			if _, err := admin.ExecContext(ctx, statement); err != nil {
				t.Fatalf("grant test-only privilege: %v", err)
			}
			t.Cleanup(func() {
				_, err := admin.ExecContext(context.Background(),
					fmt.Sprintf("REVOKE %s ON public.compute_e1_final_authorizations FROM %s", grant, login))
				if err != nil {
					t.Errorf("restore approval issuer ACL: %v", err)
				}
			})
			if err := verifyEvidenceWriterPrivileges(ctx, writer); !errors.Is(err, ErrEvidencePolicyUntrusted) {
				t.Fatalf("E1 writer accepted issuer table privilege %s: %v", grant, err)
			}
		})
	}
}

// A previously checksummed v9 is not sufficient proof of today's catalog.
// A privileged owner/restore could disable the trigger or rewrite its body;
// startup/transaction attestation must reject unsafe catalog drift.
func TestE1WriterRejectsFinalAuthorizationCatalogDrift(t *testing.T) {
	tests := []struct {
		name    string
		ddl     string
		restore string
	}{
		{
			name:    "approval ledger UNLOGGED",
			ddl:     "ALTER TABLE public.compute_e1_final_authorizations SET UNLOGGED",
			restore: "ALTER TABLE public.compute_e1_final_authorizations SET LOGGED",
		},
		{
			name:    "final approval trigger disabled",
			ddl:     "ALTER TABLE public.compute_allocation_usage_evidence_sets DISABLE TRIGGER compute_usage_evidence_z_authorization_guard",
			restore: "ALTER TABLE public.compute_allocation_usage_evidence_sets ENABLE ALWAYS TRIGGER compute_usage_evidence_z_authorization_guard",
		},
		{
			name:    "final approval trigger downgraded to origin only",
			ddl:     "ALTER TABLE public.compute_allocation_usage_evidence_sets ENABLE TRIGGER compute_usage_evidence_z_authorization_guard",
			restore: "ALTER TABLE public.compute_allocation_usage_evidence_sets ENABLE ALWAYS TRIGGER compute_usage_evidence_z_authorization_guard",
		},
		{
			name:    "final approval function replaced",
			ddl:     "CREATE OR REPLACE FUNCTION public.compute_require_e1_final_authorization() RETURNS trigger LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog,public,pg_temp AS $unsafe$ BEGIN RETURN NEW; END; $unsafe$",
			restore: "restore-original-function",
		},
		{
			name:    "final approval function search_path weakened",
			ddl:     "ALTER FUNCTION public.compute_require_e1_final_authorization() SET search_path=pg_temp,public",
			restore: "ALTER FUNCTION public.compute_require_e1_final_authorization() SET search_path = pg_catalog, public, pg_temp",
		},
		{
			name:    "immutable issuer ledger row trigger disabled",
			ddl:     "ALTER TABLE public.compute_e1_final_authorizations DISABLE TRIGGER compute_e1_final_auth_immutable",
			restore: "ALTER TABLE public.compute_e1_final_authorizations ENABLE ALWAYS TRIGGER compute_e1_final_auth_immutable",
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			admin := openIntegrationDB(t)
			writer := testEvidenceWriterDB(t, admin)
			ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
			defer cancel()
			if err := verifyEvidenceWriterPrivileges(ctx, writer); err != nil {
				t.Fatalf("baseline catalog attestation rejected: %v", err)
			}
			// Every test shares the PostgreSQL integration database. Restore the
			// *actual* DDL even if a subsequent assertion fails or is fatal;
			// otherwise a leftover UNLOGGED relation poisons later cases.
			restore := tc.restore
			if restore == "restore-original-function" {
				if err := admin.QueryRowContext(ctx,
					"SELECT pg_get_functiondef('public.compute_require_e1_final_authorization()'::regprocedure)",
				).Scan(&restore); err != nil {
					t.Fatalf("capture original final authorization function: %v", err)
				}
			}
			t.Cleanup(func() {
				if _, err := admin.ExecContext(context.Background(), restore); err != nil {
					t.Errorf("restore original E1 approval catalog after %s: %v", tc.name, err)
				}
			})
			if _, err := admin.ExecContext(ctx, tc.ddl); err != nil {
				t.Fatalf("inject privileged catalog drift: %v", err)
			}
			if err := verifyEvidenceWriterPrivileges(ctx, writer); !errors.Is(err, ErrEvidencePolicyUntrusted) {
				t.Fatalf("E1 writer accepted final authorization catalog drift %q: %v", tc.name, err)
			}
		})
	}
}
