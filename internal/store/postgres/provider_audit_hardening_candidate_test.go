package postgres

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"
)

// Validates a *candidate* forward-only audit function replacement using
// disposable database roles and transaction-scoped DDL. This does not modify
// the checked-in migration lineage or enable a production writer role.
func TestProviderAuditHardenedFunctionCandidateRestrictedWriter(t *testing.T) {
	db := openIntegrationDB(t)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	var superuser bool
	if err := db.QueryRowContext(ctx,
		"SELECT rolsuper FROM pg_roles WHERE rolname=current_user").Scan(&superuser); err != nil {
		t.Fatal(err)
	}
	if !superuser {
		t.Skip("requires disposable PostgreSQL CI superuser to provision test roles")
	}
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = tx.Rollback() }()

	owner := "ccp_provider_audit_owner_" + randomSuffix(t)
	writer := "ccp_provider_writer_" + randomSuffix(t)
	for _, statement := range []string{
		fmt.Sprintf("CREATE ROLE %s NOLOGIN NOSUPERUSER NOCREATEDB NOCREATEROLE NOINHERIT", owner),
		fmt.Sprintf("CREATE ROLE %s NOLOGIN NOSUPERUSER NOCREATEDB NOCREATEROLE NOINHERIT", writer),
		fmt.Sprintf("GRANT USAGE ON SCHEMA public TO %s, %s", owner, writer),
		fmt.Sprintf("GRANT INSERT ON public.compute_provider_lifecycle_audit TO %s", owner),
		fmt.Sprintf("GRANT SELECT, INSERT, UPDATE ON public.compute_providers TO %s", writer),
		// This candidate is installed only in this transaction, not as a migration.
		`CREATE OR REPLACE FUNCTION public.compute_provider_lifecycle_audit_append()
		RETURNS trigger LANGUAGE plpgsql SECURITY DEFINER
		SET search_path = pg_catalog, pg_temp
		AS $function$
		BEGIN
			INSERT INTO public.compute_provider_lifecycle_audit
				(provider_id, version, previous_state, state, reason_code, actor_db_role)
			VALUES (
				NEW.provider_id, NEW.version,
				CASE WHEN TG_OP='INSERT' THEN NULL ELSE OLD.state END,
				NEW.state, NEW.last_reason_code, session_user
			);
			RETURN NEW;
		END;
		$function$`,
		fmt.Sprintf("ALTER FUNCTION public.compute_provider_lifecycle_audit_append() OWNER TO %s", owner),
		"REVOKE ALL ON FUNCTION public.compute_provider_lifecycle_audit_append() FROM PUBLIC",
	} {
		if _, err := tx.ExecContext(ctx, statement); err != nil {
			t.Fatalf("candidate setup failed: %s: %v", statement[:min(len(statement), 90)], err)
		}
	}

	var securityDefiner bool
	var functionOwner string
	var settings string
	if err := tx.QueryRowContext(ctx, `SELECT p.prosecdef, pg_get_userbyid(p.proowner), COALESCE(array_to_string(p.proconfig, ';'), '')
		FROM pg_proc p WHERE p.oid='public.compute_provider_lifecycle_audit_append()'::regprocedure`).
		Scan(&securityDefiner, &functionOwner, &settings); err != nil {
		t.Fatal(err)
	}
	if !securityDefiner || functionOwner != owner || settings != "search_path=pg_catalog, pg_temp" {
		t.Fatalf("unsafe candidate function catalog: definer=%t owner=%s config=%v", securityDefiner, functionOwner, settings)
	}
	if _, err := tx.ExecContext(ctx, fmt.Sprintf("SET LOCAL ROLE %s", writer)); err != nil {
		t.Fatal(err)
	}
	// A user-controlled pg_temp table must not hijack audit writes.
	if _, err := tx.ExecContext(ctx,
		"CREATE TEMP TABLE compute_provider_lifecycle_audit (provider_id text)"); err != nil {
		t.Fatal(err)
	}
	var canForge bool
	if err := tx.QueryRowContext(ctx, `SELECT has_table_privilege(
		current_user, 'public.compute_provider_lifecycle_audit', 'INSERT')`).Scan(&canForge); err != nil {
		t.Fatal(err)
	}
	if canForge {
		t.Fatal("Provider writer has a forbidden direct audit INSERT grant")
	}
	registration := providerRegistrationFixture(t)
	if _, err := tx.ExecContext(ctx, `INSERT INTO public.compute_providers
		(provider_id, cluster_scope, adapter_source, adapter_version, capability_digest)
		VALUES($1,$2,$3,$4,$5)`,
		registration.ProviderID, registration.ClusterScope, registration.AdapterSource,
		registration.AdapterVersion, registration.CapabilityDigest); err != nil {
		t.Fatalf("restricted Provider registration failed: %v", err)
	}
	if _, err := tx.ExecContext(ctx, `UPDATE public.compute_providers
		SET state='DRAINING', version=2, last_reason_code='MAINTENANCE',
		updated_at=clock_timestamp()
		WHERE provider_id=$1 AND version=1 AND state='REGISTERED'`, registration.ProviderID); err != nil {
		t.Fatalf("restricted Provider drain failed: %v", err)
	}

	for _, forbidden := range []string{
		`INSERT INTO public.compute_provider_lifecycle_audit
			(provider_id, version, previous_state, state, reason_code, actor_db_role)
			VALUES ($1,3,'DRAINING','DRAINING','FORGED','untrusted')`,
		"TRUNCATE public.compute_provider_lifecycle_audit",
		"ALTER TABLE public.compute_providers DISABLE TRIGGER compute_provider_lifecycle_audit_append",
	} {
		if _, err := tx.ExecContext(ctx, "SAVEPOINT denied_writer_operation"); err != nil {
			t.Fatal(err)
		}
		var err error
		if strings.Contains(forbidden, "$1") {
			_, err = tx.ExecContext(ctx, forbidden, registration.ProviderID)
		} else {
			_, err = tx.ExecContext(ctx, forbidden)
		}
		if err == nil {
			t.Fatalf("restricted Provider writer could run forbidden operation: %s", forbidden)
		}
		var sqlErr interface{ SQLState() string }
		if !errors.As(err, &sqlErr) || sqlErr.SQLState() != "42501" {
			t.Fatalf("expected permission-denied 42501 for %q, got %v", forbidden, err)
		}
		if _, err := tx.ExecContext(ctx, "ROLLBACK TO SAVEPOINT denied_writer_operation"); err != nil {
			t.Fatal(err)
		}
		if _, err := tx.ExecContext(ctx, "RELEASE SAVEPOINT denied_writer_operation"); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := tx.ExecContext(ctx, "RESET ROLE"); err != nil {
		t.Fatal(err)
	}
	var state string
	var version int
	if err := tx.QueryRowContext(ctx,
		"SELECT state, version FROM public.compute_providers WHERE provider_id=$1",
		registration.ProviderID).Scan(&state, &version); err != nil {
		t.Fatal(err)
	}
	var count int
	if err := tx.QueryRowContext(ctx,
		"SELECT count(*) FROM public.compute_provider_lifecycle_audit WHERE provider_id=$1",
		registration.ProviderID).Scan(&count); err != nil {
		t.Fatal(err)
	}
	if state != "DRAINING" || version != 2 || count != 2 {
		t.Fatalf("candidate persisted wrong lifecycle evidence state=%s version=%d auditRows=%d", state, version, count)
	}
}
