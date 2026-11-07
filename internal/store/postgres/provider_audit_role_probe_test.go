package postgres

import (
	"context"
	"database/sql"
	"fmt"
	"testing"
	"time"
)

// RED-only security experiment: these tests describe the desired production
// least-privilege contract, not an accepted security baseline. They must not
// remain in a green branch until a forward-only audit migration exists.
func providerAuditWriterProbe(t *testing.T, db *sql.DB, grantAuditInsert bool) (*sql.Tx, string, ProviderRegistration) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	t.Cleanup(cancel)
	var superuser bool
	if err := db.QueryRowContext(ctx, "SELECT rolsuper FROM pg_roles WHERE rolname=current_user").Scan(&superuser); err != nil {
		t.Fatal(err)
	}
	if !superuser {
		t.Skip("disposable role experiment requires the PostgreSQL CI superuser")
	}
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = tx.Rollback() })
	role := "ccp_audit_probe_" + randomSuffix(t)
	if _, err := tx.ExecContext(ctx, fmt.Sprintf("CREATE ROLE %s NOLOGIN", role)); err != nil {
		t.Fatal(err)
	}
	if _, err := tx.ExecContext(ctx, fmt.Sprintf("GRANT USAGE ON SCHEMA public TO %s", role)); err != nil {
		t.Fatal(err)
	}
	if _, err := tx.ExecContext(ctx, fmt.Sprintf(
		"GRANT SELECT, INSERT, UPDATE ON public.compute_providers TO %s", role)); err != nil {
		t.Fatal(err)
	}
	if grantAuditInsert {
		if _, err := tx.ExecContext(ctx, fmt.Sprintf(
			"GRANT INSERT ON public.compute_provider_lifecycle_audit TO %s", role)); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := tx.ExecContext(ctx, fmt.Sprintf("SET LOCAL ROLE %s", role)); err != nil {
		t.Fatal(err)
	}
	return tx, role, providerRegistrationFixture(t)
}

func TestProviderAuditProbeRestrictedWriterShouldRegisterWithoutAuditInsert(t *testing.T) {
	db := openIntegrationDB(t)
	tx, _, registration := providerAuditWriterProbe(t, db, false)
	_, err := tx.ExecContext(context.Background(), `
		INSERT INTO public.compute_providers
			(provider_id, cluster_scope, adapter_source, adapter_version, capability_digest)
		VALUES($1,$2,$3,$4,$5)`,
		registration.ProviderID, registration.ClusterScope, registration.AdapterSource,
		registration.AdapterVersion, registration.CapabilityDigest)
	if err != nil {
		t.Fatalf("P0: restricted Provider writer cannot register without direct audit INSERT: %v", err)
	}
}

func TestProviderAuditProbeGrantingAuditInsertMustNotPermitForgery(t *testing.T) {
	db := openIntegrationDB(t)
	tx, role, registration := providerAuditWriterProbe(t, db, true)
	_, err := tx.ExecContext(context.Background(), `
		INSERT INTO public.compute_providers
			(provider_id, cluster_scope, adapter_source, adapter_version, capability_digest)
		VALUES($1,$2,$3,$4,$5)`,
		registration.ProviderID, registration.ClusterScope, registration.AdapterSource,
		registration.AdapterVersion, registration.CapabilityDigest)
	if err != nil {
		t.Fatalf("audit-INSERT workaround could not create Provider: %v", err)
	}
	_, err = tx.ExecContext(context.Background(), `
		INSERT INTO public.compute_provider_lifecycle_audit
		(provider_id, version, previous_state, state, reason_code, actor_db_role)
		VALUES ($1,2,'REGISTERED','DRAINING','FORGED_BY_WRITER',$2)`,
		registration.ProviderID, role)
	if err == nil {
		t.Fatal("P0: direct audit INSERT allowed forged DRAINING v2 while Provider remained REGISTERED")
	}
}
