package postgres

import (
	"context"
	"errors"
	"testing"
	"time"
)

// A revoked permanent SPIFFE identity must not be resurrected by a
// session-local table with the same unqualified name. The test pins one
// connection so ResolveWorkloadPrincipal runs in that session's pg_temp.
func TestResolveWorkloadPrincipalRejectsTempIdentityResurrection(t *testing.T) {
	db := openIntegrationDB(t)
	db.SetMaxOpenConns(1)
	db.SetMaxIdleConns(1)
	repo, err := NewRepository(db)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	spiffeID := "spiffe://example.org/ns/agent-runtime/sa/revoked-" + randomSuffix(t)
	tenantID := "tenant-identity-revoked-" + randomSuffix(t)
	if _, err := db.ExecContext(ctx, `INSERT INTO public.compute_service_identities
		(spiffe_id,tenant_id,service_name,status)
		VALUES($1,$2,'agent-runtime','ACTIVE')`, spiffeID, tenantID); err != nil {
		t.Fatal(err)
	}
	if _, err := db.ExecContext(ctx, `UPDATE public.compute_service_identities
		SET status='REVOKED',updated_at=pg_catalog.clock_timestamp()
		WHERE spiffe_id=$1`, spiffeID); err != nil {
		t.Fatal(err)
	}
	if _, err := db.ExecContext(ctx, `CREATE TEMP TABLE compute_service_identities (
		spiffe_id text NOT NULL, tenant_id text NOT NULL, service_name text NOT NULL,
		status text NOT NULL, not_before timestamptz NOT NULL, expires_at timestamptz
	) ON COMMIT PRESERVE ROWS`); err != nil {
		t.Fatal(err)
	}
	if _, err := db.ExecContext(ctx, `INSERT INTO pg_temp.compute_service_identities
		(spiffe_id,tenant_id,service_name,status,not_before)
		VALUES($1,$2,'agent-runtime','ACTIVE',pg_catalog.clock_timestamp()-interval '1 hour')`,
		spiffeID, tenantID); err != nil {
		t.Fatal(err)
	}
	if _, err := repo.ResolveWorkloadPrincipal(ctx, spiffeID); !errors.Is(err, ErrWorkloadIdentityNotFound) {
		t.Fatalf("revoked permanent identity was resurrected by TEMP table: %v", err)
	}
}

// A client/session TEMP grant cannot confer a Project that is missing from
// the permanent identity directory. The returned principal must have only
// active, tenant-matched Project grants in the authoritative public table.
func TestResolveWorkloadPrincipalIgnoresTempProjectGrantForgery(t *testing.T) {
	db := openIntegrationDB(t)
	db.SetMaxOpenConns(1)
	db.SetMaxIdleConns(1)
	repo, err := NewRepository(db)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	spiffeID := "spiffe://example.org/ns/agent-runtime/sa/grant-" + randomSuffix(t)
	tenantID := "tenant-identity-grant-" + randomSuffix(t)
	allowed := "project-allowed-" + randomSuffix(t)
	forged := "project-forged-" + randomSuffix(t)
	if _, err := db.ExecContext(ctx, `INSERT INTO public.compute_service_identities
		(spiffe_id,tenant_id,service_name,status)
		VALUES($1,$2,'agent-runtime','ACTIVE')`, spiffeID, tenantID); err != nil {
		t.Fatal(err)
	}
	if _, err := db.ExecContext(ctx, `INSERT INTO public.compute_service_project_grants
		(spiffe_id,tenant_id,project_id,status,granted_by)
		VALUES($1,$2,$3,'ACTIVE','identity-admin')`, spiffeID, tenantID, allowed); err != nil {
		t.Fatal(err)
	}
	if _, err := db.ExecContext(ctx, `CREATE TEMP TABLE compute_service_project_grants (
		spiffe_id text NOT NULL, tenant_id text NOT NULL, project_id text NOT NULL,
		status text NOT NULL, not_before timestamptz NOT NULL, expires_at timestamptz
	) ON COMMIT PRESERVE ROWS`); err != nil {
		t.Fatal(err)
	}
	if _, err := db.ExecContext(ctx, `INSERT INTO pg_temp.compute_service_project_grants
		(spiffe_id,tenant_id,project_id,status,not_before)
		VALUES($1,$2,$3,'ACTIVE',pg_catalog.clock_timestamp()-interval '1 hour')`,
		spiffeID, tenantID, forged); err != nil {
		t.Fatal(err)
	}
	principal, err := repo.ResolveWorkloadPrincipal(ctx, spiffeID)
	if err != nil {
		t.Fatal(err)
	}
	if principal.Subject != spiffeID || principal.TenantID != tenantID ||
		len(principal.ProjectIDs) != 1 || principal.ProjectIDs[0] != allowed {
		t.Fatalf("TEMP Project grant distorted authoritative principal: %+v", principal)
	}
}
