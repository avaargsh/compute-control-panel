package postgres

import (
	"context"
	"errors"
	"testing"
	"time"
)

func TestResolveWorkloadPrincipalReturnsActiveTenantAndProjectGrants(t *testing.T) {
	db := openIntegrationDB(t)
	repo, err := NewRepository(db)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	spiffeID := "spiffe://example.org/ns/agent-runtime/sa/" + randomSuffix(t)
	tenantID := "tenant-identity-" + randomSuffix(t)
	if _, err := db.ExecContext(ctx, `
		INSERT INTO compute_service_identities (spiffe_id, tenant_id, service_name, status)
		VALUES ($1,$2,'agent-runtime','ACTIVE')`,
		spiffeID, tenantID); err != nil {
		t.Fatal(err)
	}
	for _, projectID := range []string{"project-identity-b", "project-identity-a"} {
		if _, err := db.ExecContext(ctx, `
			INSERT INTO compute_service_project_grants
				(spiffe_id, tenant_id, project_id, status, granted_by)
			VALUES ($1,$2,$3,'ACTIVE','identity-admin')`,
			spiffeID, tenantID, projectID); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := db.ExecContext(ctx, `
		INSERT INTO compute_service_project_grants
			(spiffe_id, tenant_id, project_id, status, granted_by)
		VALUES ($1,$2,'project-identity-revoked','REVOKED','identity-admin')`,
		spiffeID, tenantID); err != nil {
		t.Fatal(err)
	}

	principal, err := repo.ResolveWorkloadPrincipal(ctx, spiffeID)
	if err != nil {
		t.Fatalf("ResolveWorkloadPrincipal: %v", err)
	}
	if principal.Subject != spiffeID || principal.TenantID != tenantID || principal.Service != "agent-runtime" {
		t.Fatalf("unexpected resolved principal: %+v", principal)
	}
	if len(principal.ProjectIDs) != 2 ||
		principal.ProjectIDs[0] != "project-identity-a" ||
		principal.ProjectIDs[1] != "project-identity-b" {
		t.Fatalf("unexpected active Project grants: %+v", principal.ProjectIDs)
	}

	if _, err := db.ExecContext(ctx, `
		UPDATE compute_service_project_grants
		SET status='REVOKED', updated_at=clock_timestamp()
		WHERE spiffe_id=$1 AND project_id='project-identity-a'`, spiffeID); err != nil {
		t.Fatal(err)
	}
	principal, err = repo.ResolveWorkloadPrincipal(ctx, spiffeID)
	if err != nil {
		t.Fatal(err)
	}
	if len(principal.ProjectIDs) != 1 || principal.ProjectIDs[0] != "project-identity-b" {
		t.Fatalf("revoked Project grant remained active: %+v", principal.ProjectIDs)
	}

	if _, err := db.ExecContext(ctx, `
		UPDATE compute_service_identities
		SET status='REVOKED', updated_at=clock_timestamp()
		WHERE spiffe_id=$1`, spiffeID); err != nil {
		t.Fatal(err)
	}
	if _, err := repo.ResolveWorkloadPrincipal(ctx, spiffeID); !errors.Is(err, ErrWorkloadIdentityNotFound) {
		t.Fatalf("revoked identity error = %v, want ErrWorkloadIdentityNotFound", err)
	}

	var auditCount int
	if err := db.QueryRowContext(ctx,
		"SELECT count(*) FROM compute_identity_directory_audit WHERE spiffe_id=$1",
		spiffeID,
	).Scan(&auditCount); err != nil {
		t.Fatal(err)
	}
	if auditCount != 6 {
		t.Fatalf("identity directory audit events = %d, want 6", auditCount)
	}
	var identityInsert, identityRevoke, grantInsert, grantRevoke int
	if err := db.QueryRowContext(ctx, `
		SELECT
			count(*) FILTER (WHERE entity_type='SERVICE_IDENTITY' AND action='INSERT'),
			count(*) FILTER (WHERE entity_type='SERVICE_IDENTITY' AND action='UPDATE'),
			count(*) FILTER (WHERE entity_type='PROJECT_GRANT' AND action='INSERT'),
			count(*) FILTER (WHERE entity_type='PROJECT_GRANT' AND action='UPDATE')
		FROM compute_identity_directory_audit WHERE spiffe_id=$1`,
		spiffeID,
	).Scan(&identityInsert, &identityRevoke, &grantInsert, &grantRevoke); err != nil {
		t.Fatal(err)
	}
	if identityInsert != 1 || identityRevoke != 1 || grantInsert != 3 || grantRevoke != 1 {
		t.Fatalf("identity audit insert/revoke identity=%d/%d grants=%d/%d",
			identityInsert, identityRevoke, grantInsert, grantRevoke)
	}
}

func TestResolveWorkloadPrincipalRejectsUnknownOrMalformedIdentity(t *testing.T) {
	db := openIntegrationDB(t)
	repo, err := NewRepository(db)
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	for _, spiffeID := range []string{
		"",
		"spiffe://",
		" spiffe://example.org/ns/service ",
		"spiffe://example.org/ns/missing/sa/identity",
	} {
		if _, err := repo.ResolveWorkloadPrincipal(ctx, spiffeID); !errors.Is(err, ErrWorkloadIdentityNotFound) {
			t.Fatalf("ResolveWorkloadPrincipal(%q) error = %v", spiffeID, err)
		}
	}
}
