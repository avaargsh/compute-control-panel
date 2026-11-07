package httpapi

import (
	"context"
	"crypto/rand"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/avaargsh/compute-control-panel/internal/store/postgres"
	_ "github.com/jackc/pgx/v5/stdlib"
)

func providerManagementTestSuffix(t *testing.T) string {
	t.Helper()
	var entropy [10]byte
	if _, err := rand.Read(entropy[:]); err != nil {
		t.Fatal(err)
	}
	return hex.EncodeToString(entropy[:])
}

// This is an actual PostgreSQL identity-directory + registry transaction flow
// behind a verified client-certificate TLS handshake. It does not attest
// production issuer policy, deployment roles or an external GPU Provider.
func TestProviderManageVerifiedTLSAndAuthoritativePostgresIdentityRevocation(t *testing.T) {
	dsn := os.Getenv("DATABASE_URL")
	if dsn == "" {
		t.Skip("DATABASE_URL not configured; PostgreSQL integration unavailable")
	}
	db, err := sql.Open("pgx", dsn)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	if err := db.PingContext(ctx); err != nil {
		t.Fatal(err)
	}
	repo, err := postgres.NewRepository(db)
	if err != nil {
		t.Fatal(err)
	}

	suffix := providerManagementTestSuffix(t)
	spiffeID := "spiffe://example.org/ns/admin/sa/manager-" + suffix
	tenantID := "tenant-provider-manage-" + suffix
	providerID := "provider-manage-" + suffix
	if _, err := db.ExecContext(ctx, `INSERT INTO compute_service_identities
		(spiffe_id, tenant_id, service_name, status)
		VALUES ($1,$2,'compute-admin','ACTIVE')`, spiffeID, tenantID); err != nil {
		t.Fatal(err)
	}
	grants := []ProviderManagementGrant{
		{Subject: spiffeID, TenantID: tenantID, Service: "compute-admin", ProviderID: providerID, Action: "register"},
		{Subject: spiffeID, TenantID: tenantID, Service: "compute-admin", ProviderID: providerID, Action: "read"},
		{Subject: spiffeID, TenantID: tenantID, Service: "compute-admin", ProviderID: providerID, Action: "drain"},
	}
	policy, err := NewExactProviderManagementAuthorizer(grants)
	if err != nil {
		t.Fatal(err)
	}
	authenticator, err := NewSPIFFEWorkloadAuthenticator("example.org", repo)
	if err != nil {
		t.Fatal(err)
	}
	handler, err := NewProviderManagementHandler(ProviderManagementDependencies{
		Authenticator: authenticator,
		Authorizer:    policy,
		Repository:    repo,
	})
	if err != nil {
		t.Fatal(err)
	}
	server, client := newProviderManagementTLSTestClient(t, handler, spiffeID)

	request := func(method, path, body string) *http.Response {
		t.Helper()
		req, err := http.NewRequestWithContext(ctx, method, server.URL+path, strings.NewReader(body))
		if err != nil {
			t.Fatal(err)
		}
		resp, err := client.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		return resp
	}
	registration := fmt.Sprintf(`{"provider_id":"%s","cluster_scope":"cluster-test","adapter_source":"kubernetes","adapter_version":"v1","capability_digest":"sha256:%s"}`,
		providerID, strings.Repeat("a", 64))
	registered := request(http.MethodPost, "/api/v1/manage/providers", registration)
	if registered.StatusCode != http.StatusCreated {
		t.Fatalf("registration status=%d", registered.StatusCode)
	}
	_ = registered.Body.Close()

	path := "/api/v1/manage/providers/" + providerID
	read := request(http.MethodGet, path, "")
	var snapshot struct {
		State   string `json:"state"`
		Version int64  `json:"version"`
	}
	if err := json.NewDecoder(read.Body).Decode(&snapshot); err != nil {
		t.Fatal(err)
	}
	_ = read.Body.Close()
	if read.StatusCode != http.StatusOK || snapshot.State != "REGISTERED" || snapshot.Version != 1 {
		t.Fatalf("read status=%d, snapshot=%+v", read.StatusCode, snapshot)
	}

	drained := request(http.MethodPost, path+"/drain",
		`{"expected_version":1,"reason_code":"MAINTENANCE"}`)
	if drained.StatusCode != http.StatusOK {
		t.Fatalf("drain status=%d", drained.StatusCode)
	}
	_ = drained.Body.Close()

	replay := request(http.MethodPost, "/api/v1/manage/providers", registration)
	var state struct {
		State   string `json:"state"`
		Version int64  `json:"version"`
	}
	if err := json.NewDecoder(replay.Body).Decode(&state); err != nil {
		t.Fatal(err)
	}
	_ = replay.Body.Close()
	if replay.StatusCode != http.StatusOK || state.State != "DRAINING" || state.Version != 2 {
		t.Fatalf("replay resurrected drained Provider: status=%d state=%+v", replay.StatusCode, state)
	}

	if _, err := db.ExecContext(ctx, `UPDATE compute_service_identities
		SET status='REVOKED', updated_at=clock_timestamp()
		WHERE spiffe_id=$1`, spiffeID); err != nil {
		t.Fatal(err)
	}
	rejected := request(http.MethodGet, path, "")
	_ = rejected.Body.Close()
	if rejected.StatusCode != http.StatusUnauthorized {
		t.Fatalf("revoked identity still used to manage Provider: status=%d", rejected.StatusCode)
	}

	var providerEvents, identityEvents int
	if err := db.QueryRowContext(ctx, `SELECT count(*) FROM compute_provider_lifecycle_audit
		WHERE provider_id=$1`, providerID).Scan(&providerEvents); err != nil {
		t.Fatal(err)
	}
	if err := db.QueryRowContext(ctx, `SELECT count(*) FROM compute_identity_directory_audit
		WHERE spiffe_id=$1`, spiffeID).Scan(&identityEvents); err != nil {
		t.Fatal(err)
	}
	if providerEvents != 2 || identityEvents != 2 {
		t.Fatalf("durable audit events Provider=%d identity=%d; want 2/2", providerEvents, identityEvents)
	}
}
