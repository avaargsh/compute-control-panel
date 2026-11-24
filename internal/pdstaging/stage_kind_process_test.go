package pdstaging

import (
	"context"
	"database/sql"
	"encoding/json"
	"os"
	"testing"
	"time"

	"github.com/avaargsh/compute-control-panel/internal/operation"
	"github.com/avaargsh/compute-control-panel/internal/store/postgres"
	_ "github.com/jackc/pgx/v5/stdlib"
)

// TestDisposablePDStageProcess is an opt-in child of the real kind/PG golden
// test. It is never compiled into a production executable or mounted as a route.
func TestDisposablePDStageProcess(t *testing.T) {
	if os.Getenv("CCP_PD_KIND_STAGE_CHILD") != "1" {
		t.Skip("requires disposable golden test parent")
	}
	if os.Getenv("CCP_DISPOSABLE_KIND_SMOKE") != "1" ||
		os.Getenv("CCP_PD_KIND_NAMESPACE") != "ccp-pd-paired-smoke" {
		t.Fatal("requires explicit disposable kind fixture")
	}
	var input struct {
		Owner                        postgres.PDIntentOwner
		IntentID, Worker, ProviderID string
		PoolID, Marker               string
		Version, Token               uint64
	}
	decoder := json.NewDecoder(os.Stdin)
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&input); err != nil {
		t.Fatal(err)
	}
	db, err := sql.Open("pgx", os.Getenv("DATABASE_URL"))
	if err != nil {
		t.Fatal("could not open fixture database")
	}
	defer db.Close()
	repo, err := postgres.NewRepository(db)
	if err != nil {
		t.Fatal(err)
	}
	namespace := os.Getenv("CCP_PD_KIND_NAMESPACE")
	cfg := Config{
		APIURL: os.Getenv("CCP_PD_KIND_API_URL"), CAPath: os.Getenv("CCP_PD_KIND_CA_PATH"),
		TokenPath: os.Getenv("CCP_PD_KIND_TOKEN_PATH"), BindingDSN: os.Getenv("CCP_PD_KIND_BINDING_DSN"),
		Namespace: namespace, LocalQueue: namespace, ResourceClaimTemplateName: "pd-gpu",
		TenantID: input.Owner.TenantID, ProjectID: input.Owner.ProjectID,
		ProviderID: input.ProviderID, PoolID: input.PoolID,
	}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	stage, err := StageInertPDJobSet(ctx, repo, input.Owner, input.IntentID, input.Version, input.Worker, input.Token, cfg)
	if err != nil {
		t.Fatalf("canonical stage failed: %v", err)
	}
	if stage.Operation.Status != operation.InFlight || stage.Operation.ID != "pd-operation-"+input.IntentID ||
		stage.Operation.Version != input.Version || stage.Operation.FenceToken != input.Token {
		t.Fatal("canonical stage changed operation authority or prematurely finalized")
	}
	if err := os.WriteFile(input.Marker, []byte("staged\n"), 0600); err != nil {
		t.Fatal(err)
	}
	// Parent must SIGKILL the process after the real durable pin and before
	// any control-plane ACK/reconciliation; normal completion is a failure.
	time.Sleep(time.Minute)
	t.Fatal("parent did not kill staging process")
}
