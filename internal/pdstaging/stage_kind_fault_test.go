package pdstaging

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"os"
	"testing"
	"time"

	"github.com/avaargsh/compute-control-panel/internal/operation"
	"github.com/avaargsh/compute-control-panel/internal/provider"
	"github.com/avaargsh/compute-control-panel/internal/store/postgres"
	_ "github.com/jackc/pgx/v5/stdlib"
)

// The wrapper is test-only and uses the attested PostgreSQL store. It
// interrupts the actual adapter at the pin boundaries, not a fake K8s API.
type pdFaultBindings struct {
	provider.KueueJobBindingStore
	db           *sql.DB
	mode, marker string
	t            *testing.T
}

func (b *pdFaultBindings) pause() {
	if err := os.WriteFile(b.marker, []byte(b.mode+"\n"), 0600); err != nil {
		b.t.Fatal(err)
	}
	time.Sleep(time.Minute)
	b.t.Fatal("parent did not kill fault process")
}

func (b *pdFaultBindings) GetCreatePin(ns string, req provider.Request) (provider.KueueCreatePin, bool, error) {
	pin, ok, err := b.KueueJobBindingStore.GetCreatePin(ns, req)
	if b.mode == "pre-post" && !ok && err == nil {
		b.pause()
	}
	return pin, ok, err
}

func (b *pdFaultBindings) PutCreatePin(ns string, req provider.Request, name, uid string) error {
	switch b.mode {
	case "post-before-pin":
		b.pause()
	case "binding-disconnect":
		if err := b.db.Close(); err != nil {
			b.t.Fatal(err)
		}
	}
	return b.KueueJobBindingStore.PutCreatePin(ns, req, name, uid)
}

func TestDisposablePDFaultProcess(t *testing.T) {
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
		PoolID, Marker, Fault        string
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
	if input.Fault != "pre-post" && input.Fault != "post-before-pin" && input.Fault != "binding-disconnect" {
		t.Fatal("unexpected fault")
	}
	stage, err := stageWith(ctx, repo, input.Owner, input.IntentID, input.Version, input.Worker, input.Token, cfg,
		func(plan provider.PDJobSetPlan) (stageWriter, error) {
			store, bindingDB, err := provider.OpenDurablePostgresKueueJobBindingStore(ctx, cfg.BindingDSN)
			if err != nil {
				return nil, err
			}
			bindings := &pdFaultBindings{KueueJobBindingStore: store, db: bindingDB, mode: input.Fault, marker: input.Marker, t: t}
			t.Cleanup(func() { _ = bindingDB.Close() })
			return provider.NewServiceAccountPDJobSetAdapterWithBindings(
				cfg.APIURL, cfg.CAPath, cfg.TokenPath, cfg.Namespace, cfg.LocalQueue, bindings, plan)
		})
	if input.Fault != "binding-disconnect" || !errors.Is(err, postgres.ErrPDStagingWriteUncertain) ||
		stage.Operation.Status != operation.Unknown {
		t.Fatalf("fault did not leave expected uncertainty: status=%s err=%v", stage.Operation.Status, err)
	}
	if err := os.WriteFile(input.Marker, []byte(input.Fault+"\n"), 0600); err != nil {
		t.Fatal(err)
	}
}
