package main

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"strings"
	"time"

	"github.com/avaargsh/compute-control-panel/internal/operation"
	"github.com/avaargsh/compute-control-panel/internal/provider"
	"github.com/avaargsh/compute-control-panel/internal/recovery"
	"github.com/avaargsh/compute-control-panel/internal/store/postgres"
)

// This disposable-only command uses exactly the already-migrated PostgreSQL
// operation journal and Kueue binding store. It is not a production worker
// bootstrap, an API endpoint, a GPU scheduler or an Allocation release gate.
const (
	journalSmokeOperationID = "ccp-kind-journal-create"
	journalSmokeSource      = "kubernetes-https-readonly-unattested"
	journalSmokeVersion     = "kueue-v1beta1-single-pod-v1"
	journalSmokeScope       = smokeNamespace
	journalSmokeCapability  = "sha256:" + "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
)

func journalCreateRequest() provider.Request {
	return provider.Request{
		OperationID: journalSmokeOperationID,
		TenantID:    "tenant-a", WorkloadID: "workload-kind-journal",
		ProviderID: "kueue-journal", IdempotencyKey: "journal-kind-create-idem",
		RequestDigest:     "sha256:" + strings.Repeat("ef", 32),
		DesiredGeneration: 1, Kind: operation.Create,
	}
}

func journalProviderRegistration() postgres.ProviderRegistration {
	return postgres.ProviderRegistration{
		ProviderID: journalCreateRequest().ProviderID, ClusterScope: journalSmokeScope,
		AdapterSource: journalSmokeSource, AdapterVersion: journalSmokeVersion,
		CapabilityDigest: journalSmokeCapability,
	}
}

func parseJournalArgs(args []string) (phase string, hold bool, err error) {
	if len(args) == 3 && args[0] == "journal" && args[1] == "create" && args[2] == "--hold" {
		return "create", true, nil
	}
	if len(args) == 2 && args[0] == "journal" && args[1] == "recover" {
		return "recover", false, nil
	}
	return "", false, errors.New("journal smoke requires 'create --hold' or 'recover'")
}

type journalSmokeReport struct {
	Phase           string           `json:"phase"`
	Status          operation.Status `json:"status"`
	PinDurable      bool             `json:"pin_durable"`
	JobName         string           `json:"job_name"`
	UID             string           `json:"uid"`
	OperationID     string           `json:"operation_id"`
	FenceToken      uint64           `json:"fence_token"`
	Version         uint64           `json:"version"`
	Observation     string           `json:"observation"`
	ObservationRows int              `json:"observation_rows"`
	OutboxEvents    int              `json:"outbox_events"`
	ReleaseAttested bool             `json:"release_attested"`
}

func journalCreate(getenv func(string) string, out io.Writer) error {
	db, adapter, err := openDurableAdapter(getenv)
	if err != nil {
		return err
	}
	defer func() { _ = db.Close() }()
	store, err := provider.NewPostgresKueueJobBindingStore(db)
	if err != nil {
		return err
	}
	repo, err := postgres.NewRepository(db)
	if err != nil {
		return err
	}
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	req := journalCreateRequest()
	registration, created, err := repo.RegisterProvider(ctx, journalProviderRegistration())
	if err != nil || !created || registration.ProviderID != req.ProviderID {
		return fmt.Errorf("register isolated journal Provider: created=%t err=%v", created, err)
	}

	normalized := json.RawMessage(`{"project_id":"project-1","resources":{"cpu":"1"}}`)
	sum := sha256.Sum256(normalized)
	_, created, err = repo.CreateWorkload(ctx, postgres.CreateWorkloadParams{
		WorkloadID: req.WorkloadID, TenantID: req.TenantID,
		ProjectID: "project-1", OwnerService: "agent-runtime",
		OwnerResourceType: "run", OwnerResourceID: "run-kind-journal",
		NormalizedRequest: normalized, RequestDigest: "sha256:" + hex.EncodeToString(sum[:]),
		IdempotencyKey: "workload-kind-journal-idem",
		EventID:        "workload-kind-journal-event", CorrelationID: "kind-journal",
	})
	if err != nil || !created {
		return fmt.Errorf("record isolated journal Workload: created=%t err=%v", created, err)
	}
	pending, created, err := repo.CreateOperation(ctx, operation.Operation{
		ID: req.OperationID, TenantID: req.TenantID,
		WorkloadID: req.WorkloadID, ProviderID: req.ProviderID,
		IdempotencyKey: req.IdempotencyKey, RequestDigest: req.RequestDigest,
		Generation: req.DesiredGeneration, Version: 1,
		Kind: operation.Create, Status: operation.Pending,
	}, "operation-kind-journal-event", "kind-journal")
	if err != nil || !created {
		return fmt.Errorf("record immutable journal operation: created=%t err=%v", created, err)
	}
	claimed, err := repo.ClaimOperation(ctx, req.TenantID, req.OperationID, pending.Version, "journal-writer", time.Minute)
	if err != nil {
		return err
	}
	started, err := repo.StartOperation(ctx, req.TenantID, req.OperationID, claimed.Version, "journal-writer", claimed.FenceToken)
	if err != nil {
		return err
	}

	// Kubernetes has committed the suspended Job, but the higher-level
	// operation has no final evidence. Record UNKNOWN before killing this
	// process; a new worker may inspect only, not repeat the side effect.
	if _, err := adapter.Apply(ctx, req); err != nil {
		return fmt.Errorf("create isolated real Job: %w", err)
	}
	pin, ok, err := store.GetCreatePin(smokeNamespace, req)
	if err != nil || !ok || pin.UID == "" || pin.Name == "" {
		return fmt.Errorf("create UID pin not durable: present=%t err=%v", ok, err)
	}
	unknown, err := repo.MarkOperationUnknown(ctx, req.TenantID, req.OperationID,
		started.Version, "journal-writer", started.FenceToken)
	if err != nil || unknown.Status != operation.Unknown {
		return fmt.Errorf("commit UNKNOWN before SIGKILL: %+v err=%v", unknown, err)
	}
	if err := writeDurable(out, journalSmokeReport{
		Phase: "journal-create", Status: unknown.Status, PinDurable: true,
		JobName: pin.Name, UID: pin.UID, OperationID: req.OperationID,
		FenceToken: unknown.FenceToken, Version: unknown.Version,
	}); err != nil {
		return err
	}
	holdUntilKilled()
	return errors.New("journal smoke process unexpectedly survived SIGKILL")
}

// journalReadOnly is a second, fresh-process adapter wrapper. Panic rather
// than silently dispatching a new Kueue Job if the recovery worker regresses.
type journalReadOnly struct{ provider.Adapter }

func (journalReadOnly) Apply(context.Context, provider.Request) (provider.Acknowledgement, error) {
	panic("journal recovery must not invoke Provider.Apply")
}

func journalRecover(getenv func(string) string, out io.Writer) error {
	db, adapter, err := openDurableAdapter(getenv)
	if err != nil {
		return err
	}
	defer func() { _ = db.Close() }()
	repo, err := postgres.NewRepository(db)
	if err != nil {
		return err
	}
	store, err := provider.NewPostgresKueueJobBindingStore(db)
	if err != nil {
		return err
	}
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	req := journalCreateRequest()
	previous, err := repo.GetOperation(ctx, req.TenantID, req.OperationID)
	if err != nil || previous.Status != operation.Unknown {
		return fmt.Errorf("journal is not UNKNOWN before recovery: %+v err=%v", previous, err)
	}
	worker, err := recovery.NewWorker(repo, map[string]recovery.RegisteredProvider{
		req.ProviderID: {
			Adapter:      journalReadOnly{Adapter: adapter},
			ClusterScope: journalSmokeScope, Source: journalSmokeSource,
			AdapterVersion: journalSmokeVersion, CapabilityDigest: journalSmokeCapability,
		},
	})
	if err != nil {
		return err
	}
	next, err := worker.RecoverUnknown(ctx, req.TenantID, req.OperationID, "journal-recovery", time.Minute)
	if err != nil {
		return fmt.Errorf("inspect-only fenced recovery: %w", err)
	}
	pin, ok, err := store.GetCreatePin(smokeNamespace, req)
	if err != nil || !ok || pin.UID == "" || pin.UID != next.ProviderResourceUID ||
		next.ProviderResourceID != smokeNamespace+"/"+pin.Name ||
		next.Status != operation.Succeeded || next.FenceToken <= previous.FenceToken ||
		next.Version <= previous.Version {
		return fmt.Errorf("recovery was not fenced to original durable Job UID: %+v pin=%+v found=%t err=%v",
			next, pin, ok, err)
	}
	var observations, events int
	var result, source, version, uid string
	if err := db.QueryRowContext(ctx, `SELECT count(*) FROM public.compute_resource_observations WHERE tenant_id=$1 AND compute_operation_id=$2`,
		req.TenantID, req.OperationID).Scan(&observations); err != nil {
		return err
	}
	if err := db.QueryRowContext(ctx, `SELECT operation_result, source, adapter_version, provider_resource_uid FROM public.compute_resource_observations WHERE tenant_id=$1 AND compute_operation_id=$2`,
		req.TenantID, req.OperationID).Scan(&result, &source, &version, &uid); err != nil {
		return err
	}
	if err := db.QueryRowContext(ctx, `SELECT count(*) FROM public.compute_outbox_events WHERE tenant_id=$1 AND aggregate_type='compute_operation' AND aggregate_id=$2`,
		req.TenantID, req.OperationID).Scan(&events); err != nil {
		return err
	}
	if observations != 1 || events != 2 || result != string(operation.ResourcePresent) ||
		source != journalSmokeSource || version != journalSmokeVersion || uid != pin.UID {
		return fmt.Errorf("journal evidence not atomically committed: rows=%d events=%d result=%q source=%q version=%q uid=%q",
			observations, events, result, source, version, uid)
	}
	return writeDurable(out, journalSmokeReport{
		Phase: "journal-recover", Status: next.Status, PinDurable: true,
		JobName: pin.Name, UID: next.ProviderResourceUID, OperationID: req.OperationID,
		FenceToken: next.FenceToken, Version: next.Version,
		Observation: result, ObservationRows: observations, OutboxEvents: events,
	})
}

func runJournal(args []string, getenv func(string) string, out io.Writer) error {
	phase, _, err := parseJournalArgs(args)
	if err != nil {
		return err
	}
	if err := requireDisposableSmoke(getenv); err != nil {
		return err
	}
	switch phase {
	case "create":
		return journalCreate(getenv, out)
	case "recover":
		return journalRecover(getenv, out)
	default:
		return errors.New("invalid journal smoke phase")
	}
}
