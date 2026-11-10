package main

import (
	"context"
	"crypto/sha256"
	"database/sql"
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
	journalSmokeOperationID     = "ccp-kind-journal-create"
	journalDeleteOperationID    = "ccp-kind-journal-delete"
	journalSuccessorOperationID = "ccp-kind-journal-delete-successor"
	journalSmokeSource          = "kubernetes-https-readonly-unattested"
	journalSmokeVersion         = "kueue-v1beta1-single-pod-v1"
	journalSmokeScope           = smokeNamespace
	journalSmokeCapability      = "sha256:" + "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
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

// journalDeleteRequest names the suspended Job created by journalCreateRequest.
// Tenant, workload, provider, and generation stay on that Job's annotations.
// The operation id, digest, and idempotency key are a different DELETE.
func journalDeleteRequest(resourceID, uid string) provider.Request {
	req := journalCreateRequest()
	req.Kind = operation.Delete
	req.OperationID = journalDeleteOperationID
	req.IdempotencyKey = "journal-kind-delete-idem"
	req.RequestDigest = "sha256:" + strings.Repeat("d4", 32)
	req.TargetResourceID = resourceID
	req.TargetResourceUID = uid
	return req
}

// journalSuccessorRequest is a second DELETE of the original UID. It exists so
// recovery can observe a same-name successor without issuing another DELETE.
func journalSuccessorRequest(resourceID, uid string) provider.Request {
	req := journalDeleteRequest(resourceID, uid)
	req.OperationID = journalSuccessorOperationID
	req.IdempotencyKey = "journal-kind-delete-successor-idem"
	req.RequestDigest = "sha256:" + strings.Repeat("d5", 32)
	return req
}

func journalProviderRegistration() postgres.ProviderRegistration {
	return postgres.ProviderRegistration{
		ProviderID: journalCreateRequest().ProviderID, ClusterScope: journalSmokeScope,
		AdapterSource: journalSmokeSource, AdapterVersion: journalSmokeVersion,
		CapabilityDigest: journalSmokeCapability,
	}
}

func parseJournalArgs(args []string) (phase string, hold bool, err error) {
	switch {
	case len(args) == 3 && args[0] == "journal" && args[1] == "create" && args[2] == "--hold":
		return "create", true, nil
	case len(args) == 2 && args[0] == "journal" && args[1] == "recover":
		return "recover", false, nil
	case len(args) == 3 && args[0] == "journal" && args[1] == "delete" && args[2] == "--hold":
		return "delete", true, nil
	case len(args) == 2 && args[0] == "journal" && args[1] == "recover-delete":
		return "recover-delete", false, nil
	case len(args) == 3 && args[0] == "journal" && args[1] == "successor" && args[2] == "--hold":
		return "successor", true, nil
	case len(args) == 2 && args[0] == "journal" && args[1] == "recover-successor":
		return "recover-successor", false, nil
	case len(args) == 2 && args[0] == "journal" && args[1] == "fail-closed":
		return "fail-closed", false, nil
	default:
		return "", false, errors.New("journal smoke phase is not one of create --hold, recover, delete --hold, recover-delete, successor --hold, recover-successor, fail-closed")
	}
}

type journalSmokeReport struct {
	Phase            string           `json:"phase"`
	Status           operation.Status `json:"status"`
	PinDurable       bool             `json:"pin_durable"`
	TombstoneDurable bool             `json:"tombstone_durable"`
	Acknowledged     bool             `json:"acknowledged"`
	JobName          string           `json:"job_name"`
	UID              string           `json:"uid"`
	TargetUID        string           `json:"target_uid"`
	OperationID      string           `json:"operation_id"`
	FenceToken       uint64           `json:"fence_token"`
	Version          uint64           `json:"version"`
	Observation      string           `json:"observation"`
	ReasonCode       string           `json:"reason_code"`
	ObservationRows  int              `json:"observation_rows"`
	OutboxEvents     int              `json:"outbox_events"`
	ReleaseAttested  bool             `json:"release_attested"`
}

type journalFailClosedReport struct {
	Phase           string `json:"phase"`
	FailedClosed    bool   `json:"failed_closed"`
	Reason          string `json:"reason"`
	DeleteApplied   bool   `json:"delete_applied"`
	ReleaseAttested bool   `json:"release_attested"`
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
	worker, err := newJournalWorker(repo, adapter, req.ProviderID)
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

func newJournalWorker(repo *postgres.Repository, adapter provider.Adapter, providerID string) (*recovery.Worker, error) {
	return recovery.NewWorker(repo, map[string]recovery.RegisteredProvider{
		providerID: {
			Adapter:          journalReadOnly{Adapter: adapter},
			ClusterScope:     journalSmokeScope,
			Source:           journalSmokeSource,
			AdapterVersion:   journalSmokeVersion,
			CapabilityDigest: journalSmokeCapability,
		},
	})
}

func ensureJournalProvider(ctx context.Context, repo *postgres.Repository) error {
	registration, _, err := repo.RegisterProvider(ctx, journalProviderRegistration())
	if err != nil {
		return fmt.Errorf("journal Provider registration: %w", err)
	}
	want := journalProviderRegistration()
	if registration.ProviderID != want.ProviderID ||
		registration.ClusterScope != want.ClusterScope ||
		registration.AdapterSource != want.AdapterSource ||
		registration.AdapterVersion != want.AdapterVersion ||
		registration.CapabilityDigest != want.CapabilityDigest {
		return errors.New("journal Provider fingerprint mismatch")
	}
	return nil
}

// openJournalDB opens only the disposable journal database. The successor
// recorder uses it so that phase cannot construct a Kubernetes client.
func openJournalDB(getenv func(string) string) (*sql.DB, error) {
	if err := requireDisposableSmoke(getenv); err != nil {
		return nil, err
	}
	dsn := strings.TrimSpace(getenv("CCP_KUEUE_BINDING_DSN"))
	if dsn == "" {
		return nil, errEmptyBindingDSN
	}
	db, err := sql.Open("pgx", dsn)
	if err != nil {
		return nil, err
	}
	db.SetMaxOpenConns(2)
	return db, nil
}

func journalDelete(getenv func(string) string, out io.Writer) error {
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
	if err := ensureJournalProvider(ctx, repo); err != nil {
		return err
	}
	pin, ok, err := store.GetCreatePin(smokeNamespace, journalCreateRequest())
	if err != nil || !ok || pin.UID == "" || pin.Name == "" {
		return fmt.Errorf("journal create pin missing before DELETE: present=%t err=%v", ok, err)
	}
	req := journalDeleteRequest(smokeNamespace+"/"+pin.Name, pin.UID)
	if err := req.Validate(); err != nil {
		return err
	}
	pending, created, err := repo.CreateOperation(ctx, operation.Operation{
		ID: req.OperationID, TenantID: req.TenantID,
		WorkloadID: req.WorkloadID, ProviderID: req.ProviderID,
		IdempotencyKey: req.IdempotencyKey, RequestDigest: req.RequestDigest,
		Generation: req.DesiredGeneration, Version: 1,
		Kind: operation.Delete, Status: operation.Pending,
		TargetResourceID: req.TargetResourceID, TargetResourceUID: req.TargetResourceUID,
	}, "operation-kind-journal-delete-event", "kind-journal-delete")
	if err != nil || !created {
		return fmt.Errorf("record journal DELETE operation: created=%t err=%v", created, err)
	}
	claimed, err := repo.ClaimOperation(ctx, req.TenantID, req.OperationID, pending.Version, "journal-delete-writer", time.Minute)
	if err != nil {
		return err
	}
	started, err := repo.StartOperation(ctx, req.TenantID, req.OperationID, claimed.Version, "journal-delete-writer", claimed.FenceToken)
	if err != nil {
		return err
	}
	if _, err := adapter.Apply(ctx, req); err != nil {
		return fmt.Errorf("uid-fenced delete: %w", err)
	}
	tomb, ok, err := store.GetDeleteTombstone(smokeNamespace, req)
	if err != nil || !ok || !tomb.Acknowledged || tomb.UID != pin.UID || tomb.Name != pin.Name {
		return fmt.Errorf("acknowledged delete tombstone not durable: %+v present=%t err=%v", tomb, ok, err)
	}
	unknown, err := repo.MarkOperationUnknown(ctx, req.TenantID, req.OperationID,
		started.Version, "journal-delete-writer", started.FenceToken)
	if err != nil || unknown.Status != operation.Unknown || unknown.ProviderResourceUID != "" ||
		unknown.TargetResourceUID != pin.UID {
		return fmt.Errorf("commit DELETE UNKNOWN before SIGKILL: %+v err=%v", unknown, err)
	}
	if err := writeDurable(out, journalSmokeReport{
		Phase: "journal-delete", Status: unknown.Status,
		TombstoneDurable: true, Acknowledged: true,
		JobName: pin.Name, UID: pin.UID, TargetUID: pin.UID, OperationID: req.OperationID,
		FenceToken: unknown.FenceToken, Version: unknown.Version,
	}); err != nil {
		return err
	}
	holdUntilKilled()
	return errors.New("journal delete smoke process unexpectedly survived SIGKILL")
}

func journalDeleteEvidence(ctx context.Context, db *sql.DB, tenantID, operationID string) (observations int, result, source, version, resourceUID, targetUID, reason string, events int, err error) {
	if err = db.QueryRowContext(ctx, `SELECT count(*) FROM public.compute_resource_observations WHERE tenant_id=$1 AND compute_operation_id=$2`,
		tenantID, operationID).Scan(&observations); err != nil {
		return
	}
	if err = db.QueryRowContext(ctx, `SELECT operation_result, source, adapter_version,
		coalesce(provider_resource_uid,''), coalesce(details->>'TargetResourceUID',''),
		coalesce(details->>'ReasonCode','')
		FROM public.compute_resource_observations WHERE tenant_id=$1 AND compute_operation_id=$2`,
		tenantID, operationID).Scan(&result, &source, &version, &resourceUID, &targetUID, &reason); err != nil {
		return
	}
	err = db.QueryRowContext(ctx, `SELECT count(*) FROM public.compute_outbox_events WHERE tenant_id=$1 AND aggregate_type='compute_operation' AND aggregate_id=$2`,
		tenantID, operationID).Scan(&events)
	return
}

// journalRecoverDelete is a fresh process. It runs only Worker.RecoverUnknown.
// TARGET_ABSENT here is the stored tombstone for that UID plus a 404. It is
// not Allocation RELEASED, Kueue quota release, or device release, and it does
// not read or write compute_operations.provider_resource_uid.
func journalRecoverDelete(getenv func(string) string, out io.Writer) error {
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
	previous, err := repo.GetOperation(ctx, journalCreateRequest().TenantID, journalDeleteOperationID)
	if err != nil || previous.Status != operation.Unknown || previous.Kind != operation.Delete ||
		previous.TargetResourceUID == "" || previous.ProviderResourceUID != "" {
		return fmt.Errorf("DELETE journal is not UNKNOWN before recovery: %+v err=%v", previous, err)
	}
	req := journalDeleteRequest(previous.TargetResourceID, previous.TargetResourceUID)
	if req.TenantID != previous.TenantID || req.WorkloadID != previous.WorkloadID ||
		req.ProviderID != previous.ProviderID || req.IdempotencyKey != previous.IdempotencyKey ||
		req.RequestDigest != previous.RequestDigest || req.DesiredGeneration != previous.Generation {
		return fmt.Errorf("DELETE recovery request does not match the journal row: %+v", previous)
	}
	worker, err := newJournalWorker(repo, adapter, req.ProviderID)
	if err != nil {
		return err
	}
	next, err := worker.RecoverUnknown(ctx, req.TenantID, req.OperationID, "journal-delete-recovery", time.Minute)
	if err != nil {
		return fmt.Errorf("inspect-only DELETE recovery: %w", err)
	}
	tomb, ok, err := store.GetDeleteTombstone(smokeNamespace, req)
	if err != nil || !ok || !tomb.Acknowledged || tomb.UID != previous.TargetResourceUID || tomb.Name == "" {
		return fmt.Errorf("DELETE tombstone drifted during recovery: %+v present=%t err=%v", tomb, ok, err)
	}
	if next.Status != operation.Succeeded || next.ProviderResourceUID != "" || next.ProviderResourceID != "" ||
		next.TargetResourceUID != previous.TargetResourceUID || next.TargetResourceID != previous.TargetResourceID ||
		next.FenceToken <= previous.FenceToken || next.Version <= previous.Version {
		return fmt.Errorf("DELETE recovery was not exact-UID TARGET_ABSENT finality: %+v", next)
	}
	observations, result, source, version, resourceUID, targetUID, reason, events, err := journalDeleteEvidence(
		ctx, db, req.TenantID, req.OperationID)
	if err != nil {
		return err
	}
	if observations != 1 || events != 2 || result != string(operation.TargetAbsent) || reason != "" ||
		source != journalSmokeSource || version != journalSmokeVersion ||
		resourceUID != "" || targetUID != previous.TargetResourceUID {
		return fmt.Errorf("DELETE journal evidence not exact-UID TARGET_ABSENT: rows=%d events=%d result=%q reason=%q source=%q version=%q resource_uid=%q target_uid=%q",
			observations, events, result, reason, source, version, resourceUID, targetUID)
	}
	return writeDurable(out, journalSmokeReport{
		Phase: "journal-recover-delete", Status: next.Status,
		TombstoneDurable: true, Acknowledged: true,
		JobName: tomb.Name, TargetUID: targetUID, OperationID: req.OperationID,
		FenceToken: next.FenceToken, Version: next.Version,
		Observation: result, ObservationRows: observations, OutboxEvents: events,
	})
}

// journalSuccessor records a second DELETE of the original UID and does not
// call Kubernetes. A later Inspect-only worker must not delete the successor.
func journalSuccessor(getenv func(string) string, out io.Writer) error {
	db, err := openJournalDB(getenv)
	if err != nil {
		return err
	}
	defer func() { _ = db.Close() }()
	repo, err := postgres.NewRepository(db)
	if err != nil {
		return err
	}
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	if err := ensureJournalProvider(ctx, repo); err != nil {
		return err
	}
	original, err := repo.GetOperation(ctx, journalCreateRequest().TenantID, journalDeleteOperationID)
	if err != nil || original.Kind != operation.Delete || original.TargetResourceUID == "" || original.TargetResourceID == "" {
		return fmt.Errorf("original DELETE target is not journaled: %+v err=%v", original, err)
	}
	req := journalSuccessorRequest(original.TargetResourceID, original.TargetResourceUID)
	if err := req.Validate(); err != nil {
		return err
	}
	pending, created, err := repo.CreateOperation(ctx, operation.Operation{
		ID: req.OperationID, TenantID: req.TenantID,
		WorkloadID: req.WorkloadID, ProviderID: req.ProviderID,
		IdempotencyKey: req.IdempotencyKey, RequestDigest: req.RequestDigest,
		Generation: req.DesiredGeneration, Version: 1,
		Kind: operation.Delete, Status: operation.Pending,
		TargetResourceID: req.TargetResourceID, TargetResourceUID: req.TargetResourceUID,
	}, "operation-kind-journal-successor-event", "kind-journal-successor")
	if err != nil || !created {
		return fmt.Errorf("record successor DELETE operation: created=%t err=%v", created, err)
	}
	claimed, err := repo.ClaimOperation(ctx, req.TenantID, req.OperationID, pending.Version, "journal-successor-writer", time.Minute)
	if err != nil {
		return err
	}
	started, err := repo.StartOperation(ctx, req.TenantID, req.OperationID, claimed.Version, "journal-successor-writer", claimed.FenceToken)
	if err != nil {
		return err
	}
	unknown, err := repo.MarkOperationUnknown(ctx, req.TenantID, req.OperationID,
		started.Version, "journal-successor-writer", started.FenceToken)
	if err != nil || unknown.Status != operation.Unknown || unknown.ProviderResourceUID != "" ||
		unknown.TargetResourceUID != original.TargetResourceUID {
		return fmt.Errorf("commit successor DELETE UNKNOWN before SIGKILL: %+v err=%v", unknown, err)
	}
	_, name, _ := strings.Cut(original.TargetResourceID, "/")
	if err := writeDurable(out, journalSmokeReport{
		Phase: "journal-successor", Status: unknown.Status,
		JobName: name, TargetUID: unknown.TargetResourceUID, OperationID: req.OperationID,
		FenceToken: unknown.FenceToken, Version: unknown.Version,
	}); err != nil {
		return err
	}
	holdUntilKilled()
	return errors.New("journal successor smoke process unexpectedly survived SIGKILL")
}

func journalRecoverSuccessor(getenv func(string) string, out io.Writer) error {
	db, adapter, err := openDurableAdapter(getenv)
	if err != nil {
		return err
	}
	defer func() { _ = db.Close() }()
	repo, err := postgres.NewRepository(db)
	if err != nil {
		return err
	}
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	previous, err := repo.GetOperation(ctx, journalCreateRequest().TenantID, journalSuccessorOperationID)
	if err != nil || previous.Status != operation.Unknown || previous.Kind != operation.Delete ||
		previous.TargetResourceUID == "" || previous.ProviderResourceUID != "" {
		return fmt.Errorf("successor DELETE journal is not UNKNOWN before recovery: %+v err=%v", previous, err)
	}
	req := journalSuccessorRequest(previous.TargetResourceID, previous.TargetResourceUID)
	worker, err := newJournalWorker(repo, adapter, req.ProviderID)
	if err != nil {
		return err
	}
	next, err := worker.RecoverUnknown(ctx, req.TenantID, req.OperationID, "journal-successor-recovery", time.Minute)
	if err != nil {
		return fmt.Errorf("inspect-only successor recovery: %w", err)
	}
	if next.Status != operation.ManualReview || next.ManualReviewCode != "JOB_UID_REPLACED" ||
		next.ProviderResourceUID != "" || next.ProviderResourceID != "" ||
		next.TargetResourceUID != previous.TargetResourceUID || next.TargetResourceID != previous.TargetResourceID ||
		next.FenceToken <= previous.FenceToken || next.Version <= previous.Version {
		return fmt.Errorf("successor recovery adopted or deleted a different UID: %+v", next)
	}
	observations, result, source, version, resourceUID, targetUID, reason, events, err := journalDeleteEvidence(
		ctx, db, req.TenantID, req.OperationID)
	if err != nil {
		return err
	}
	if observations != 1 || events != 2 || result != string(operation.ObservationConflict) ||
		reason != "JOB_UID_REPLACED" || source != journalSmokeSource || version != journalSmokeVersion ||
		resourceUID != "" || targetUID != "" {
		return fmt.Errorf("successor observation was not JOB_UID_REPLACED: rows=%d events=%d result=%q reason=%q resource_uid=%q target_uid=%q",
			observations, events, result, reason, resourceUID, targetUID)
	}
	_, name, _ := strings.Cut(previous.TargetResourceID, "/")
	return writeDurable(out, journalSmokeReport{
		Phase: "journal-recover-successor", Status: next.Status,
		JobName: name, TargetUID: next.TargetResourceUID, OperationID: req.OperationID,
		FenceToken: next.FenceToken, Version: next.Version,
		Observation: result, ReasonCode: reason,
		ObservationRows: observations, OutboxEvents: events,
	})
}

// journalFailClosed attempts Worker.RecoverUnknown and must not Apply. An
// empty DSN stops before an adapter exists. An unreachable DSN fails on the
// journal read. Neither path deletes a Job.
func journalFailClosed(getenv func(string) string, out io.Writer) error {
	if err := requireDisposableSmoke(getenv); err != nil {
		return err
	}
	if strings.TrimSpace(getenv("CCP_KUEUE_BINDING_DSN")) == "" {
		if err := writeDurable(out, journalFailClosedReport{
			Phase: "journal-fail-closed", FailedClosed: true, Reason: "empty_dsn",
		}); err != nil {
			return err
		}
		return errEmptyBindingDSN
	}
	db, adapter, err := openDurableAdapter(getenv)
	if err != nil {
		return err
	}
	defer func() { _ = db.Close() }()
	repo, err := postgres.NewRepository(db)
	if err != nil {
		return err
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	worker, err := newJournalWorker(repo, adapter, journalCreateRequest().ProviderID)
	if err != nil {
		return err
	}
	_, err = worker.RecoverUnknown(ctx, journalCreateRequest().TenantID, journalDeleteOperationID, "journal-fail-closed", time.Minute)
	report := journalFailClosedReport{
		Phase: "journal-fail-closed", Reason: "unreachable", FailedClosed: err != nil,
	}
	if writeErr := writeDurable(out, report); writeErr != nil {
		return writeErr
	}
	if err == nil {
		return errors.New("unreachable database did not fail closed")
	}
	return errors.New("unreachable database failed closed")
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
	case "delete":
		return journalDelete(getenv, out)
	case "recover-delete":
		return journalRecoverDelete(getenv, out)
	case "successor":
		return journalSuccessor(getenv, out)
	case "recover-successor":
		return journalRecoverSuccessor(getenv, out)
	case "fail-closed":
		return journalFailClosed(getenv, out)
	default:
		return errors.New("invalid journal smoke phase")
	}
}
