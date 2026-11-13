package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"strings"
	"time"

	"github.com/avaargsh/compute-control-panel/internal/operation"
	"github.com/avaargsh/compute-control-panel/internal/provider"
	"github.com/avaargsh/compute-control-panel/internal/recovery"
	"github.com/avaargsh/compute-control-panel/internal/store/postgres"
)

// Disposable kind proof for a dropped watch: one queued Job is submitted,
// CCP exits, and a later process rebuilds only from an API re-list plus the
// durable binding and operation journal. This file does not create Workload
// custom resources, mount an HTTP handler, or decide Allocation RELEASED.
const (
	relistCreateID   = "ccp-kind-relist-create"
	relistDeleteID   = "ccp-kind-relist-delete"
	relistSuccessor  = "ccp-kind-relist-successor"
	relistProvider   = "kueue-relist"
	relistSource     = "kubernetes-https-readonly-unattested"
	relistVersion    = "kueue-v1beta1-single-pod-v1"
	relistCapability = "sha256:c4c4c4c4c4c4c4c4c4c4c4c4c4c4c4c4c4c4c4c4c4c4c4c4c4c4c4c4c4c4c4c4"
	relistWorkload   = "workload-relist"
)

func relistCreateRequest() provider.Request {
	return provider.Request{
		OperationID: relistCreateID, TenantID: "tenant-a",
		WorkloadID: relistWorkload, ProviderID: relistProvider,
		IdempotencyKey:    "ccp-kind-relist-create-key",
		RequestDigest:     "sha256:" + strings.Repeat("31", 32),
		DesiredGeneration: 1, Kind: operation.Create,
	}
}

func relistDeleteRequest(resourceID, uid string) provider.Request {
	req := relistCreateRequest()
	req.Kind = operation.Delete
	req.OperationID = relistDeleteID
	req.IdempotencyKey = "ccp-kind-relist-delete-key"
	req.RequestDigest = "sha256:" + strings.Repeat("32", 32)
	req.TargetResourceID = resourceID
	req.TargetResourceUID = uid
	return req
}

func relistSuccessorRequest(resourceID, uid string) provider.Request {
	req := relistDeleteRequest(resourceID, uid)
	req.OperationID = relistSuccessor
	req.IdempotencyKey = "ccp-kind-relist-successor-key"
	req.RequestDigest = "sha256:" + strings.Repeat("33", 32)
	return req
}

func parseRelistArgs(args []string) (string, error) {
	switch {
	case len(args) == 2 && args[0] == "relist" && (args[1] == "record" || args[1] == "window" || args[1] == "converge" || args[1] == "recover-delete" || args[1] == "recover-successor"):
		return args[1], nil
	case len(args) == 3 && args[0] == "relist" && (args[1] == "delete" || args[1] == "successor") && args[2] == "--hold":
		return args[1], nil
	default:
		return "", errors.New("relist phase is not one of record, window, converge, delete --hold, recover-delete, successor --hold, recover-successor")
	}
}

type relistReport struct {
	Phase                        string `json:"phase"`
	Projection                   string `json:"projection"`
	Stage                        string `json:"stage"`
	Inconclusive                 bool   `json:"inconclusive"`
	Result                       string `json:"result"`
	ReasonCode                   string `json:"reason_code"`
	Status                       string `json:"status"`
	QuotaReserved                bool   `json:"quota_reserved"`
	Admitted                     bool   `json:"admitted"`
	ClusterQueue                 string `json:"cluster_queue"`
	WorkloadNamespace            string `json:"workload_namespace"`
	WorkloadName                 string `json:"workload_name"`
	WorkloadUID                  string `json:"workload_uid"`
	WorkloadResourceVersion      string `json:"workload_resource_version"`
	JobUID                       string `json:"job_uid"`
	JobName                      string `json:"job_name"`
	JobResourceVersion           string `json:"job_resource_version"`
	PodUID                       string `json:"pod_uid"`
	PodResourceVersion           string `json:"pod_resource_version"`
	PodScheduled                 bool   `json:"pod_scheduled"`
	NodeName                     string `json:"node_name"`
	TargetUID                    string `json:"target_uid"`
	PriorWorkloadResourceVersion string `json:"prior_workload_resource_version"`
	PriorJobResourceVersion      string `json:"prior_job_resource_version"`
	Attempts                     int    `json:"attempts"`
	StaleRejected                bool   `json:"stale_rejected"`
	CrossRejected                bool   `json:"cross_rejected"`
	Deduped                      bool   `json:"deduped"`
	KeptStale                    bool   `json:"kept_stale"`
	PinDurable                   bool   `json:"pin_durable"`
	TombstoneDurable             bool   `json:"tombstone_durable"`
	Acknowledged                 bool   `json:"acknowledged"`
	AbsenceObserved              bool   `json:"absence_observed"`
	Allocated                    bool   `json:"allocated"`
	Active                       bool   `json:"active"`
	Ready                        bool   `json:"ready"`
	Costable                     bool   `json:"costable"`
	Released                     bool   `json:"released"`
	ReleaseAttested              bool   `json:"release_attested"`
	OperationID                  string `json:"operation_id"`
	ControllerRestart            bool   `json:"controller_restart"`
}

func relistInactive(r relistReport) bool {
	return !r.Allocated && !r.Active && !r.Ready && !r.Costable && !r.Released && !r.ReleaseAttested && !r.KeptStale
}

func reportFromRelist(phase string, p provider.RelistProjection, attempts int) relistReport {
	return relistReport{
		Phase: phase, Projection: p.Phase, Stage: string(p.Stage), Inconclusive: p.Inconclusive,
		QuotaReserved: p.QuotaReserved, Admitted: p.Admitted, ClusterQueue: p.ClusterQueue,
		WorkloadNamespace: p.WorkloadNamespace, WorkloadName: p.WorkloadName,
		WorkloadUID: p.WorkloadUID, WorkloadResourceVersion: p.WorkloadResourceVersion,
		JobUID: p.JobUID, JobName: p.JobName, JobResourceVersion: p.JobResourceVersion,
		PodUID: p.PodUID, PodResourceVersion: p.PodResourceVersion,
		PodScheduled: p.PodScheduled, NodeName: p.NodeName, Attempts: attempts,
		OperationID: relistCreateID,
	}
}

func projectionFromReport(r relistReport) (provider.RelistProjection, error) {
	p := provider.RelistProjection{
		Phase: r.Projection, Stage: provider.KueueObservationStage(r.Stage), Inconclusive: r.Inconclusive,
		QuotaReserved: r.QuotaReserved, Admitted: r.Admitted, ClusterQueue: r.ClusterQueue,
		WorkloadNamespace: r.WorkloadNamespace, WorkloadName: r.WorkloadName,
		WorkloadUID: r.WorkloadUID, WorkloadResourceVersion: r.WorkloadResourceVersion,
		JobNamespace: smokeNamespace, JobName: r.JobName, JobUID: r.JobUID,
		JobResourceVersion: r.JobResourceVersion, PodUID: r.PodUID,
		PodResourceVersion: r.PodResourceVersion, PodScheduled: r.PodScheduled, NodeName: r.NodeName,
	}
	if _, err := provider.AdvanceRelistFence(provider.RelistFence{}, p); err != nil {
		return provider.RelistProjection{}, err
	}
	return p, nil
}

func openRelistJournal(getenv func(string) string) (*sql.DB, error) {
	if err := requireDisposableSmoke(getenv); err != nil {
		return nil, err
	}
	dsn := strings.TrimSpace(getenv("CCP_KUEUE_JOURNAL_DSN"))
	binding := strings.TrimSpace(getenv("CCP_KUEUE_BINDING_DSN"))
	if dsn == "" {
		return nil, errors.New("CCP_KUEUE_JOURNAL_DSN is empty")
	}
	if binding == "" {
		return nil, errEmptyBindingDSN
	}
	if dsn == binding {
		return nil, errors.New("journal DSN must not be the restricted binding login")
	}
	db, err := sql.Open("pgx", dsn)
	if err != nil {
		return nil, err
	}
	db.SetMaxOpenConns(2)
	return db, nil
}

func relistProviderRegistration() postgres.ProviderRegistration {
	return postgres.ProviderRegistration{
		ProviderID: relistProvider, ClusterScope: smokeNamespace,
		AdapterSource: relistSource, AdapterVersion: relistVersion,
		CapabilityDigest: relistCapability,
	}
}

func ensureRelistProvider(ctx context.Context, repo *postgres.Repository) error {
	registration, _, err := repo.RegisterProvider(ctx, relistProviderRegistration())
	if err != nil {
		return err
	}
	want := relistProviderRegistration()
	if registration.ProviderID != want.ProviderID || registration.ClusterScope != want.ClusterScope ||
		registration.AdapterSource != want.AdapterSource || registration.AdapterVersion != want.AdapterVersion ||
		registration.CapabilityDigest != want.CapabilityDigest {
		return errors.New("relist provider fingerprint mismatch")
	}
	return nil
}

func ensureRelistWorkload(ctx context.Context, repo *postgres.Repository) error {
	normalized := json.RawMessage(`{"project_id":"project-relist","workload_id":"` + relistWorkload + `","resources":{"cpu":"100m","memory":"32Mi"}}`)
	digest, err := postgres.DigestRequest(normalized)
	if err != nil {
		return err
	}
	created, _, err := repo.CreateWorkload(ctx, postgres.CreateWorkloadParams{
		WorkloadID: relistWorkload, TenantID: "tenant-a", ProjectID: "project-relist",
		OwnerService: "agent-runtime", OwnerResourceType: "run", OwnerResourceID: "run-" + relistWorkload,
		NormalizedRequest: normalized, RequestDigest: digest,
		IdempotencyKey: "workload-relist-idem", EventID: "workload-relist-event",
		CorrelationID: "kind-relist",
	})
	if err != nil {
		return err
	}
	if created.WorkloadID != relistWorkload || created.TenantID != "tenant-a" {
		return errors.New("relist workload identity mismatch")
	}
	return nil
}

func relistWorker(repo *postgres.Repository, adapter provider.Adapter) (*recovery.Worker, error) {
	return recovery.NewWorker(repo, map[string]recovery.RegisteredProvider{
		relistProvider: {
			Adapter:          journalReadOnly{Adapter: adapter},
			ClusterScope:     smokeNamespace,
			Source:           relistSource,
			AdapterVersion:   relistVersion,
			CapabilityDigest: relistCapability,
		},
	})
}

func loadRelistPin(store *provider.PostgresKueueJobBindingStore) (provider.KueueCreatePin, error) {
	pin, ok, err := store.GetCreatePin(smokeNamespace, relistCreateRequest())
	if err != nil {
		return provider.KueueCreatePin{}, err
	}
	if !ok || pin.UID == "" || pin.Name == "" {
		return provider.KueueCreatePin{}, errors.New("relist create pin is not stored")
	}
	return pin, nil
}

func relistRead(ctx context.Context, adapter *provider.KueueJobAdapter) func() (provider.KueueJobEvidence, error) {
	req := relistCreateRequest()
	return func() (provider.KueueJobEvidence, error) {
		collected, err := adapter.CollectCreate(ctx, req)
		if err != nil {
			select {
			case <-ctx.Done():
			case <-time.After(100 * time.Millisecond):
			}
			return provider.KueueJobEvidence{}, err
		}
		return collected.Evidence, nil
	}
}

func relistRecord(getenv func(string) string, out io.Writer) error {
	adapter, err := openAdmissionAdapter(getenv)
	if err != nil {
		return err
	}
	ctx, cancel := context.WithTimeout(context.Background(), 150*time.Second)
	defer cancel()
	req := relistCreateRequest()
	if _, err := adapter.Apply(ctx, req); err != nil {
		return fmt.Errorf("queued relist create: %w", err)
	}
	var projected provider.RelistProjection
	attempts := 0
	for {
		attempts++
		evidence, err := relistRead(ctx, adapter)()
		if err != nil && !retryableAdmissionRead(err) {
			return err
		}
		if err == nil {
			got, perr := provider.ProjectRelistSnapshot(evidence)
			if perr == nil && got.JobUID != "" && got.WorkloadUID != "" && got.WorkloadResourceVersion != "" &&
				got.JobResourceVersion != "" && !got.Released && !got.Ready {
				projected = got
				break
			}
		}
		select {
		case <-ctx.Done():
			return fmt.Errorf("relist observation was not recorded: %w", ctx.Err())
		case <-time.After(200 * time.Millisecond):
		}
	}
	present, err := stableJobObservation(ctx, func(ctx context.Context) (operation.Observation, error) {
		return adapter.Inspect(ctx, req)
	})
	if err != nil || present.Result != operation.ResourcePresent || present.ResourceUID != projected.JobUID {
		return fmt.Errorf("recorded job was not pinned: %+v: %w", present, err)
	}
	db, store, err := openAdmissionPinStore(getenv)
	if err != nil {
		return err
	}
	pin, err := loadRelistPin(store)
	_ = db.Close()
	if err != nil || pin.UID != projected.JobUID || pin.Name != projected.JobName {
		return fmt.Errorf("binding pin does not match the recorded job: %+v: %w", pin, err)
	}
	journal, err := openRelistJournal(getenv)
	if err != nil {
		return err
	}
	defer func() { _ = journal.Close() }()
	repo, err := postgres.NewRepository(journal)
	if err != nil {
		return err
	}
	if err := ensureRelistProvider(ctx, repo); err != nil {
		return err
	}
	if err := ensureRelistWorkload(ctx, repo); err != nil {
		return fmt.Errorf("journal workload: %w", err)
	}
	pending, created, err := repo.CreateOperation(ctx, operation.Operation{
		ID: req.OperationID, TenantID: req.TenantID, WorkloadID: req.WorkloadID,
		ProviderID: req.ProviderID, IdempotencyKey: req.IdempotencyKey,
		RequestDigest: req.RequestDigest, Generation: req.DesiredGeneration, Version: 1,
		Kind: operation.Create, Status: operation.Pending,
	}, "operation-"+req.OperationID, "kind-relist")
	if err != nil || !created {
		return fmt.Errorf("record relist create: created=%t err=%v", created, err)
	}
	claimed, err := repo.ClaimOperation(ctx, req.TenantID, req.OperationID, pending.Version, "relist-writer", time.Minute)
	if err != nil {
		return err
	}
	started, err := repo.StartOperation(ctx, req.TenantID, req.OperationID, claimed.Version, "relist-writer", claimed.FenceToken)
	if err != nil {
		return err
	}
	unknown, err := repo.MarkOperationUnknown(ctx, req.TenantID, req.OperationID, started.Version, "relist-writer", started.FenceToken)
	if err != nil || unknown.Status != operation.Unknown || unknown.ProviderResourceUID != "" {
		return fmt.Errorf("relist journal was not UNKNOWN: %+v err=%v", unknown, err)
	}
	report := reportFromRelist("record", projected, attempts)
	report.Result = string(present.Result)
	report.Status = string(unknown.Status)
	report.PinDurable = true
	report.OperationID = req.OperationID
	if !relistInactive(report) || (report.Projection != "NOT_READY" && report.Projection != "UNKNOWN") {
		return errors.New("recorded relist observation was promoted")
	}
	return writeDurable(out, report)
}

func relistWindow(getenv func(string) string, out io.Writer) error {
	adapter, err := openAdmissionAdapter(getenv)
	if err != nil {
		return err
	}
	db, store, err := openAdmissionPinStore(getenv)
	if err != nil {
		return err
	}
	defer func() { _ = db.Close() }()
	pin, err := loadRelistPin(store)
	if err != nil {
		return err
	}
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()
	fence, attempts, err := provider.ConvergeRelist(provider.RelistAttemptLimit, pin.UID, relistRead(ctx, adapter))
	report := relistReport{
		Phase: "window", JobUID: pin.UID, JobName: pin.Name, Attempts: attempts,
		OperationID: relistCreateID, ControllerRestart: getenv("CCP_KUEUE_RELIST_RESTART") == "1",
		PinDurable: true, Projection: "UNKNOWN", Inconclusive: true, Status: string(operation.Unknown),
	}
	if err != nil {
		if errors.Is(err, provider.ErrCrossKueueRelist) || !errors.Is(err, provider.ErrRelistInconclusive) {
			return err
		}
		if !relistInactive(report) {
			return errors.New("restart window released the job")
		}
		return writeDurable(out, report)
	}
	if fence.JobUID != pin.UID || (fence.Phase != "NOT_READY" && fence.Phase != "UNKNOWN") {
		return fmt.Errorf("restart window fenced the wrong object: %+v", fence)
	}
	report.Projection = fence.Phase
	report.Inconclusive = fence.Phase == "UNKNOWN"
	report.WorkloadUID = fence.WorkloadUID
	report.WorkloadResourceVersion = fence.WorkloadResourceVersion
	report.JobResourceVersion = fence.JobResourceVersion
	report.PodUID = fence.PodUID
	report.PodResourceVersion = fence.PodResourceVersion
	report.Admitted = fence.Phase == "NOT_READY"
	report.QuotaReserved = fence.Phase == "NOT_READY"
	if !relistInactive(report) {
		return errors.New("controller restart produced a ready or released projection")
	}
	return writeDurable(out, report)
}

func readRelistPrior(getenv func(string) string) (relistReport, provider.RelistProjection, error) {
	path := strings.TrimSpace(getenv("CCP_KUEUE_RELIST_PRIOR"))
	if path == "" {
		return relistReport{}, provider.RelistProjection{}, errors.New("recorded observation file is missing")
	}
	raw, err := os.ReadFile(path)
	if err != nil {
		return relistReport{}, provider.RelistProjection{}, fmt.Errorf("read recorded observation: %w", err)
	}
	var prior relistReport
	if json.Unmarshal(raw, &prior) != nil || prior.Phase != "record" || prior.JobUID == "" || prior.WorkloadUID == "" {
		return relistReport{}, provider.RelistProjection{}, errors.New("recorded observation is not the relist record")
	}
	projected, err := projectionFromReport(prior)
	if err != nil {
		return relistReport{}, provider.RelistProjection{}, fmt.Errorf("recorded observation is not a fenceable snapshot: %w", err)
	}
	return prior, projected, nil
}

func relistConverge(getenv func(string) string, out io.Writer) error {
	priorReport, prior, err := readRelistPrior(getenv)
	if err != nil {
		return err
	}
	adapter, err := openAdmissionAdapter(getenv)
	if err != nil {
		return err
	}
	pins, store, err := openAdmissionPinStore(getenv)
	if err != nil {
		return err
	}
	defer func() { _ = pins.Close() }()
	pin, err := loadRelistPin(store)
	if err != nil {
		return err
	}
	if pin.UID != prior.JobUID || priorReport.OperationID != relistCreateID {
		return errors.New("binding pin is not the recorded job uid")
	}
	journal, err := openRelistJournal(getenv)
	if err != nil {
		return err
	}
	defer func() { _ = journal.Close() }()
	repo, err := postgres.NewRepository(journal)
	if err != nil {
		return err
	}
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()
	recorded, err := repo.GetOperation(ctx, relistCreateRequest().TenantID, relistCreateID)
	if err != nil || recorded.Status != operation.Unknown || recorded.Kind != operation.Create ||
		recorded.ProviderResourceUID != "" {
		return fmt.Errorf("fresh process replayed the pre-restart journal: %+v err=%v", recorded, err)
	}
	read := relistRead(ctx, adapter)
	fence, attempts, err := provider.ConvergeRelist(provider.RelistAttemptLimit-5, pin.UID, read)
	if err != nil {
		return fmt.Errorf("relist did not converge: %w", err)
	}
	if fence.Phase != "NOT_READY" || fence.JobUID != pin.UID || fence.WorkloadUID == "" ||
		fence.WorkloadUID != prior.WorkloadUID || attempts < 1 || attempts > provider.RelistAttemptLimit {
		return fmt.Errorf("relist converged to the wrong state: %+v attempts=%d", fence, attempts)
	}
	var current provider.RelistProjection
	for attempts < provider.RelistAttemptLimit {
		attempts++
		evidence, readErr := read()
		if readErr != nil {
			continue
		}
		projected, projErr := provider.ProjectRelistSnapshot(evidence)
		if projErr != nil || projected.JobUID != pin.UID {
			if projErr == nil && projected.JobUID != pin.UID {
				return errors.Join(provider.ErrInvalidKueueObservation, provider.ErrCrossKueueRelist)
			}
			continue
		}
		next, advErr := provider.AdvanceRelistFence(fence, projected)
		if errors.Is(advErr, provider.ErrStaleKueueRelist) || errors.Is(advErr, provider.ErrRelistInconclusive) {
			continue
		}
		if advErr != nil {
			return advErr
		}
		fence = next
		current = projected
		break
	}
	if current.JobUID != fence.JobUID || current.WorkloadUID != fence.WorkloadUID ||
		current.JobResourceVersion != fence.JobResourceVersion ||
		current.WorkloadResourceVersion != fence.WorkloadResourceVersion || current.Phase != "NOT_READY" {
		return fmt.Errorf("relist projection drifted from the fenced read: %+v fence=%+v", current, fence)
	}
	if current.JobResourceVersion == prior.JobResourceVersion && current.WorkloadResourceVersion == prior.WorkloadResourceVersion &&
		current.PodResourceVersion == prior.PodResourceVersion && current.PodUID == prior.PodUID {
		return errors.New("relist kept the stale pre-restart observation")
	}
	kept, staleErr := provider.AdvanceRelistFence(fence, prior)
	if !errors.Is(staleErr, provider.ErrStaleKueueRelist) || kept != fence {
		return fmt.Errorf("older observation overwrote the relist fence: %+v err=%v", kept, staleErr)
	}
	duplicate, err := provider.AdvanceRelistFence(fence, current)
	if err != nil || duplicate != fence {
		return fmt.Errorf("current observation was not de-duplicated: %+v err=%v", duplicate, err)
	}
	successor := current
	successor.JobUID = "relist-successor-uid"
	successor.WorkloadUID = "relist-successor-workload"
	successor.JobResourceVersion = current.JobResourceVersion
	crossed, crossErr := provider.AdvanceRelistFence(fence, successor)
	if !errors.Is(crossErr, provider.ErrCrossKueueRelist) || crossed != fence || crossed.JobUID != pin.UID {
		return fmt.Errorf("same-name successor was projected onto the original: %+v err=%v", crossed, crossErr)
	}
	still, err := repo.GetOperation(ctx, relistCreateRequest().TenantID, relistCreateID)
	if err != nil || still.Status != operation.Unknown || still.ProviderResourceUID != "" {
		return fmt.Errorf("converge wrote the stale observation into the journal: %+v err=%v", still, err)
	}
	report := reportFromRelist("converge", current, attempts)
	report.Status = string(still.Status)
	report.PinDurable = true
	report.StaleRejected = true
	report.CrossRejected = true
	report.Deduped = true
	report.PriorWorkloadResourceVersion = prior.WorkloadResourceVersion
	report.PriorJobResourceVersion = prior.JobResourceVersion
	report.TargetUID = pin.UID
	if !relistInactive(report) || report.Projection != "NOT_READY" || report.KeptStale {
		return errors.New("converged relist observation was promoted")
	}
	return writeDurable(out, report)
}

func relistJournalDelete(ctx context.Context, repo *postgres.Repository, store *provider.PostgresKueueJobBindingStore, adapter *provider.KueueJobAdapter, remove provider.Request) (provider.KueueCreatePin, error) {
	pin, err := loadRelistPin(store)
	if err != nil {
		return provider.KueueCreatePin{}, err
	}
	if remove.TargetResourceUID != pin.UID || remove.TargetResourceID != smokeNamespace+"/"+pin.Name {
		return provider.KueueCreatePin{}, errors.New("delete request is not the pinned job")
	}
	pending, created, err := repo.CreateOperation(ctx, operation.Operation{
		ID: remove.OperationID, TenantID: remove.TenantID, WorkloadID: remove.WorkloadID,
		ProviderID: remove.ProviderID, IdempotencyKey: remove.IdempotencyKey,
		RequestDigest: remove.RequestDigest, Generation: remove.DesiredGeneration, Version: 1,
		Kind: operation.Delete, Status: operation.Pending,
		TargetResourceID: remove.TargetResourceID, TargetResourceUID: remove.TargetResourceUID,
	}, "operation-"+remove.OperationID, "kind-relist-delete")
	if err != nil || !created {
		return provider.KueueCreatePin{}, fmt.Errorf("record delete operation: created=%t err=%v", created, err)
	}
	claimed, err := repo.ClaimOperation(ctx, remove.TenantID, remove.OperationID, pending.Version, "relist-delete-writer", time.Minute)
	if err != nil {
		return provider.KueueCreatePin{}, err
	}
	started, err := repo.StartOperation(ctx, remove.TenantID, remove.OperationID, claimed.Version, "relist-delete-writer", claimed.FenceToken)
	if err != nil {
		return provider.KueueCreatePin{}, err
	}
	if _, err := adapter.Apply(ctx, remove); err != nil {
		return provider.KueueCreatePin{}, fmt.Errorf("uid-fenced delete: %w", err)
	}
	tomb, ok, err := store.GetDeleteTombstone(smokeNamespace, remove)
	if err != nil || !ok || !tomb.Acknowledged || tomb.UID != pin.UID || tomb.Name != pin.Name {
		return provider.KueueCreatePin{}, fmt.Errorf("delete tombstone not acknowledged: %+v present=%t err=%v", tomb, ok, err)
	}
	unknown, err := repo.MarkOperationUnknown(ctx, remove.TenantID, remove.OperationID, started.Version, "relist-delete-writer", started.FenceToken)
	if err != nil || unknown.Status != operation.Unknown || unknown.ProviderResourceUID != "" || unknown.TargetResourceUID != pin.UID {
		return provider.KueueCreatePin{}, fmt.Errorf("delete journal was not UNKNOWN: %+v err=%v", unknown, err)
	}
	return pin, nil
}

func relistDelete(getenv func(string) string, out io.Writer) error {
	journal, err := openRelistJournal(getenv)
	if err != nil {
		return err
	}
	defer func() { _ = journal.Close() }()
	repo, err := postgres.NewRepository(journal)
	if err != nil {
		return err
	}
	pins, store, err := openAdmissionPinStore(getenv)
	if err != nil {
		return err
	}
	defer func() { _ = pins.Close() }()
	adapter, err := openAdmissionAdapter(getenv)
	if err != nil {
		return err
	}
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	if err := ensureRelistProvider(ctx, repo); err != nil {
		return err
	}
	if err := ensureRelistWorkload(ctx, repo); err != nil {
		return err
	}
	pin, err := loadRelistPin(store)
	if err != nil {
		return err
	}
	if _, err := relistJournalDelete(ctx, repo, store, adapter, relistDeleteRequest(smokeNamespace+"/"+pin.Name, pin.UID)); err != nil {
		return err
	}
	if err := writeDurable(out, relistReport{
		Phase: "delete", Status: string(operation.Unknown), Projection: "NOT_READY",
		TombstoneDurable: true, Acknowledged: true, JobUID: pin.UID, JobName: pin.Name,
		TargetUID: pin.UID, OperationID: relistDeleteID, PinDurable: true,
	}); err != nil {
		return err
	}
	holdUntilKilled()
	return errors.New("relist delete process unexpectedly survived SIGKILL")
}

func relistRecoverDelete(getenv func(string) string, out io.Writer) error {
	journal, err := openRelistJournal(getenv)
	if err != nil {
		return err
	}
	defer func() { _ = journal.Close() }()
	repo, err := postgres.NewRepository(journal)
	if err != nil {
		return err
	}
	pins, store, err := openAdmissionPinStore(getenv)
	if err != nil {
		return err
	}
	defer func() { _ = pins.Close() }()
	adapter, err := openAdmissionAdapter(getenv)
	if err != nil {
		return err
	}
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	pin, err := loadRelistPin(store)
	if err != nil {
		return err
	}
	remove := relistDeleteRequest(smokeNamespace+"/"+pin.Name, pin.UID)
	previous, err := repo.GetOperation(ctx, remove.TenantID, remove.OperationID)
	if err != nil || previous.Status != operation.Unknown || previous.Kind != operation.Delete ||
		previous.TargetResourceUID != pin.UID || previous.ProviderResourceUID != "" {
		return fmt.Errorf("delete journal is not UNKNOWN: %+v err=%v", previous, err)
	}
	worker, err := relistWorker(repo, adapter)
	if err != nil {
		return err
	}
	next, err := worker.RecoverUnknown(ctx, remove.TenantID, remove.OperationID, "relist-delete-recovery", time.Minute)
	if err != nil {
		return fmt.Errorf("inspect-only delete recovery: %w", err)
	}
	tomb, ok, err := store.GetDeleteTombstone(smokeNamespace, remove)
	if err != nil || !ok || !tomb.Acknowledged || tomb.UID != pin.UID {
		return fmt.Errorf("tombstone drifted: %+v present=%t err=%v", tomb, ok, err)
	}
	if next.Status != operation.Succeeded || next.ProviderResourceUID != "" || next.TargetResourceUID != pin.UID ||
		next.FenceToken <= previous.FenceToken {
		return fmt.Errorf("delete recovery was not TARGET_ABSENT: %+v", next)
	}
	observations, result, source, version, resourceUID, targetUID, reason, events, err := journalDeleteEvidence(ctx, journal, remove.TenantID, remove.OperationID)
	if err != nil {
		return err
	}
	if observations != 1 || events != 2 || result != string(operation.TargetAbsent) || reason != "" ||
		source != relistSource || version != relistVersion || resourceUID != "" || targetUID != pin.UID {
		return fmt.Errorf("TARGET_ABSENT evidence mismatch: rows=%d events=%d result=%q reason=%q source=%q version=%q resource=%q target=%q",
			observations, events, result, reason, source, version, resourceUID, targetUID)
	}
	obs, err := adapter.Inspect(ctx, remove)
	if err != nil || obs.Result != operation.TargetAbsent || obs.TargetResourceUID != pin.UID || obs.ResourceUID != "" {
		return fmt.Errorf("fresh inspect was not TARGET_ABSENT: %+v err=%v", obs, err)
	}
	return writeDurable(out, relistReport{
		Phase: "recover-delete", Status: string(next.Status), Projection: "NOT_READY",
		Result: string(obs.Result), TargetUID: pin.UID, JobName: pin.Name,
		AbsenceObserved: true, TombstoneDurable: true, Acknowledged: true,
		OperationID: relistDeleteID, PinDurable: true,
	})
}

func relistSuccessorHold(getenv func(string) string, out io.Writer) error {
	journal, err := openRelistJournal(getenv)
	if err != nil {
		return err
	}
	defer func() { _ = journal.Close() }()
	repo, err := postgres.NewRepository(journal)
	if err != nil {
		return err
	}
	pins, store, err := openAdmissionPinStore(getenv)
	if err != nil {
		return err
	}
	defer func() { _ = pins.Close() }()
	ctx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
	defer cancel()
	if err := ensureRelistProvider(ctx, repo); err != nil {
		return err
	}
	pin, err := loadRelistPin(store)
	if err != nil {
		return err
	}
	original, err := repo.GetOperation(ctx, "tenant-a", relistDeleteID)
	if err != nil || original.Kind != operation.Delete || original.TargetResourceUID != pin.UID {
		return fmt.Errorf("original delete target is not journaled: %+v err=%v", original, err)
	}
	remove := relistSuccessorRequest(smokeNamespace+"/"+pin.Name, pin.UID)
	pending, created, err := repo.CreateOperation(ctx, operation.Operation{
		ID: remove.OperationID, TenantID: remove.TenantID, WorkloadID: remove.WorkloadID,
		ProviderID: remove.ProviderID, IdempotencyKey: remove.IdempotencyKey,
		RequestDigest: remove.RequestDigest, Generation: remove.DesiredGeneration, Version: 1,
		Kind: operation.Delete, Status: operation.Pending,
		TargetResourceID: remove.TargetResourceID, TargetResourceUID: remove.TargetResourceUID,
	}, "operation-"+remove.OperationID, "kind-relist-successor")
	if err != nil || !created {
		return fmt.Errorf("record successor delete: created=%t err=%v", created, err)
	}
	claimed, err := repo.ClaimOperation(ctx, remove.TenantID, remove.OperationID, pending.Version, "relist-successor-writer", time.Minute)
	if err != nil {
		return err
	}
	started, err := repo.StartOperation(ctx, remove.TenantID, remove.OperationID, claimed.Version, "relist-successor-writer", claimed.FenceToken)
	if err != nil {
		return err
	}
	unknown, err := repo.MarkOperationUnknown(ctx, remove.TenantID, remove.OperationID, started.Version, "relist-successor-writer", started.FenceToken)
	if err != nil || unknown.Status != operation.Unknown || unknown.TargetResourceUID != pin.UID || unknown.ProviderResourceUID != "" {
		return fmt.Errorf("successor journal was not UNKNOWN: %+v err=%v", unknown, err)
	}
	if err := writeDurable(out, relistReport{
		Phase: "successor", Status: string(unknown.Status), Projection: "NOT_READY",
		JobUID: pin.UID, JobName: pin.Name, TargetUID: pin.UID, OperationID: relistSuccessor,
	}); err != nil {
		return err
	}
	holdUntilKilled()
	return errors.New("relist successor process unexpectedly survived SIGKILL")
}

func relistRecoverSuccessor(getenv func(string) string, out io.Writer) error {
	journal, err := openRelistJournal(getenv)
	if err != nil {
		return err
	}
	defer func() { _ = journal.Close() }()
	repo, err := postgres.NewRepository(journal)
	if err != nil {
		return err
	}
	pins, store, err := openAdmissionPinStore(getenv)
	if err != nil {
		return err
	}
	defer func() { _ = pins.Close() }()
	adapter, err := openAdmissionAdapter(getenv)
	if err != nil {
		return err
	}
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	pin, err := loadRelistPin(store)
	if err != nil {
		return err
	}
	remove := relistSuccessorRequest(smokeNamespace+"/"+pin.Name, pin.UID)
	previous, err := repo.GetOperation(ctx, remove.TenantID, remove.OperationID)
	if err != nil || previous.Status != operation.Unknown || previous.TargetResourceUID != pin.UID {
		return fmt.Errorf("successor journal is not UNKNOWN: %+v err=%v", previous, err)
	}
	worker, err := relistWorker(repo, adapter)
	if err != nil {
		return err
	}
	next, err := worker.RecoverUnknown(ctx, remove.TenantID, remove.OperationID, "relist-successor-recovery", time.Minute)
	if err != nil {
		return err
	}
	if next.Status != operation.ManualReview || next.ManualReviewCode != "JOB_UID_REPLACED" ||
		next.ProviderResourceUID != "" || next.TargetResourceUID != pin.UID || next.FenceToken <= previous.FenceToken {
		return fmt.Errorf("successor recovery did not leave JOB_UID_REPLACED: %+v", next)
	}
	obs, err := adapter.Inspect(ctx, relistCreateRequest())
	if err != nil || obs.Result != operation.ObservationConflict || obs.ReasonCode != "JOB_UID_REPLACED" || obs.ResourceUID != "" {
		return fmt.Errorf("create inspect adopted the successor: %+v err=%v", obs, err)
	}
	return writeDurable(out, relistReport{
		Phase: "recover-successor", Status: string(next.Status), Projection: "NOT_READY",
		Result: string(obs.Result), ReasonCode: obs.ReasonCode, JobName: pin.Name,
		TargetUID: pin.UID, OperationID: relistSuccessor,
	})
}

func runRelist(args []string, getenv func(string) string, out io.Writer) error {
	phase, err := parseRelistArgs(args)
	if err != nil {
		return err
	}
	switch phase {
	case "record":
		return relistRecord(getenv, out)
	case "window":
		return relistWindow(getenv, out)
	case "converge":
		return relistConverge(getenv, out)
	case "delete":
		return relistDelete(getenv, out)
	case "recover-delete":
		return relistRecoverDelete(getenv, out)
	case "successor":
		return relistSuccessorHold(getenv, out)
	case "recover-successor":
		return relistRecoverSuccessor(getenv, out)
	default:
		return errors.New("unknown relist phase")
	}
}
