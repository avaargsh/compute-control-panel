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

// Disposable kind proof for one Kueue withinClusterQueue preemption. The
// queued Jobs are created only by NewQueuedServiceAccountKueueJobAdapter with
// the ccp_kueue_job_binding login. This file does not create Workload custom
// resources, mount an HTTP handler, or decide Allocation RELEASED.
const (
	preemptQueue        = "ccp-preempt"
	preemptLowPriority  = "ccp-preempt-low"
	preemptHighPriority = "ccp-preempt-high"
	preemptLowCreateID  = "ccp-kind-preempt-low"
	preemptHighCreateID = "ccp-kind-preempt-high"
	preemptLowDeleteID  = "ccp-kind-preempt-low-delete"
	preemptHighDeleteID = "ccp-kind-preempt-high-delete"
	preemptLowSuccessor = "ccp-kind-preempt-low-successor"
	preemptHighSuccess  = "ccp-kind-preempt-high-successor"
	preemptProviderID   = "kueue-preempt"
	preemptSource       = "kubernetes-https-readonly-unattested"
	preemptVersion      = "kueue-v1beta1-single-pod-v1"
	preemptCapability   = "sha256:b7b7b7b7b7b7b7b7b7b7b7b7b7b7b7b7b7b7b7b7b7b7b7b7b7b7b7b7b7b7b7b7"
)

func preemptLowRequest() provider.Request {
	return provider.Request{
		OperationID: preemptLowCreateID, TenantID: "tenant-a",
		WorkloadID: "workload-preempt-low", ProviderID: preemptProviderID,
		IdempotencyKey:    "ccp-kind-preempt-low-key",
		RequestDigest:     "sha256:" + strings.Repeat("17", 32),
		DesiredGeneration: 4, Kind: operation.Create,
	}
}

func preemptHighRequest() provider.Request {
	return provider.Request{
		OperationID: preemptHighCreateID, TenantID: "tenant-a",
		WorkloadID: "workload-preempt-high", ProviderID: preemptProviderID,
		IdempotencyKey:    "ccp-kind-preempt-high-key",
		RequestDigest:     "sha256:" + strings.Repeat("18", 32),
		DesiredGeneration: 5, Kind: operation.Create,
	}
}

func preemptDeleteRequest(create provider.Request, operationID, idempotency, digest, resourceID, uid string) provider.Request {
	create.Kind = operation.Delete
	create.OperationID = operationID
	create.IdempotencyKey = idempotency
	create.RequestDigest = digest
	create.TargetResourceID = resourceID
	create.TargetResourceUID = uid
	return create
}

func preemptLowDelete(resourceID, uid string) provider.Request {
	return preemptDeleteRequest(preemptLowRequest(), preemptLowDeleteID, "ccp-kind-preempt-low-delete-key",
		"sha256:"+strings.Repeat("19", 32), resourceID, uid)
}

func preemptHighDelete(resourceID, uid string) provider.Request {
	return preemptDeleteRequest(preemptHighRequest(), preemptHighDeleteID, "ccp-kind-preempt-high-delete-key",
		"sha256:"+strings.Repeat("1a", 32), resourceID, uid)
}

func preemptLowSuccessorRequest(resourceID, uid string) provider.Request {
	return preemptDeleteRequest(preemptLowRequest(), preemptLowSuccessor, "ccp-kind-preempt-low-successor-key",
		"sha256:"+strings.Repeat("1b", 32), resourceID, uid)
}

func preemptHighSuccessorRequest(resourceID, uid string) provider.Request {
	return preemptDeleteRequest(preemptHighRequest(), preemptHighSuccess, "ccp-kind-preempt-high-successor-key",
		"sha256:"+strings.Repeat("1c", 32), resourceID, uid)
}

func parsePreemptionArgs(args []string) (string, bool, error) {
	switch {
	case len(args) == 2 && args[0] == "preemption" && (args[1] == "submit-low" || args[1] == "submit-high" || args[1] == "recover-delete" || args[1] == "recover-successor"):
		return args[1], false, nil
	case len(args) == 3 && args[0] == "preemption" && (args[1] == "delete" || args[1] == "successor") && args[2] == "--hold":
		return args[1], true, nil
	default:
		return "", false, errors.New("preemption phase is not one of submit-low, submit-high, delete --hold, recover-delete, successor --hold, recover-successor")
	}
}

type preemptJobReport struct {
	Projection              string `json:"projection"`
	Disposition             string `json:"disposition"`
	Stage                   string `json:"stage"`
	Evicted                 bool   `json:"evicted"`
	EvictedReason           string `json:"evicted_reason"`
	Preempted               bool   `json:"preempted"`
	PreemptedReason         string `json:"preempted_reason"`
	QuotaReserved           bool   `json:"quota_reserved"`
	Admitted                bool   `json:"admitted"`
	ClusterQueue            string `json:"cluster_queue"`
	WorkloadNamespace       string `json:"workload_namespace"`
	WorkloadName            string `json:"workload_name"`
	WorkloadUID             string `json:"workload_uid"`
	WorkloadResourceVersion string `json:"workload_resource_version"`
	JobUID                  string `json:"job_uid"`
	JobName                 string `json:"job_name"`
	JobResourceVersion      string `json:"job_resource_version"`
	PodUID                  string `json:"pod_uid"`
	PodScheduled            bool   `json:"pod_scheduled"`
	NodeName                string `json:"node_name"`
	Allocated               bool   `json:"allocated"`
	Active                  bool   `json:"active"`
	Ready                   bool   `json:"ready"`
	Costable                bool   `json:"costable"`
	Released                bool   `json:"released"`
	OperationID             string `json:"operation_id"`
	PinDurable              bool   `json:"pin_durable"`
}

type preemptSubmitReport struct {
	Phase           string           `json:"phase"`
	ReleaseAttested bool             `json:"release_attested"`
	StaleRejected   bool             `json:"stale_rejected"`
	CrossRejected   bool             `json:"cross_rejected"`
	Deduped         bool             `json:"deduped"`
	Low             preemptJobReport `json:"low"`
	High            preemptJobReport `json:"high"`
	Projection      string           `json:"projection,omitempty"`
	Disposition     string           `json:"disposition,omitempty"`
	Evicted         bool             `json:"evicted,omitempty"`
	EvictedReason   string           `json:"evicted_reason,omitempty"`
	Preempted       bool             `json:"preempted,omitempty"`
	PreemptedReason string           `json:"preempted_reason,omitempty"`
	QuotaReserved   bool             `json:"quota_reserved,omitempty"`
	Admitted        bool             `json:"admitted,omitempty"`
	ClusterQueue    string           `json:"cluster_queue,omitempty"`
	WorkloadUID     string           `json:"workload_uid,omitempty"`
	WorkloadRV      string           `json:"workload_resource_version,omitempty"`
	JobUID          string           `json:"job_uid,omitempty"`
	JobName         string           `json:"job_name,omitempty"`
	JobRV           string           `json:"job_resource_version,omitempty"`
	PodScheduled    bool             `json:"pod_scheduled,omitempty"`
	Allocated       bool             `json:"allocated"`
	Active          bool             `json:"active"`
	Ready           bool             `json:"ready"`
	Costable        bool             `json:"costable"`
	Released        bool             `json:"released"`
	OperationID     string           `json:"operation_id,omitempty"`
	PinDurable      bool             `json:"pin_durable,omitempty"`
	WorkloadName    string           `json:"workload_name,omitempty"`
	WorkloadNS      string           `json:"workload_namespace,omitempty"`
	Stage           string           `json:"stage,omitempty"`
	PodUID          string           `json:"pod_uid,omitempty"`
	NodeName        string           `json:"node_name,omitempty"`
}

func reportFromProjection(p provider.KueuePreemptionProjection, operationID string, pin bool) preemptJobReport {
	return preemptJobReport{
		Projection: p.Phase, Disposition: p.Disposition, Stage: string(p.Stage),
		Evicted: p.Evicted, EvictedReason: p.EvictedReason,
		Preempted: p.Preempted, PreemptedReason: p.PreemptedReason,
		QuotaReserved: p.QuotaReserved, Admitted: p.Admitted, ClusterQueue: p.ClusterQueue,
		WorkloadNamespace: p.WorkloadNamespace, WorkloadName: p.WorkloadName,
		WorkloadUID: p.WorkloadUID, WorkloadResourceVersion: p.WorkloadResourceVersion,
		JobUID: p.JobUID, JobName: p.JobName, JobResourceVersion: p.JobResourceVersion,
		PodUID: p.PodUID, PodScheduled: p.PodScheduled, NodeName: p.NodeName,
		Allocated: p.Allocated, Active: p.Active, Ready: p.Ready, Costable: p.Costable, Released: p.Released,
		OperationID: operationID, PinDurable: pin,
	}
}

func projectionFromReport(r preemptJobReport) provider.KueuePreemptionProjection {
	return provider.KueuePreemptionProjection{
		Phase: r.Projection, Disposition: r.Disposition, Stage: provider.KueueObservationStage(r.Stage),
		Evicted: r.Evicted, EvictedReason: r.EvictedReason, Preempted: r.Preempted, PreemptedReason: r.PreemptedReason,
		QuotaReserved: r.QuotaReserved, Admitted: r.Admitted, ClusterQueue: r.ClusterQueue,
		WorkloadNamespace: r.WorkloadNamespace, WorkloadName: r.WorkloadName,
		WorkloadUID: r.WorkloadUID, WorkloadResourceVersion: r.WorkloadResourceVersion,
		JobNamespace: smokeNamespace, JobName: r.JobName, JobUID: r.JobUID, JobResourceVersion: r.JobResourceVersion,
		PodUID: r.PodUID, PodScheduled: r.PodScheduled, NodeName: r.NodeName,
		Allocated: r.Allocated, Active: r.Active, Ready: r.Ready, Costable: r.Costable, Released: r.Released,
	}
}

func openPreemptAdapter(getenv func(string) string, priority string) (*provider.KueueJobAdapter, error) {
	adapter, err := openQueuedBindingAdapter(getenv, preemptQueue)
	if err != nil {
		return nil, err
	}
	if err := adapter.UseQueuedWorkloadPriorityClass(priority); err != nil {
		return nil, err
	}
	return adapter, nil
}

func collectPreemptJob(ctx context.Context, adapter *provider.KueueJobAdapter, req provider.Request) (provider.KueueJobEvidence, error) {
	collected, err := adapter.CollectCreate(ctx, req)
	if err != nil {
		return provider.KueueJobEvidence{}, err
	}
	return collected.Evidence, nil
}

func loadPreemptPin(store *provider.PostgresKueueJobBindingStore, req provider.Request) (provider.KueueCreatePin, error) {
	pin, ok, err := store.GetCreatePin(smokeNamespace, req)
	if err != nil {
		return provider.KueueCreatePin{}, err
	}
	if !ok || pin.UID == "" || pin.Name == "" {
		return provider.KueueCreatePin{}, errors.New("preemption create pin is not stored")
	}
	return pin, nil
}

func openPreemptJournal(getenv func(string) string) (*sql.DB, error) {
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

func preemptProviderRegistration() postgres.ProviderRegistration {
	return postgres.ProviderRegistration{
		ProviderID: preemptProviderID, ClusterScope: smokeNamespace,
		AdapterSource: preemptSource, AdapterVersion: preemptVersion,
		CapabilityDigest: preemptCapability,
	}
}

func ensurePreemptProvider(ctx context.Context, repo *postgres.Repository) error {
	registration, _, err := repo.RegisterProvider(ctx, preemptProviderRegistration())
	if err != nil {
		return err
	}
	want := preemptProviderRegistration()
	if registration.ProviderID != want.ProviderID || registration.ClusterScope != want.ClusterScope ||
		registration.AdapterSource != want.AdapterSource || registration.AdapterVersion != want.AdapterVersion ||
		registration.CapabilityDigest != want.CapabilityDigest {
		return errors.New("preemption provider fingerprint mismatch")
	}
	return nil
}

func ensurePreemptWorkload(ctx context.Context, repo *postgres.Repository, workloadID, idempotency, eventID string) error {
	normalized := json.RawMessage(`{"project_id":"project-preempt","workload_id":"` + workloadID + `","resources":{"cpu":"100m","memory":"32Mi"}}`)
	digest, err := postgres.DigestRequest(normalized)
	if err != nil {
		return err
	}
	_, _, err = repo.CreateWorkload(ctx, postgres.CreateWorkloadParams{
		WorkloadID: workloadID, TenantID: "tenant-a", ProjectID: "project-preempt",
		OwnerService: "agent-runtime", OwnerResourceType: "run", OwnerResourceID: "run-" + workloadID,
		NormalizedRequest: normalized, RequestDigest: digest,
		IdempotencyKey: idempotency, EventID: eventID, CorrelationID: "kind-preempt",
	})
	return err
}

func preemptWorker(repo *postgres.Repository, adapter provider.Adapter) (*recovery.Worker, error) {
	return recovery.NewWorker(repo, map[string]recovery.RegisteredProvider{
		preemptProviderID: {
			Adapter:          journalReadOnly{Adapter: adapter},
			ClusterScope:     smokeNamespace,
			Source:           preemptSource,
			AdapterVersion:   preemptVersion,
			CapabilityDigest: preemptCapability,
		},
	})
}

func waitPreemptAdmission(ctx context.Context, adapter *provider.KueueJobAdapter, req provider.Request) (provider.KueuePreemptionProjection, error) {
	var got provider.KueuePreemptionProjection
	var have bool
	var placeDeadline time.Time
	for {
		evidence, err := collectPreemptJob(ctx, adapter, req)
		if err != nil && !retryableAdmissionRead(err) {
			return provider.KueuePreemptionProjection{}, err
		}
		if err == nil {
			projected, perr := provider.ProjectKueueAdmissionBeforePreemption(evidence)
			if perr == nil && projected.ClusterQueue == preemptQueue && !projected.Released && !projected.Ready {
				got = projected
				if !have {
					have = true
					placeDeadline = time.Now().Add(15 * time.Second)
				}
				if projected.PodScheduled {
					return got, nil
				}
			}
		}
		wait := 200 * time.Millisecond
		if have && !time.Now().Before(placeDeadline) {
			return got, nil
		}
		select {
		case <-ctx.Done():
			if have {
				return got, nil
			}
			return provider.KueuePreemptionProjection{}, fmt.Errorf("queued job was not admitted: %w", ctx.Err())
		case <-time.After(wait):
		}
	}
}

func preemptSubmitLow(getenv func(string) string, out io.Writer) error {
	adapter, err := openPreemptAdapter(getenv, preemptLowPriority)
	if err != nil {
		return err
	}
	ctx, cancel := context.WithTimeout(context.Background(), 150*time.Second)
	defer cancel()
	req := preemptLowRequest()
	if _, err := adapter.Apply(ctx, req); err != nil {
		return fmt.Errorf("low priority queued create: %w", err)
	}
	projected, err := waitPreemptAdmission(ctx, adapter, req)
	if err != nil {
		return err
	}
	present, err := stableJobObservation(ctx, func(ctx context.Context) (operation.Observation, error) {
		return adapter.Inspect(ctx, req)
	})
	if err != nil || present.Result != operation.ResourcePresent || present.ResourceUID != projected.JobUID {
		return fmt.Errorf("low priority job was not pinned: %+v: %w", present, err)
	}
	db, store, err := openAdmissionPinStore(getenv)
	if err != nil {
		return err
	}
	defer func() { _ = db.Close() }()
	pin, err := loadPreemptPin(store, req)
	if err != nil || pin.UID != projected.JobUID {
		return fmt.Errorf("low priority binding pin: %+v: %w", pin, err)
	}
	job := reportFromProjection(projected, req.OperationID, true)
	if job.Allocated || job.Active || job.Ready || job.Costable || job.Released || job.Projection != "ADMITTED" {
		return errors.New("low priority admission was promoted")
	}
	report := preemptSubmitReport{
		Phase: "submit-low", Projection: job.Projection, Disposition: job.Disposition, Stage: job.Stage,
		Evicted: job.Evicted, EvictedReason: job.EvictedReason, Preempted: job.Preempted, PreemptedReason: job.PreemptedReason,
		QuotaReserved: job.QuotaReserved, Admitted: job.Admitted, ClusterQueue: job.ClusterQueue,
		WorkloadUID: job.WorkloadUID, WorkloadRV: job.WorkloadResourceVersion, WorkloadName: job.WorkloadName,
		WorkloadNS: job.WorkloadNamespace, JobUID: job.JobUID, JobName: job.JobName, JobRV: job.JobResourceVersion,
		PodUID: job.PodUID, PodScheduled: job.PodScheduled, NodeName: job.NodeName,
		OperationID: job.OperationID, PinDurable: true, Low: job,
	}
	return writeDurable(out, report)
}

func preemptSubmitHigh(getenv func(string) string, out io.Writer) error {
	priorPath := strings.TrimSpace(getenv("CCP_KUEUE_PREEMPTION_PRIOR"))
	if priorPath == "" {
		return errors.New("admitted projection file is missing")
	}
	raw, err := os.ReadFile(priorPath)
	if err != nil {
		return fmt.Errorf("read admitted projection: %w", err)
	}
	var priorReport preemptSubmitReport
	if json.Unmarshal(raw, &priorReport) != nil || priorReport.Phase != "submit-low" || priorReport.Projection != "ADMITTED" {
		return errors.New("admitted projection file is not the low priority observation")
	}
	prior := projectionFromReport(priorReport.Low)
	if prior.JobUID == "" {
		prior = projectionFromReport(preemptJobReport{
			Projection: priorReport.Projection, Disposition: priorReport.Disposition, Stage: priorReport.Stage,
			QuotaReserved: priorReport.QuotaReserved, Admitted: priorReport.Admitted, ClusterQueue: priorReport.ClusterQueue,
			WorkloadNamespace: priorReport.WorkloadNS, WorkloadName: priorReport.WorkloadName,
			WorkloadUID: priorReport.WorkloadUID, WorkloadResourceVersion: priorReport.WorkloadRV,
			JobName: priorReport.JobName, JobUID: priorReport.JobUID, JobResourceVersion: priorReport.JobRV,
			PodUID: priorReport.PodUID, PodScheduled: priorReport.PodScheduled, NodeName: priorReport.NodeName,
		})
	}
	priorFence, err := provider.AdvanceKueuePreemptionFence(provider.KueuePreemptionFence{}, prior)
	if err != nil {
		return fmt.Errorf("admitted fence: %w", err)
	}
	lowAdapter, err := openPreemptAdapter(getenv, preemptLowPriority)
	if err != nil {
		return err
	}
	highAdapter, err := openPreemptAdapter(getenv, preemptHighPriority)
	if err != nil {
		return err
	}
	ctx, cancel := context.WithTimeout(context.Background(), 180*time.Second)
	defer cancel()
	highReq := preemptHighRequest()
	if _, err := highAdapter.Apply(ctx, highReq); err != nil {
		return fmt.Errorf("high priority queued create: %w", err)
	}
	var lowProjected, highProjected provider.KueuePreemptionProjection
	var lastLow, lastHigh string
	for {
		lowEvidence, lowErr := collectPreemptJob(ctx, lowAdapter, preemptLowRequest())
		highEvidence, highErr := collectPreemptJob(ctx, highAdapter, highReq)
		if lowErr != nil && !retryableAdmissionRead(lowErr) {
			return lowErr
		}
		if highErr != nil && !retryableAdmissionRead(highErr) {
			return highErr
		}
		var lowOK, highOK error
		if lowErr == nil {
			lowProjected, lowOK = provider.ProjectKueuePreemptedWorkload(lowEvidence)
			lastLow = fmt.Sprintf("uid=%s rv=%s evicted=%s/%s preempted=%s/%s quota=%s admitted=%s",
				lowEvidence.KueueWorkload.UID, lowEvidence.KueueWorkload.ResourceVersion,
				lowEvidence.KueueEvictedCondition.Status, lowEvidence.KueueEvictedCondition.Reason,
				lowEvidence.KueuePreemptedCondition.Status, lowEvidence.KueuePreemptedCondition.Reason,
				lowEvidence.KueueQuotaCondition.Status, lowEvidence.KueueAdmissionCondition.Status)
		} else {
			lowOK = lowErr
			lastLow = lowErr.Error()
		}
		if highErr == nil {
			highProjected, highOK = provider.ProjectKueueAdmissionBeforePreemption(highEvidence)
		} else {
			highOK = highErr
			lastHigh = highErr.Error()
		}
		var fencedNow provider.KueuePreemptionFence
		var fenceErr error
		if lowOK == nil {
			fencedNow, fenceErr = provider.AdvanceKueuePreemptionFence(priorFence, lowProjected)
		}
		if lowOK == nil && highOK == nil && fenceErr == nil && fencedNow.Disposition == "PREEMPTED" &&
			highProjected.ClusterQueue == preemptQueue &&
			lowProjected.JobUID == prior.JobUID && lowProjected.WorkloadUID == prior.WorkloadUID &&
			highProjected.JobUID != lowProjected.JobUID && highProjected.WorkloadUID != lowProjected.WorkloadUID {
			break
		}
		if highOK == nil {
			lastHigh = highProjected.JobUID + " admitted"
		}
		select {
		case <-ctx.Done():
			return fmt.Errorf("preemption did not stabilize: low=%s high=%s: %w", lastLow, lastHigh, ctx.Err())
		case <-time.After(200 * time.Millisecond):
		}
	}
	fenced, err := provider.AdvanceKueuePreemptionFence(priorFence, lowProjected)
	if err != nil || fenced.Disposition != "PREEMPTED" || fenced.JobUID != prior.JobUID {
		return fmt.Errorf("preemption fence: %+v: %w", fenced, err)
	}
	staleFence, staleErr := provider.AdvanceKueuePreemptionFence(fenced, prior)
	if !errors.Is(staleErr, provider.ErrStaleKueuePreemption) || staleFence != fenced {
		return fmt.Errorf("stale admitted observation overwrote preemption: %+v: %w", staleFence, staleErr)
	}
	crossed, crossErr := provider.AdvanceKueuePreemptionFence(fenced, highProjected)
	if !errors.Is(crossErr, provider.ErrCrossKueuePreemption) || crossed != fenced {
		return fmt.Errorf("high priority projection overwrote low priority fence: %+v: %w", crossed, crossErr)
	}
	highFence, err := provider.AdvanceKueuePreemptionFence(provider.KueuePreemptionFence{}, highProjected)
	if err != nil || highFence.JobUID != highProjected.JobUID || highFence.JobUID == fenced.JobUID {
		return fmt.Errorf("high priority fence: %+v: %w", highFence, err)
	}
	deduped, err := provider.AdvanceKueuePreemptionFence(highFence, highProjected)
	if err != nil || deduped != highFence {
		return fmt.Errorf("duplicate high priority observation was not deduped: %+v: %w", deduped, err)
	}
	db, store, err := openAdmissionPinStore(getenv)
	if err != nil {
		return err
	}
	defer func() { _ = db.Close() }()
	lowPin, err := loadPreemptPin(store, preemptLowRequest())
	if err != nil || lowPin.UID != lowProjected.JobUID {
		return fmt.Errorf("low pin drifted: %+v: %w", lowPin, err)
	}
	highPin, err := loadPreemptPin(store, highReq)
	if err != nil || highPin.UID != highProjected.JobUID || highPin.UID == lowPin.UID {
		return fmt.Errorf("high pin was not its own binding: %+v: %w", highPin, err)
	}
	lowPresent, err := stableJobObservation(ctx, func(ctx context.Context) (operation.Observation, error) {
		return lowAdapter.Inspect(ctx, preemptLowRequest())
	})
	if err != nil || lowPresent.Result != operation.ResourcePresent || lowPresent.ResourceUID != lowProjected.JobUID {
		return fmt.Errorf("preempted job inspect: %+v: %w", lowPresent, err)
	}
	highPresent, err := stableJobObservation(ctx, func(ctx context.Context) (operation.Observation, error) {
		return highAdapter.Inspect(ctx, highReq)
	})
	if err != nil || highPresent.Result != operation.ResourcePresent || highPresent.ResourceUID != highProjected.JobUID {
		return fmt.Errorf("high priority job inspect: %+v: %w", highPresent, err)
	}
	low := reportFromProjection(lowProjected, preemptLowCreateID, true)
	high := reportFromProjection(highProjected, preemptHighCreateID, true)
	if low.Released || low.Ready || low.Allocated || low.Active || low.Costable || low.Projection != "NOT_READY" ||
		high.Released || high.Ready || high.Allocated || high.Active || high.Costable || high.Projection != "ADMITTED" {
		return errors.New("preemption projection was promoted")
	}
	return writeDurable(out, preemptSubmitReport{
		Phase: "submit-high", StaleRejected: true, CrossRejected: true, Deduped: true,
		Low: low, High: high,
	})
}

type preemptDeleteReport struct {
	Phase            string `json:"phase"`
	Status           string `json:"status"`
	TombstoneDurable bool   `json:"tombstone_durable"`
	Acknowledged     bool   `json:"acknowledged"`
	UID              string `json:"uid"`
	TargetUID        string `json:"target_uid"`
	LowJobName       string `json:"low_job_name"`
	HighJobName      string `json:"high_job_name"`
	LowOperationID   string `json:"low_operation_id"`
	HighOperationID  string `json:"high_operation_id"`
	ReleaseAttested  bool   `json:"release_attested"`
	Ready            bool   `json:"ready"`
	Released         bool   `json:"released"`
}

func preemptJournalDelete(ctx context.Context, repo *postgres.Repository, store *provider.PostgresKueueJobBindingStore, adapter *provider.KueueJobAdapter, create, remove provider.Request) (provider.KueueCreatePin, error) {
	pin, err := loadPreemptPin(store, create)
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
	}, "operation-"+remove.OperationID, "kind-preempt-delete")
	if err != nil || !created {
		return provider.KueueCreatePin{}, fmt.Errorf("record delete operation %s: created=%t err=%v", remove.OperationID, created, err)
	}
	claimed, err := repo.ClaimOperation(ctx, remove.TenantID, remove.OperationID, pending.Version, "preempt-delete-writer", time.Minute)
	if err != nil {
		return provider.KueueCreatePin{}, err
	}
	started, err := repo.StartOperation(ctx, remove.TenantID, remove.OperationID, claimed.Version, "preempt-delete-writer", claimed.FenceToken)
	if err != nil {
		return provider.KueueCreatePin{}, err
	}
	if _, err := adapter.Apply(ctx, remove); err != nil {
		return provider.KueueCreatePin{}, fmt.Errorf("uid-fenced delete %s: %w", pin.UID, err)
	}
	tomb, ok, err := store.GetDeleteTombstone(smokeNamespace, remove)
	if err != nil || !ok || !tomb.Acknowledged || tomb.UID != pin.UID || tomb.Name != pin.Name {
		return provider.KueueCreatePin{}, fmt.Errorf("delete tombstone not acknowledged for %s: %+v present=%t err=%v", pin.UID, tomb, ok, err)
	}
	unknown, err := repo.MarkOperationUnknown(ctx, remove.TenantID, remove.OperationID, started.Version, "preempt-delete-writer", started.FenceToken)
	if err != nil || unknown.Status != operation.Unknown || unknown.ProviderResourceUID != "" || unknown.TargetResourceUID != pin.UID {
		return provider.KueueCreatePin{}, fmt.Errorf("delete journal was not UNKNOWN for %s: %+v err=%v", pin.UID, unknown, err)
	}
	return pin, nil
}

func preemptDelete(getenv func(string) string, out io.Writer) error {
	db, err := openPreemptJournal(getenv)
	if err != nil {
		return err
	}
	defer func() { _ = db.Close() }()
	repo, err := postgres.NewRepository(db)
	if err != nil {
		return err
	}
	pins, store, err := openAdmissionPinStore(getenv)
	if err != nil {
		return err
	}
	defer func() { _ = pins.Close() }()
	lowAdapter, err := openPreemptAdapter(getenv, preemptLowPriority)
	if err != nil {
		return err
	}
	highAdapter, err := openPreemptAdapter(getenv, preemptHighPriority)
	if err != nil {
		return err
	}
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()
	if err := ensurePreemptProvider(ctx, repo); err != nil {
		return err
	}
	if err := ensurePreemptWorkload(ctx, repo, "workload-preempt-low", "workload-preempt-low-idem", "workload-preempt-low-event"); err != nil {
		return fmt.Errorf("journal workload low: %w", err)
	}
	if err := ensurePreemptWorkload(ctx, repo, "workload-preempt-high", "workload-preempt-high-idem", "workload-preempt-high-event"); err != nil {
		return fmt.Errorf("journal workload high: %w", err)
	}
	lowPin, err := loadPreemptPin(store, preemptLowRequest())
	if err != nil {
		return err
	}
	highPin, err := loadPreemptPin(store, preemptHighRequest())
	if err != nil {
		return err
	}
	if lowPin.UID == highPin.UID || lowPin.Name == highPin.Name {
		return errors.New("low and high priority jobs are not distinct")
	}
	if _, err := preemptJournalDelete(ctx, repo, store, lowAdapter, preemptLowRequest(),
		preemptLowDelete(smokeNamespace+"/"+lowPin.Name, lowPin.UID)); err != nil {
		return err
	}
	if _, err := preemptJournalDelete(ctx, repo, store, highAdapter, preemptHighRequest(),
		preemptHighDelete(smokeNamespace+"/"+highPin.Name, highPin.UID)); err != nil {
		return err
	}
	if err := writeDurable(out, preemptDeleteReport{
		Phase: "delete", Status: string(operation.Unknown), TombstoneDurable: true, Acknowledged: true,
		UID: lowPin.UID, TargetUID: highPin.UID, LowJobName: lowPin.Name, HighJobName: highPin.Name,
		LowOperationID: preemptLowDeleteID, HighOperationID: preemptHighDeleteID,
	}); err != nil {
		return err
	}
	holdUntilKilled()
	return errors.New("preemption delete process unexpectedly survived SIGKILL")
}

func preemptRecoverOne(ctx context.Context, repo *postgres.Repository, store *provider.PostgresKueueJobBindingStore, worker *recovery.Worker, db *sql.DB, remove provider.Request) error {
	previous, err := repo.GetOperation(ctx, remove.TenantID, remove.OperationID)
	if err != nil || previous.Status != operation.Unknown || previous.Kind != operation.Delete ||
		previous.TargetResourceUID != remove.TargetResourceUID || previous.ProviderResourceUID != "" {
		return fmt.Errorf("delete journal is not UNKNOWN: %+v err=%v", previous, err)
	}
	next, err := worker.RecoverUnknown(ctx, remove.TenantID, remove.OperationID, "preempt-delete-recovery", time.Minute)
	if err != nil {
		return fmt.Errorf("inspect-only delete recovery: %w", err)
	}
	tomb, ok, err := store.GetDeleteTombstone(smokeNamespace, remove)
	if err != nil || !ok || !tomb.Acknowledged || tomb.UID != remove.TargetResourceUID {
		return fmt.Errorf("tombstone drifted: %+v present=%t err=%v", tomb, ok, err)
	}
	if next.Status != operation.Succeeded || next.ProviderResourceUID != "" || next.ProviderResourceID != "" ||
		next.TargetResourceUID != remove.TargetResourceUID || next.FenceToken <= previous.FenceToken {
		return fmt.Errorf("delete recovery was not TARGET_ABSENT for the pinned UID: %+v", next)
	}
	observations, result, source, version, resourceUID, targetUID, reason, events, err := journalDeleteEvidence(ctx, db, remove.TenantID, remove.OperationID)
	if err != nil {
		return err
	}
	if observations != 1 || events != 2 || result != string(operation.TargetAbsent) || reason != "" ||
		source != preemptSource || version != preemptVersion || resourceUID != "" || targetUID != remove.TargetResourceUID {
		return fmt.Errorf("TARGET_ABSENT evidence mismatch: rows=%d events=%d result=%q reason=%q source=%q version=%q resource=%q target=%q",
			observations, events, result, reason, source, version, resourceUID, targetUID)
	}
	return nil
}

func preemptRecoverDelete(getenv func(string) string, out io.Writer) error {
	db, err := openPreemptJournal(getenv)
	if err != nil {
		return err
	}
	defer func() { _ = db.Close() }()
	repo, err := postgres.NewRepository(db)
	if err != nil {
		return err
	}
	pins, store, err := openAdmissionPinStore(getenv)
	if err != nil {
		return err
	}
	defer func() { _ = pins.Close() }()
	lowAdapter, err := openPreemptAdapter(getenv, preemptLowPriority)
	if err != nil {
		return err
	}
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()
	lowPin, err := loadPreemptPin(store, preemptLowRequest())
	if err != nil {
		return err
	}
	highPin, err := loadPreemptPin(store, preemptHighRequest())
	if err != nil {
		return err
	}
	worker, err := preemptWorker(repo, lowAdapter)
	if err != nil {
		return err
	}
	lowRemove := preemptLowDelete(smokeNamespace+"/"+lowPin.Name, lowPin.UID)
	highRemove := preemptHighDelete(smokeNamespace+"/"+highPin.Name, highPin.UID)
	if err := preemptRecoverOne(ctx, repo, store, worker, db, lowRemove); err != nil {
		return fmt.Errorf("low: %w", err)
	}
	if err := preemptRecoverOne(ctx, repo, store, worker, db, highRemove); err != nil {
		return fmt.Errorf("high: %w", err)
	}
	return writeDurable(out, preemptDeleteReport{
		Phase: "recover-delete", Status: string(operation.Succeeded), TombstoneDurable: true, Acknowledged: true,
		UID: lowPin.UID, TargetUID: highPin.UID, LowJobName: lowPin.Name, HighJobName: highPin.Name,
		LowOperationID: preemptLowDeleteID, HighOperationID: preemptHighDeleteID,
	})
}

func preemptSuccessor(getenv func(string) string, out io.Writer) error {
	db, err := openPreemptJournal(getenv)
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
	if err := ensurePreemptProvider(ctx, repo); err != nil {
		return err
	}
	record := func(originalID string, remove provider.Request) error {
		original, err := repo.GetOperation(ctx, remove.TenantID, originalID)
		if err != nil || original.Kind != operation.Delete || original.TargetResourceUID == "" || original.TargetResourceUID != remove.TargetResourceUID {
			return fmt.Errorf("original delete target is not journaled: %+v err=%v", original, err)
		}
		pending, created, err := repo.CreateOperation(ctx, operation.Operation{
			ID: remove.OperationID, TenantID: remove.TenantID, WorkloadID: remove.WorkloadID,
			ProviderID: remove.ProviderID, IdempotencyKey: remove.IdempotencyKey,
			RequestDigest: remove.RequestDigest, Generation: remove.DesiredGeneration, Version: 1,
			Kind: operation.Delete, Status: operation.Pending,
			TargetResourceID: remove.TargetResourceID, TargetResourceUID: remove.TargetResourceUID,
		}, "operation-"+remove.OperationID, "kind-preempt-successor")
		if err != nil || !created {
			return fmt.Errorf("record successor delete: created=%t err=%v", created, err)
		}
		claimed, err := repo.ClaimOperation(ctx, remove.TenantID, remove.OperationID, pending.Version, "preempt-successor-writer", time.Minute)
		if err != nil {
			return err
		}
		started, err := repo.StartOperation(ctx, remove.TenantID, remove.OperationID, claimed.Version, "preempt-successor-writer", claimed.FenceToken)
		if err != nil {
			return err
		}
		unknown, err := repo.MarkOperationUnknown(ctx, remove.TenantID, remove.OperationID, started.Version, "preempt-successor-writer", started.FenceToken)
		if err != nil || unknown.Status != operation.Unknown || unknown.TargetResourceUID != remove.TargetResourceUID || unknown.ProviderResourceUID != "" {
			return fmt.Errorf("successor journal was not UNKNOWN: %+v err=%v", unknown, err)
		}
		return nil
	}
	pins, store, err := openAdmissionPinStore(getenv)
	if err != nil {
		return err
	}
	defer func() { _ = pins.Close() }()
	lowPin, err := loadPreemptPin(store, preemptLowRequest())
	if err != nil {
		return err
	}
	highPin, err := loadPreemptPin(store, preemptHighRequest())
	if err != nil {
		return err
	}
	if err := record(preemptLowDeleteID, preemptLowSuccessorRequest(smokeNamespace+"/"+lowPin.Name, lowPin.UID)); err != nil {
		return err
	}
	if err := record(preemptHighDeleteID, preemptHighSuccessorRequest(smokeNamespace+"/"+highPin.Name, highPin.UID)); err != nil {
		return err
	}
	if err := writeDurable(out, preemptDeleteReport{
		Phase: "successor", Status: string(operation.Unknown), UID: lowPin.UID, TargetUID: highPin.UID,
		LowJobName: lowPin.Name, HighJobName: highPin.Name,
		LowOperationID: preemptLowSuccessor, HighOperationID: preemptHighSuccess,
	}); err != nil {
		return err
	}
	holdUntilKilled()
	return errors.New("preemption successor process unexpectedly survived SIGKILL")
}

func preemptRecoverSuccessor(getenv func(string) string, out io.Writer) error {
	db, err := openPreemptJournal(getenv)
	if err != nil {
		return err
	}
	defer func() { _ = db.Close() }()
	repo, err := postgres.NewRepository(db)
	if err != nil {
		return err
	}
	pins, store, err := openAdmissionPinStore(getenv)
	if err != nil {
		return err
	}
	defer func() { _ = pins.Close() }()
	lowAdapter, err := openPreemptAdapter(getenv, preemptLowPriority)
	if err != nil {
		return err
	}
	highAdapter, err := openPreemptAdapter(getenv, preemptHighPriority)
	if err != nil {
		return err
	}
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()
	lowPin, err := loadPreemptPin(store, preemptLowRequest())
	if err != nil {
		return err
	}
	highPin, err := loadPreemptPin(store, preemptHighRequest())
	if err != nil {
		return err
	}
	lowWorker, err := preemptWorker(repo, lowAdapter)
	if err != nil {
		return err
	}
	highWorker, err := preemptWorker(repo, highAdapter)
	if err != nil {
		return err
	}
	check := func(worker *recovery.Worker, adapter *provider.KueueJobAdapter, create, remove provider.Request) error {
		previous, err := repo.GetOperation(ctx, remove.TenantID, remove.OperationID)
		if err != nil || previous.Status != operation.Unknown || previous.TargetResourceUID != remove.TargetResourceUID {
			return fmt.Errorf("successor journal is not UNKNOWN: %+v err=%v", previous, err)
		}
		next, err := worker.RecoverUnknown(ctx, remove.TenantID, remove.OperationID, "preempt-successor-recovery", time.Minute)
		if err != nil {
			return err
		}
		if next.Status != operation.ManualReview || next.ManualReviewCode != "JOB_UID_REPLACED" ||
			next.ProviderResourceUID != "" || next.TargetResourceUID != remove.TargetResourceUID ||
			next.FenceToken <= previous.FenceToken {
			return fmt.Errorf("successor recovery did not leave JOB_UID_REPLACED: %+v", next)
		}
		obs, err := adapter.Inspect(ctx, create)
		if err != nil || obs.Result != operation.ObservationConflict || obs.ReasonCode != "JOB_UID_REPLACED" || obs.ResourceUID != "" {
			return fmt.Errorf("create inspect did not leave the successor: %+v err=%v", obs, err)
		}
		return nil
	}
	if err := check(lowWorker, lowAdapter, preemptLowRequest(), preemptLowSuccessorRequest(smokeNamespace+"/"+lowPin.Name, lowPin.UID)); err != nil {
		return fmt.Errorf("low successor: %w", err)
	}
	if err := check(highWorker, highAdapter, preemptHighRequest(), preemptHighSuccessorRequest(smokeNamespace+"/"+highPin.Name, highPin.UID)); err != nil {
		return fmt.Errorf("high successor: %w", err)
	}
	return writeDurable(out, preemptDeleteReport{
		Phase: "recover-successor", Status: string(operation.ManualReview),
		UID: lowPin.UID, TargetUID: highPin.UID, LowJobName: lowPin.Name, HighJobName: highPin.Name,
		LowOperationID: preemptLowSuccessor, HighOperationID: preemptHighSuccess,
	})
}

func runPreemption(args []string, getenv func(string) string, out io.Writer) error {
	phase, _, err := parsePreemptionArgs(args)
	if err != nil {
		return err
	}
	switch phase {
	case "submit-low":
		return preemptSubmitLow(getenv, out)
	case "submit-high":
		return preemptSubmitHigh(getenv, out)
	case "delete":
		return preemptDelete(getenv, out)
	case "recover-delete":
		return preemptRecoverDelete(getenv, out)
	case "successor":
		return preemptSuccessor(getenv, out)
	case "recover-successor":
		return preemptRecoverSuccessor(getenv, out)
	default:
		return errors.New("unknown preemption phase")
	}
}
