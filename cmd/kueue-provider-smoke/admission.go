package main

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"io"
	"strings"
	"time"

	"github.com/avaargsh/compute-control-panel/internal/operation"
	"github.com/avaargsh/compute-control-panel/internal/provider"
)

const admissionLocalQueue = "ccp-smoke"

// admissionCreateRequest is a different queued operation from the memory
// smoke and the suspended durable Job. Kueue owns queue admission. This
// request does not add a second queue label or unsuspend step.
func admissionCreateRequest() provider.Request {
	return provider.Request{
		OperationID: "ccp-kind-admission-unplaced", TenantID: "tenant-a",
		WorkloadID: "workload-a", ProviderID: "kueue",
		IdempotencyKey:    "ccp-kind-admission-unplaced-key",
		RequestDigest:     "sha256:" + strings.Repeat("ef", 32),
		DesiredGeneration: 7, Kind: operation.Create,
	}
}

func admissionDeleteRequest(resourceID, uid string) provider.Request {
	req := admissionCreateRequest()
	req.Kind = operation.Delete
	req.OperationID = "ccp-kind-admission-unplaced-delete"
	req.IdempotencyKey = "ccp-kind-admission-unplaced-delete-key"
	req.TargetResourceID = resourceID
	req.TargetResourceUID = uid
	return req
}

func parseAdmissionArgs(args []string) (phase, subject string, err error) {
	if len(args) == 0 || args[0] != "admission" {
		return "", "", errors.New("unknown kueue-provider-smoke command")
	}
	rest := args[1:]
	if len(rest) == 0 {
		return "", "", errors.New("admission phase required")
	}
	phase, rest = rest[0], rest[1:]
	switch phase {
	case "create", "delete":
		if len(rest) == 0 {
			return phase, "", nil
		}
	case "inspect":
		if len(rest) == 1 && (rest[0] == "create" || rest[0] == "delete") {
			return phase, rest[0], nil
		}
	}
	return "", "", errors.New("unknown admission phase")
}

type admissionReport struct {
	Phase                   string `json:"phase"`
	Projection              string `json:"projection"`
	Stage                   string `json:"stage"`
	Result                  string `json:"result"`
	ReasonCode              string `json:"reason_code"`
	QuotaReserved           bool   `json:"quota_reserved"`
	Admitted                bool   `json:"admitted"`
	ClusterQueue            string `json:"cluster_queue"`
	PodSetAssignments       string `json:"pod_set_assignments"`
	WorkloadNamespace       string `json:"workload_namespace"`
	WorkloadName            string `json:"workload_name"`
	WorkloadUID             string `json:"workload_uid"`
	WorkloadResourceVersion string `json:"workload_resource_version"`
	JobUID                  string `json:"job_uid"`
	JobName                 string `json:"job_name"`
	JobResourceVersion      string `json:"job_resource_version"`
	PodUID                  string `json:"pod_uid"`
	PodResourceVersion      string `json:"pod_resource_version"`
	PodScheduled            bool   `json:"pod_scheduled"`
	NodeBound               bool   `json:"node_bound"`
	NodeName                string `json:"node_name"`
	TargetUID               string `json:"target_uid"`
	AbsenceObserved         bool   `json:"absence_observed"`
	Allocated               bool   `json:"allocated"`
	Active                  bool   `json:"active"`
	Ready                   bool   `json:"ready"`
	Costable                bool   `json:"costable"`
	Released                bool   `json:"released"`
	ReleaseAttested         bool   `json:"release_attested"`
	OperationID             string `json:"operation_id"`
}

func openAdmissionAdapter(getenv func(string) string) (*provider.KueueJobAdapter, error) {
	if err := requireDisposableSmoke(getenv); err != nil {
		return nil, err
	}
	dsn := strings.TrimSpace(getenv("CCP_KUEUE_BINDING_DSN"))
	if dsn == "" {
		return nil, errEmptyBindingDSN
	}
	apiURL, err := apiEndpoint(getenv)
	if err != nil {
		return nil, err
	}
	ca, token := durableCredentialPaths(getenv)
	// This is the queued in-cluster constructor. It refuses a superuser DSN
	// and an in-memory binding store before it posts a Job.
	return provider.NewQueuedServiceAccountKueueJobAdapter(
		apiURL, ca, token, smokeNamespace, admissionLocalQueue, dsn,
	)
}

func openAdmissionPinStore(getenv func(string) string) (*sql.DB, *provider.PostgresKueueJobBindingStore, error) {
	if err := requireDisposableSmoke(getenv); err != nil {
		return nil, nil, err
	}
	dsn := strings.TrimSpace(getenv("CCP_KUEUE_BINDING_DSN"))
	if dsn == "" {
		return nil, nil, errEmptyBindingDSN
	}
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	store, db, err := provider.OpenDurablePostgresKueueJobBindingStore(ctx, dsn)
	if err != nil {
		return nil, nil, err
	}
	return db, store, nil
}

func loadAdmissionPin(store *provider.PostgresKueueJobBindingStore) (provider.KueueCreatePin, error) {
	pin, ok, err := store.GetCreatePin(smokeNamespace, admissionCreateRequest())
	if err != nil {
		return provider.KueueCreatePin{}, err
	}
	if !ok || pin.UID == "" || pin.Name == "" {
		return provider.KueueCreatePin{}, errors.New("admission create pin is not stored")
	}
	return pin, nil
}

func retryableAdmissionRead(err error) bool {
	return errors.Is(err, provider.ErrInvalidKueueObservation) &&
		!strings.Contains(err.Error(), "Kubernetes API returned HTTP")
}

func admissionCreate(getenv func(string) string, out io.Writer) error {
	adapter, err := openAdmissionAdapter(getenv)
	if err != nil {
		return err
	}
	ctx, cancel := context.WithTimeout(context.Background(), 150*time.Second)
	defer cancel()
	create := admissionCreateRequest()
	if _, err := adapter.Apply(ctx, create); err != nil {
		return fmt.Errorf("queued admission create: %w", err)
	}
	var projected provider.AdmissionPlacementProjection
	for {
		collected, err := adapter.CollectCreate(ctx, create)
		if err != nil {
			if !retryableAdmissionRead(err) {
				return err
			}
		} else if collected.Evidence.PodScheduled || collected.Evidence.PodNodeName != "" ||
			collected.Stage == provider.KueuePodPlaced {
			return errors.New("pod was node-bound; admission was not kept separate from placement")
		} else if collected.Stage == provider.KueueAdmitted {
			projected, err = provider.ProjectAdmissionWithoutPlacement(collected.Evidence)
			if err == nil {
				break
			}
			if collected.Evidence.Pod.UID != "" {
				return fmt.Errorf("admitted pod was not an unplaced projection: %w", err)
			}
		}
		select {
		case <-ctx.Done():
			return fmt.Errorf("admitted unplaced observation did not stabilize: %w", ctx.Err())
		case <-time.After(200 * time.Millisecond):
		}
	}
	present, err := stableJobObservation(ctx, func(ctx context.Context) (operation.Observation, error) {
		return adapter.Inspect(ctx, create)
	})
	if err != nil || present.Result != operation.ResourcePresent || present.ResourceUID != projected.JobUID {
		return fmt.Errorf("inspect did not stay on the admitted job: %+v: %w", present, err)
	}
	if projected.Phase != "NOT_READY" || projected.Allocated || projected.Active ||
		projected.Ready || projected.Costable || projected.Released || projected.NodeBound {
		return errors.New("admission projection was not NOT_READY")
	}
	return writeDurable(out, admissionReport{
		Phase: "create", Projection: projected.Phase, Stage: string(projected.Stage),
		Result: string(present.Result), QuotaReserved: projected.QuotaReserved,
		Admitted: projected.Admitted, ClusterQueue: projected.ClusterQueue,
		PodSetAssignments: projected.PodSetAssignmentsJSON,
		WorkloadNamespace: projected.WorkloadNamespace, WorkloadName: projected.WorkloadName,
		WorkloadUID: projected.WorkloadUID, WorkloadResourceVersion: projected.WorkloadResourceVersion,
		JobUID: projected.JobUID, JobName: projected.JobName, JobResourceVersion: projected.JobResourceVersion,
		PodUID: projected.PodUID, PodResourceVersion: projected.PodResourceVersion,
		PodScheduled: projected.PodScheduled, NodeBound: projected.NodeBound, NodeName: projected.NodeName,
		AbsenceObserved: false, OperationID: create.OperationID,
	})
}

func admissionDelete(getenv func(string) string, out io.Writer) error {
	db, store, err := openAdmissionPinStore(getenv)
	if err != nil {
		return err
	}
	adapter, err := openAdmissionAdapter(getenv)
	if err != nil {
		_ = db.Close()
		return err
	}
	ctx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
	defer cancel()
	pin, err := loadAdmissionPin(store)
	if err != nil {
		_ = db.Close()
		return err
	}
	remove := admissionDeleteRequest(smokeNamespace+"/"+pin.Name, pin.UID)
	if _, err := adapter.Apply(ctx, remove); err != nil {
		_ = db.Close()
		return err
	}
	tomb, ok, err := store.GetDeleteTombstone(smokeNamespace, remove)
	_ = db.Close()
	if err != nil {
		return err
	}
	if !ok || !tomb.Acknowledged || tomb.UID != pin.UID || tomb.Name != pin.Name {
		return errors.New("admission delete tombstone is not stored")
	}
	// The DELETE acknowledgement is not absence. A later process must Inspect.
	return writeDurable(out, admissionReport{
		Phase: "delete", Projection: "NOT_READY", JobUID: tomb.UID, JobName: tomb.Name,
		TargetUID: tomb.UID, AbsenceObserved: false, OperationID: remove.OperationID,
	})
}

func admissionInspect(getenv func(string) string, out io.Writer, subject string) error {
	db, store, err := openAdmissionPinStore(getenv)
	if err != nil {
		return err
	}
	defer func() { _ = db.Close() }()
	adapter, err := openAdmissionAdapter(getenv)
	if err != nil {
		return err
	}
	ctx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
	defer cancel()
	pin, err := loadAdmissionPin(store)
	if err != nil {
		return err
	}
	switch subject {
	case "delete":
		remove := admissionDeleteRequest(smokeNamespace+"/"+pin.Name, pin.UID)
		var obs operation.Observation
		for {
			obs, err = adapter.Inspect(ctx, remove)
			if err != nil {
				return err
			}
			if obs.Result == operation.TargetAbsent {
				break
			}
			if obs.Result != operation.ObservationConflict || obs.ReasonCode != "DELETE_TARGET_STILL_PRESENT" {
				break
			}
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(200 * time.Millisecond):
			}
		}
		report := admissionReport{
			Phase: "inspect-delete", Projection: "NOT_READY", Result: string(obs.Result),
			ReasonCode: obs.ReasonCode, JobUID: obs.ResourceUID, TargetUID: obs.TargetResourceUID,
			AbsenceObserved: obs.Result == operation.TargetAbsent && obs.TargetResourceUID == pin.UID && obs.ResourceUID == "",
			OperationID:     remove.OperationID,
		}
		if err := writeDurable(out, report); err != nil {
			return err
		}
		if !report.AbsenceObserved {
			return errors.New("inspect-first absence was not TARGET_ABSENT for the pinned UID")
		}
		return nil
	case "create":
		create := admissionCreateRequest()
		obs, err := adapter.Inspect(ctx, create)
		if err != nil {
			return err
		}
		report := admissionReport{
			Phase: "inspect-create", Projection: "NOT_READY", Result: string(obs.Result),
			ReasonCode: obs.ReasonCode, JobUID: obs.ResourceUID, TargetUID: obs.TargetResourceUID,
			AbsenceObserved: false, OperationID: create.OperationID,
		}
		if err := writeDurable(out, report); err != nil {
			return err
		}
		if obs.Result != operation.ObservationConflict || obs.ReasonCode != "JOB_UID_REPLACED" || obs.ResourceUID != "" {
			return errors.New("same-name successor was not left as JOB_UID_REPLACED")
		}
		return nil
	default:
		return errors.New("unknown admission inspect subject")
	}
}

func runAdmission(args []string, getenv func(string) string, out io.Writer) error {
	phase, subject, err := parseAdmissionArgs(args)
	if err != nil {
		return err
	}
	switch phase {
	case "create":
		return admissionCreate(getenv, out)
	case "delete":
		return admissionDelete(getenv, out)
	case "inspect":
		return admissionInspect(getenv, out, subject)
	default:
		return errors.New("unknown admission phase")
	}
}
