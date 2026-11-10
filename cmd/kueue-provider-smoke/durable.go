package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"io"
	"os"
	"strings"
	"time"

	_ "github.com/jackc/pgx/v5/stdlib"

	"github.com/avaargsh/compute-control-panel/internal/operation"
	"github.com/avaargsh/compute-control-panel/internal/provider"
)

// errEmptyBindingDSN is the disposable smoke refusal when no binding DSN is
// configured. The process stops before it builds an adapter, so it cannot
// adopt or delete a Job.
var errEmptyBindingDSN = errors.New("CCP_KUEUE_BINDING_DSN is empty")

// durableCreateRequest is a different operation from the in-memory lifecycle
// smoke so the two Jobs never share a name. The Job stays suspended and has
// no Kueue queue label.
func durableCreateRequest() provider.Request {
	return provider.Request{
		OperationID: "ccp-kind-durable-create", TenantID: "tenant-a",
		WorkloadID: "workload-a", ProviderID: "kueue",
		IdempotencyKey:    "ccp-kind-durable-create-key",
		RequestDigest:     "sha256:" + strings.Repeat("cd", 32),
		DesiredGeneration: 7, Kind: operation.Create,
	}
}

func durableDeleteRequest(resourceID, uid string) provider.Request {
	req := durableCreateRequest()
	req.Kind = operation.Delete
	req.OperationID = "ccp-kind-durable-delete"
	req.IdempotencyKey = "ccp-kind-durable-delete-key"
	req.TargetResourceID = resourceID
	req.TargetResourceUID = uid
	return req
}

func requireDisposableSmoke(getenv func(string) string) error {
	if !disposableAllowed(getenv) || getenv("POD_NAMESPACE") != smokeNamespace {
		return errors.New("requires disposable kind opt-in and isolated smoke namespace")
	}
	return nil
}

func durableCredentialPaths(getenv func(string) string) (string, string) {
	ca := getenv("CCP_KUEUE_SA_CA")
	token := getenv("CCP_KUEUE_SA_TOKEN")
	if ca == "" {
		ca = serviceAccountFiles + "/ca.crt"
	}
	if token == "" {
		token = serviceAccountFiles + "/token"
	}
	return ca, token
}

// openDurableAdapter uses the ServiceAccount constructor with a PostgreSQL
// binding store. NewKueueJobAdapter's private memory store is unchanged.
// An empty DSN is refused here. A later query error is returned to the
// adapter, which fails closed.
func openDurableAdapter(getenv func(string) string) (*sql.DB, *provider.KueueJobAdapter, error) {
	if err := requireDisposableSmoke(getenv); err != nil {
		return nil, nil, err
	}
	dsn := strings.TrimSpace(getenv("CCP_KUEUE_BINDING_DSN"))
	if dsn == "" {
		return nil, nil, errEmptyBindingDSN
	}
	apiURL, err := apiEndpoint(getenv)
	if err != nil {
		return nil, nil, err
	}
	db, err := sql.Open("pgx", dsn)
	if err != nil {
		return nil, nil, err
	}
	db.SetMaxOpenConns(2)
	store, err := provider.NewPostgresKueueJobBindingStore(db)
	if err != nil {
		_ = db.Close()
		return nil, nil, err
	}
	ca, token := durableCredentialPaths(getenv)
	adapter, err := provider.NewServiceAccountKueueJobAdapterWithBindings(
		apiURL, ca, token, smokeNamespace, store,
	)
	if err != nil {
		_ = db.Close()
		return nil, nil, err
	}
	return db, adapter, nil
}

func writeDurable(out io.Writer, v any) error {
	if err := json.NewEncoder(out).Encode(v); err != nil {
		return err
	}
	// A redirected file is durable once Write returns. Sync can fail on a
	// terminal; the driver only needs the bytes already written.
	if f, ok := out.(*os.File); ok {
		_ = f.Sync()
	}
	return nil
}

// holdUntilKilled keeps the process alive after the binding row is durable
// so a driver can SIGKILL it. SIGKILL cannot be caught or handled.
func holdUntilKilled() {
	c := make(chan struct{})
	<-c
}

func parseDurableArgs(args []string) (phase, subject string, hold bool, err error) {
	if len(args) == 0 || args[0] != "durable" {
		return "", "", false, errors.New("unknown kueue-provider-smoke command")
	}
	rest := args[1:]
	if len(rest) == 0 {
		return "", "", false, errors.New("durable phase required")
	}
	phase, rest = rest[0], rest[1:]
	switch phase {
	case "create", "delete":
		if len(rest) == 0 {
			return phase, "", false, nil
		}
		if len(rest) == 1 && rest[0] == "--hold" {
			return phase, "", true, nil
		}
	case "inspect", "replay":
		if len(rest) == 1 && (rest[0] == "create" || rest[0] == "delete") {
			return phase, rest[0], false, nil
		}
	case "fail-closed":
		if len(rest) == 0 {
			return phase, "", false, nil
		}
	}
	return "", "", false, errors.New("unknown durable phase")
}

type durableCreateReport struct {
	Phase           string `json:"phase"`
	PinDurable      bool   `json:"pin_durable"`
	UID             string `json:"uid"`
	JobName         string `json:"job_name"`
	ResourceID      string `json:"resource_id"`
	OperationID     string `json:"operation_id"`
	ReleaseAttested bool   `json:"release_attested"`
}

type durableDeleteReport struct {
	Phase            string `json:"phase"`
	TombstoneDurable bool   `json:"tombstone_durable"`
	Acknowledged     bool   `json:"acknowledged"`
	UID              string `json:"uid"`
	JobName          string `json:"job_name"`
	OperationID      string `json:"operation_id"`
	ReleaseAttested  bool   `json:"release_attested"`
}

type durableReplayReport struct {
	Phase           string `json:"phase"`
	Result          string `json:"result"`
	UID             string `json:"uid"`
	JobName         string `json:"job_name"`
	OperationID     string `json:"operation_id"`
	ReleaseAttested bool   `json:"release_attested"`
}

type durableInspectReport struct {
	Phase           string `json:"phase"`
	Result          string `json:"result"`
	ReasonCode      string `json:"reason_code"`
	UID             string `json:"uid"`
	TargetUID       string `json:"target_uid"`
	ReleaseAttested bool   `json:"release_attested"`
}

type failClosedReport struct {
	Phase          string `json:"phase"`
	FailedClosed   bool   `json:"failed_closed"`
	Reason         string `json:"reason"`
	CreateApplied  bool   `json:"create_applied"`
	DeleteApplied  bool   `json:"delete_applied"`
	InspectPresent bool   `json:"inspect_present"`
}

func loadCreatePin(store *provider.PostgresKueueJobBindingStore) (provider.KueueCreatePin, error) {
	pin, ok, err := store.GetCreatePin(smokeNamespace, durableCreateRequest())
	if err != nil {
		return provider.KueueCreatePin{}, err
	}
	if !ok || pin.UID == "" || pin.Name == "" {
		return provider.KueueCreatePin{}, errors.New("durable create pin is not stored")
	}
	return pin, nil
}

func durableCreate(getenv func(string) string, out io.Writer, hold bool) error {
	db, adapter, err := openDurableAdapter(getenv)
	if err != nil {
		return err
	}
	store, err := provider.NewPostgresKueueJobBindingStore(db)
	if err != nil {
		_ = db.Close()
		return err
	}
	ctx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
	defer cancel()
	create := durableCreateRequest()
	if _, err := adapter.Apply(ctx, create); err != nil {
		_ = db.Close()
		return err
	}
	pin, err := loadCreatePin(store)
	_ = db.Close()
	if err != nil {
		return err
	}
	if err := writeDurable(out, durableCreateReport{
		Phase: "create", PinDurable: true, UID: pin.UID, JobName: pin.Name,
		ResourceID: smokeNamespace + "/" + pin.Name, OperationID: create.OperationID,
	}); err != nil {
		return err
	}
	if hold {
		holdUntilKilled()
	}
	return nil
}

func durableDelete(getenv func(string) string, out io.Writer, hold bool) error {
	db, adapter, err := openDurableAdapter(getenv)
	if err != nil {
		return err
	}
	store, err := provider.NewPostgresKueueJobBindingStore(db)
	if err != nil {
		_ = db.Close()
		return err
	}
	ctx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
	defer cancel()
	pin, err := loadCreatePin(store)
	if err != nil {
		_ = db.Close()
		return err
	}
	remove := durableDeleteRequest(smokeNamespace+"/"+pin.Name, pin.UID)
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
		return errors.New("durable delete tombstone is not stored")
	}
	if err := writeDurable(out, durableDeleteReport{
		Phase: "delete", TombstoneDurable: true, Acknowledged: true,
		UID: tomb.UID, JobName: tomb.Name, OperationID: remove.OperationID,
	}); err != nil {
		return err
	}
	if hold {
		holdUntilKilled()
	}
	return nil
}

func durableReplay(getenv func(string) string, out io.Writer, subject string) error {
	db, adapter, err := openDurableAdapter(getenv)
	if err != nil {
		return err
	}
	store, err := provider.NewPostgresKueueJobBindingStore(db)
	if err != nil {
		_ = db.Close()
		return err
	}
	ctx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
	defer cancel()
	switch subject {
	case "create":
		create := durableCreateRequest()
		if _, err := adapter.Apply(ctx, create); err != nil {
			_ = db.Close()
			return err
		}
		pin, err := loadCreatePin(store)
		_ = db.Close()
		if err != nil {
			return err
		}
		return writeDurable(out, durableReplayReport{
			Phase: "replay-create", Result: "applied", UID: pin.UID,
			JobName: pin.Name, OperationID: create.OperationID,
		})
	case "delete":
		pin, err := loadCreatePin(store)
		if err != nil {
			_ = db.Close()
			return err
		}
		remove := durableDeleteRequest(smokeNamespace+"/"+pin.Name, pin.UID)
		if _, err := adapter.Apply(ctx, remove); err != nil {
			_ = db.Close()
			return err
		}
		tomb, ok, err := store.GetDeleteTombstone(smokeNamespace, remove)
		_ = db.Close()
		if err != nil {
			return err
		}
		if !ok || !tomb.Acknowledged || tomb.UID != pin.UID {
			return errors.New("replay did not load the acknowledged tombstone")
		}
		return writeDurable(out, durableReplayReport{
			Phase: "replay-delete", Result: "applied", UID: tomb.UID,
			JobName: tomb.Name, OperationID: remove.OperationID,
		})
	default:
		_ = db.Close()
		return errors.New("unknown durable replay subject")
	}
}

func durableInspect(getenv func(string) string, out io.Writer, subject string) error {
	db, adapter, err := openDurableAdapter(getenv)
	if err != nil {
		return err
	}
	store, err := provider.NewPostgresKueueJobBindingStore(db)
	if err != nil {
		_ = db.Close()
		return err
	}
	ctx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
	defer cancel()
	defer func() { _ = db.Close() }()
	switch subject {
	case "create":
		create := durableCreateRequest()
		obs, err := stableJobObservation(ctx, func(ctx context.Context) (operation.Observation, error) {
			return adapter.Inspect(ctx, create)
		})
		if err != nil {
			return err
		}
		return writeDurable(out, durableInspectReport{
			Phase: "inspect-create", Result: string(obs.Result), ReasonCode: obs.ReasonCode,
			UID: obs.ResourceUID, TargetUID: obs.TargetResourceUID,
		})
	case "delete":
		pin, err := loadCreatePin(store)
		if err != nil {
			return err
		}
		remove := durableDeleteRequest(smokeNamespace+"/"+pin.Name, pin.UID)
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
		if obs.Result == operation.TargetAbsent && obs.TargetResourceUID != pin.UID {
			if werr := writeDurable(out, durableInspectReport{
				Phase: "inspect-delete", Result: string(obs.Result), ReasonCode: obs.ReasonCode,
				UID: obs.ResourceUID, TargetUID: obs.TargetResourceUID,
			}); werr != nil {
				return werr
			}
			return errors.New("TARGET_ABSENT was not for the pinned UID")
		}
		return writeDurable(out, durableInspectReport{
			Phase: "inspect-delete", Result: string(obs.Result), ReasonCode: obs.ReasonCode,
			UID: obs.ResourceUID, TargetUID: obs.TargetResourceUID,
		})
	default:
		return errors.New("unknown durable inspect subject")
	}
}

// durableFailClosed proves a fresh process does not adopt or delete when the
// binding database is missing or unreachable. An empty DSN returns before the
// adapter is built. A DSN that cannot serve the binding tables surfaces as a
// store error from Apply, Inspect, and DELETE.
func durableFailClosed(getenv func(string) string, out io.Writer) error {
	if err := requireDisposableSmoke(getenv); err != nil {
		return err
	}
	if strings.TrimSpace(getenv("CCP_KUEUE_BINDING_DSN")) == "" {
		if err := writeDurable(out, failClosedReport{
			Phase: "fail-closed", FailedClosed: true, Reason: "empty_dsn",
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
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	report := failClosedReport{Phase: "fail-closed", Reason: "unreachable"}
	create := durableCreateRequest()
	if _, err := adapter.Apply(ctx, create); err == nil {
		report.CreateApplied = true
	}
	obs, err := adapter.Inspect(ctx, create)
	if err == nil && obs.Result == operation.ResourcePresent {
		report.InspectPresent = true
	}
	name := getenv("CCP_KUEUE_SMOKE_TARGET_NAME")
	uid := getenv("CCP_KUEUE_SMOKE_TARGET_UID")
	if name == "" || uid == "" {
		report.FailedClosed = false
		_ = writeDurable(out, report)
		return errors.New("fail-closed delete attempt needs the live job name and UID")
	}
	remove := durableDeleteRequest(smokeNamespace+"/"+name, uid)
	if _, err := adapter.Apply(ctx, remove); err == nil {
		report.DeleteApplied = true
	}
	report.FailedClosed = !report.CreateApplied && !report.DeleteApplied && !report.InspectPresent
	if err := writeDurable(out, report); err != nil {
		return err
	}
	if !report.FailedClosed {
		return errors.New("unreachable database did not fail closed")
	}
	return errors.New("unreachable database failed closed")
}

func runDurable(args []string, getenv func(string) string, out io.Writer) error {
	phase, subject, hold, err := parseDurableArgs(args)
	if err != nil {
		return err
	}
	switch phase {
	case "create":
		return durableCreate(getenv, out, hold)
	case "delete":
		return durableDelete(getenv, out, hold)
	case "replay":
		return durableReplay(getenv, out, subject)
	case "inspect":
		return durableInspect(getenv, out, subject)
	case "fail-closed":
		return durableFailClosed(getenv, out)
	default:
		return errors.New("unknown durable phase")
	}
}
