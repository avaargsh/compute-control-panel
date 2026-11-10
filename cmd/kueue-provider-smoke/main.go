// kueue-provider-smoke is a disposable-kind-only executable that invokes
// the actual KueueJobAdapter against kube-apiserver with a scoped ServiceAccount.
// It must never be used to infer production GPU readiness or release.
package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/avaargsh/compute-control-panel/internal/operation"
	"github.com/avaargsh/compute-control-panel/internal/provider"
)

const (
	smokeNamespace      = "ccp-kueue-smoke"
	serviceAccountFiles = "/var/run/secrets/kubernetes.io/serviceaccount"
)

func disposableAllowed(getenv func(string) string) bool {
	return getenv != nil && getenv("CCP_DISPOSABLE_KIND_SMOKE") == "1"
}

func lifecycleCreateRequest() provider.Request {
	return provider.Request{
		OperationID: "ccp-kind-lifecycle-create", TenantID: "tenant-a",
		WorkloadID: "workload-a", ProviderID: "kueue",
		IdempotencyKey:    "ccp-kind-create-key",
		RequestDigest:     "sha256:" + strings.Repeat("ab", 32),
		DesiredGeneration: 7, Kind: operation.Create,
	}
}

type lifecycleEvidence struct {
	CreatedUID         string                      `json:"created_uid"`
	CreatedResult      operation.ObservationResult `json:"created_result"`
	AfterAdapterReinit operation.ObservationResult `json:"after_adapter_reinit"`
	RejectedWrongUID   bool                        `json:"rejected_wrong_uid"`
	DeletedResult      operation.ObservationResult `json:"deleted_result"`
	AfterDeleteReinit  operation.ObservationResult `json:"after_delete_reinit"`
	ReleaseAttested    bool                        `json:"release_attested"`
}

func apiEndpoint(getenv func(string) string) (string, error) {
	host := getenv("KUBERNETES_SERVICE_HOST")
	port := getenv("KUBERNETES_SERVICE_PORT_HTTPS")
	p, err := strconv.Atoi(port)
	if err != nil || p < 1 || p > 65535 || host == "" ||
		host != strings.TrimSpace(host) || strings.ContainsAny(host, "/?@#\\") {
		return "", errors.New("invalid Kubernetes API service host or HTTPS port")
	}
	return "https://" + net.JoinHostPort(host, port), nil
}

func run(ctx context.Context, getenv func(string) string, out io.Writer) error {
	if !disposableAllowed(getenv) || getenv("POD_NAMESPACE") != smokeNamespace {
		return errors.New("requires disposable kind opt-in and isolated smoke namespace")
	}
	apiURL, err := apiEndpoint(getenv)
	if err != nil {
		return err
	}
	adapter, err := provider.NewServiceAccountKueueJobAdapter(
		apiURL, serviceAccountFiles+"/ca.crt", serviceAccountFiles+"/token", smokeNamespace,
	)
	if err != nil {
		return err
	}
	create := lifecycleCreateRequest()
	if _, err := adapter.Apply(ctx, create); err != nil {
		return fmt.Errorf("real create: %w", err)
	}
	present, err := adapter.Inspect(ctx, create)
	if err != nil || present.Result != operation.ResourcePresent || present.ResourceUID == "" {
		return fmt.Errorf("created Job not pinned: %+v: %w", present, err)
	}
	// A newly started adapter has no persisted UID receipt. The matching Job
	// MUST remain unresolved, not be adopted from its annotations.
	restarted, err := provider.NewServiceAccountKueueJobAdapter(
		apiURL, serviceAccountFiles+"/ca.crt", serviceAccountFiles+"/token", smokeNamespace,
	)
	if err != nil {
		return err
	}
	unbound, err := restarted.Inspect(ctx, create)
	if err != nil || unbound.Result != operation.OperationInProgress ||
		unbound.ResourceID != "" || unbound.ResourceUID != "" {
		return fmt.Errorf("restart adopted unbound Job UID: %+v: %w", unbound, err)
	}
	if _, err := restarted.Apply(ctx, create); !errors.Is(err, provider.ErrAcknowledgementLost) {
		return fmt.Errorf("restart replay improperly acknowledged: %v", err)
	}

	remove := create
	remove.Kind = operation.Delete
	remove.OperationID = "ccp-kind-lifecycle-delete"
	remove.IdempotencyKey = "ccp-kind-delete-key"
	remove.TargetResourceID = present.ResourceID
	remove.TargetResourceUID = present.ResourceUID

	wrong := remove
	wrong.TargetResourceUID = "not-the-original-job-uid"
	if _, err := adapter.Apply(ctx, wrong); !errors.Is(err, provider.ErrOperationConflict) {
		return fmt.Errorf("wrong UID delete was not rejected: %v", err)
	}
	unchanged, err := adapter.Inspect(ctx, create)
	if err != nil || unchanged.Result != operation.ResourcePresent ||
		unchanged.ResourceUID != present.ResourceUID {
		return fmt.Errorf("wrong UID altered Job: %+v: %w", unchanged, err)
	}
	if _, err := adapter.Apply(ctx, remove); err != nil {
		return fmt.Errorf("UID-fenced DELETE failed: %w", err)
	}
	var gone operation.Observation
	for {
		gone, err = adapter.Inspect(ctx, remove)
		if err != nil {
			return fmt.Errorf("inspect DELETE: %w", err)
		}
		if gone.Result == operation.TargetAbsent {
			break
		}
		if gone.Result != operation.ObservationConflict || gone.ReasonCode != "DELETE_TARGET_STILL_PRESENT" {
			return fmt.Errorf("unproven delete result: %+v", gone)
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(200 * time.Millisecond):
		}
	}
	if gone.TargetResourceUID != present.ResourceUID {
		return errors.New("observed delete UID mismatch")
	}
	postRestart, err := provider.NewServiceAccountKueueJobAdapter(
		apiURL, serviceAccountFiles+"/ca.crt", serviceAccountFiles+"/token", smokeNamespace,
	)
	if err != nil {
		return err
	}
	unintended, err := postRestart.Inspect(ctx, remove)
	if err != nil || unintended.Result != operation.ObservationConflict ||
		unintended.ReasonCode != "MISSING_AUTHORITATIVE_OPERATION" {
		return fmt.Errorf("restart treated 404 as durable delete: %+v: %w", unintended, err)
	}
	return json.NewEncoder(out).Encode(lifecycleEvidence{
		CreatedUID: present.ResourceUID, CreatedResult: present.Result,
		AfterAdapterReinit: unbound.Result, RejectedWrongUID: true,
		DeletedResult: gone.Result, AfterDeleteReinit: unintended.Result,
		ReleaseAttested: false,
	})
}

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	bounded, cancel := context.WithTimeout(ctx, 90*time.Second)
	defer cancel()
	if err := run(bounded, os.Getenv, os.Stdout); err != nil {
		fmt.Fprintln(os.Stderr, "kueue-provider-smoke:", err)
		os.Exit(1)
	}
}
