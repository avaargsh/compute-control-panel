// kueue-provider-smoke is a disposable-kind-only executable that invokes
// the actual KueueJobAdapter against kube-apiserver with a scoped ServiceAccount.
// With no arguments it uses a private in-memory binding store. The durable
// phases use PostgresKueueJobBindingStore when CCP_KUEUE_BINDING_DSN is set,
// as separate processes. This must never be used to infer production GPU
// readiness, quota release, or Allocation RELEASED.
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
	BoundReinitCreate  operation.ObservationResult `json:"bound_adapter_reinit_create"`
	RejectedWrongUID   bool                        `json:"rejected_wrong_uid"`
	DeletedResult      operation.ObservationResult `json:"deleted_result"`
	AfterDeleteReinit  operation.ObservationResult `json:"after_delete_reinit"`
	BoundReinitDelete  operation.ObservationResult `json:"bound_adapter_reinit_delete"`
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

// stableJobObservation retries only a temporarily inconsistent Job/Kueue/Pod
// read during initial controller convergence. Every iteration performs fresh
// UID-pinned API reads. Identity conflicts, unauthorized API responses and
// changed Job specs are terminal; this never promotes an unproven snapshot.
func stableJobObservation(
	ctx context.Context, observe func(context.Context) (operation.Observation, error),
) (operation.Observation, error) {
	bounded, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	for {
		snapshot, err := observe(bounded)
		if err != nil || snapshot.Result != operation.ObservationConflict ||
			snapshot.ReasonCode != "KUEUE_OBSERVATION_UNPROVEN" {
			return snapshot, err
		}
		select {
		case <-bounded.Done():
			return snapshot, fmt.Errorf("UID-pinned Kubernetes snapshot did not stabilize: %w", bounded.Err())
		case <-time.After(100 * time.Millisecond):
		}
	}
}

func run(ctx context.Context, getenv func(string) string, out io.Writer) error {
	if err := requireDisposableSmoke(getenv); err != nil {
		return err
	}
	apiURL, err := apiEndpoint(getenv)
	if err != nil {
		return err
	}
	bindings := provider.NewMemoryKueueJobBindingStore()
	adapter, err := provider.NewServiceAccountKueueJobAdapterWithBindings(
		apiURL, serviceAccountFiles+"/ca.crt", serviceAccountFiles+"/token", smokeNamespace, bindings,
	)
	if err != nil {
		return err
	}
	create := lifecycleCreateRequest()
	if _, err := adapter.Apply(ctx, create); err != nil {
		return fmt.Errorf("real create: %w", err)
	}
	present, err := stableJobObservation(ctx, func(ctx context.Context) (operation.Observation, error) {
		return adapter.Inspect(ctx, create)
	})
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

	// Re-create only the adapter object with the caller-owned binding store.
	// This does NOT simulate an OS process restart or persistent storage.
	bound, err := provider.NewServiceAccountKueueJobAdapterWithBindings(
		apiURL, serviceAccountFiles+"/ca.crt", serviceAccountFiles+"/token", smokeNamespace, bindings,
	)
	if err != nil {
		return err
	}
	if _, err := bound.Apply(ctx, create); err != nil {
		return fmt.Errorf("trusted pin replay failed: %w", err)
	}
	boundPresent, err := stableJobObservation(ctx, func(ctx context.Context) (operation.Observation, error) {
		return bound.Inspect(ctx, create)
	})
	if err != nil || boundPresent.Result != operation.ResourcePresent ||
		boundPresent.ResourceUID != present.ResourceUID {
		return fmt.Errorf("bound adapter adopted wrong UID: %+v: %w", boundPresent, err)
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
	unchanged, err := stableJobObservation(ctx, func(ctx context.Context) (operation.Observation, error) {
		return adapter.Inspect(ctx, create)
	})
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
	boundAgain, err := provider.NewServiceAccountKueueJobAdapterWithBindings(
		apiURL, serviceAccountFiles+"/ca.crt", serviceAccountFiles+"/token", smokeNamespace, bindings,
	)
	if err != nil {
		return err
	}
	boundGone, err := boundAgain.Inspect(ctx, remove)
	if err != nil || boundGone.Result != operation.TargetAbsent ||
		boundGone.TargetResourceUID != present.ResourceUID {
		return fmt.Errorf("bound tombstone not carried across adapters: %+v: %w", boundGone, err)
	}
	if _, err := boundAgain.Apply(ctx, remove); err != nil {
		return fmt.Errorf("bound delete replay was not idempotent: %w", err)
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
		AfterAdapterReinit: unbound.Result, BoundReinitCreate: boundPresent.Result,
		RejectedWrongUID: true, DeletedResult: gone.Result,
		AfterDeleteReinit: unintended.Result, BoundReinitDelete: boundGone.Result,
		ReleaseAttested: false,
	})
}

func main() {
	if len(os.Args) > 1 {
		// Durable phases are separate processes. A --hold phase blocks until
		// SIGKILL, so this path does not use the memory-smoke deadline.
		if err := runDurable(os.Args[1:], os.Getenv, os.Stdout); err != nil {
			fmt.Fprintln(os.Stderr, "kueue-provider-smoke:", err)
			os.Exit(1)
		}
		return
	}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	bounded, cancel := context.WithTimeout(ctx, 90*time.Second)
	defer cancel()
	if err := run(bounded, os.Getenv, os.Stdout); err != nil {
		fmt.Fprintln(os.Stderr, "kueue-provider-smoke:", err)
		os.Exit(1)
	}
}
