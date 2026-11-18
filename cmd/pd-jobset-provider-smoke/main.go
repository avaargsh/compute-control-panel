// pd-jobset-provider-smoke exercises the actual suspended/AdmissionGatedBy
// Prefill+Decode JobSet writer over Kubernetes ServiceAccount HTTPS in a
// disposable Kind cluster. This is not production authorization, a durable
// UID journal or physical GPU readiness evidence.
package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"os"
	"strings"
	"time"

	"github.com/avaargsh/compute-control-panel/internal/operation"
	"github.com/avaargsh/compute-control-panel/internal/provider"
)

const (
	namespace = "ccp-pd-paired-smoke"
	files     = "/var/run/secrets/kubernetes.io/serviceaccount"
)

func run(ctx context.Context) error {
	if os.Getenv("CCP_DISPOSABLE_KIND_SMOKE") != "1" ||
		os.Getenv("POD_NAMESPACE") != namespace {
		return errors.New("disposable JobSet cluster and namespace opt-in required")
	}
	host, port := os.Getenv("KUBERNETES_SERVICE_HOST"), os.Getenv("KUBERNETES_SERVICE_PORT_HTTPS")
	if host == "" || port == "" || strings.ContainsAny(host, "/\\?@# ") ||
		strings.ContainsAny(port, "/\\?@# ") {
		return errors.New("untrusted Kubernetes endpoint")
	}
	api := "https://" + net.JoinHostPort(host, port)
	id := strings.Repeat("c", 64)
	plan := provider.PDJobSetPlan{
		IntentID: id, TenantID: "tenant-a", WorkloadID: "workload-pd-kind",
		ProviderID: "pd-provider", Generation: 2,
		PrefillReplicas: 1, DecodeReplicas: 1,
		PrefillGPUsPerReplica: 1, DecodeGPUsPerReplica: 1,
		ResourceClaimTemplateName: "pd-gpu",
	}
	req := provider.Request{
		OperationID: "pd-operation-" + id,
		TenantID:    plan.TenantID, WorkloadID: plan.WorkloadID,
		ProviderID: plan.ProviderID, DesiredGeneration: plan.Generation,
		IdempotencyKey: "pd-create-" + id,
		RequestDigest:  "sha256:" + id, Kind: operation.Create,
	}
	pins := provider.NewMemoryKueueJobBindingStore()
	mkAdapter := func(bindings provider.KueueJobBindingStore) (*provider.PDJobSetAdapter, error) {
		return provider.NewServiceAccountPDJobSetAdapterWithBindings(
			api, files+"/ca.crt", files+"/token", namespace, namespace, bindings, plan)
	}
	writer, err := mkAdapter(pins)
	if err != nil {
		return fmt.Errorf("create authenticated test-only writer: %w", err)
	}
	if _, err := writer.Apply(ctx, req); err != nil {
		return fmt.Errorf("actual Kubernetes JobSet POST/UID pin: %w", err)
	}
	original, err := writer.Inspect(ctx, req)
	if err != nil || original.Result != operation.ResourcePresent ||
		original.ResourceUID == "" || original.ResourceVersion == "" {
		return fmt.Errorf("created JobSet lacks exact trusted UID: %+v %v", original, err)
	}
	// Replay and same-store constructor re-initialization must preserve the
	// actual UID without creating a second JobSet.
	if _, err := writer.Apply(ctx, req); err != nil {
		return fmt.Errorf("idempotent POST replay: %w", err)
	}
	restarted, err := mkAdapter(pins)
	if err != nil {
		return err
	}
	if _, err := restarted.Apply(ctx, req); err != nil {
		return fmt.Errorf("same-store restart replay: %w", err)
	}
	bound, err := restarted.Inspect(ctx, req)
	if err != nil || bound.Result != operation.ResourcePresent ||
		bound.ResourceUID != original.ResourceUID {
		return fmt.Errorf("reinitialized writer lost UID: %+v %v", bound, err)
	}
	// A real Kubernetes JobSet with the right metadata is NOT proof of a
	// CREATE acknowledgement if the durable UID receipt was lost.
	unbound, err := mkAdapter(provider.NewMemoryKueueJobBindingStore())
	if err != nil {
		return err
	}
	unknown, err := unbound.Inspect(ctx, req)
	if err != nil || unknown.Result != operation.OperationInProgress ||
		unknown.ResourceUID != "" || unknown.ResourceID != "" {
		return fmt.Errorf("unbound JobSet incorrectly adopted: %+v %v", unknown, err)
	}
	if _, err := unbound.Apply(ctx, req); !errors.Is(err, provider.ErrAcknowledgementLost) {
		return fmt.Errorf("unbound CREATE replay was not UNKNOWN: %v", err)
	}
	return json.NewEncoder(os.Stdout).Encode(map[string]any{
		"mode":                      "DISPOSABLE_REAL_JOBSET_API",
		"uid":                       original.ResourceUID,
		"resource_id":               original.ResourceID,
		"provider_create_pinned":    true,
		"same_uid_on_restart":       true,
		"unbound_state":             string(unknown.Result),
		"lost_ack_requires_inspect": true,
		"one_gpu_per_replica":       true,
		"physical_gpu_verified":     false,
		"driver_prepared":           false,
		"allocation_ready":          false,
		"released":                  false,
		"cost_final":                false,
	})
}

func main() {
	ctx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
	defer cancel()
	if err := run(ctx); err != nil {
		fmt.Fprintln(os.Stderr, "FAIL: PD JobSet actual-controller provider writer:", err)
		os.Exit(1)
	}
}
