package provider

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/avaargsh/compute-control-panel/internal/operation"
)

// The queued writer is opt-in: the legacy suspended writer must stay inert.
// Kueue owns the suspend transition and may change it between reads.
func TestQueuedKueueJobIsUIDPinnedAcrossAdmissionSuspendTransitions(t *testing.T) {
	api := &kueueAPI{t: t, jobs: make(map[string]kubeObject)}
	server := httptest.NewTLSServer(http.HandlerFunc(api.serve))
	defer server.Close()
	client := server.Client()
	client.Timeout = 3 * time.Second
	adapter, err := NewQueuedKueueJobAdapterWithBindings(
		server.URL, "tenant-a", "ccp-smoke", client, NewMemoryKueueJobBindingStore(),
	)
	if err != nil {
		t.Fatal(err)
	}
	req := kueueAdapterRequest()
	if _, err := adapter.Apply(context.Background(), req); err != nil {
		t.Fatal(err)
	}
	if got := api.postCount(); got != 1 {
		t.Fatalf("expected exactly one Job POST, got %d", got)
	}
	var submitted kueueBatchJob
	for _, call := range api.snapshot() {
		if call.method == http.MethodPost {
			if err := json.Unmarshal(call.body, &submitted); err != nil {
				t.Fatal(err)
			}
		}
	}
	if submitted.Metadata.Labels["kueue.x-k8s.io/queue-name"] != "ccp-smoke" ||
		submitted.Metadata.Labels["kueue.x-k8s.io/priority-class"] != "" ||
		submitted.Spec.Suspend ||
		submitted.Spec.Template.Spec.Containers[0].Resources.Requests["cpu"] != "100m" ||
		submitted.Spec.Template.Spec.Containers[0].Resources.Requests["memory"] != "32Mi" {
		t.Fatalf("Job was not explicitly queued with bounded resource requests: %+v", submitted)
	}

	for _, suspended := range []bool{true, false, true} {
		api.mu.Lock()
		for name, job := range api.jobs {
			job.Spec.Suspend = &suspended
			api.jobs[name] = job
		}
		api.mu.Unlock()
		if _, err := adapter.Apply(context.Background(), req); err != nil {
			t.Fatalf("Kueue suspend=%t broke UID-pinned replay: %v", suspended, err)
		}
		observed, err := adapter.Inspect(context.Background(), req)
		if err != nil || observed.Result != operation.ResourcePresent || observed.ResourceUID != "job-uid-created" {
			t.Fatalf("Kueue suspend=%t was not inspected under pinned UID: %+v, %v", suspended, observed, err)
		}
	}
	if api.postCount() != 1 {
		t.Fatalf("replay created a second Job")
	}
	// Name reuse cannot move the durable CREATE receipt to a successor UID.
	api.setReplacement("successor-uid", "")
	observed, err := adapter.Inspect(context.Background(), req)
	if err != nil || observed.Result != operation.ObservationConflict {
		t.Fatalf("successor UID was adopted: %+v: %v", observed, err)
	}
	if _, err := adapter.Apply(context.Background(), req); !errors.Is(err, ErrOperationConflict) {
		t.Fatalf("successor UID replay was not rejected: %v", err)
	}
	api.setReplacement("", "")
	// A queue reassignment is not an allowed Kueue suspend transition.
	api.mu.Lock()
	for name, job := range api.jobs {
		job.Metadata.Labels["kueue.x-k8s.io/queue-name"] = "other-queue"
		api.jobs[name] = job
	}
	api.mu.Unlock()
	if _, err := adapter.Apply(context.Background(), req); !errors.Is(err, ErrOperationConflict) {
		t.Fatalf("reassigned LocalQueue was silently adopted: %v", err)
	}
}

func TestQueuedKueueJobRejectsInvalidLocalQueue(t *testing.T) {
	client := &http.Client{Timeout: time.Second}
	for _, queue := range []string{"", "Different/Namespace", "..", "team queue"} {
		if _, err := NewQueuedKueueJobAdapterWithBindings(
			"https://kubernetes.default.svc", "tenant-a", queue, client, NewMemoryKueueJobBindingStore(),
		); !errors.Is(err, ErrInvalidKueueObservation) {
			t.Fatalf("local queue %q accepted: %v", queue, err)
		}
	}
}

func TestQueuedServiceAccountAdapterRequiresDurableBindingStore(t *testing.T) {
	// The queued runtime path must not silently reuse the test-only
	// WithBindings constructor with a private memory store.
	api := "https://kubernetes.default.svc"
	ca, token, namespace, queue := "/not-read-before-dsn", "/not-read-before-dsn", "tenant-a", "ccp-smoke"
	if _, err := NewQueuedServiceAccountKueueJobAdapter(api, ca, token, namespace, queue, ""); !errors.Is(err, ErrKueueBindingDSNEmpty) {
		t.Fatalf("queued constructor did not refuse missing durable binding: %v", err)
	}
	if _, err := NewQueuedServiceAccountKueueJobAdapter(api, ca, token, namespace, "Bad/Queue", ""); !errors.Is(err, ErrInvalidKueueObservation) {
		t.Fatalf("invalid LocalQueue did not fail before contacting DB: %v", err)
	}
	unreachable := "postgres://ccp_kueue_job_binding@127.0.0.1:1/postgres?sslmode=disable&connect_timeout=1"
	if _, err := NewQueuedServiceAccountKueueJobAdapter(api, ca, token, namespace, queue, unreachable); !errors.Is(err, ErrKueueBindingUnavailable) {
		t.Fatalf("queued constructor accepted unreachable binding database: %v", err)
	}
}
