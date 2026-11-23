package kubeinventory

import (
	"context"
	"crypto/x509"
	"encoding/pem"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/avaargsh/compute-control-panel/internal/observation"
)

func fixtureReader(t *testing.T, handler http.HandlerFunc) (*Reader, string) {
	t.Helper()
	server := httptest.NewTLSServer(handler)
	t.Cleanup(server.Close)
	certFile := filepath.Join(t.TempDir(), "kube-ca.pem")
	if err := os.WriteFile(certFile, pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: server.Certificate().Raw}), 0600); err != nil {
		t.Fatal(err)
	}
	tokenFile := filepath.Join(t.TempDir(), "sa-token")
	if err := os.WriteFile(tokenFile, []byte("node-reader-token\n"), 0600); err != nil {
		t.Fatal(err)
	}
	now := time.Date(2026, 11, 21, 8, 0, 0, 0, time.UTC)
	reader, err := NewReader(Config{
		APIServer: server.URL, CAFile: certFile, TokenFile: tokenFile,
		ClusterID: "cluster-a", TenantID: "tenant-a", ProjectID: "project-a",
		ResourceNames:  []string{"nvidia.com/gpu", "huawei.com/Ascend910"},
		ObservationTTL: time.Minute, Timeout: 3 * time.Second,
		Now: func() time.Time { return now },
	})
	if err != nil {
		t.Fatal(err)
	}
	// Verify that the TLS authority used by the test is a concrete X.509
	// certificate, not an insecure client transport.
	if _, err := x509.ParseCertificate(server.Certificate().Raw); err != nil {
		t.Fatal(err)
	}
	return reader, tokenFile
}

func TestReadOnlyKubernetesNodesWithGPUResourceQuantities(t *testing.T) {
	calls := 0
	reader, _ := fixtureReader(t, func(w http.ResponseWriter, r *http.Request) {
		calls++
		if r.Method != http.MethodGet || r.URL.Path != "/api/v1/nodes" ||
			r.URL.Query().Get("limit") != "100" ||
			r.Header.Get("Authorization") != "Bearer node-reader-token" {
			t.Errorf("unexpected Kubernetes request: method=%s path=%s query=%q auth=%q", r.Method, r.URL.Path, r.URL.RawQuery, r.Header.Get("Authorization"))
			w.WriteHeader(http.StatusUnauthorized)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		switch r.URL.Query().Get("continue") {
		case "":
			_, _ = fmt.Fprint(w, `{"metadata":{"continue":"page-2","resourceVersion":"777"},"items":[{"metadata":{"name":"worker-a","uid":"node-uid-a","resourceVersion":"700"},"status":{"capacity":{"nvidia.com/gpu":"4"},"allocatable":{"nvidia.com/gpu":"3"},"conditions":[{"type":"Ready","status":"True"}]}}]}`)
		case "page-2":
			_, _ = fmt.Fprint(w, `{"metadata":{"continue":"","resourceVersion":"777"},"items":[{"metadata":{"name":"worker-b","uid":"node-uid-b","resourceVersion":"701"},"status":{"capacity":{"huawei.com/Ascend910":"8"},"allocatable":{"huawei.com/Ascend910":"8"},"conditions":[{"type":"Ready","status":"False"}]}}]}`)
		default:
			t.Errorf("unexpected continue token")
			w.WriteHeader(http.StatusBadRequest)
		}
	})
	snapshot, err := reader.ListObservations(context.Background(), "tenant-a", []string{"project-a"})
	if err != nil {
		t.Fatal(err)
	}
	if calls != 2 || snapshot.DataMode != observation.ModeLive || len(snapshot.Items) != 4 {
		t.Fatalf("calls=%d mode=%s items=%+v", calls, snapshot.DataMode, snapshot.Items)
	}
	var nodeHealthy, nodeFailed, nvidiaGPU, ascendGPU bool
	for _, record := range snapshot.Items {
		if record.TenantID != "tenant-a" || record.ProjectID != "project-a" ||
			record.Source == "" || record.SourceRevision == "" || !record.ExpiresAt.After(record.ObservedAt) {
			t.Fatalf("bad provenance/scope: %+v", record)
		}
		view, err := observation.Project(record, record.ObservedAt.Add(time.Second))
		if err != nil {
			t.Fatal(err)
		}
		switch record.Resource.Kind + ":" + record.Resource.Name {
		case "Node:worker-a":
			nodeHealthy = view.Status == observation.StatusAvailable && record.Resource.UID == "node-uid-a"
		case "Node:worker-b":
			nodeFailed = view.Status == observation.StatusUnavailable
		case "NodeGPUResource:worker-a/nvidia.com/gpu":
			nvidiaGPU = record.Quantity != nil && record.Quantity.Capacity == "4" &&
				record.Quantity.Allocatable == "3" && view.Status == observation.StatusUnknown &&
				record.Resource.UID == "" && view.ReasonCode == "NODE_ALLOCATABLE_NOT_FREE_CAPACITY"
		case "NodeGPUResource:worker-b/huawei.com/Ascend910":
			ascendGPU = record.Quantity != nil && record.Quantity.ResourceName == "huawei.com/Ascend910" &&
				record.Quantity.Allocatable == "8" && view.Status == observation.StatusUnknown
		}
	}
	if !(nodeHealthy && nodeFailed && nvidiaGPU && ascendGPU) {
		t.Fatalf("unexpected node/GPU facts: nodeHealthy=%v nodeFailed=%v nvidia=%v ascend=%v", nodeHealthy, nodeFailed, nvidiaGPU, ascendGPU)
	}
}

func TestReaderFiltersByAuthenticatedScopeBeforeCallingKubernetes(t *testing.T) {
	calls := 0
	reader, _ := fixtureReader(t, func(w http.ResponseWriter, r *http.Request) {
		calls++
		w.WriteHeader(http.StatusInternalServerError)
	})
	if _, err := reader.ListObservations(context.Background(), "other-tenant", []string{"project-a"}); !errors.Is(err, ErrScope) {
		t.Fatalf("expected tenant denial, got %v", err)
	}
	got, err := reader.ListObservations(context.Background(), "tenant-a", []string{"other-project"})
	if err != nil || got.DataMode != observation.ModeLive || len(got.Items) != 0 {
		t.Fatalf("ungranted project should not reveal cluster: got=%+v err=%v", got, err)
	}
	if calls != 0 {
		t.Fatalf("Kubernetes API accessed for denied scope: %d", calls)
	}
}

func TestReaderRequiresVerifiedTLSConfiguration(t *testing.T) {
	server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()
	temp := t.TempDir()
	caFile := filepath.Join(temp, "valid-ca.pem")
	tokenFile := filepath.Join(temp, "valid-token")
	if err := os.WriteFile(caFile, pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: server.Certificate().Raw}), 0600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(tokenFile, []byte("test-token"), 0600); err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct {
		name string
		mod  func(*Config)
	}{
		{"http forbidden", func(c *Config) { c.APIServer = "http://localhost:6443" }},
		{"userinfo forbidden", func(c *Config) { c.APIServer = "https://user:pass@localhost:6443" }},
		{"query forbidden", func(c *Config) { c.APIServer = "https://localhost:6443/api?x=y" }},
		{"no CA file", func(c *Config) { c.CAFile = "" }},
		{"no token file", func(c *Config) { c.TokenFile = "" }},
		{"no project", func(c *Config) { c.ProjectID = "" }},
		{"no GPU resource", func(c *Config) { c.ResourceNames = nil }},
		{"bad resource name", func(c *Config) { c.ResourceNames = []string{"../secret"} }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			cfg := Config{APIServer: server.URL, CAFile: caFile, TokenFile: tokenFile, ClusterID: "c", TenantID: "t", ProjectID: "p", ResourceNames: []string{"nvidia.com/gpu"}, ObservationTTL: time.Minute}
			tc.mod(&cfg)
			if _, err := NewReader(cfg); err == nil {
				t.Fatal("insecure/invalid config accepted")
			}
		})
	}
}

func TestKubernetesRBACFailureIsSanitized(t *testing.T) {
	reader, tokenFile := fixtureReader(t, func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusForbidden)
		_, _ = fmt.Fprint(w, "internal cluster secret: RBAC details")
	})
	_, err := reader.ListObservations(context.Background(), "tenant-a", []string{"project-a"})
	if !errors.Is(err, ErrForbidden) || strings.Contains(err.Error(), "secret") {
		t.Fatalf("403 not categorized/sanitized: %v", err)
	}
	if err := os.WriteFile(tokenFile, []byte("\n"), 0600); err != nil {
		t.Fatal(err)
	}
	_, err = reader.ListObservations(context.Background(), "tenant-a", []string{"project-a"})
	if !errors.Is(err, ErrCredentials) {
		t.Fatalf("empty or rotated-away token not denied: %v", err)
	}
}

func TestPaginationRepetitionFailsClosed(t *testing.T) {
	reader, _ := fixtureReader(t, func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = fmt.Fprint(w, `{"metadata":{"continue":"repeat-me","resourceVersion":"1"},"items":[]}`)
	})
	_, err := reader.ListObservations(context.Background(), "tenant-a", []string{"project-a"})
	if !errors.Is(err, ErrInvalidResponse) {
		t.Fatalf("repeated continuation should fail: %v", err)
	}
}

func TestInvalidNodeUIDOrGPUQuantityFailsClosed(t *testing.T) {
	for _, payload := range []string{
		`{"metadata":{"resourceVersion":"1"},"items":[{"metadata":{"name":"a","resourceVersion":"1"}}]}`,
		`{"metadata":{"resourceVersion":"1"},"items":[{"metadata":{"name":"a","uid":"u1","resourceVersion":"1"},"status":{"capacity":{"nvidia.com/gpu":"not-a-number"},"allocatable":{"nvidia.com/gpu":"1"}}}]}`,
		`{"metadata":{"resourceVersion":"1"},"items":[{"metadata":{"name":"a","uid":"u1","resourceVersion":"1"},"status":{"capacity":{"nvidia.com/gpu":"1"},"allocatable":{"nvidia.com/gpu":"2"}}}]}`,
	} {
		reader, _ := fixtureReader(t, func(w http.ResponseWriter, r *http.Request) { _, _ = fmt.Fprint(w, payload) })
		_, err := reader.ListObservations(context.Background(), "tenant-a", []string{"project-a"})
		if !errors.Is(err, ErrInvalidResponse) {
			t.Fatalf("malformed input accepted: payload=%s err=%v", payload, err)
		}
	}
}
