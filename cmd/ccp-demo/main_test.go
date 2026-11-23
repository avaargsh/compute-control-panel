package main

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/avaargsh/compute-control-panel/internal/observation"
)

func TestDemoHTTPProvidesReadOnlySharedObservationSchema(t *testing.T) {
	now := time.Date(2026, 11, 21, 9, 0, 0, 0, time.UTC)
	handler, err := demoHandler(func() time.Time { return now })
	if err != nil {
		t.Fatal(err)
	}
	w := httptest.NewRecorder()
	handler.ServeHTTP(w, httptest.NewRequest(http.MethodGet, "/api/v1/observations", nil))
	if w.Code != http.StatusOK || w.Header().Get("Cache-Control") != "no-store" ||
		!strings.Contains(w.Header().Get("Content-Type"), "application/json") {
		t.Fatalf("invalid response code/headers: status=%d headers=%v", w.Code, w.Header())
	}
	var payload struct {
		DataMode    observation.Mode   `json:"data_mode"`
		GeneratedAt time.Time          `json:"generated_at"`
		Items       []observation.View `json:"items"`
	}
	if err := json.Unmarshal(w.Body.Bytes(), &payload); err != nil {
		t.Fatal(err)
	}
	if payload.DataMode != observation.ModeDemo || !payload.GeneratedAt.Equal(now) || len(payload.Items) != 5 {
		t.Fatalf("wrong demo contract: %+v", payload)
	}
	gpuResources := 0
	for _, item := range payload.Items {
		if item.Source != "synthetic/demo" || item.EvidenceRef != "" {
			t.Fatalf("synthetic data shown as live evidence: %+v", item)
		}
		if item.Resource.Kind == "NodeGPUResource" {
			gpuResources++
			if item.Status != observation.StatusUnknown || item.Quantity == nil ||
				item.Resource.UID != "" || item.ReasonCode != "NODE_ALLOCATABLE_NOT_FREE_CAPACITY" {
				t.Fatalf("GPU resource falsely reported free/verified: %+v", item)
			}
		}
	}
	if gpuResources != 2 {
		t.Fatalf("expected two synthetic GPU resource quantity records, got %d", gpuResources)
	}
	if strings.Contains(w.Body.String(), "demo-tenant") || strings.Contains(w.Body.String(), "physical_gpu_uid") {
		t.Fatal("internal scope or physical GPU credentials leaked")
	}
}

func TestDemoConsoleOnlyServesStaticAndReadOnlyRoutes(t *testing.T) {
	handler, err := demoHandler(time.Now)
	if err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct {
		method, path string
		want         int
	}{
		{"GET", "/", 200},
		{"GET", "/assets/app.js", 200},
		{"GET", "/assets/styles.css", 200},
		{"GET", "/api/v1/observations", 200},
		{"POST", "/api/v1/observations", 405},
		{"GET", "/api/v1/observations?project_id=other", 400},
		{"GET", "/api/v1/observations?", 400},
		{"GET", "/?", 404},
		{"GET", "/api/v1/workloads", 404},
		{"GET", "/admin", 404},
	} {
		t.Run(tc.method+" "+tc.path, func(t *testing.T) {
			w := httptest.NewRecorder()
			handler.ServeHTTP(w, httptest.NewRequest(tc.method, tc.path, nil))
			if w.Code != tc.want {
				t.Fatalf("got %d want %d body=%s", w.Code, tc.want, w.Body.String())
			}
			if w.Header().Get("Cache-Control") != "no-store" {
				t.Fatal("demo response should never be cached")
			}
		})
	}
}

func TestDemoConsoleMarkupAlwaysLabelsSyntheticMode(t *testing.T) {
	handler, err := demoHandler(time.Now)
	if err != nil {
		t.Fatal(err)
	}
	w := httptest.NewRecorder()
	handler.ServeHTTP(w, httptest.NewRequest("GET", "/", nil))
	for _, word := range []string{"DEMO", "模拟数据", "Dashboard", "Clusters", "GPUs"} {
		if !strings.Contains(w.Body.String(), word) {
			t.Fatalf("missing demo UX label %q", word)
		}
	}
}

func TestDemoServerRefusesPublicListenAddresses(t *testing.T) {
	for _, addr := range []string{"", ":8081", "0.0.0.0:8081", "[::]:8081", "localhost:8081", "192.168.1.1:8081", "127.0.0.1:notaport"} {
		if err := validateDemoListenAddr(addr); err == nil {
			t.Errorf("insecure or invalid bind address accepted: %q", addr)
		}
	}
	for _, addr := range []string{"127.0.0.1:8081", "127.0.0.1:0"} {
		if err := validateDemoListenAddr(addr); err != nil {
			t.Errorf("valid loopback bind rejected: %q %v", addr, err)
		}
	}
}

func TestDemoHTTPRequiresClock(t *testing.T) {
	if _, err := demoHandler(nil); err == nil {
		t.Fatal("missing trusted clock accepted")
	}
}
