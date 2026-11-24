package main

import (
	"crypto/tls"
	"encoding/pem"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/avaargsh/compute-control-panel/internal/consoleauth"
	"github.com/avaargsh/compute-control-panel/internal/kubeinventory"
	"golang.org/x/crypto/bcrypt"
)

func TestLiveConsoleRequiresSessionAndRealSource(t *testing.T) {
	var calls atomic.Int32
	var denied atomic.Bool
	source := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		if r.Method != http.MethodGet || r.URL.Path != "/api/v1/nodes" || r.Header.Get("Authorization") != "Bearer fixture-token" {
			t.Error("unexpected inventory request")
			w.WriteHeader(http.StatusForbidden)
			return
		}
		if denied.Load() {
			w.WriteHeader(http.StatusForbidden)
			return
		}
		_, _ = w.Write([]byte(`{"metadata":{"resourceVersion":"42"},"items":[{"metadata":{"name":"node-live","uid":"uid-live","resourceVersion":"42"},"status":{"conditions":[{"type":"Ready","status":"True"}]}}]}`))
	}))
	defer source.Close()
	dir := t.TempDir()
	ca := filepath.Join(dir, "ca.pem")
	token := filepath.Join(dir, "token")
	if err := os.WriteFile(ca, pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: source.Certificate().Raw}), 0600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(token, []byte("fixture-token"), 0600); err != nil {
		t.Fatal(err)
	}
	hash, err := bcrypt.GenerateFromPassword([]byte("test-only-password"), bcrypt.DefaultCost)
	if err != nil {
		t.Fatal(err)
	}
	origin := "https://ccp.example.test"
	cfg := liveConfig{
		Auth: consoleauth.Config{
			Username: "admin", PasswordBcryptHash: hash,
			TenantID: "tenant-a", ProjectID: "project-a", Origin: origin,
			SessionTTL: time.Hour, Now: time.Now,
		},
		Inventory: kubeinventory.Config{
			APIServer: source.URL, CAFile: ca, TokenFile: token, ClusterID: "cluster-a",
			TenantID: "tenant-a", ProjectID: "project-a", ResourceNames: []string{"nvidia.com/gpu"},
			ObservationTTL: time.Minute, Timeout: 3 * time.Second,
		},
	}
	h, err := newLiveHandler(cfg)
	if err != nil {
		t.Fatal(err)
	}
	request := func(method, path, body string, cookie *http.Cookie) *httptest.ResponseRecorder {
		req := httptest.NewRequest(method, origin+path, strings.NewReader(body))
		req.TLS = &tls.ConnectionState{}
		if method == http.MethodPost {
			req.Header.Set("Origin", origin)
			req.Header.Set("Content-Type", "application/json")
		}
		if cookie != nil {
			req.AddCookie(cookie)
		}
		w := httptest.NewRecorder()
		h.ServeHTTP(w, req)
		return w
	}
	if w := request(http.MethodGet, "/api/v1/observations", "", nil); w.Code != http.StatusUnauthorized || calls.Load() != 0 {
		t.Fatalf("anonymous request accessed source: status=%d calls=%d", w.Code, calls.Load())
	}
	login := request(http.MethodPost, "/api/v1/console/login", `{"username":"admin","password":"test-only-password"}`, nil)
	if login.Code != http.StatusOK || len(login.Result().Cookies()) != 1 {
		t.Fatalf("login failed: %d", login.Code)
	}
	cookie := login.Result().Cookies()[0]
	if w := request(http.MethodGet, "/api/v1/observations", "", cookie); w.Code != http.StatusOK ||
		!strings.Contains(w.Body.String(), "node-live") || !strings.Contains(w.Body.String(), `"data_mode":"live"`) ||
		w.Header().Get("Cache-Control") != "no-store" {
		t.Fatalf("live read failed: status=%d body=%s", w.Code, w.Body.String())
	}
	denied.Store(true)
	if w := request(http.MethodGet, "/api/v1/observations", "", cookie); w.Code != http.StatusBadGateway ||
		strings.Contains(w.Body.String(), "node-live") || strings.Contains(w.Body.String(), `"data_mode":"demo"`) {
		t.Fatalf("source denial leaked data or fell back to demo: status=%d body=%s", w.Code, w.Body.String())
	}
	cfg.Inventory.ProjectID = "other-project"
	if _, err := newLiveHandler(cfg); err == nil {
		t.Fatal("mismatched source and session scope accepted")
	}
}

func TestConsoleListenRequiresExplicitLoopbackTLS(t *testing.T) {
	for _, addr := range []string{":8443", "0.0.0.0:8443", "localhost:8443", "127.0.0.1:0", "127.0.0.1:65536"} {
		if err := validateListen(addr); err == nil {
			t.Fatalf("unsafe bind accepted: %s", addr)
		}
	}
	if err := validateListen("127.0.0.1:8443"); err != nil {
		t.Fatal(err)
	}
}
