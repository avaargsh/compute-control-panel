package main

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"encoding/pem"
	"io"
	"net"
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

func TestConsoleTLSListenerSessionLifecycle(t *testing.T) {
	var calls atomic.Int32
	source := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		if r.Method != http.MethodGet || r.URL.Path != "/api/v1/nodes" || r.Header.Get("Authorization") != "Bearer fixture-token" {
			w.WriteHeader(http.StatusForbidden)
			return
		}
		_, _ = io.WriteString(w, `{"metadata":{"resourceVersion":"42"},"items":[{"metadata":{"name":"node-live","uid":"uid-live","resourceVersion":"42"},"status":{"conditions":[{"type":"Ready","status":"True"}]}}]}`)
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
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer ln.Close()
	origin := "https://" + ln.Addr().String()
	h, err := newLiveHandler(liveConfig{
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
	})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- serveConsole(ctx, ln, source.TLS.Certificates[0], h) }()
	defer func() {
		cancel()
		select {
		case err := <-done:
			if err != nil {
				t.Errorf("console shutdown: %v", err)
			}
		case <-time.After(10 * time.Second):
			t.Error("console did not stop")
		}
	}()
	roots := x509.NewCertPool()
	roots.AddCert(source.Certificate())
	legacyTransport := &http.Transport{TLSClientConfig: &tls.Config{RootCAs: roots, MaxVersion: tls.VersionTLS12}}
	defer legacyTransport.CloseIdleConnections()
	legacy := &http.Client{Transport: legacyTransport, Timeout: 5 * time.Second}
	if resp, err := legacy.Get(origin + "/api/v1/observations"); err == nil {
		resp.Body.Close()
		t.Fatal("console accepted TLS 1.2")
	}
	transport := &http.Transport{TLSClientConfig: &tls.Config{RootCAs: roots, MinVersion: tls.VersionTLS13}}
	defer transport.CloseIdleConnections()
	client := &http.Client{Transport: transport, Timeout: 5 * time.Second}
	request := func(method, path, body, requestOrigin, csrf string, cookie *http.Cookie) (*http.Response, string) {
		t.Helper()
		req, err := http.NewRequest(method, origin+path, strings.NewReader(body))
		if err != nil {
			t.Fatal(err)
		}
		if requestOrigin != "" {
			req.Header.Set("Origin", requestOrigin)
		}
		if body != "" {
			req.Header.Set("Content-Type", "application/json")
		}
		if csrf != "" {
			req.Header.Set("X-CCP-CSRF", csrf)
		}
		if cookie != nil {
			req.AddCookie(cookie)
		}
		resp, err := client.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		defer resp.Body.Close()
		data, err := io.ReadAll(resp.Body)
		if err != nil {
			t.Fatal(err)
		}
		if resp.TLS == nil || resp.TLS.Version != tls.VersionTLS13 {
			t.Fatal("response did not use TLS 1.3")
		}
		return resp, string(data)
	}
	if r, _ := request("GET", "/api/v1/observations", "", "", "", nil); r.StatusCode != http.StatusUnauthorized || calls.Load() != 0 {
		t.Fatalf("anonymous upstream read: status=%d calls=%d", r.StatusCode, calls.Load())
	}
	loginBody := `{"username":"admin","password":"test-only-password"}`
	if r, _ := request("POST", "/api/v1/console/login", loginBody, "https://foreign.invalid", "", nil); r.StatusCode != http.StatusForbidden {
		t.Fatalf("foreign origin accepted: %d", r.StatusCode)
	}
	login, _ := request("POST", "/api/v1/console/login", loginBody, origin, "", nil)
	if login.StatusCode != http.StatusOK || len(login.Cookies()) != 1 {
		t.Fatalf("login rejected: %d", login.StatusCode)
	}
	cookie := login.Cookies()[0]
	if !cookie.Secure || !cookie.HttpOnly || cookie.SameSite != http.SameSiteStrictMode || cookie.Name != "__Host-ccp_session" {
		t.Fatal("insecure session cookie")
	}
	session, body := request("GET", "/api/v1/console/session", "", "", "", cookie)
	var identity struct {
		ProjectID string `json:"project_id"`
		CSRFToken string `json:"csrf_token"`
	}
	if session.StatusCode != http.StatusOK || json.Unmarshal([]byte(body), &identity) != nil || identity.ProjectID != "project-a" || identity.CSRFToken == "" {
		t.Fatal("session scope or CSRF token missing")
	}
	read, body := request("GET", "/api/v1/observations", "", "", "", cookie)
	if read.StatusCode != http.StatusOK || !strings.Contains(body, "node-live") || !strings.Contains(body, `"data_mode":"live"`) || read.Header.Get("Cache-Control") != "no-store" {
		t.Fatalf("authenticated live read failed: %d", read.StatusCode)
	}
	if r, _ := request("POST", "/api/v1/console/logout", "", origin, "", cookie); r.StatusCode != http.StatusForbidden {
		t.Fatalf("logout without CSRF accepted: %d", r.StatusCode)
	}
	if r, _ := request("POST", "/api/v1/console/logout", "", origin, identity.CSRFToken, cookie); r.StatusCode != http.StatusNoContent {
		t.Fatalf("logout rejected: %d", r.StatusCode)
	}
	before := calls.Load()
	if r, _ := request("GET", "/api/v1/observations", "", "", "", cookie); r.StatusCode != http.StatusUnauthorized || calls.Load() != before {
		t.Fatal("revoked cookie reached upstream")
	}
}
