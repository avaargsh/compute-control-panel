package consoleauth

import (
	"bytes"
	"context"
	"crypto/tls"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/avaargsh/compute-control-panel/internal/observation"
	"golang.org/x/crypto/bcrypt"
)

type testReader struct {
	calls    int
	tenant   string
	projects []string
	mismatch bool
}

func (r *testReader) ListObservations(_ context.Context, tenant string, projects []string) (observation.Snapshot, error) {
	r.calls++
	r.tenant = tenant
	r.projects = append([]string(nil), projects...)
	now := time.Date(2026, 11, 21, 1, 30, 0, 0, time.UTC)
	project := "project-a"
	if r.mismatch {
		project = "project-secret"
	}
	return observation.Snapshot{
		DataMode: observation.ModeLive,
		Items: []observation.Record{{
			TenantID: "tenant-a", ProjectID: project,
			Resource: observation.ResourceRef{ClusterID: "cluster-a", Kind: "Node", Name: "node-a", UID: "node-uid", ResourceVersion: "11"},
			Source:   "kubernetes/core/v1/node.status", Condition: observation.ConditionAvailable,
			ObservedAt: now, ExpiresAt: now.Add(3 * time.Minute),
		}},
	}, nil
}

func sessionConfig(t *testing.T, now func() time.Time) Config {
	t.Helper()
	hash, err := bcrypt.GenerateFromPassword([]byte("this-is-a-long-test-password"), bcrypt.DefaultCost)
	if err != nil {
		t.Fatal(err)
	}
	return Config{
		Username:           "admin",
		PasswordBcryptHash: hash,
		TenantID:           "tenant-a", ProjectID: "project-a",
		Origin:     "https://ccp.example.test",
		SessionTTL: time.Hour, Now: now,
	}
}

func tlsRequest(method, path, origin string, body string) *http.Request {
	req := httptest.NewRequest(method, "https://ccp.example.test"+path, strings.NewReader(body))
	req.TLS = &tls.ConnectionState{}
	if origin != "" {
		req.Header.Set("Origin", origin)
	}
	if body != "" {
		req.Header.Set("Content-Type", "application/json")
	}
	return req
}

func response(handler http.Handler, req *http.Request) *httptest.ResponseRecorder {
	w := httptest.NewRecorder()
	handler.ServeHTTP(w, req)
	return w
}

func TestConsoleSessionLifecycleAndScopedRead(t *testing.T) {
	now := time.Date(2026, 11, 21, 1, 30, 30, 0, time.UTC)
	reader := &testReader{}
	handler, err := NewHandler(sessionConfig(t, func() time.Time { return now }), reader)
	if err != nil {
		t.Fatal(err)
	}
	unauth := response(handler, tlsRequest("GET", "/api/v1/observations", "", ""))
	if unauth.Code != http.StatusUnauthorized || reader.calls != 0 {
		t.Fatalf("unprotected observation read: %d calls=%d", unauth.Code, reader.calls)
	}
	body := `{"username":"admin","password":"this-is-a-long-test-password"}`
	login := response(handler, tlsRequest("POST", "/api/v1/console/login", "https://ccp.example.test", body))
	if login.Code != http.StatusOK {
		t.Fatalf("login failed: %d %s", login.Code, login.Body.String())
	}
	cookies := login.Result().Cookies()
	if len(cookies) != 1 {
		t.Fatalf("expected exactly one session cookie, got %d", len(cookies))
	}
	cookie := cookies[0]
	if cookie.Name != "__Host-ccp_session" || cookie.Value == "" || !cookie.Secure ||
		!cookie.HttpOnly || cookie.SameSite != http.SameSiteStrictMode || cookie.Path != "/" || cookie.Domain != "" {
		t.Fatalf("insecure session cookie: %+v", cookie)
	}
	me := tlsRequest("GET", "/api/v1/console/session", "", "")
	me.AddCookie(cookie)
	status := response(handler, me)
	if status.Code != http.StatusOK || status.Header().Get("Cache-Control") != "no-store" {
		t.Fatalf("session status rejected: %d %s", status.Code, status.Body.String())
	}
	var session struct {
		Authenticated bool   `json:"authenticated"`
		ProjectID     string `json:"project_id"`
		CSRFToken     string `json:"csrf_token"`
	}
	if err := json.Unmarshal(status.Body.Bytes(), &session); err != nil ||
		!session.Authenticated || session.ProjectID != "project-a" || len(session.CSRFToken) < 32 {
		t.Fatalf("bad server session data %+v err=%v", session, err)
	}
	obsRequest := tlsRequest("GET", "/api/v1/observations", "", "")
	obsRequest.AddCookie(cookie)
	obs := response(handler, obsRequest)
	if obs.Code != http.StatusOK || reader.calls != 1 ||
		reader.tenant != "tenant-a" || len(reader.projects) != 1 || reader.projects[0] != "project-a" {
		t.Fatalf("scoped observation read failed: %d calls=%d tenant=%s projects=%v", obs.Code, reader.calls, reader.tenant, reader.projects)
	}
	if strings.Contains(obs.Body.String(), "tenant-a") || !strings.Contains(obs.Body.String(), "node-a") {
		t.Fatalf("unsafe or missing observation: %s", obs.Body.String())
	}
	logoutMissing := tlsRequest("POST", "/api/v1/console/logout", "https://ccp.example.test", "")
	logoutMissing.AddCookie(cookie)
	if got := response(handler, logoutMissing).Code; got != http.StatusForbidden {
		t.Fatalf("logout without CSRF accepted: %d", got)
	}
	logout := tlsRequest("POST", "/api/v1/console/logout", "https://ccp.example.test", "")
	logout.AddCookie(cookie)
	logout.Header.Set("X-CCP-CSRF", session.CSRFToken)
	if got := response(handler, logout).Code; got != http.StatusNoContent {
		t.Fatalf("logout rejected: %d", got)
	}
	replay := tlsRequest("GET", "/api/v1/observations", "", "")
	replay.AddCookie(cookie)
	if got := response(handler, replay).Code; got != http.StatusUnauthorized {
		t.Fatalf("revoked session reused: %d", got)
	}
}

func TestConsoleSessionRequiresTLSAndSameOriginForMutation(t *testing.T) {
	now := time.Date(2026, 11, 21, 1, 30, 30, 0, time.UTC)
	reader := &testReader{}
	h, err := NewHandler(sessionConfig(t, func() time.Time { return now }), reader)
	if err != nil {
		t.Fatal(err)
	}
	loginBody := `{"username":"admin","password":"this-is-a-long-test-password"}`
	cases := []struct {
		name string
		req  *http.Request
	}{
		{"missing origin", tlsRequest("POST", "/api/v1/console/login", "", loginBody)},
		{"foreign origin", tlsRequest("POST", "/api/v1/console/login", "https://evil.test", loginBody)},
		{"foreign host", tlsRequest("POST", "/api/v1/console/login", "https://ccp.example.test", loginBody)},
		{"cross-site fetch", tlsRequest("POST", "/api/v1/console/login", "https://ccp.example.test", loginBody)},
		{"plain HTTP", tlsRequest("POST", "/api/v1/console/login", "https://ccp.example.test", loginBody)},
	}
	cases[2].req.Host = "other.example.test"
	cases[3].req.Header.Set("Sec-Fetch-Site", "cross-site")
	cases[4].req.TLS = nil
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			w := response(h, tc.req)
			if w.Code != http.StatusForbidden || len(w.Result().Cookies()) != 0 {
				t.Fatalf("unsafe mutation accepted: %d %s", w.Code, w.Body.String())
			}
		})
	}
}

func TestConsoleSessionBadPasswordAndThrottle(t *testing.T) {
	now := time.Date(2026, 11, 21, 1, 30, 30, 0, time.UTC)
	reader := &testReader{}
	h, err := NewHandler(sessionConfig(t, func() time.Time { return now }), reader)
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 5; i++ {
		body := `{"username":"admin","password":"wrong-password"}`
		w := response(h, tlsRequest("POST", "/api/v1/console/login", "https://ccp.example.test", body))
		if w.Code != http.StatusUnauthorized || len(w.Result().Cookies()) != 0 {
			t.Fatalf("invalid password gave session: %d", w.Code)
		}
	}
	body := `{"username":"admin","password":"this-is-a-long-test-password"}`
	w := response(h, tlsRequest("POST", "/api/v1/console/login", "https://ccp.example.test", body))
	if w.Code != http.StatusTooManyRequests || len(w.Result().Cookies()) != 0 {
		t.Fatalf("global throttle bypassed: %d", w.Code)
	}
}

func TestConsoleSessionExpiresAndRejectsCrossProjectSource(t *testing.T) {
	now := time.Date(2026, 11, 21, 1, 30, 30, 0, time.UTC)
	reader := &testReader{mismatch: true}
	h, err := NewHandler(sessionConfig(t, func() time.Time { return now }), reader)
	if err != nil {
		t.Fatal(err)
	}
	body := `{"username":"admin","password":"this-is-a-long-test-password"}`
	login := response(h, tlsRequest("POST", "/api/v1/console/login", "https://ccp.example.test", body))
	if login.Code != http.StatusOK {
		t.Fatalf("login failed %d", login.Code)
	}
	cookie := login.Result().Cookies()[0]
	req := tlsRequest("GET", "/api/v1/observations", "", "")
	req.AddCookie(cookie)
	w := response(h, req)
	if w.Code != http.StatusBadGateway || strings.Contains(w.Body.String(), "project-secret") {
		t.Fatalf("cross-project record leaked: %d %s", w.Code, w.Body.String())
	}
	now = now.Add(2 * time.Hour)
	req = tlsRequest("GET", "/api/v1/observations", "", "")
	req.AddCookie(cookie)
	if got := response(h, req).Code; got != http.StatusUnauthorized {
		t.Fatalf("expired session admitted: %d", got)
	}
}

func TestConsoleAuthRejectsInvalidConfigAndUnsafeRoutes(t *testing.T) {
	now := time.Date(2026, 11, 21, 1, 30, 30, 0, time.UTC)
	cfg := sessionConfig(t, func() time.Time { return now })
	if _, err := NewHandler(cfg, nil); err == nil {
		t.Fatal("nil observation reader accepted")
	}
	cases := []struct {
		name   string
		change func(*Config)
	}{
		{"username missing", func(c *Config) { c.Username = "" }},
		{"hash missing", func(c *Config) { c.PasswordBcryptHash = nil }},
		{"scope missing", func(c *Config) { c.ProjectID = "" }},
		{"HTTP origin", func(c *Config) { c.Origin = "http://ccp.example.test" }},
		{"origin has path", func(c *Config) { c.Origin = "https://ccp.example.test/login" }},
		{"ttl zero", func(c *Config) { c.SessionTTL = 0 }},
		{"ttl too long", func(c *Config) { c.SessionTTL = 48 * time.Hour }},
		{"clock absent", func(c *Config) { c.Now = nil }},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			bad := cfg
			tc.change(&bad)
			if _, err := NewHandler(bad, &testReader{}); err == nil {
				t.Fatal("invalid session config accepted")
			}
		})
	}
	h, err := NewHandler(cfg, &testReader{})
	if err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct {
		method, path string
		want         int
	}{
		{"POST", "/api/v1/workloads", http.StatusNotFound},
		{"GET", "/api/v1/workloads", http.StatusNotFound},
		{"POST", "/api/v1/observations", http.StatusMethodNotAllowed},
		{"GET", "/api/v1/observations?project_id=other", http.StatusBadRequest},
		{"GET", "/api/v1/console/login", http.StatusMethodNotAllowed},
	} {
		w := response(h, tlsRequest(tc.method, tc.path, "", ""))
		if w.Code != tc.want {
			t.Fatalf("%s %s code=%d, want %d", tc.method, tc.path, w.Code, tc.want)
		}
	}
}

func TestConsoleRejectsMalformedLoginAndOversizedBody(t *testing.T) {
	now := time.Date(2026, 11, 21, 1, 30, 30, 0, time.UTC)
	h, err := NewHandler(sessionConfig(t, func() time.Time { return now }), &testReader{})
	if err != nil {
		t.Fatal(err)
	}
	for _, input := range []string{
		`{"username":"admin","password":"this-is-a-long-test-password","project_id":"project-secret"}`,
		`{"username":123,"password":"test"}`,
		`{"username":"admin","password":"test"}{}`,
		string(bytes.Repeat([]byte("x"), 5000)),
	} {
		w := response(h, tlsRequest("POST", "/api/v1/console/login", "https://ccp.example.test", input))
		if w.Code != http.StatusBadRequest || len(w.Result().Cookies()) != 0 {
			t.Fatalf("malformed login accepted or wrong error: %d", w.Code)
		}
	}
}
