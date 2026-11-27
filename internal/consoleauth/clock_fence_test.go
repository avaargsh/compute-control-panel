package consoleauth

import (
	"net/http"
	"testing"
	"time"
)

func TestConsoleClockRollbackRevokesSession(t *testing.T) {
	start := time.Date(2026, 11, 21, 1, 30, 30, 0, time.UTC)
	now := start
	reader := &testReader{}
	h, err := NewHandler(sessionConfig(t, func() time.Time { return now }), reader)
	if err != nil {
		t.Fatal(err)
	}
	login := func() *http.Cookie {
		t.Helper()
		w := response(h, tlsRequest("POST", "/api/v1/console/login", "https://ccp.example.test", `{"username":"admin","password":"this-is-a-long-test-password"}`))
		if w.Code != http.StatusOK || len(w.Result().Cookies()) != 1 {
			t.Fatalf("login failed: %d", w.Code)
		}
		return w.Result().Cookies()[0]
	}
	read := func(cookie *http.Cookie) int {
		t.Helper()
		req := tlsRequest("GET", "/api/v1/observations", "", "")
		req.AddCookie(cookie)
		return response(h, req).Code
	}
	status := func(cookie *http.Cookie) int {
		t.Helper()
		req := tlsRequest("GET", "/api/v1/console/session", "", "")
		req.AddCookie(cookie)
		return response(h, req).Code
	}
	cookie := login()
	now = start.Add(20 * time.Minute)
	if code := status(cookie); code != http.StatusOK {
		t.Fatalf("valid session rejected: %d", code)
	}
	// Roll back to a time AFTER issuance but BEFORE the latest observed time.
	now = start.Add(10 * time.Minute)
	if code := read(cookie); code != http.StatusUnauthorized || reader.calls != 0 {
		t.Fatalf("rollback extended authorization: status=%d source calls=%d", code, reader.calls)
	}
	w := response(h, tlsRequest("POST", "/api/v1/console/login", "https://ccp.example.test", `{"username":"admin","password":"this-is-a-long-test-password"}`))
	if w.Code != http.StatusServiceUnavailable || len(w.Result().Cookies()) != 0 {
		t.Fatalf("rollback allowed new session: %d", w.Code)
	}
	now = start.Add(20 * time.Minute)
	if code := status(cookie); code != http.StatusUnauthorized {
		t.Fatalf("revoked session resurrected after clock recovery: %d", code)
	}
	fresh := login()
	if code := status(fresh); code != http.StatusOK {
		t.Fatalf("clock recovery did not allow new session: %d", code)
	}
	now = time.Time{}
	if code := status(fresh); code != http.StatusUnauthorized {
		t.Fatalf("zero clock accepted: %d", code)
	}
	now = start.Add(21 * time.Minute)
	if code := status(fresh); code != http.StatusUnauthorized {
		t.Fatalf("zero-clock-revoked session resurrected: %d", code)
	}
}
