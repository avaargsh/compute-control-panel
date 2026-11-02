package httpapi

import (
    "encoding/json"
    "net/http"
    "net/http/httptest"
    "sync/atomic"
    "testing"
)

func TestHealthTransitions(t *testing.T) {
    var ready atomic.Bool
    h := Handler(&ready)
    for _, tc := range []struct{ path string; code int }{
        {"/livez", 200}, {"/readyz", 503},
    } {
        w := httptest.NewRecorder()
        h.ServeHTTP(w, httptest.NewRequest(http.MethodGet, tc.path, nil))
        if w.Code != tc.code { t.Fatalf("%s: got %d want %d", tc.path, w.Code, tc.code) }
    }
    ready.Store(true)
    w := httptest.NewRecorder()
    h.ServeHTTP(w, httptest.NewRequest(http.MethodGet, "/readyz", nil))
    if w.Code != 200 { t.Fatalf("ready: %d", w.Code) }
    ready.Store(false)
    w = httptest.NewRecorder()
    h.ServeHTTP(w, httptest.NewRequest(http.MethodGet, "/readyz", nil))
    if w.Code != 503 { t.Fatalf("drained: %d", w.Code) }
}

func TestMetadataAndMethodBoundary(t *testing.T) {
    h := Handler(new(atomic.Bool))
    w := httptest.NewRecorder()
    h.ServeHTTP(w, httptest.NewRequest(http.MethodGet, "/api/v1/meta", nil))
    if w.Code != 200 { t.Fatalf("meta: %d", w.Code) }
    var v struct { Service string `json:"service"`; APIVersion string `json:"api_version"` }
    if err := json.Unmarshal(w.Body.Bytes(), &v); err != nil { t.Fatal(err) }
    if v.Service != "compute-control-panel" || v.APIVersion != "v1" { t.Fatalf("unexpected metadata: %+v", v) }
    w = httptest.NewRecorder()
    h.ServeHTTP(w, httptest.NewRequest(http.MethodPost, "/api/v1/meta", nil))
    if w.Code != 405 || w.Header().Get("Allow") != "GET" { t.Fatalf("POST: %d", w.Code) }
    w = httptest.NewRecorder()
    h.ServeHTTP(w, httptest.NewRequest("GET", "/api/v1/workloads", nil))
    if w.Code != 404 { t.Fatalf("not-implemented API must 404: %d", w.Code) }
}
