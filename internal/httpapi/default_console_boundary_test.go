package httpapi

import (
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
)

func TestDefaultHandlerDoesNotMountConsoleRoutes(t *testing.T) {
	var ready atomic.Bool
	ready.Store(true)
	handler := Handler(&ready)

	// Use each route's valid method. A 401/403 would still mean the
	// feature has been registered on the wrong listener.
	for _, route := range []struct{ method, path string }{
		{http.MethodGet, "/api/v1/observations"},
		{http.MethodGet, "/api/v1/console/session"},
		{http.MethodPost, "/api/v1/console/login"},
		{http.MethodPost, "/api/v1/console/logout"},
	} {
		t.Run(route.method+" "+route.path, func(t *testing.T) {
			res := httptest.NewRecorder()
			req := httptest.NewRequest(route.method, route.path, nil)
			handler.ServeHTTP(res, req)
			if res.Code != http.StatusNotFound {
				t.Fatalf("default listener mounted Console route: got HTTP %d, want %d", res.Code, http.StatusNotFound)
			}
		})
	}
}
