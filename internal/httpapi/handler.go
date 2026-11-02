// Package httpapi provides read-only operational endpoints for the initial slice.
package httpapi

import (
	"encoding/json"
	"net/http"
	"sync/atomic"
)

func Handler(ready *atomic.Bool) http.Handler {
	mux := http.NewServeMux()
	get := func(path string, fn http.HandlerFunc) {
		mux.HandleFunc(path, func(w http.ResponseWriter, r *http.Request) {
			if r.Method != http.MethodGet {
				w.Header().Set("Allow", "GET")
				http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
				return
			}
			fn(w, r)
		})
	}
	get("/livez", func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "text/plain; charset=utf-8")
		_, _ = w.Write([]byte("ok\n"))
	})
	get("/readyz", func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "text/plain; charset=utf-8")
		if !ready.Load() {
			http.Error(w, "not ready", http.StatusServiceUnavailable)
			return
		}
		_, _ = w.Write([]byte("ready\n"))
	})
	get("/api/v1/meta", func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(struct {
			Service    string `json:"service"`
			APIVersion string `json:"api_version"`
		}{Service: "compute-control-panel", APIVersion: "v1"})
	})
	return mux
}
