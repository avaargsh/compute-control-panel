// ccp-demo is an intentionally synthetic, loopback-only product preview.
// It is never an authenticated production control plane or Kubernetes client.
package main

import (
	"context"
	"embed"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io/fs"
	"log/slog"
	"net"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"syscall"
	"time"

	"github.com/avaargsh/compute-control-panel/internal/demoinventory"
	"github.com/avaargsh/compute-control-panel/internal/observation"
)

//go:embed assets/*
var demoAssets embed.FS

const demoTenantID = "demo-tenant"
const demoProjectID = "demo-project"

func demoHandler(now func() time.Time) (http.Handler, error) {
	reader, err := demoinventory.NewReader(demoTenantID, demoProjectID, now)
	if err != nil {
		return nil, err
	}
	assets, err := fs.Sub(demoAssets, "assets")
	if err != nil {
		return nil, err
	}
	index, err := fs.ReadFile(assets, "index.html")
	if err != nil {
		return nil, err
	}
	mux := http.NewServeMux()
	mux.HandleFunc("GET /api/v1/observations", func(w http.ResponseWriter, r *http.Request) {
		if r.URL.RawQuery != "" {
			http.Error(w, "Query parameters are not supported in Demo mode", http.StatusBadRequest)
			return
		}
		snapshot, err := reader.ListObservations(r.Context(), demoTenantID, []string{demoProjectID})
		if err != nil || observation.ValidateMode(snapshot.DataMode) != nil ||
			snapshot.DataMode != observation.ModeDemo {
			http.Error(w, "Demo data source is unavailable", http.StatusServiceUnavailable)
			return
		}
		generatedAt := now().UTC()
		if generatedAt.IsZero() {
			http.Error(w, "Demo clock is unavailable", http.StatusServiceUnavailable)
			return
		}
		items := make([]observation.View, 0, len(snapshot.Items))
		for _, record := range snapshot.Items {
			if record.TenantID != demoTenantID || record.ProjectID != demoProjectID ||
				record.Source != "synthetic/demo" || record.EvidenceRef != "" {
				http.Error(w, "Invalid Demo provenance", http.StatusServiceUnavailable)
				return
			}
			view, err := observation.Project(record, generatedAt)
			if err != nil {
				http.Error(w, "Invalid Demo observation", http.StatusServiceUnavailable)
				return
			}
			items = append(items, view)
		}
		w.Header().Set("Content-Type", "application/json; charset=utf-8")
		_ = json.NewEncoder(w).Encode(struct {
			DataMode    observation.Mode   `json:"data_mode"`
			GeneratedAt time.Time          `json:"generated_at"`
			Items       []observation.View `json:"items"`
		}{DataMode: observation.ModeDemo, GeneratedAt: generatedAt, Items: items})
	})
	mux.Handle("GET /assets/", http.StripPrefix("/assets/", http.FileServer(http.FS(assets))))
	mux.HandleFunc("GET /", func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/" || r.URL.RawQuery != "" {
			http.NotFound(w, r)
			return
		}
		w.Header().Set("Content-Type", "text/html; charset=utf-8")
		_, _ = w.Write(index)
	})
	// Defense in depth. The process never includes or exposes any live reader,
	// database, control plane mutation endpoint or Kubernetes credentials.
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Cache-Control", "no-store")
		w.Header().Set("X-Content-Type-Options", "nosniff")
		w.Header().Set("X-Frame-Options", "DENY")
		w.Header().Set("Referrer-Policy", "no-referrer")
		w.Header().Set("Content-Security-Policy", "default-src 'none'; script-src 'self'; style-src 'self'; connect-src 'self'; img-src 'self'; base-uri 'none'; form-action 'none'; frame-ancestors 'none'")
		mux.ServeHTTP(w, r)
	}), nil
}

func validateDemoListenAddr(addr string) error {
	host, port, err := net.SplitHostPort(addr)
	if err != nil || host != "127.0.0.1" {
		return errors.New("demo preview must bind explicitly to 127.0.0.1")
	}
	num, err := strconv.Atoi(port)
	if err != nil || num < 0 || num > 65535 {
		return errors.New("demo preview listen port is invalid")
	}
	return nil
}

func run(ctx context.Context, addr string) error {
	if err := validateDemoListenAddr(addr); err != nil {
		return err
	}
	h, err := demoHandler(time.Now)
	if err != nil {
		return err
	}
	ln, err := net.Listen("tcp", addr)
	if err != nil {
		return err
	}
	defer ln.Close()

	srv := &http.Server{
		Handler:           h,
		ReadHeaderTimeout: 5 * time.Second,
		ReadTimeout:       10 * time.Second,
		WriteTimeout:      10 * time.Second,
		IdleTimeout:       30 * time.Second,
		MaxHeaderBytes:    1 << 16,
	}
	done := make(chan error, 1)
	go func() { done <- srv.Serve(ln) }()
	fmt.Printf("CCP Demo — synthetic observations only: http://%s\n", ln.Addr().String())
	fmt.Println("No production credentials, devices or Kubernetes resources are accessed.")
	select {
	case err := <-done:
		if errors.Is(err, http.ErrServerClosed) {
			return nil
		}
		return err
	case <-ctx.Done():
		shutdown, cancel := context.WithTimeout(context.Background(), 8*time.Second)
		defer cancel()
		if err := srv.Shutdown(shutdown); err != nil {
			_ = srv.Close()
			return err
		}
		return nil
	}
}

func main() {
	addr := flag.String("addr", "127.0.0.1:8081", "loopback-only Demo HTTP address")
	flag.Parse()
	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()
	if err := run(ctx, *addr); err != nil {
		slog.Error("CCP Demo stopped", "error", err)
		os.Exit(1)
	}
}

