package main

import (
	"context"
	"errors"
	"log/slog"
	"net"
	"net/http"
	"os"
	"os/signal"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/avaargsh/compute-control-panel/internal/httpapi"
)

// run owns service admission and shutdown. Domain API writes will only be
// admitted once a durable store and authorization boundary are integrated.
func run(ctx context.Context, addr string) error {
	ln, err := net.Listen("tcp", addr)
	if err != nil {
		return err
	}
	var ready atomic.Bool
	srv := &http.Server{
		Handler:           httpapi.Handler(&ready),
		ReadHeaderTimeout: 5 * time.Second,
		ReadTimeout:       10 * time.Second,
		WriteTimeout:      10 * time.Second,
		IdleTimeout:       60 * time.Second,
		MaxHeaderBytes:    1 << 16,
	}
	stopped := make(chan error, 1)
	ready.Store(true)
	go func() { stopped <- srv.Serve(ln) }()
	select {
	case err := <-stopped:
		if errors.Is(err, http.ErrServerClosed) {
			return nil
		}
		return err
	case <-ctx.Done():
		ready.Store(false)
		drain, cancel := context.WithTimeout(context.Background(), 15*time.Second)
		defer cancel()
		if err := srv.Shutdown(drain); err != nil {
			_ = srv.Close()
			return err
		}
		return nil
	}
}

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	addr := os.Getenv("LISTEN_ADDR")
	if addr == "" {
		addr = "127.0.0.1:8080"
	}
	if err := run(ctx, addr); err != nil {
		slog.Error("control plane stopped", "error", err)
		os.Exit(1)
	}
}
