package main

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"net"
	"net/http"
	"testing"
	"time"
)

func TestRunReportsBoundAddressAndServesHealth(t *testing.T) {
	reader, writer := io.Pipe()
	defer reader.Close()
	defer writer.Close()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	stopped := make(chan error, 1)
	go func() {
		stopped <- run(ctx, "127.0.0.1:0", slog.New(slog.NewJSONHandler(writer, nil)))
	}()
	events := make(chan map[string]any, 1)
	go func() {
		scanner := bufio.NewScanner(reader)
		var event map[string]any
		if scanner.Scan() {
			_ = json.Unmarshal(scanner.Bytes(), &event)
		}
		events <- event
	}()

	var addr string
	select {
	case event := <-events:
		if event["msg"] != "control plane listening" {
			t.Fatalf("missing startup event: %+v", event)
		}
		addr, _ = event["addr"].(string)
		host, port, err := net.SplitHostPort(addr)
		if err != nil || host != "127.0.0.1" || port == "0" || port == "" {
			t.Fatalf("startup event must contain the assigned loopback port: %q", addr)
		}
	case err := <-stopped:
		t.Fatalf("server stopped before reporting its address: %v", err)
	case <-time.After(3 * time.Second):
		t.Fatal("server did not report its bound address")
	}

	client := &http.Client{Timeout: 2 * time.Second}
	for _, endpoint := range []string{"/livez", "/readyz", "/api/v1/meta"} {
		resp, err := client.Get("http://" + addr + endpoint)
		if err != nil {
			t.Fatal(err)
		}
		resp.Body.Close()
		if resp.StatusCode != http.StatusOK {
			t.Errorf("GET %s: got %d", endpoint, resp.StatusCode)
		}
	}
	cancel()
	select {
	case err := <-stopped:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("server did not stop")
	}
}

func TestRunDoesNotReportSuccessfulBindOnOccupiedPort(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer listener.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	var output bytes.Buffer
	logger := slog.New(slog.NewJSONHandler(&output, nil))
	if err := run(ctx, listener.Addr().String(), logger); err == nil {
		t.Fatal("occupied address unexpectedly accepted")
	}
	if output.Len() != 0 {
		t.Fatalf("failed bind must not emit a startup event: %s", output.String())
	}
}
