package main

import (
	"context"
	"encoding/json"
	"io"
	"net"
	"net/http"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/avaargsh/compute-control-panel/internal/observation"
)

func TestQuickstartTroubleshootingQuotesStartupErrors(t *testing.T) {
	doc, err := os.ReadFile("../../docs/product/DEMO-CONSOLE-QUICKSTART.md")
	if err != nil {
		t.Fatal(err)
	}
	section := string(doc)
	heading := strings.Index(section, "## Troubleshooting\n")
	if heading < 0 {
		t.Fatal("quickstart is missing ## Troubleshooting")
	}
	section = section[heading:]
	for _, phrase := range []string{
		"demo preview must bind explicitly to 127.0.0.1",
		"demo preview listen port is invalid",
		"listen tcp 127.0.0.1:8081: bind: address already in use",
		"Query parameters are not supported in Demo mode",
		`"data_mode":"demo"`,
		`"source":"synthetic/demo"`,
		"Go 1.27.0",
		"Go 1.27.4",
		"physical GPU readiness",
	} {
		if !strings.Contains(section, phrase) {
			t.Fatalf("troubleshooting section missing %q", phrase)
		}
	}

	for addr, want := range map[string]string{
		"":                   "demo preview must bind explicitly to 127.0.0.1",
		"0.0.0.0:8081":       "demo preview must bind explicitly to 127.0.0.1",
		"localhost:8081":     "demo preview must bind explicitly to 127.0.0.1",
		"127.0.0.1:notaport": "demo preview listen port is invalid",
		"127.0.0.1:65536":    "demo preview listen port is invalid",
	} {
		err := run(context.Background(), addr)
		if err == nil || err.Error() != want {
			t.Fatalf("addr %q: got %v want %q", addr, err, want)
		}
	}
}

func TestRunLoopbackObservationsMatchQuickstart(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	addr := ln.Addr().String()
	if err := ln.Close(); err != nil {
		t.Fatal(err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	errCh := make(chan error, 1)
	go func() { errCh <- run(ctx, addr) }()

	client := &http.Client{Timeout: 2 * time.Second}
	observationsURL := "http://" + addr + "/api/v1/observations"
	var resp *http.Response
	deadline := time.Now().Add(3 * time.Second)
	for {
		resp, err = client.Get(observationsURL)
		if err == nil {
			break
		}
		select {
		case serveErr := <-errCh:
			t.Fatalf("demo exited before serving: %v (last client error %v)", serveErr, err)
		default:
		}
		if time.Now().After(deadline) {
			t.Fatalf("demo did not accept connections: %v", err)
		}
		time.Sleep(20 * time.Millisecond)
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatal(err)
	}
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status %d body %s", resp.StatusCode, body)
	}
	var payload struct {
		DataMode observation.Mode   `json:"data_mode"`
		Items    []observation.View `json:"items"`
	}
	if err := json.Unmarshal(body, &payload); err != nil {
		t.Fatal(err)
	}
	if payload.DataMode != observation.ModeDemo || len(payload.Items) == 0 {
		t.Fatalf("unexpected payload: %+v", payload)
	}
	for _, item := range payload.Items {
		if item.Source != "synthetic/demo" {
			t.Fatalf("non-synthetic source: %+v", item)
		}
	}

	queryResp, err := client.Get(observationsURL + "?project_id=other")
	if err != nil {
		t.Fatal(err)
	}
	defer queryResp.Body.Close()
	queryBody, err := io.ReadAll(queryResp.Body)
	if err != nil {
		t.Fatal(err)
	}
	if queryResp.StatusCode != http.StatusBadRequest || !strings.Contains(string(queryBody), "Query parameters are not supported in Demo mode") {
		t.Fatalf("query: status %d body %s", queryResp.StatusCode, queryBody)
	}

	workloadsResp, err := client.Get("http://" + addr + "/api/v1/workloads")
	if err != nil {
		t.Fatal(err)
	}
	defer workloadsResp.Body.Close()
	if workloadsResp.StatusCode != http.StatusNotFound {
		t.Fatalf("workloads status %d", workloadsResp.StatusCode)
	}

	busy := run(context.Background(), addr)
	if busy == nil || !strings.Contains(busy.Error(), "bind: address already in use") {
		t.Fatalf("expected address in use, got %v", busy)
	}

	cancel()
	select {
	case serveErr := <-errCh:
		if serveErr != nil {
			t.Fatal(serveErr)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("demo did not stop after cancel")
	}
}
