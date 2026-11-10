package provider

import (
	"bufio"
	"bytes"
	"context"
	"crypto/tls"
	"crypto/x509"
	"database/sql"
	"encoding/json"
	"encoding/pem"
	"errors"
	"fmt"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/avaargsh/compute-control-panel/internal/operation"
	_ "github.com/jackc/pgx/v5/stdlib"
)

// The child is the same test executable in a new OS process with a fresh
// PostgreSQL connection. It never speaks to an external Kubernetes cluster.
func TestKueueSIGKILLChild(t *testing.T) {
	if os.Getenv("CCP_KUEUE_SIGKILL_CHILD") != "1" {
		return
	}
	rawURL := os.Getenv("CCP_KUEUE_SIGKILL_URL")
	endpoint, err := url.Parse(rawURL)
	if err != nil || endpoint.Scheme != "https" || endpoint.User != nil ||
		!isLoopbackHost(endpoint.Hostname()) {
		t.Fatal("SIGKILL child requires local HTTPS fake Kubernetes API")
	}
	certPEM, err := os.ReadFile(os.Getenv("CCP_KUEUE_SIGKILL_CA"))
	if err != nil {
		t.Fatal(err)
	}
	roots := x509.NewCertPool()
	if !roots.AppendCertsFromPEM(certPEM) {
		t.Fatal("fake API test CA is invalid")
	}
	var req Request
	if err := json.Unmarshal([]byte(os.Getenv("CCP_KUEUE_SIGKILL_REQUEST")), &req); err != nil {
		t.Fatal(err)
	}
	db, err := sql.Open("pgx", os.Getenv("DATABASE_URL"))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	bindings, err := NewPostgresKueueJobBindingStore(db)
	if err != nil {
		t.Fatal(err)
	}
	client := &http.Client{
		Timeout: 3 * time.Second,
		Transport: &http.Transport{
			TLSClientConfig: &tls.Config{RootCAs: roots},
		},
	}
	adapter, err := NewKueueJobAdapterWithBindings(rawURL, "tenant-a", client, bindings)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 8*time.Second)
	defer cancel()

	switch os.Getenv("CCP_KUEUE_SIGKILL_ACTION") {
	case "create":
		if _, err := adapter.Apply(ctx, req); err != nil {
			t.Fatalf("initial CREATE: %v", err)
		}
	case "create-lost":
		if _, err := adapter.Apply(ctx, req); !errors.Is(err, ErrAcknowledgementLost) {
			t.Fatalf("CREATE lost ACK did not stay unknown: %v", err)
		}
	case "delete-lost":
		if _, err := adapter.Apply(ctx, req); !errors.Is(err, ErrAcknowledgementLost) {
			t.Fatalf("DELETE lost ACK did not stay unknown: %v", err)
		}
	case "verify-create":
		seen, err := adapter.Inspect(ctx, req)
		if err != nil || seen.Result != operation.ResourcePresent || seen.ResourceUID != "job-uid-created" {
			t.Fatalf("restarted CREATE Inspect: %+v %v", seen, err)
		}
		if _, err := adapter.Apply(ctx, req); err != nil {
			t.Fatalf("restarted CREATE replay: %v", err)
		}
	case "verify-unbound":
		seen, err := adapter.Inspect(ctx, req)
		if err != nil || seen.Result != operation.OperationInProgress ||
			seen.ResourceUID != "" || seen.ResourceID != "" {
			t.Fatalf("unbound CREATE became trusted: %+v %v", seen, err)
		}
		if _, err := adapter.Apply(ctx, req); !errors.Is(err, ErrAcknowledgementLost) {
			t.Fatalf("lost CREATE ACK was blindly retried: %v", err)
		}
	case "verify-delete-absent":
		seen, err := adapter.Inspect(ctx, req)
		if err != nil || seen.Result != operation.TargetAbsent ||
			seen.TargetResourceUID != req.TargetResourceUID {
			t.Fatalf("restarted DELETE absent: %+v %v", seen, err)
		}
		if _, err := adapter.Apply(ctx, req); err != nil {
			t.Fatalf("restarted DELETE replay: %v", err)
		}
	case "verify-delete-successor":
		seen, err := adapter.Inspect(ctx, req)
		if err != nil || seen.Result != operation.ObservationConflict ||
			seen.ReasonCode != "JOB_UID_REPLACED" {
			t.Fatalf("restarted DELETE adopted successor: %+v %v", seen, err)
		}
		if _, err := adapter.Apply(ctx, req); !errors.Is(err, ErrOperationConflict) {
			t.Fatalf("restarted DELETE touched successor: %v", err)
		}
	default:
		t.Fatal("unknown SIGKILL test action")
	}
	switch os.Getenv("CCP_KUEUE_SIGKILL_ACTION") {
	case "create", "create-lost", "delete-lost":
		fmt.Fprintln(os.Stdout, "READY")
		// The parent terminates this real process only after the adapter has
		// returned, so acknowledged pins or uncertain tombstones are durable.
		for {
			time.Sleep(time.Hour)
		}
	default:
		fmt.Fprintln(os.Stdout, "VERIFIED")
	}
}

func isLoopbackHost(host string) bool {
	if host == "localhost" {
		return true
	}
	ip := net.ParseIP(host)
	return ip != nil && ip.IsLoopback()
}

func newSIGKILLFakeKubeAPI(t *testing.T, api *kueueAPI) (*httptest.Server, string) {
	t.Helper()
	api.t = t
	api.jobs = make(map[string]kubeObject)
	server := httptest.NewTLSServer(http.HandlerFunc(api.serve))
	t.Cleanup(server.Close)
	caPath := filepath.Join(t.TempDir(), "fake-kube-ca.pem")
	cert := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: server.Certificate().Raw})
	if err := os.WriteFile(caPath, cert, 0600); err != nil {
		t.Fatal(err)
	}
	return server, caPath
}

func runKueueSIGKILLProcess(t *testing.T, server *httptest.Server, caPath, action string, req Request, kill bool) {
	t.Helper()
	requestJSON, err := json.Marshal(req)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	cmd := exec.CommandContext(ctx, os.Args[0], "-test.run=^TestKueueSIGKILLChild$")
	cmd.Env = append(os.Environ(),
		"CCP_KUEUE_SIGKILL_CHILD=1",
		"CCP_KUEUE_SIGKILL_URL="+server.URL,
		"CCP_KUEUE_SIGKILL_CA="+caPath,
		"CCP_KUEUE_SIGKILL_ACTION="+action,
		"CCP_KUEUE_SIGKILL_REQUEST="+string(requestJSON),
	)
	if !kill {
		output, err := cmd.CombinedOutput()
		if err != nil || !bytes.Contains(output, []byte("VERIFIED")) {
			t.Fatalf("fresh process %s failed: %v output=%s", action, err, output)
		}
		return
	}
	var stderr bytes.Buffer
	cmd.Stderr = &stderr
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		t.Fatal(err)
	}
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	type childLine struct {
		line string
		err  error
	}
	ready := make(chan childLine, 1)
	go func() {
		line, err := bufio.NewReader(stdout).ReadString('\n')
		ready <- childLine{line: line, err: err}
	}()
	select {
	case got := <-ready:
		if got.err != nil || strings.TrimSpace(got.line) != "READY" {
			_ = cmd.Process.Kill()
			_ = cmd.Wait()
			t.Fatalf("child %s did not reach durable checkpoint: line=%q err=%v stderr=%s",
				action, got.line, got.err, stderr.String())
		}
	case <-ctx.Done():
		_ = cmd.Process.Kill()
		_ = cmd.Wait()
		t.Fatalf("child %s timed out before checkpoint: %v stderr=%s", action, ctx.Err(), stderr.String())
	}
	if err := cmd.Process.Kill(); err != nil {
		t.Fatalf("kill child %s: %v", action, err)
	}
	err = cmd.Wait()
	var exit *exec.ExitError
	if !errors.As(err, &exit) {
		t.Fatalf("child %s did not exit from SIGKILL: %v", action, err)
	}
	status, ok := exit.Sys().(syscall.WaitStatus)
	if !ok || !status.Signaled() || status.Signal() != syscall.SIGKILL {
		t.Fatalf("child %s was not SIGKILLed: %v", action, exit.Sys())
	}
}

// This test proves subprocess death and DB-backed binding reload against a
// local TLS fake API. It does not prove real Kubernetes/DRA release or persist
// the owning ComputeOperation journal's UNKNOWN -> recovery transition.
func TestKueuePostgresBindingSIGKILLRecovery(t *testing.T) {
	if runtime.GOOS != "linux" {
		t.Skip("SIGKILL wait-status assertion is Linux-specific")
	}
	if os.Getenv("DATABASE_URL") == "" {
		t.Skip("disposable PostgreSQL DATABASE_URL not configured")
	}
	db := openProviderPostgresDB(t)
	store, err := NewPostgresKueueJobBindingStore(db)
	if err != nil {
		t.Fatal(err)
	}

	t.Run("acknowledged CREATE survives process death", func(t *testing.T) {
		req := uniqueKueueRequest(t)
		api := &kueueAPI{}
		server, caPath := newSIGKILLFakeKubeAPI(t, api)
		runKueueSIGKILLProcess(t, server, caPath, "create", req, true)
		pin, ok, err := store.GetCreatePin("tenant-a", req)
		if err != nil || !ok || pin.UID != "job-uid-created" {
			t.Fatalf("committed CREATE pin missing: %+v found=%v err=%v", pin, ok, err)
		}
		runKueueSIGKILLProcess(t, server, caPath, "verify-create", req, false)
		if api.postCount() != 1 || api.storedJobs() != 1 {
			t.Fatalf("CREATE replay duplicated Job: posts=%d jobs=%d", api.postCount(), api.storedJobs())
		}
	})

	t.Run("lost CREATE ACK stays unbound after process death", func(t *testing.T) {
		req := uniqueKueueRequest(t)
		api := &kueueAPI{loseCreateBody: true}
		server, caPath := newSIGKILLFakeKubeAPI(t, api)
		runKueueSIGKILLProcess(t, server, caPath, "create-lost", req, true)
		if _, ok, err := store.GetCreatePin("tenant-a", req); err != nil || ok {
			t.Fatalf("unacknowledged CREATE acquired a pin: found=%v err=%v", ok, err)
		}
		runKueueSIGKILLProcess(t, server, caPath, "verify-unbound", req, false)
		if api.postCount() != 1 || api.storedJobs() != 1 {
			t.Fatalf("lost CREATE ACK caused duplicate: posts=%d jobs=%d", api.postCount(), api.storedJobs())
		}
	})

	t.Run("lost DELETE ACK never deletes successor after process death", func(t *testing.T) {
		create := uniqueKueueRequest(t)
		api := &kueueAPI{loseDeleteBody: true}
		server, caPath := newSIGKILLFakeKubeAPI(t, api)
		runKueueSIGKILLProcess(t, server, caPath, "create", create, true)
		pin, ok, err := store.GetCreatePin("tenant-a", create)
		if err != nil || !ok {
			t.Fatalf("missing CREATE pin: found=%v err=%v", ok, err)
		}
		remove := create
		remove.Kind = operation.Delete
		remove.OperationID = "del-" + create.OperationID
		remove.IdempotencyKey = "idem-" + remove.OperationID
		remove.TargetResourceID = "tenant-a/" + pin.Name
		remove.TargetResourceUID = pin.UID
		runKueueSIGKILLProcess(t, server, caPath, "delete-lost", remove, true)
		tomb, ok, err := store.GetDeleteTombstone("tenant-a", remove)
		if err != nil || !ok || tomb.Acknowledged || tomb.UID != pin.UID {
			t.Fatalf("uncertain DELETE tombstone missing: %+v found=%v err=%v", tomb, ok, err)
		}
		runKueueSIGKILLProcess(t, server, caPath, "verify-delete-absent", remove, false)
		api.plantJob(pin.Name, "job-uid-successor")
		runKueueSIGKILLProcess(t, server, caPath, "verify-delete-successor", remove, false)
		if api.postCount() != 1 || api.deleteCount() != 1 || api.storedJobs() != 1 {
			t.Fatalf("DELETE replay touched successor: posts=%d deletes=%d jobs=%d",
				api.postCount(), api.deleteCount(), api.storedJobs())
		}
	})
}
