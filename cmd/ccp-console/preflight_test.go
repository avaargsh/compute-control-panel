package main

import (
	"crypto/x509"
	"io"
	"math/big"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"

	"github.com/avaargsh/compute-control-panel/internal/consoleauth"
	"github.com/avaargsh/compute-control-panel/internal/kubeinventory"
	"golang.org/x/crypto/bcrypt"
)

func TestPrepareConsoleDoesNotBindOrReadTheUpstream(t *testing.T) {
	var requests atomic.Int32
	api := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		requests.Add(1)
		_, _ = io.WriteString(w, "{}")
	}))
	t.Cleanup(api.Close)
	occupied, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = occupied.Close() })

	now := time.Now()
	cert, key := writeFixtureTLS(t, &x509.Certificate{
		SerialNumber: big.NewInt(1), IPAddresses: []net.IP{net.ParseIP(consoleHost)},
		NotBefore: now.Add(-time.Hour), NotAfter: now.Add(time.Hour),
		ExtKeyUsage: []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
	})
	hash, err := bcrypt.GenerateFromPassword([]byte("local-preflight-fixture"), bcrypt.DefaultCost)
	if err != nil {
		t.Fatal(err)
	}
	token := filepath.Join(t.TempDir(), "node-reader.token")
	if err := os.WriteFile(token, []byte("local-preflight-fixture"), 0o600); err != nil {
		t.Fatal(err)
	}
	addr := occupied.Addr().String()
	cfg := liveConfig{
		Auth: consoleauth.Config{
			Username: "admin", PasswordBcryptHash: hash, TenantID: "tenant-a",
			ProjectID: "project-a", Origin: "https://" + addr, SessionTTL: time.Hour, Now: time.Now,
		},
		Inventory: kubeinventory.Config{
			APIServer: api.URL, CAFile: cert, TokenFile: token, ClusterID: "test-cluster",
			TenantID: "tenant-a", ProjectID: "project-a", ResourceNames: []string{"nvidia.com/gpu"},
			ObservationTTL: time.Minute, Timeout: time.Second,
		},
	}
	if _, handler, err := prepareConsole(addr, cert, key, cfg); err != nil || handler == nil {
		t.Fatalf("valid local configuration rejected: %v", err)
	}
	cfg.Inventory.CAFile = key
	if _, _, err := prepareConsole(addr, cert, key, cfg); err == nil {
		t.Fatal("invalid upstream CA accepted in local preflight")
	}
	if requests.Load() != 0 {
		t.Fatal("local preflight contacted the upstream")
	}
}
