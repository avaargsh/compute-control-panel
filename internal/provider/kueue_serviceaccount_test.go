package provider

import (
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"crypto/x509"
	"encoding/pem"
	"errors"
	"fmt"
	"math/big"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

func serviceAccountTestServer(t *testing.T) (*httptest.Server, *atomic.Value, *atomic.Int32) {
	t.Helper()
	expected := new(atomic.Value)
	expected.Store("test-token-a")
	reads := new(atomic.Int32)
	srv := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		reads.Add(1)
		if r.Method != http.MethodGet || r.Header.Get("Authorization") != "Bearer "+expected.Load().(string) {
			http.Error(w, "forbidden", http.StatusForbidden)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		switch r.URL.Path {
		case "/apis/batch/v1/namespaces/tenant-a/jobs/job-a":
			fmt.Fprint(w, testKubeJob)
		case "/apis/kueue.x-k8s.io/v1beta1/namespaces/tenant-a/workloads":
			fmt.Fprint(w, testKueueWorkloads)
		case "/api/v1/namespaces/tenant-a/pods":
			if r.URL.Query().Get("labelSelector") != "batch.kubernetes.io/controller-uid=job-uid-a" {
				t.Errorf("unexpected Pod selector: %q", r.URL.RawQuery)
			}
			fmt.Fprint(w, testKubePods)
		default:
			http.NotFound(w, r)
		}
	}))
	return srv, expected, reads
}

func serviceAccountTestFiles(t *testing.T, srv *httptest.Server) (string, string) {
	t.Helper()
	d := t.TempDir()
	caPath := filepath.Join(d, "ca.crt")
	tokenPath := filepath.Join(d, "token")
	cert := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: srv.Certificate().Raw})
	if err := os.WriteFile(caPath, cert, 0600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(tokenPath, []byte("test-token-a\n"), 0600); err != nil {
		t.Fatal(err)
	}
	return caPath, tokenPath
}

func TestServiceAccountReaderCollectsOverPinnedTLSAndUsesRotatedToken(t *testing.T) {
	srv, expected, reads := serviceAccountTestServer(t)
	defer srv.Close()
	ca, tokenFile := serviceAccountTestFiles(t, srv)
	t.Setenv("HTTPS_PROXY", "http://127.0.0.1:1")
	reader, err := NewServiceAccountKubernetesObservationReader(srv.URL, ca, tokenFile)
	if err != nil {
		t.Fatal(err)
	}
	for i, token := range []string{"test-token-a", "test-token-b"} {
		if err := os.WriteFile(tokenFile, []byte(token+"\n"), 0600); err != nil {
			t.Fatal(err)
		}
		expected.Store(token)
		got, err := reader.Collect(context.Background(), observedJobQuery())
		if err != nil || got.Stage != KueuePodPlaced || got.Evidence.Job.UID != "job-uid-a" {
			t.Fatalf("collection %d: stage=%s err=%v", i, got.Stage, err)
		}
		if got.Provenance.Source != "kubernetes-https-readonly-unattested" {
			t.Fatalf("source unexpectedly promoted: %q", got.Provenance.Source)
		}
	}
	if reads.Load() != 8 {
		t.Fatalf("expected exactly four read-only API GETs per observation, got %d", reads.Load())
	}
}

func TestServiceAccountReaderRejectsWrongCAAndUnusableCredentials(t *testing.T) {
	srv, _, _ := serviceAccountTestServer(t)
	defer srv.Close()
	ca, tokenFile := serviceAccountTestFiles(t, srv)

	tests := []struct {
		name string
		url  string
		ca   string
		key  string
	}{
		{"HTTP endpoint", strings.Replace(srv.URL, "https://", "http://", 1), ca, tokenFile},
		{"empty CA path", srv.URL, "", tokenFile},
		{"empty token path", srv.URL, ca, ""},
		{"nonexistent CA", srv.URL, filepath.Join(t.TempDir(), "missing"), tokenFile},
		{"nonexistent token", srv.URL, ca, filepath.Join(t.TempDir(), "missing")},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if _, err := NewServiceAccountKubernetesObservationReader(tc.url, tc.ca, tc.key); !errors.Is(err, ErrInvalidKueueObservation) {
				t.Fatalf("unsafe bootstrap accepted: %v", err)
			}
		})
	}

	invalidCA := filepath.Join(t.TempDir(), "not-ca.pem")
	if err := os.WriteFile(invalidCA, []byte("invalid cert"), 0600); err != nil {
		t.Fatal(err)
	}
	if _, err := NewServiceAccountKubernetesObservationReader(srv.URL, invalidCA, tokenFile); !errors.Is(err, ErrInvalidKueueObservation) {
		t.Fatalf("invalid CA accepted: %v", err)
	}
	for _, invalid := range []string{"", "abc def", "a\nb", "Bearer abc"} {
		if err := os.WriteFile(tokenFile, []byte(invalid), 0600); err != nil {
			t.Fatal(err)
		}
		if _, err := NewServiceAccountKubernetesObservationReader(srv.URL, ca, tokenFile); !errors.Is(err, ErrInvalidKueueObservation) {
			t.Fatalf("invalid ServiceAccount token accepted: %v", err)
		}
	}
}

func TestServiceAccountReaderRejectsTokenLossAndIncorrectRootWithoutLeakingToken(t *testing.T) {
	srv, expected, _ := serviceAccountTestServer(t)
	defer srv.Close()
	ca, tokenFile := serviceAccountTestFiles(t, srv)
	reader, err := NewServiceAccountKubernetesObservationReader(srv.URL, ca, tokenFile)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.Remove(tokenFile); err != nil {
		t.Fatal(err)
	}
	got, err := reader.Collect(context.Background(), observedJobQuery())
	if err == nil || got.Stage != KueueUnproven ||
		got.Evidence != (KueueJobEvidence{}) || got.Provenance != (KueueEvidenceProvenance{}) {
		t.Fatalf("missing token unexpectedly trusted: %+v err=%v", got, err)
	}
	if err := os.WriteFile(tokenFile, []byte("wrong-secret-token"), 0600); err != nil {
		t.Fatal(err)
	}
	expected.Store("test-token-a")
	got, err = reader.Collect(context.Background(), observedJobQuery())
	if err == nil || strings.Contains(err.Error(), "wrong-secret-token") ||
		got.Stage != KueueUnproven || got.Evidence != (KueueJobEvidence{}) {
		t.Fatalf("unauthorized token trusted or leaked: %+v err=%v", got, err)
	}

	// httptest.NewTLSServer reuses a built-in certificate. Generate an
	// independent, valid CA to test real TLS trust rejection rather than an
	// unrelated HTTP 403 from the test server.
	publicKey, privateKey, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	now := time.Now()
	certificate := &x509.Certificate{
		SerialNumber: big.NewInt(42),
		NotBefore: now.Add(-time.Minute),
		NotAfter: now.Add(time.Hour),
		IsCA: true,
		BasicConstraintsValid: true,
		KeyUsage: x509.KeyUsageCertSign,
	}
	der, err := x509.CreateCertificate(rand.Reader, certificate, certificate, publicKey, privateKey)
	if err != nil {
		t.Fatal(err)
	}
	wrongCA := filepath.Join(t.TempDir(), "unrelated-ca.crt")
	if err := os.WriteFile(wrongCA, pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der}), 0600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(tokenFile, []byte("test-token-a"), 0600); err != nil {
		t.Fatal(err)
	}
	wrongRootReader, err := NewServiceAccountKubernetesObservationReader(srv.URL, wrongCA, tokenFile)
	if err != nil {
		t.Fatal(err)
	}
	got, err = wrongRootReader.Collect(context.Background(), observedJobQuery())
	if err == nil || got.Stage != KueueUnproven || got.Evidence != (KueueJobEvidence{}) {
		t.Fatalf("TLS endpoint not rejected by independent CA pin: stage=%s err=%v", got.Stage, err)
	}
}
