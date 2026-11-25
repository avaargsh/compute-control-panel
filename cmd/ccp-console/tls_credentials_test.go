package main

import (
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"crypto/x509"
	"encoding/pem"
	"math/big"
	"net"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func writeFixtureTLS(t *testing.T, cert *x509.Certificate) (string, string) {
	t.Helper()
	pub, key, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	der, err := x509.CreateCertificate(rand.Reader, cert, cert, pub, key)
	if err != nil {
		t.Fatal(err)
	}
	keyDER, err := x509.MarshalPKCS8PrivateKey(key)
	if err != nil {
		t.Fatal(err)
	}
	dir := t.TempDir()
	certPath, keyPath := filepath.Join(dir, "cert.pem"), filepath.Join(dir, "key.pem")
	if err := os.WriteFile(certPath, pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der}), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(keyPath, pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: keyDER}), 0o600); err != nil {
		t.Fatal(err)
	}
	return certPath, keyPath
}

func TestRunRejectsUnusableServingCertificateBeforeStarting(t *testing.T) {
	now := time.Now()
	addr := "127.0.0.1:8443"
	var cfg liveConfig
	cfg.Auth.Origin = "https://" + addr
	for _, tc := range []struct {
		name, diagnostic string
		modify           func(*x509.Certificate)
	}{
		{"DNS only", "not valid for 127.0.0.1", func(c *x509.Certificate) { c.DNSNames, c.IPAddresses = []string{"localhost"}, nil }},
		{"wrong IP", "not valid for 127.0.0.1", func(c *x509.Certificate) { c.IPAddresses = []net.IP{net.ParseIP("127.0.0.2")} }},
		{"expired", "outside its validity period", func(c *x509.Certificate) { c.NotBefore, c.NotAfter = now.Add(-2*time.Hour), now.Add(-time.Hour) }},
		{"not yet valid", "outside its validity period", func(c *x509.Certificate) { c.NotBefore, c.NotAfter = now.Add(time.Hour), now.Add(2*time.Hour) }},
		{"key encipherment only", "not valid for digital signatures", func(c *x509.Certificate) { c.KeyUsage = x509.KeyUsageKeyEncipherment }},
		{"client-only extended key usage", "not valid for server authentication", func(c *x509.Certificate) { c.ExtKeyUsage = []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth} }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			cert := x509.Certificate{
				SerialNumber: big.NewInt(1), IPAddresses: []net.IP{net.ParseIP("127.0.0.1")},
				NotBefore: now.Add(-time.Hour), NotAfter: now.Add(time.Hour),
				ExtKeyUsage: []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
			}
			tc.modify(&cert)
			certPath, keyPath := writeFixtureTLS(t, &cert)
			err := run(context.Background(), addr, certPath, keyPath, cfg)
			if err == nil || !strings.Contains(err.Error(), tc.diagnostic) {
				t.Fatalf("got %v, want certificate diagnostic %q before using inventory", err, tc.diagnostic)
			}
		})
	}
}

func TestLoadConsoleCertificateAllowsUsableTLS13Leaf(t *testing.T) {
	now := time.Now()
	for _, tc := range []struct {
		name     string
		usage    x509.KeyUsage
		extended []x509.ExtKeyUsage
	}{
		{name: "no optional usage extensions"},
		{name: "digital signature and server auth", usage: x509.KeyUsageDigitalSignature, extended: []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			certPath, keyPath := writeFixtureTLS(t, &x509.Certificate{
				SerialNumber: big.NewInt(1), IPAddresses: []net.IP{net.ParseIP("127.0.0.1")},
				NotBefore: now.Add(-time.Hour), NotAfter: now.Add(time.Hour),
				KeyUsage: tc.usage, ExtKeyUsage: tc.extended,
			})
			if _, err := loadConsoleCertificate(certPath, keyPath, now); err != nil {
				t.Fatal(err)
			}
		})
	}
}
