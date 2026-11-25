// ccp-console is an opt-in local HTTPS API for a single administrator.
// It does not mount routes in the default CCP service or serve a Demo fallback.
package main

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/asn1"
	"errors"
	"flag"
	"log/slog"
	"net"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/avaargsh/compute-control-panel/internal/consoleauth"
	"github.com/avaargsh/compute-control-panel/internal/kubeinventory"
)

type liveConfig struct {
	Auth      consoleauth.Config
	Inventory kubeinventory.Config
}

func newLiveHandler(cfg liveConfig) (http.Handler, error) {
	if cfg.Auth.TenantID != cfg.Inventory.TenantID || cfg.Auth.ProjectID != cfg.Inventory.ProjectID {
		return nil, errors.New("console session and inventory scope must match")
	}
	cfg.Inventory.Now = cfg.Auth.Now
	reader, err := kubeinventory.NewReader(cfg.Inventory)
	if err != nil {
		return nil, err
	}
	return consoleauth.NewHandler(cfg.Auth, reader)
}

const consoleHost = "127.0.0.1"

var oidKeyUsage = asn1.ObjectIdentifier{2, 5, 29, 15}

func validateListen(addr string) error {
	host, port, err := net.SplitHostPort(addr)
	if err != nil || host != consoleHost {
		return errors.New("console must bind explicitly to 127.0.0.1")
	}
	n, err := strconv.Atoi(port)
	if err != nil || n < 1 || n > 65535 || strconv.Itoa(n) != port {
		return errors.New("console requires an explicit valid port")
	}
	return nil
}

// This checks the presented leaf, not browser trust or the full issuing chain.
func loadConsoleCertificate(certFile, keyFile string, now time.Time) (tls.Certificate, error) {
	cert, err := tls.LoadX509KeyPair(certFile, keyFile)
	if err != nil {
		return tls.Certificate{}, errors.New("console TLS credentials unavailable")
	}
	leaf := cert.Leaf
	if leaf == nil {
		leaf, err = x509.ParseCertificate(cert.Certificate[0])
		if err != nil {
			return tls.Certificate{}, errors.New("console TLS certificate is invalid")
		}
	}
	if leaf.VerifyHostname(consoleHost) != nil {
		return tls.Certificate{}, errors.New("console TLS certificate is not valid for " + consoleHost + "; include this IP in the subject alternative names")
	}
	if now.Before(leaf.NotBefore) || now.After(leaf.NotAfter) {
		return tls.Certificate{}, errors.New("console TLS certificate is outside its validity period")
	}
	// TLS 1.3 uses a certificate to sign the handshake (RFC 8446 §4.4.2.2).
	for _, ext := range leaf.Extensions {
		if ext.Id.Equal(oidKeyUsage) && leaf.KeyUsage&x509.KeyUsageDigitalSignature == 0 {
			return tls.Certificate{}, errors.New("console TLS certificate is not valid for digital signatures")
		}
	}
	if len(leaf.ExtKeyUsage)+len(leaf.UnknownExtKeyUsage) != 0 {
		for _, usage := range leaf.ExtKeyUsage {
			if usage == x509.ExtKeyUsageServerAuth || usage == x509.ExtKeyUsageAny {
				return cert, nil
			}
		}
		return tls.Certificate{}, errors.New("console TLS certificate is not valid for server authentication")
	}
	return cert, nil
}

// prepareConsole validates the same inputs for startup and offline preflight.
// It constructs the handlers without binding a listener or calling Kubernetes.
func prepareConsole(addr, certFile, keyFile string, cfg liveConfig) (tls.Certificate, http.Handler, error) {
	if err := validateListen(addr); err != nil {
		return tls.Certificate{}, nil, err
	}
	if certFile == "" || keyFile == "" || cfg.Auth.Origin != "https://"+addr {
		return tls.Certificate{}, nil, errors.New("console requires explicit TLS certificate, key and matching HTTPS origin")
	}
	cert, err := loadConsoleCertificate(certFile, keyFile, time.Now())
	if err != nil {
		return tls.Certificate{}, nil, err
	}
	h, err := newLiveHandler(cfg)
	if err != nil {
		return tls.Certificate{}, nil, err
	}
	return cert, h, nil
}

func run(ctx context.Context, addr, certFile, keyFile string, cfg liveConfig, logger *slog.Logger) error {
	cert, h, err := prepareConsole(addr, certFile, keyFile, cfg)
	if err != nil {
		return err
	}
	ln, err := net.Listen("tcp", addr)
	if err != nil {
		return err
	}
	defer ln.Close()
	return serveConsole(ctx, ln, cert, h, logger)
}

// serveConsole is shared by the opt-in command and real TLS acceptance tests.
func serveConsole(ctx context.Context, ln net.Listener, cert tls.Certificate, h http.Handler, logger *slog.Logger) error {
	srv := &http.Server{
		Handler:           h,
		TLSConfig:         &tls.Config{MinVersion: tls.VersionTLS13, Certificates: []tls.Certificate{cert}},
		ReadHeaderTimeout: 5 * time.Second,
		ReadTimeout:       10 * time.Second,
		WriteTimeout:      30 * time.Second,
		IdleTimeout:       30 * time.Second,
		MaxHeaderBytes:    1 << 16,
	}
	done := make(chan error, 1)
	logger.Info("console HTTPS listener bound", "url", "https://"+ln.Addr().String())
	go func() { done <- srv.ServeTLS(ln, "", "") }()
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
	addr := flag.String("addr", consoleHost+":8443", "explicit loopback HTTPS address")
	cert := flag.String("tls-cert", "", "TLS certificate file")
	key := flag.String("tls-key", "", "TLS private key file")
	user := flag.String("admin-user", "", "configured administrator username")
	hashFile := flag.String("admin-bcrypt-file", "", "file containing pre-provisioned bcrypt verifier")
	tenant := flag.String("tenant", "", "trusted tenant ID")
	project := flag.String("project", "", "trusted project ID")
	cluster := flag.String("cluster", "", "trusted cluster ID")
	api := flag.String("kube-api", "", "Kubernetes HTTPS origin")
	ca := flag.String("kube-ca", "", "Kubernetes CA file")
	token := flag.String("kube-token-file", "", "rotatable read-only ServiceAccount token file")
	resources := flag.String("gpu-resources", "", "comma-separated explicit extended resource names")
	checkConfig := flag.Bool("check-config", false, "validate local configuration without listening or contacting Kubernetes")
	flag.Parse()

	// Secret material is read from files, never command-line passwords.
	hash, err := os.ReadFile(*hashFile)
	if err != nil || len(hash) > 256 {
		slog.Error("console bcrypt verifier unavailable")
		os.Exit(1)
	}
	cfg := liveConfig{
		Auth: consoleauth.Config{
			Username: *user, PasswordBcryptHash: []byte(strings.TrimSpace(string(hash))),
			TenantID: *tenant, ProjectID: *project, Origin: "https://" + *addr,
			SessionTTL: time.Hour, Now: time.Now,
		},
		Inventory: kubeinventory.Config{
			APIServer: *api, CAFile: *ca, TokenFile: *token, ClusterID: *cluster,
			TenantID: *tenant, ProjectID: *project, ResourceNames: strings.Split(*resources, ","),
			ObservationTTL: time.Minute, Timeout: 3 * time.Second,
		},
	}
	if *checkConfig {
		if _, _, err := prepareConsole(*addr, *cert, *key, cfg); err != nil {
			slog.Error("console configuration invalid", "error", err)
			os.Exit(1)
		}
		slog.Info("console local configuration valid (no listener or Kubernetes request)")
		return
	}
	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()
	if err := run(ctx, *addr, *cert, *key, cfg, slog.Default()); err != nil {
		slog.Error("console stopped", "error", err)
		os.Exit(1)
	}
}
