package provider

import (
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"net/http"
	"os"
	"regexp"
	"strings"
	"time"
)

// serviceAccountTransport reads the projected ServiceAccount token afresh for
// every request so Kubernetes token rotations do not require a process restart.
// The token is never included in returned errors, evidence or source labels.
type serviceAccountTransport struct {
	tokenPath string
	transport http.RoundTripper
}

var serviceAccountTokenPattern = regexp.MustCompile(`^[A-Za-z0-9._~-]+$`)

func readServiceAccountToken(path string) (string, error) {
	if strings.TrimSpace(path) == "" {
		return "", ErrInvalidKueueObservation
	}
	contents, err := os.ReadFile(path)
	if err != nil || len(contents) > 16384 {
		return "", fmt.Errorf("%w: ServiceAccount token is unavailable or too large", ErrInvalidKueueObservation)
	}
	token := strings.TrimSpace(string(contents))
	if token == "" || !serviceAccountTokenPattern.MatchString(token) {
		return "", fmt.Errorf("%w: malformed ServiceAccount token", ErrInvalidKueueObservation)
	}
	return token, nil
}

func (t *serviceAccountTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	token, err := readServiceAccountToken(t.tokenPath)
	if err != nil {
		return nil, err
	}
	copyRequest := req.Clone(req.Context())
	copyRequest.Header.Set("Authorization", "Bearer "+token)
	return t.transport.RoundTrip(copyRequest)
}

// NewServiceAccountKubernetesObservationReader uses a dedicated HTTPS client,
// explicitly pinned to the supplied Kubernetes ServiceAccount CA bundle.
// Mounted tokens are re-read on every API GET; proxies and redirects are
// disabled to prevent credential forwarding. The caller must obtain the HTTPS
// API URL and mounted file paths from a trusted Pod configuration, and grant
// only GET Jobs / LIST Pods and Kueue Workloads in its namespace.
//
// This connection proves only a successful TLS/API read at the configured
// endpoint, not independent provider attestation, device allocation, resource
// cleanup finality or trusted production evidence. The Collector provenance
// remains marked UNATTESTED.
func NewServiceAccountKubernetesObservationReader(
	apiURL, caPath, tokenPath string,
) (*KubernetesObservationReader, error) {
	if strings.TrimSpace(caPath) == "" || strings.TrimSpace(tokenPath) == "" {
		return nil, ErrInvalidKueueObservation
	}
	if _, err := readServiceAccountToken(tokenPath); err != nil {
		return nil, err
	}
	caPEM, err := os.ReadFile(caPath)
	if err != nil || len(caPEM) == 0 || len(caPEM) > 1<<20 {
		return nil, fmt.Errorf("%w: Kubernetes CA bundle unavailable or invalid", ErrInvalidKueueObservation)
	}
	roots := x509.NewCertPool()
	if !roots.AppendCertsFromPEM(caPEM) {
		return nil, fmt.Errorf("%w: Kubernetes CA bundle contains no usable certificates", ErrInvalidKueueObservation)
	}

	transport := &http.Transport{
		Proxy: nil,
		TLSClientConfig: &tls.Config{
			RootCAs:    roots,
			MinVersion: tls.VersionTLS12,
		},
	}
	client := &http.Client{
		Timeout: 15 * time.Second,
		Transport: &serviceAccountTransport{
			tokenPath: tokenPath,
			transport: transport,
		},
	}
	reader, err := NewKubernetesObservationReader(apiURL, client)
	if err != nil {
		transport.CloseIdleConnections()
		return nil, err
	}
	return reader, nil
}
