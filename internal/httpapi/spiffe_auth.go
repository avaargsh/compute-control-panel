package httpapi

import (
	"context"
	"crypto/x509"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"strings"
)

var ErrWorkloadAuthentication = errors.New("workload service identity authentication failed")

// WorkloadIdentityResolver maps an already TLS-verified SPIFFE ID to the
// authoritative Tenant, Service and Project grants maintained by the identity
// directory. It must perform an exact identity lookup and return Subject equal
// to the supplied SPIFFE ID.
type WorkloadIdentityResolver interface {
	ResolveWorkloadPrincipal(context.Context, string) (WorkloadPrincipal, error)
}

// SPIFFEWorkloadAuthenticator authenticates a workload from a client
// certificate whose chain has already been verified by net/http's TLS server.
// Configure tls.Config.ClientAuth = tls.RequireAndVerifyClientCert and a
// narrowly scoped ClientCAs bundle before wiring this authenticator.
type SPIFFEWorkloadAuthenticator struct {
	trustDomain string
	resolver    WorkloadIdentityResolver
}

func NewSPIFFEWorkloadAuthenticator(trustDomain string, resolver WorkloadIdentityResolver) (*SPIFFEWorkloadAuthenticator, error) {
	domain := strings.TrimSpace(strings.ToLower(trustDomain))
	if domain == "" || strings.ContainsAny(domain, "/:@?#") || strings.Contains(domain, "..") {
		return nil, errors.New("a valid SPIFFE trust domain is required")
	}
	if resolver == nil {
		return nil, errors.New("a workload identity resolver is required")
	}
	return &SPIFFEWorkloadAuthenticator{trustDomain: domain, resolver: resolver}, nil
}

func (a *SPIFFEWorkloadAuthenticator) Authenticate(ctx context.Context, request *http.Request) (WorkloadPrincipal, error) {
	if request == nil || request.TLS == nil ||
		len(request.TLS.PeerCertificates) == 0 ||
		len(request.TLS.VerifiedChains) == 0 ||
		len(request.TLS.VerifiedChains[0]) == 0 {
		return WorkloadPrincipal{}, ErrWorkloadAuthentication
	}
	leaf := request.TLS.PeerCertificates[0]
	verifiedLeaf := request.TLS.VerifiedChains[0][0]
	if leaf == nil || verifiedLeaf == nil || !sameCertificate(leaf, verifiedLeaf) {
		return WorkloadPrincipal{}, ErrWorkloadAuthentication
	}

	spiffeID, err := uniqueSPIFFEID(leaf.URIs, a.trustDomain)
	if err != nil {
		return WorkloadPrincipal{}, ErrWorkloadAuthentication
	}
	principal, err := a.resolver.ResolveWorkloadPrincipal(ctx, spiffeID)
	if err != nil || !validWorkloadPrincipal(principal) || principal.Subject != spiffeID {
		return WorkloadPrincipal{}, ErrWorkloadAuthentication
	}

	// Do not leak mutable resolver-owned slices into authorization handlers.
	principal.ProjectIDs = append([]string(nil), principal.ProjectIDs...)
	return principal, nil
}

func sameCertificate(a, b *x509.Certificate) bool {
	return a != nil && b != nil && len(a.Raw) > 0 && string(a.Raw) == string(b.Raw)
}

func uniqueSPIFFEID(uris []*url.URL, trustDomain string) (string, error) {
	var found string
	for _, uri := range uris {
		if uri == nil || uri.Scheme != "spiffe" {
			continue
		}
		if uri.Host == "" || uri.User != nil || uri.RawQuery != "" ||
			uri.Fragment != "" || uri.Opaque != "" || uri.Path == "" ||
			!strings.HasPrefix(uri.Path, "/") {
			return "", fmt.Errorf("malformed SPIFFE URI")
		}
		if uri.Host != trustDomain {
			return "", fmt.Errorf("SPIFFE trust domain mismatch")
		}
		for _, segment := range strings.Split(strings.TrimPrefix(uri.Path, "/"), "/") {
			if segment == "" || segment == "." || segment == ".." {
				return "", fmt.Errorf("malformed SPIFFE path")
			}
		}
		if found != "" {
			return "", fmt.Errorf("multiple SPIFFE identities in certificate")
		}
		found = uri.String()
	}
	if found == "" {
		return "", fmt.Errorf("certificate has no SPIFFE identity")
	}
	return found, nil
}
