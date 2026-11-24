package hostidentity

import (
	"bytes"
	"context"
	"crypto/x509"
	"errors"
	"net/http"
	"net/url"
	"reflect"
	"strings"
	"time"
)

var ErrHostAuthentication = errors.New("host identity authentication failed")

// HostIdentityResolver resolves an already verified host SPIFFE subject to
// exactly one immutable HostIdentity binding. It must not infer identity from
// a HostID, Node name, request header, or caller-supplied binding tuple.
type HostIdentityResolver interface {
	ResolveHostBinding(context.Context, string) (Binding, error)
}

// HostSPIFFEAuthenticator verifies a TLS-authenticated host identity and
// resolves its immutable binding. It is deliberately separate from workload
// Tenant/Project authorization.
//
// This primitive does not make revocation atomic with a later database write.
// Callers must recheck the active authorization/revocation fence in the same
// transaction as session start or heartbeat persistence. It is not a listener
// and must not be treated as machine attestation or GPU readiness.
type HostSPIFFEAuthenticator struct {
	trustDomain string
	resolver    HostIdentityResolver
	now         func() time.Time
}

// NewHostSPIFFEAuthenticator constructs a fail-closed host certificate
// authenticator. The clock is explicit so certificate validity can be checked
// on every request, including requests on a TLS connection that outlives a cert.
func NewHostSPIFFEAuthenticator(
	trustDomain string,
	resolver HostIdentityResolver,
	now func() time.Time,
) (*HostSPIFFEAuthenticator, error) {
	domain := strings.ToLower(strings.TrimSpace(trustDomain))
	if !validSPIFFETrustDomain(domain) || isNilHostResolver(resolver) || now == nil {
		return nil, errors.New("host SPIFFE authentication requires a valid trust domain, resolver and clock")
	}
	return &HostSPIFFEAuthenticator{trustDomain: domain, resolver: resolver, now: now}, nil
}

func isNilHostResolver(resolver HostIdentityResolver) bool {
	if resolver == nil {
		return true
	}
	value := reflect.ValueOf(resolver)
	switch value.Kind() {
	case reflect.Chan, reflect.Func, reflect.Interface, reflect.Map, reflect.Pointer, reflect.Slice:
		return value.IsNil()
	default:
		return false
	}
}

func validSPIFFETrustDomain(domain string) bool {
	if domain == "" || len(domain) > 253 ||
		strings.ContainsAny(domain, "/:@?#") ||
		strings.Contains(domain, "..") ||
		strings.HasPrefix(domain, ".") ||
		strings.HasSuffix(domain, ".") {
		return false
	}
	for _, label := range strings.Split(domain, ".") {
		if label == "" || len(label) > 63 || label[0] == '-' || label[len(label)-1] == '-' {
			return false
		}
		for _, r := range label {
			if !(r >= 'a' && r <= 'z' || r >= '0' && r <= '9' || r == '-') {
				return false
			}
		}
	}
	return true
}

func validHostBinding(binding Binding) bool {
	return validHostIdentityField(binding.HostID, 256) &&
		validHostIdentityField(binding.ProviderID, 256) &&
		validHostIdentityField(binding.ClusterScope, 1024) &&
		validHostIdentityField(binding.NodeUID, 256) &&
		validHostIdentityField(binding.Subject, 1024)
}

func validHostIdentityField(value string, maxBytes int) bool {
	return len(value) > 0 && len(value) <= maxBytes && validIdentity(value)
}

// Authenticate accepts only a TLS peer certificate that net/http reports in a
// verified chain. The exact verified leaf is checked again, then its one
// canonical host-class SPIFFE URI is resolved to the immutable binding.
func (a *HostSPIFFEAuthenticator) Authenticate(ctx context.Context, request *http.Request) (Binding, error) {
	if a == nil || a.resolver == nil || a.now == nil || ctx == nil ||
		request == nil || request.TLS == nil ||
		len(request.TLS.PeerCertificates) == 0 ||
		len(request.TLS.VerifiedChains) == 0 ||
		len(request.TLS.VerifiedChains[0]) == 0 {
		return Binding{}, ErrHostAuthentication
	}
	leaf := request.TLS.PeerCertificates[0]
	verifiedLeaf := request.TLS.VerifiedChains[0][0]
	if leaf == nil || verifiedLeaf == nil || len(leaf.Raw) == 0 ||
		!bytes.Equal(leaf.Raw, verifiedLeaf.Raw) {
		return Binding{}, ErrHostAuthentication
	}

	now := a.now().UTC()
	if now.IsZero() || leaf.NotBefore.IsZero() || leaf.NotAfter.IsZero() ||
		now.Before(leaf.NotBefore) || !now.Before(leaf.NotAfter) {
		return Binding{}, ErrHostAuthentication
	}
	subject, err := uniqueHostSPIFFEID(leaf.URIs, a.trustDomain)
	if err != nil {
		return Binding{}, ErrHostAuthentication
	}
	binding, err := a.resolver.ResolveHostBinding(ctx, subject)
	if err != nil || !validHostBinding(binding) || binding.Subject != subject {
		return Binding{}, ErrHostAuthentication
	}
	return binding, nil
}

func uniqueHostSPIFFEID(uris []*url.URL, trustDomain string) (string, error) {
	var found string
	for _, uri := range uris {
		if uri == nil || uri.Scheme != "spiffe" {
			continue
		}
		if uri.Host != trustDomain || uri.User != nil ||
			uri.RawQuery != "" || uri.ForceQuery ||
			uri.Fragment != "" || uri.RawFragment != "" ||
			uri.Opaque != "" || uri.RawPath != "" ||
			uri.Path == "" || !strings.HasPrefix(uri.Path, "/") ||
			uri.EscapedPath() != uri.Path {
			return "", errors.New("malformed or foreign host SPIFFE URI")
		}
		segments := strings.Split(strings.TrimPrefix(uri.Path, "/"), "/")
		if len(segments) < 2 || segments[0] != "host" {
			return "", errors.New("SPIFFE URI is not a host identity")
		}
		for _, segment := range segments {
			if segment == "" || segment == "." || segment == ".." {
				return "", errors.New("non-canonical host SPIFFE path")
			}
			for _, r := range segment {
				if !(r >= 'a' && r <= 'z' || r >= 'A' && r <= 'Z' ||
					r >= '0' && r <= '9' || r == '-' || r == '_' || r == '.') {
					return "", errors.New("unsupported host SPIFFE path character")
				}
			}
		}
		canonical := "spiffe://" + trustDomain + uri.Path
		if uri.String() != canonical || found != "" {
			return "", errors.New("ambiguous or non-canonical host SPIFFE identity")
		}
		found = canonical
	}
	if found == "" {
		return "", errors.New("host SPIFFE identity is missing")
	}
	return found, nil
}
