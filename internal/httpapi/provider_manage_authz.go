package httpapi

import (
	"context"
	"errors"
	"net/url"
	"strings"
)

var ErrProviderManagementDenied = errors.New("provider management authorization denied")

// ProviderManagementGrant binds one explicitly configured Provider action
// to one verified SPIFFE principal resolved from the trusted identity directory.
// Neither Service nor Project grants alone imply Provider administrator rights.
type ProviderManagementGrant struct {
	Subject    string
	TenantID   string
	Service    string
	ProviderID string
	Action     string
}

// ExactProviderManagementAuthorizer is a deliberately small, immutable
// bootstrap policy. Grants are supplied by a trusted deployment component;
// this type does not load policy from request fields, headers or Project grants.
// Replacing or revoking a grant requires updating that trusted configuration.
// Identity-directory revocation is still checked on every authentication.
type ExactProviderManagementAuthorizer struct {
	grants map[ProviderManagementGrant]struct{}
}

func validProviderManagementGrant(grant ProviderManagementGrant) bool {
	if grant.Subject == "" || strings.Contains(grant.Subject, "*") ||
		!validProviderManageLabel(grant.TenantID) ||
		!validProviderManageLabel(grant.Service) ||
		strings.ContainsAny(grant.TenantID, "*?") ||
		strings.ContainsAny(grant.Service, "*?") ||
		!validProviderManageID(grant.ProviderID) {
		return false
	}
	switch grant.Action {
	case "register", "read", "drain":
	default:
		return false
	}
	uri, err := url.Parse(grant.Subject)
	if err != nil || uri.Scheme != "spiffe" || uri.Host == "" ||
		uri.Host != strings.ToLower(uri.Host) || uri.String() != grant.Subject {
		return false
	}
	canonical, err := uniqueSPIFFEID([]*url.URL{uri}, uri.Host)
	return err == nil && canonical == grant.Subject
}

// NewExactProviderManagementAuthorizer rejects missing, ambiguous or duplicate
// grant configurations rather than constructing a permissive fallback.
func NewExactProviderManagementAuthorizer(grants []ProviderManagementGrant) (*ExactProviderManagementAuthorizer, error) {
	if len(grants) == 0 {
		return nil, errors.New("Provider management requires explicit grants")
	}
	policy := &ExactProviderManagementAuthorizer{grants: make(map[ProviderManagementGrant]struct{}, len(grants))}
	for _, grant := range grants {
		if !validProviderManagementGrant(grant) {
			return nil, errors.New("invalid Provider management grant")
		}
		if _, duplicate := policy.grants[grant]; duplicate {
			return nil, errors.New("duplicate Provider management grant")
		}
		policy.grants[grant] = struct{}{}
	}
	return policy, nil
}

func (p *ExactProviderManagementAuthorizer) AuthorizeProviderManage(
	_ context.Context, principal WorkloadPrincipal, action, providerID string,
) error {
	if p == nil {
		return ErrProviderManagementDenied
	}
	grant := ProviderManagementGrant{
		Subject:    principal.Subject,
		TenantID:   principal.TenantID,
		Service:    principal.Service,
		ProviderID: providerID,
		Action:     action,
	}
	if _, allowed := p.grants[grant]; !allowed {
		return ErrProviderManagementDenied
	}
	return nil
}

var _ ProviderManagementAuthorizer = (*ExactProviderManagementAuthorizer)(nil)
