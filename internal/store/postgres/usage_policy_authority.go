package postgres

import (
	"context"
	"errors"
	"fmt"
	"maps"
	"strings"
	"time"

	"github.com/avaargsh/compute-control-panel/internal/economics"
)

// ErrEvidencePolicyUntrusted is a hard deny: an E1 derivation may not become
// durable FINAL data without a current, scoped, independently supplied policy.
var ErrEvidencePolicyUntrusted = errors.New("E1 evidence policy is not authoritative")

// TrustedEvidencePolicy is supplied by a separately configured policy
// authority, NOT by Provider observations or the original E1 caller. This
// envelope is a trust decision for one tenant and Provider scope; it is not a
// signature or a claim that source observations were authenticated.
type TrustedEvidencePolicy struct {
	TenantID       string
	ProviderID     string
	Policy         economics.EvidencePolicy
	EffectiveFrom  time.Time
	EffectiveUntil time.Time
	Revoked        bool
}

// EvidencePolicyAuthority is the injected, trusted *reader* of approved policy
// snapshots. Implementations must verify version, tenant/Provider scope and
// revocation from their own authenticated registry, not echo caller input.
type EvidencePolicyAuthority interface {
	ResolveEvidencePolicy(ctx context.Context, tenantID, providerID, version string) (TrustedEvidencePolicy, error)
}

// EvidencePolicyAuthorityFunc is an adapter for immutable policy registry
// readers. Passing a caller-controlled closure defeats this trust boundary.
type EvidencePolicyAuthorityFunc func(context.Context, string, string, string) (TrustedEvidencePolicy, error)

func (fn EvidencePolicyAuthorityFunc) ResolveEvidencePolicy(ctx context.Context, tenantID, providerID, version string) (TrustedEvidencePolicy, error) {
	return fn(ctx, tenantID, providerID, version)
}

// resolveTrustedUsagePolicy must run BEFORE E1 FINAL derivation and persistence.
// The caller supplies only the requested version; every material allowlist,
// timestamp and skew value must match the independently resolved policy.
func resolveTrustedUsagePolicy(
	ctx context.Context, authority EvidencePolicyAuthority, p PersistUsageEvidenceParams, now time.Time,
) (economics.EvidencePolicy, error) {
	if authority == nil {
		return economics.EvidencePolicy{}, ErrEvidencePolicyUntrusted
	}
	tenant, provider, version := p.Evidence.TenantID, p.Evidence.ProviderID, p.Policy.Version
	if tenant == "" || provider == "" || version == "" ||
		strings.TrimSpace(tenant) != tenant || strings.TrimSpace(provider) != provider ||
		strings.TrimSpace(version) != version ||
		len(tenant) > 255 || len(provider) > 255 || len(version) > 255 {
		return economics.EvidencePolicy{}, fmt.Errorf("%w: invalid policy lookup scope", ErrEvidencePolicyUntrusted)
	}
	approved, err := authority.ResolveEvidencePolicy(ctx, tenant, provider, version)
	if err != nil {
		return economics.EvidencePolicy{}, fmt.Errorf("%w: policy lookup unavailable", ErrEvidencePolicyUntrusted)
	}
	policy := approved.Policy
	if approved.Revoked || approved.TenantID != tenant || approved.ProviderID != provider ||
		policy.Version != version || approved.EffectiveFrom.IsZero() ||
		approved.EffectiveUntil.IsZero() || approved.EffectiveUntil.Before(approved.EffectiveFrom) ||
		now.Before(approved.EffectiveFrom) || !now.Before(approved.EffectiveUntil) ||
		policy.AsOf.IsZero() || policy.AsOf.After(now) || policy.MaximumClockSkew < 0 ||
		!policy.AsOf.Equal(p.Policy.AsOf) ||
		policy.MaximumClockSkew != p.Policy.MaximumClockSkew ||
		!maps.Equal(policy.TrustedSourceTimestampAdapters, p.Policy.TrustedSourceTimestampAdapters) ||
		!maps.Equal(policy.CertifiedDeviceAdapters, p.Policy.CertifiedDeviceAdapters) {
		return economics.EvidencePolicy{}, fmt.Errorf("%w: policy not current or caller snapshot differs", ErrEvidencePolicyUntrusted)
	}
	// Freeze mutable maps returned by the authoritative adapter before using
	// them in E1 derivation and constructing the persisted audit envelope.
	policy.TrustedSourceTimestampAdapters = maps.Clone(policy.TrustedSourceTimestampAdapters)
	policy.CertifiedDeviceAdapters = maps.Clone(policy.CertifiedDeviceAdapters)
	return policy, nil
}
