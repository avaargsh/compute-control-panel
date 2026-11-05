package postgres

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/avaargsh/compute-control-panel/internal/economics"
)

// The constructor with no trusted policy source must never convert caller
// configured adapter allowlists into authoritative E1 FINAL evidence.
func TestEvidencePolicyAuthorityFailsClosed(t *testing.T) {
	policy := economics.EvidencePolicy{
		Version: "e1-approved-v1",
		AsOf: time.Date(2026, 11, 4, 12, 10, 0, 0, time.UTC),
		MaximumClockSkew: 2 * time.Minute,
		TrustedSourceTimestampAdapters: map[string]bool{"provider@v1": true, "dra@v1": true},
		CertifiedDeviceAdapters: map[string]bool{},
	}
	p := PersistUsageEvidenceParams{
		Evidence: economics.AllocationUsageEvidence{TenantID: "tenant-a", ProviderID: "provider-a"},
		Policy: policy,
	}
	ctx := context.Background()
	now := time.Now().UTC()
	approved := TrustedEvidencePolicy{
		TenantID: "tenant-a", ProviderID: "provider-a", Policy: policy,
		EffectiveFrom: now.Add(-time.Hour), EffectiveUntil: now.Add(time.Hour),
	}
	tests := []struct {
		name string
		authority EvidencePolicyAuthority
		params PersistUsageEvidenceParams
	}{
		{"missing policy authority", nil, p},
		{"authority outage", EvidencePolicyAuthorityFunc(func(context.Context,string,string,string)(TrustedEvidencePolicy,error){
			return TrustedEvidencePolicy{}, errors.New("policy service offline")
		}), p},
		{"wrong tenant", staticEvidencePolicyAuthority{policy: func() TrustedEvidencePolicy { x:=approved; x.TenantID="tenant-b"; return x }()}, p},
		{"wrong Provider", staticEvidencePolicyAuthority{policy: func() TrustedEvidencePolicy { x:=approved; x.ProviderID="provider-b"; return x }()}, p},
		{"revoked policy", staticEvidencePolicyAuthority{policy: func() TrustedEvidencePolicy { x:=approved; x.Revoked=true; return x }()}, p},
		{"expired policy", staticEvidencePolicyAuthority{policy: func() TrustedEvidencePolicy { x:=approved; x.EffectiveUntil=now.Add(-time.Minute); return x }()}, p},
		{"future effective policy", staticEvidencePolicyAuthority{policy: func() TrustedEvidencePolicy { x:=approved; x.EffectiveFrom=now.Add(time.Minute); return x }()}, p},
		{"version mismatch", staticEvidencePolicyAuthority{policy: func() TrustedEvidencePolicy { x:=approved; x.Policy.Version="v2"; return x }()}, p},
		{"caller adds certified adapter", staticEvidencePolicyAuthority{policy: approved}, func() PersistUsageEvidenceParams{
			x:=p; x.Policy.CertifiedDeviceAdapters=map[string]bool{"forged@device@v1": true}; return x
		}()},
		{"caller extends trusted timestamp sources", staticEvidencePolicyAuthority{policy: approved}, func() PersistUsageEvidenceParams{
			x:=p; x.Policy.TrustedSourceTimestampAdapters=map[string]bool{"provider@v1":true,"dra@v1":true,"kueue@v2":true}; return x
		}()},
		{"caller raises clock skew", staticEvidencePolicyAuthority{policy: approved}, func() PersistUsageEvidenceParams{
			x:=p; x.Policy.MaximumClockSkew=24*time.Hour; return x
		}()},
		{"caller changes AsOf", staticEvidencePolicyAuthority{policy: approved}, func() PersistUsageEvidenceParams{
			x:=p; x.Policy.AsOf=x.Policy.AsOf.Add(time.Hour); return x
		}()},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if _, err := resolveTrustedUsagePolicy(ctx, tc.authority, tc.params, now); !errors.Is(err, ErrEvidencePolicyUntrusted) {
				t.Fatalf("untrusted policy accepted: %v", err)
			}
		})
	}
	got, err := resolveTrustedUsagePolicy(ctx, staticEvidencePolicyAuthority{policy: approved}, p, now)
	if err != nil || got.Version != policy.Version || !got.TrustedSourceTimestampAdapters["provider@v1"] {
		t.Fatalf("reviewed policy rejected: got=%+v err=%v", got, err)
	}
}

type staticEvidencePolicyAuthority struct { policy TrustedEvidencePolicy }
func (a staticEvidencePolicyAuthority) ResolveEvidencePolicy(_ context.Context, _, _, _ string) (TrustedEvidencePolicy,error) {
	return a.policy,nil
}
