package recovery_test

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/avaargsh/compute-control-panel/internal/operation"
	"github.com/avaargsh/compute-control-panel/internal/provider"
	"github.com/avaargsh/compute-control-panel/internal/recovery"
	"github.com/avaargsh/compute-control-panel/internal/store/postgres"
)

type unexpectedProviderInspect struct{ calls int }

func (*unexpectedProviderInspect) Apply(context.Context, provider.Request) (provider.Acknowledgement, error) {
	panic("recovery must not mutate provider during registration check")
}
func (a *unexpectedProviderInspect) Inspect(context.Context, provider.Request) (operation.Observation, error) {
	a.calls++
	return operation.Observation{}, errors.New("unexpected external Inspect")
}

func TestRecoverUnknownRejectsMismatchedDurableProviderFingerprintBeforeLease(t *testing.T) {
	for _, tc := range []struct {
		name    string
		source  string
		version string
		digest  string
	}{
		{
			name:    "source mismatch",
			source:  "another-provider-source",
			version: "v0-test",
			digest:  "sha256:" + strings.Repeat("a", 64),
		},
		{
			name:    "adapter version mismatch",
			source:  "fake-integration",
			version: "v99-unknown",
			digest:  "sha256:" + strings.Repeat("a", 64),
		},
		{
			name:    "capability digest mismatch",
			source:  "fake-integration",
			version: "v0-test",
			digest:  "sha256:" + strings.Repeat("b", 64),
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			_, repo, original := setupUnknown(t)
			ctx := context.Background()
			if _, created, err := repo.RegisterProvider(ctx, postgres.ProviderRegistration{
				ProviderID:       original.ProviderID,
				ClusterScope:     "cluster-test-recovery",
				AdapterSource:    tc.source,
				AdapterVersion:   tc.version,
				CapabilityDigest: tc.digest,
			}); err != nil || !created {
				t.Fatalf("register authoritative Provider: created=%v, err=%v", created, err)
			}
			inspector := &unexpectedProviderInspect{}
			worker, err := recovery.NewWorker(repo, adapterFor(inspector))
			if err != nil {
				t.Fatal(err)
			}
			_, err = worker.RecoverUnknown(ctx, original.TenantID, original.ID, "reconciler", time.Minute)
			if err == nil || !strings.Contains(err.Error(), "fingerprint mismatch") {
				t.Fatalf("mismatched registry allowed recovery or returned wrong error: %v", err)
			}
			if inspector.calls != 0 {
				t.Fatalf("unbound Provider was inspected before trust check: calls=%d", inspector.calls)
			}
			persisted, err := repo.GetOperation(ctx, original.TenantID, original.ID)
			if err != nil || persisted.Status != operation.Unknown ||
				persisted.Version != original.Version || persisted.FenceToken != original.FenceToken {
				t.Fatalf("mismatched Provider consumed recovery lease: operation=%+v err=%v", persisted, err)
			}
		})
	}
}
