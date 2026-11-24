package httpapi

import (
	"context"
	"errors"
	"time"

	"github.com/avaargsh/compute-control-panel/internal/provider"
)

// DRAReceiptVerifierSource resolves an independently DBA-registered signer.
// It must not inspect historical Kubernetes, Kueue, Pod, Allocation or Claim
// evidence; those reads belong to DRAReceiptEvidenceResolver after signature
// and freshness verification.
type DRAReceiptVerifierSource interface {
	ResolveRegisteredDRAUnprepareVerifier(context.Context, string, provider.DRAUnprepareEvent) (*provider.DRAUnprepareVerifier, time.Time, error)
}

// RegisteredDRAReceiptVerifierResolver adapts the restricted PostgreSQL
// source registry to the HTTP ingress contract. The tenant scope is taken
// only from the already authenticated workload principal, never from JSON.
type RegisteredDRAReceiptVerifierResolver struct {
	source DRAReceiptVerifierSource
}

func NewRegisteredDRAReceiptVerifierResolver(source DRAReceiptVerifierSource) (*RegisteredDRAReceiptVerifierResolver, error) {
	if missingProviderManagementDependency(source) {
		return nil, errors.New("DRA receipt verifier requires a restricted registered-source reader")
	}
	return &RegisteredDRAReceiptVerifierResolver{source: source}, nil
}

func (r *RegisteredDRAReceiptVerifierResolver) ResolveDRAReceiptVerifier(
	ctx context.Context, principal WorkloadPrincipal, event provider.DRAUnprepareEvent,
) (DRAReceiptVerifierResolution, error) {
	if r == nil || missingProviderManagementDependency(r.source) || ctx == nil ||
		!validWorkloadPrincipal(principal) || principal.Service != "dra-node-driver" {
		return DRAReceiptVerifierResolution{}, provider.ErrDRAReceiptSourceUntrusted
	}
	verifier, databaseNow, err := r.source.ResolveRegisteredDRAUnprepareVerifier(ctx, principal.TenantID, event)
	if err != nil {
		return DRAReceiptVerifierResolution{}, err
	}
	if verifier == nil || databaseNow.IsZero() {
		return DRAReceiptVerifierResolution{}, provider.ErrDRAReceiptJournalUnavailable
	}
	return DRAReceiptVerifierResolution{Verifier: verifier, DatabaseNow: databaseNow}, nil
}

var _ DRAReceiptVerifierResolver = (*RegisteredDRAReceiptVerifierResolver)(nil)
var _ DRAReceiptVerifierSource = (*provider.DRAUnprepareReceiptJournal)(nil)
