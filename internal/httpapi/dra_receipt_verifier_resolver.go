package httpapi

import (
	"context"
	"errors"
	"time"

	"github.com/avaargsh/compute-control-panel/internal/provider"
)

// registeredDRAReceiptVerifierSource resolves an independently DBA-registered
// signer. It must not inspect historical Kubernetes, Kueue, Pod, Allocation or
// Claim evidence; those reads belong to DRAReceiptEvidenceResolver after
// signature and freshness verification. Keep this interface package-private:
// production construction below accepts only the restricted journal type.
type registeredDRAReceiptVerifierSource interface {
	ResolveRegisteredDRAUnprepareVerifier(context.Context, string, provider.DRAUnprepareEvent) (*provider.DRAUnprepareVerifier, time.Time, error)
}

// RegisteredDRAReceiptVerifierResolver adapts the restricted PostgreSQL
// source registry to the HTTP ingress contract. The tenant scope is taken
// only from the already authenticated workload principal, never from JSON.
type RegisteredDRAReceiptVerifierResolver struct {
	source registeredDRAReceiptVerifierSource
}

// NewRegisteredDRAReceiptVerifierResolver accepts only the journal type whose
// ResolveRegisteredDRAUnprepareVerifier method rejects the unchecked journal
// constructor. This prevents production wiring from injecting a caller-chosen
// key resolver that merely satisfies a public interface.
func NewRegisteredDRAReceiptVerifierResolver(journal *provider.DRAUnprepareReceiptJournal) (*RegisteredDRAReceiptVerifierResolver, error) {
	if journal == nil {
		return nil, errors.New("DRA receipt verifier requires a restricted PostgreSQL receipt journal")
	}
	return &RegisteredDRAReceiptVerifierResolver{source: journal}, nil
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
var _ registeredDRAReceiptVerifierSource = (*provider.DRAUnprepareReceiptJournal)(nil)
