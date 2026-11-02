// Package recovery coordinates inspect-first recovery of durable provider
// operations. It does not schedule GPU resources, issue new provider mutations,
// or expose an HTTP endpoint for caller-supplied inspection evidence.
package recovery

import (
	"context"
	"crypto/rand"
	"database/sql"
	"encoding/hex"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/avaargsh/compute-control-panel/internal/operation"
	"github.com/avaargsh/compute-control-panel/internal/provider"
	"github.com/avaargsh/compute-control-panel/internal/store/postgres"
)

var (
	ErrUnregisteredProvider = errors.New("compute provider is not registered for recovery")
	ErrInvalidConfiguration = errors.New("invalid recovery worker configuration")
)

// Journal is the durable ComputeOperation authority. The worker never writes
// operation state directly and never treats an external acknowledgement as
// proof of the provider resource's final state.
type Journal interface {
	GetOperation(context.Context, string, string) (operation.Operation, error)
	ExpireOperationLease(context.Context, string, string, uint64) (operation.Operation, error)
	BeginOperationReconciliation(context.Context, string, string, uint64, string, uint64, string, time.Duration) (operation.Operation, error)
	ReconcileOperation(context.Context, string, string, uint64, string, uint64, postgres.InspectionEvidence) (operation.Operation, error)
}

// RegisteredProvider is configured by a trusted bootstrap, not by an API
// caller. Source and adapter version are audit labels, not cryptographic
// attestations; real provider authentication remains a production gate.
type RegisteredProvider struct {
	Adapter provider.Adapter
	Source string
	AdapterVersion string
}

type Worker struct {
	journal Journal
	providers map[string]RegisteredProvider
}

func NewWorker(journal Journal, providers map[string]RegisteredProvider) (*Worker, error) {
	if journal == nil {
		return nil, ErrInvalidConfiguration
	}
	copyOfProviders := make(map[string]RegisteredProvider, len(providers))
	for providerID, registered := range providers {
		if !validLabel(providerID) || registered.Adapter == nil ||
			!validLabel(registered.Source) || !validLabel(registered.AdapterVersion) {
			return nil, ErrInvalidConfiguration
		}
		copyOfProviders[providerID] = registered
	}
	return &Worker{journal: journal, providers: copyOfProviders}, nil
}

func validLabel(value string) bool {
	return value != "" && value == strings.TrimSpace(value) && len(value) <= 256
}

// RecoverUnknown performs a bounded, single-shot recovery attempt. It accepts
// a durable UNKNOWN or an expired lease, fences its previous owner, and uses
// ONLY Provider.Inspect. Any provider or persistence failure leaves a leased
// RECONCILING state until expiry; another worker cannot blindly re-dispatch.
// This is a library entry point, not an externally reachable API operation.
func (w *Worker) RecoverUnknown(ctx context.Context, tenantID, operationID, owner string, ttl time.Duration) (operation.Operation, error) {
	if !validLabel(tenantID) || !validLabel(operationID) || !validLabel(owner) || ttl <= 0 {
		return operation.Operation{}, operation.ErrInvalid
	}
	current, err := w.journal.GetOperation(ctx, tenantID, operationID)
	if err != nil {
		return operation.Operation{}, err
	}
	registered, ok := w.providers[current.ProviderID]
	if !ok {
		return operation.Operation{}, ErrUnregisteredProvider
	}

	switch current.Status {
	case operation.Unknown:
		// Already lost its lease. It must be inspected, never replayed.
	case operation.Leased, operation.InFlight, operation.Reconciling:
		// PostgreSQL decides whether the old lease has expired. An active
		// lease returns ErrLeaseActive, which is safe to retry after expiry.
		current, err = w.journal.ExpireOperationLease(ctx, tenantID, operationID, current.Version)
		if err != nil {
			return operation.Operation{}, err
		}
	default:
		return operation.Operation{}, operation.ErrTransition
	}

	claimed, err := w.journal.BeginOperationReconciliation(
		ctx, tenantID, operationID, current.Version, "", 0, owner, ttl)
	if err != nil {
		return operation.Operation{}, err
	}
	request := provider.Request{
		OperationID: claimed.ID,
		TenantID: claimed.TenantID,
		WorkloadID: claimed.WorkloadID,
		ProviderID: claimed.ProviderID,
		IdempotencyKey: claimed.IdempotencyKey,
		RequestDigest: claimed.RequestDigest,
		DesiredGeneration: claimed.Generation,
		Kind: claimed.Kind,
		TargetResourceID: claimed.TargetResourceID,
		TargetResourceUID: claimed.TargetResourceUID,
	}
	if err := request.Validate(); err != nil {
		return operation.Operation{}, fmt.Errorf("invalid durable recovery request: %w", err)
	}
	obs, err := registered.Adapter.Inspect(ctx, request)
	if err != nil {
		return operation.Operation{}, fmt.Errorf("authoritative provider inspect failed: %w", err)
	}
	observationID, err := randomID("provider-inspection-")
	if err != nil {
		return operation.Operation{}, err
	}
	eventID, err := randomID("compute-event-")
	if err != nil {
		return operation.Operation{}, err
	}
	next, err := w.journal.ReconcileOperation(ctx, tenantID, operationID,
		claimed.Version, owner, claimed.FenceToken, postgres.InspectionEvidence{
			ObservationID: observationID,
			EventID: eventID,
			CorrelationID: "provider-inspection-" + claimed.ID,
			Source: registered.Source,
			AdapterVersion: registered.AdapterVersion,
			Observation: obs,
		})
	if err != nil {
		return operation.Operation{}, fmt.Errorf("persist provider inspection: %w", err)
	}
	return next, nil
}

func randomID(prefix string) (string, error) {
	var bytes [16]byte
	if _, err := rand.Read(bytes[:]); err != nil {
		return "", fmt.Errorf("generate recovery event identity: %w", err)
	}
	return prefix + hex.EncodeToString(bytes[:]), nil
}

var _ Journal = (*postgres.Repository)(nil)
var _ = sql.ErrNoRows
