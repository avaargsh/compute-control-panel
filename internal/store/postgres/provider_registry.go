package postgres

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"regexp"
	"strings"
	"time"
)

// The Provider registry is an internal bootstrap-only lifecycle boundary.
// It does not prove healthy GPU capacity or authorize placement. No public
// HTTP handler mounts these methods, and this first slice does not expose
// READY/RETIRED until authenticated source and release evidence exists.
var (
	ErrProviderInvalid    = errors.New("invalid Provider registration or lifecycle request")
	ErrProviderConflict   = errors.New("Provider registration fingerprint conflict")
	ErrProviderStale      = errors.New("stale Provider version")
	ErrProviderTransition = errors.New("forbidden Provider lifecycle transition")
)

type ProviderState string

const (
	ProviderRegistered ProviderState = "REGISTERED"
	ProviderDraining   ProviderState = "DRAINING"
)

type ProviderRegistration struct {
	ProviderID       string
	ClusterScope     string
	AdapterSource    string
	AdapterVersion   string
	CapabilityDigest string
}

type ProviderRecord struct {
	ProviderRegistration
	State          ProviderState
	Version        int64
	LastReasonCode string
	RegisteredAt   time.Time
	UpdatedAt      time.Time
}

var providerReasonPattern = regexp.MustCompile(`^[A-Z][A-Z0-9_]{0,127}$`)

func providerRegistryLabel(s string) bool {
	return s != "" && s == strings.TrimSpace(s) && len(s) <= 256 && !strings.ContainsRune(s, 0)
}

func (p ProviderRegistration) validate() error {
	for _, field := range []string{p.ProviderID, p.ClusterScope, p.AdapterSource, p.AdapterVersion} {
		if !providerRegistryLabel(field) {
			return ErrProviderInvalid
		}
	}
	if !validSHA256Digest(p.CapabilityDigest) {
		return ErrProviderInvalid
	}
	return nil
}

const providerRecordColumns = `provider_id, cluster_scope, adapter_source, adapter_version,
	capability_digest, state, version, last_reason_code, registered_at, updated_at`

type providerRecordScanner interface{ Scan(...any) error }

func scanProvider(row providerRecordScanner) (ProviderRecord, error) {
	var p ProviderRecord
	err := row.Scan(&p.ProviderID, &p.ClusterScope, &p.AdapterSource,
		&p.AdapterVersion, &p.CapabilityDigest, &p.State, &p.Version,
		&p.LastReasonCode, &p.RegisteredAt, &p.UpdatedAt)
	return p, err
}

// RegisterProvider stores only immutable provider identity and capability
// digest. A matching replay returns the durable state (which may be DRAINING)
// and never resurrects the provider. Replacing a capability snapshot requires
// an explicit future versioned migration/rotation contract.
func (r *Repository) RegisterProvider(ctx context.Context, p ProviderRegistration) (ProviderRecord, bool, error) {
	if err := p.validate(); err != nil {
		return ProviderRecord{}, false, err
	}
	const insert = `INSERT INTO public.compute_providers
		(provider_id, cluster_scope, adapter_source, adapter_version, capability_digest)
		VALUES($1,$2,$3,$4,$5)
		ON CONFLICT (provider_id) DO NOTHING
		RETURNING ` + providerRecordColumns
	got, err := scanProvider(r.db.QueryRowContext(ctx, insert, p.ProviderID,
		p.ClusterScope, p.AdapterSource, p.AdapterVersion, p.CapabilityDigest))
	if err == nil {
		return got, true, nil
	}
	if !errors.Is(err, sql.ErrNoRows) {
		return ProviderRecord{}, false, fmt.Errorf("insert Provider registration: %w", err)
	}
	existing, err := r.GetProvider(ctx, p.ProviderID)
	if err != nil {
		return ProviderRecord{}, false, fmt.Errorf("read existing Provider registration: %w", err)
	}
	if existing.ProviderRegistration != p {
		return ProviderRecord{}, false, ErrProviderConflict
	}
	return existing, false, nil
}

// GetProvider reads the global bootstrap registry. Tenant/project policy
// and Provider selection must be enforced at separate authenticated entry
// points before callers can consume this information for capacity choices.
func (r *Repository) GetProvider(ctx context.Context, providerID string) (ProviderRecord, error) {
	if !providerRegistryLabel(providerID) {
		return ProviderRecord{}, ErrProviderInvalid
	}
	got, err := scanProvider(r.db.QueryRowContext(ctx,
		"SELECT "+providerRecordColumns+" FROM public.compute_providers WHERE provider_id=$1",
		providerID))
	if err != nil {
		return ProviderRecord{}, err
	}
	return got, nil
}

// BeginProviderDrain atomically makes a registered Provider ineligible for
// future placement decisions once those decisions are integrated with the
// registry. It does not delete active workloads, withdraw Kueue capacity,
// confirm resource absence or settle Provider cost liability.
func (r *Repository) BeginProviderDrain(
	ctx context.Context, providerID string, expectedVersion int64, reasonCode string,
) (ProviderRecord, error) {
	if !providerRegistryLabel(providerID) || expectedVersion <= 0 ||
		!providerReasonPattern.MatchString(reasonCode) ||
		reasonCode == "REGISTERED" {
		return ProviderRecord{}, ErrProviderInvalid
	}
	const update = `UPDATE public.compute_providers SET state='DRAINING',
		version=version+1, last_reason_code=$3, updated_at=pg_catalog.clock_timestamp()
		WHERE provider_id=$1 AND version=$2 AND state='REGISTERED'
		RETURNING ` + providerRecordColumns
	got, err := scanProvider(r.db.QueryRowContext(ctx, update,
		providerID, expectedVersion, reasonCode))
	if err == nil {
		return got, nil
	}
	if !errors.Is(err, sql.ErrNoRows) {
		return ProviderRecord{}, fmt.Errorf("begin Provider drain: %w", err)
	}
	current, err := r.GetProvider(ctx, providerID)
	if err != nil {
		return ProviderRecord{}, err
	}
	if current.Version != expectedVersion {
		return ProviderRecord{}, ErrProviderStale
	}
	return ProviderRecord{}, ErrProviderTransition
}
