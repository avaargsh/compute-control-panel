package postgres

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/avaargsh/compute-control-panel/internal/economics"
)

const evidencePolicyFenceDomain = "compute.economics/e1-policy-fence/v1"

// evidencePolicyFenceDigest binds policy content, tenant/Provider scope and
// effective window. encoding/json has deterministic sorted string map keys.
// This is not a signature: the independently-administered PostgreSQL policy
// registry must be writable only by a trusted provisioning/revocation role.
func evidencePolicyFenceDigest(approved TrustedEvidencePolicy) (string, error) {
	if approved.TenantID == "" || approved.ProviderID == "" ||
		approved.Policy.Version == "" || approved.Policy.AsOf.IsZero() ||
		approved.EffectiveFrom.IsZero() || approved.EffectiveUntil.IsZero() ||
		!approved.EffectiveUntil.After(approved.EffectiveFrom) ||
		approved.Policy.MaximumClockSkew < 0 {
		return "", ErrEvidencePolicyUntrusted
	}
	// PostgreSQL timestamptz persists microseconds; reject imprecise
	// in-memory windows instead of accepting a different recorded value.
	if !approved.EffectiveFrom.Equal(approved.EffectiveFrom.Truncate(time.Microsecond)) ||
		!approved.EffectiveUntil.Equal(approved.EffectiveUntil.Truncate(time.Microsecond)) {
		return "", fmt.Errorf("%w: noncanonical policy effective window", ErrEvidencePolicyUntrusted)
	}
	payload, err := json.Marshal(struct {
		TenantID       string
		ProviderID     string
		Policy         economics.EvidencePolicy
		EffectiveFrom  time.Time
		EffectiveUntil time.Time
	}{
		TenantID: approved.TenantID, ProviderID: approved.ProviderID,
		Policy: approved.Policy,
		EffectiveFrom: approved.EffectiveFrom.UTC(),
		EffectiveUntil: approved.EffectiveUntil.UTC(),
	})
	if err != nil {
		return "", fmt.Errorf("%w: encode policy snapshot: %v", ErrEvidencePolicyUntrusted, err)
	}
	sum := sha256.Sum256(append([]byte(evidencePolicyFenceDomain+"\x00"), payload...))
	return "sha256:" + hex.EncodeToString(sum[:]), nil
}

// lockTrustedEvidencePolicy is a second, transaction-scoped authority gate.
// Read the exact administratively-provisioned policy row FOR SHARE in the
// same SQL transaction as the immutable E1 insert. Admin UPDATE/DELETE of
// this row cannot pass the lock until the writer commits or rolls back.
// Revocations that commit first are rechecked after the row lock is obtained.
func lockTrustedEvidencePolicy(
	ctx context.Context, tx *sql.Tx, approved TrustedEvidencePolicy,
) error {
	if tx == nil || approved.Revoked || approved.Revision < 1 {
		return ErrEvidencePolicyUntrusted
	}
	expectedDigest, err := evidencePolicyFenceDigest(approved)
	if err != nil {
		return err
	}
	var digest string
	var revision int64
	var revoked bool
	var effectiveFrom, effectiveUntil time.Time
	err = tx.QueryRowContext(ctx, `
		SELECT policy_digest,revision,revoked,effective_from,effective_until
		FROM compute_evidence_policy_fences
		WHERE tenant_id=$1 AND provider_id=$2 AND policy_version=$3
		FOR SHARE
	`, approved.TenantID, approved.ProviderID, approved.Policy.Version).Scan(
		&digest, &revision, &revoked, &effectiveFrom, &effectiveUntil,
	)
	if errors.Is(err, sql.ErrNoRows) {
		return ErrEvidencePolicyUntrusted
	}
	if err != nil {
		return fmt.Errorf("%w: policy fence read unavailable: %v", ErrEvidencePolicyUntrusted, err)
	}
	// Query the database clock after acquiring the row lock, not before.
	var dbNow time.Time
	if err := tx.QueryRowContext(ctx, "SELECT clock_timestamp()").Scan(&dbNow); err != nil {
		return fmt.Errorf("%w: read authoritative policy fence clock: %v", ErrEvidencePolicyUntrusted, err)
	}
	if revoked || revision < 1 || uint64(revision) != approved.Revision ||
		digest != expectedDigest ||
		!effectiveFrom.Equal(approved.EffectiveFrom) ||
		!effectiveUntil.Equal(approved.EffectiveUntil) ||
		dbNow.Before(effectiveFrom) || !dbNow.Before(effectiveUntil) {
		return fmt.Errorf("%w: policy revoked, expired, reconfigured or stale revision", ErrEvidencePolicyUntrusted)
	}
	return nil
}
