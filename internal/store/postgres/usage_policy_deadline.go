package postgres

import (
	"context"
	"database/sql"
	"fmt"
	"time"
)

// recheckTrustedEvidencePolicyDeadline runs after potentially blocking E1 SQL
// and immediately before transaction commit. The policy FOR SHARE lock fences
// administrative revocation but cannot freeze the wall clock.
// This is a final pre-commit check, not a guarantee about the exact instant
// at which the database makes the commit durable.
func recheckTrustedEvidencePolicyDeadline(
	ctx context.Context, tx *sql.Tx, approved TrustedEvidencePolicy,
) error {
	if tx == nil || approved.EffectiveFrom.IsZero() ||
		approved.EffectiveUntil.IsZero() ||
		!approved.EffectiveUntil.After(approved.EffectiveFrom) {
		return ErrEvidencePolicyUntrusted
	}
	var dbNow time.Time
	if err := tx.QueryRowContext(ctx, "SELECT clock_timestamp()").Scan(&dbNow); err != nil {
		return fmt.Errorf("%w: recheck policy clock before commit: %v", ErrEvidencePolicyUntrusted, err)
	}
	if dbNow.Before(approved.EffectiveFrom) || !dbNow.Before(approved.EffectiveUntil) {
		return fmt.Errorf("%w: E1 policy expired before commit", ErrEvidencePolicyUntrusted)
	}
	return nil
}
