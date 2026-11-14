package provider

import (
	"bytes"
	"context"
	"database/sql"
	"errors"
	"fmt"
	"time"
)

// ErrDRAReceiptSourceUntrusted means an otherwise well-formed signed receipt
// lacks an independently DBA-provisioned and currently valid source binding.
// This is NOT an indication that the GPU is still present or already released.
var ErrDRAReceiptSourceUntrusted = errors.New("DRA receipt signer is not registered, in scope, current, or unrevoked")

// verifyRegisteredDRAUnprepareSource is deliberately invoked through the
// restricted journal's EXACT write connection. A caller-supplied Ed25519
// verifier cannot bootstrap its own trust; its public key and freshness
// policy must be no weaker than the independent DBA registry record.
//
// A registry row attests only to a provisioned public key and scope. A real
// driver transport, actual RPC result, device removal and liability end still
// require independent end-to-end evidence. No RELEASED or FINAL transition.
func verifyRegisteredDRAUnprepareSource(
	ctx context.Context, q draJournalQueryRow, verifier *DRAUnprepareVerifier,
	release ReleaseEvidenceCorrelation, signed SignedDRAUnprepare, asOf time.Time,
) error {
	if verifier == nil || q == nil {
		return ErrDRAReceiptSourceUntrusted
	}
	e := signed.Event
	var key []byte
	var maximumAgeSeconds, futureSkewSeconds int64
	err := q.QueryRowContext(ctx, `
		SELECT k.public_key, k.maximum_age_seconds, k.future_skew_seconds
		FROM public.dra_unprepare_source_keys k
		WHERE k.cluster_id=$1 AND k.source_id=$2 AND k.key_id=$3
		  AND k.tenant_id=$4 AND k.provider_id=$5 AND k.driver=$6
		  AND k.valid_from <= $7 AND k.valid_until > $7
		  AND k.valid_from <= $8 AND k.valid_until > $8
		  AND NOT EXISTS (
		    SELECT 1 FROM public.dra_unprepare_source_revocations r
		    WHERE r.cluster_id=k.cluster_id AND r.source_id=k.source_id AND r.key_id=k.key_id
		  )
	`, e.ClusterID, e.SourceID, e.KeyID, release.TenantID,
		release.ProviderID, e.Driver, e.CompletedAt, asOf,
	).Scan(&key, &maximumAgeSeconds, &futureSkewSeconds)
	if errors.Is(err, sql.ErrNoRows) {
		return ErrDRAReceiptSourceUntrusted
	}
	if err != nil {
		return fmt.Errorf("%w: read trusted source registry: %v", ErrDRAReceiptJournalUnavailable, err)
	}
	if !bytes.Equal(key, verifier.authority.PublicKey) ||
		verifier.maximumAge > time.Duration(maximumAgeSeconds)*time.Second ||
		verifier.futureSkew > time.Duration(futureSkewSeconds)*time.Second {
		return ErrDRAReceiptSourceUntrusted
	}
	return nil
}
