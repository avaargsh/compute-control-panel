package provider

import (
	"bytes"
	"context"
	"crypto/ed25519"
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
// PostgreSQL clock_timestamp() is the independent live authorization clock:
// a caller-chosen historical asOf cannot resurrect expired keys or events.
// FOR UPDATE on the source key must be held until the journal INSERT commits,
// or until the caller's separate read-only evidence preflight transaction ends.
// That lock serializes concurrent revocation FK KEY SHARE locks. Use only
// inside a transaction owned by the restricted journal writer or an
// independently provisioned evidence reader; the latter's production
// least-privilege role is NOT established by this helper.
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
	var serverNow time.Time
	err := q.QueryRowContext(ctx, `
		SELECT k.public_key, k.maximum_age_seconds, k.future_skew_seconds,
		       pg_catalog.clock_timestamp()
		FROM public.dra_unprepare_source_keys k
		WHERE k.cluster_id=$1 AND k.source_id=$2 AND k.key_id=$3
		  AND k.tenant_id=$4 AND k.provider_id=$5 AND k.driver=$6
		  AND k.valid_from <= $7 AND k.valid_until > $7
		  AND k.valid_from <= $8 AND k.valid_until > $8
		  -- Caller-supplied asOf is NOT an authorization clock. A
		  -- historical asOf must not resurrect an expired source key.
		  AND k.valid_from <= pg_catalog.clock_timestamp()
		  AND k.valid_until > pg_catalog.clock_timestamp()
		  AND NOT EXISTS (
		    SELECT 1 FROM public.dra_unprepare_source_revocations r
		    WHERE r.cluster_id=k.cluster_id AND r.source_id=k.source_id AND r.key_id=k.key_id
		  )
		FOR UPDATE OF k
	`, e.ClusterID, e.SourceID, e.KeyID, release.TenantID,
		release.ProviderID, e.Driver, e.CompletedAt, asOf,
	).Scan(&key, &maximumAgeSeconds, &futureSkewSeconds, &serverNow)
	if errors.Is(err, sql.ErrNoRows) {
		return ErrDRAReceiptSourceUntrusted
	}
	if err != nil {
		return fmt.Errorf("%w: read trusted source registry: %v", ErrDRAReceiptJournalUnavailable, err)
	}
	if !bytes.Equal(key, verifier.authority.PublicKey) ||
		verifier.maximumAge > time.Duration(maximumAgeSeconds)*time.Second ||
		verifier.futureSkew > time.Duration(futureSkewSeconds)*time.Second ||
		// Independently obtained PostgreSQL time anchors freshness even
		// when a caller forges/backdates the asOf observation timestamp.
		e.CompletedAt.Before(serverNow.Add(-verifier.maximumAge)) ||
		e.CompletedAt.After(serverNow.Add(verifier.futureSkew)) {
		return ErrDRAReceiptSourceUntrusted
	}
	return nil
}

// ResolveRegisteredDRAUnprepareVerifier resolves a signer exclusively from
// the independently DBA-provisioned source registry. It is deliberately
// candidate-free: no Provider Operation, Kueue, Pod or ResourceClaim history
// is queried before the HTTP ingress verifies the signed envelope.
//
// The restricted journal login is required because it is the only existing
// runtime role granted read access to source keys/revocations. The lookup
// re-attests the journal schema and role on the same transaction, samples one
// PostgreSQL clock_timestamp(), requires the key to be valid both at the event
// time and now, rejects revocation, and holds FOR UPDATE while resolving.
// The journal repeats the live-clock and revocation checks when it records.
func (j *DRAUnprepareReceiptJournal) ResolveRegisteredDRAUnprepareVerifier(
	ctx context.Context, tenantID string, event DRAUnprepareEvent,
) (*DRAUnprepareVerifier, time.Time, error) {
	if j == nil || j.db == nil || !j.restricted {
		return nil, time.Time{}, ErrDRAReceiptJournalRole
	}
	if ctx == nil || !validDRAIdentity(tenantID) ||
		!validDRAIdentity(event.ClusterID) || !validDRAIdentity(event.SourceID) ||
		!validDRAIdentity(event.KeyID) || !validDRAIdentity(event.Driver) ||
		event.CompletedAt.IsZero() {
		return nil, time.Time{}, ErrDRAReceiptSourceUntrusted
	}

	conn, err := j.db.Conn(ctx)
	if err != nil {
		return nil, time.Time{}, fmt.Errorf("%w: open source-registry session: %v", ErrDRAReceiptJournalUnavailable, err)
	}
	defer conn.Close()
	tx, err := conn.BeginTx(ctx, nil)
	if err != nil {
		return nil, time.Time{}, fmt.Errorf("%w: begin source-registry transaction: %v", ErrDRAReceiptJournalUnavailable, err)
	}
	defer tx.Rollback()
	if err := verifyDRAJournalSchema(ctx, tx); err != nil {
		return nil, time.Time{}, err
	}
	if err := verifyDRAJournalRole(ctx, tx); err != nil {
		return nil, time.Time{}, err
	}

	var providerID string
	var publicKey []byte
	var maximumAgeSeconds, futureSkewSeconds int64
	var databaseNow time.Time
	err = tx.QueryRowContext(ctx, `
		WITH db_clock AS MATERIALIZED (
			SELECT pg_catalog.clock_timestamp() AS now
		)
		SELECT k.provider_id, k.public_key, k.maximum_age_seconds,
		       k.future_skew_seconds, db_clock.now
		FROM public.dra_unprepare_source_keys k
		CROSS JOIN db_clock
		WHERE k.cluster_id=$1 AND k.source_id=$2 AND k.key_id=$3
		  AND k.tenant_id=$4 AND k.driver=$5
		  AND k.valid_from <= $6 AND k.valid_until > $6
		  AND k.valid_from <= db_clock.now AND k.valid_until > db_clock.now
		  AND NOT EXISTS (
		    SELECT 1 FROM public.dra_unprepare_source_revocations r
		    WHERE r.cluster_id=k.cluster_id AND r.source_id=k.source_id AND r.key_id=k.key_id
		  )
		FOR UPDATE OF k
	`, event.ClusterID, event.SourceID, event.KeyID, tenantID,
		event.Driver, event.CompletedAt,
	).Scan(&providerID, &publicKey, &maximumAgeSeconds, &futureSkewSeconds, &databaseNow)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, time.Time{}, ErrDRAReceiptSourceUntrusted
	}
	if err != nil {
		return nil, time.Time{}, fmt.Errorf("%w: resolve DBA-registered DRA signer: %v", ErrDRAReceiptJournalUnavailable, err)
	}
	if !validDRAIdentity(providerID) || len(publicKey) != ed25519.PublicKeySize || databaseNow.IsZero() {
		return nil, time.Time{}, ErrDRAReceiptSourceUntrusted
	}
	verifier, err := NewDRAUnprepareVerifier(DRAUnprepareAuthority{
		ClusterID: event.ClusterID, SourceID: event.SourceID, KeyID: event.KeyID,
		Driver: event.Driver, PublicKey: ed25519.PublicKey(publicKey),
	}, time.Duration(maximumAgeSeconds)*time.Second, time.Duration(futureSkewSeconds)*time.Second)
	if err != nil {
		return nil, time.Time{}, ErrDRAReceiptSourceUntrusted
	}
	if err := tx.Commit(); err != nil {
		return nil, time.Time{}, fmt.Errorf("%w: finish source-registry lookup: %v", ErrDRAReceiptJournalUnavailable, err)
	}
	return verifier, databaseNow.UTC(), nil
}
