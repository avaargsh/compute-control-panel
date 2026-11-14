package provider

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"time"
)

// ErrDRAReceiptReplayConflict rejects reuse of a source event identity with
// changed evidence, or a second event/source for the same Claim-device epoch.
var ErrDRAReceiptReplayConflict = errors.New("DRA Unprepare receipt replay conflicts with immutable journal")

// ErrDRAReceiptJournalUnavailable rejects a missing or failed journal. A
// caller must never treat a persistence failure as evidence of release.
var ErrDRAReceiptJournalUnavailable = errors.New("DRA Unprepare receipt journal unavailable")

// DRAUnprepareReceiptJournal is an evidence-only PostgreSQL replay fence. It
// accepts no public API input and does not change an Allocation, operation,
// quota, GPU claim, usage interval or financial status.
//
// CRITICAL: The caller must independently authenticate the driver transport,
// cluster/Provider binding and verifier's provisioned public key. The current
// repository does not wire such an authority: a caller-supplied verifier
// alone cannot turn a custom signed event into production driver attestation.
type DRAUnprepareReceiptJournal struct {
	db         *sql.DB
	restricted bool
}

func NewDRAUnprepareReceiptJournal(db *sql.DB) (*DRAUnprepareReceiptJournal, error) {
	if db == nil {
		return nil, ErrDRAReceiptJournalUnavailable
	}
	return &DRAUnprepareReceiptJournal{db: db}, nil
}

// draJournalSQL is implemented by both *sql.DB and a dedicated *sql.Conn.
// Restricted journal sessions attest privileges on the very connection used
// to append and inspect the receipt, never on a different pooled session.
type draJournalSQL interface {
	ExecContext(context.Context, string, ...any) (sql.Result, error)
	QueryRowContext(context.Context, string, ...any) *sql.Row
}

type draReceiptJournalRecord struct {
	Release       ReleaseEvidenceCorrelation `json:"release"`
	Event         DRAUnprepareEvent          `json:"event"`
	PayloadSHA256 string                     `json:"payload_sha256"`
	KeySHA256     string                     `json:"key_sha256"`
	Signature     []byte                     `json:"signature"`
}

func draJournalDigest(domain string, payload []byte) string {
	bytes := make([]byte, 0, len(domain)+len(payload))
	bytes = append(bytes, domain...)
	bytes = append(bytes, payload...)
	sum := sha256.Sum256(bytes)
	return "sha256:" + hex.EncodeToString(sum[:])
}

// VerifyAndRecord independently rechecks the exact signed event/CREATE/DELETE
// evidence with the supplied verifier before writing one immutable row.
// true means a new journal row was committed, NOT physical GPU release.
// false,nil is an exact replay of the same receipt; no new effect is granted.
// Conflicting event ID, signing key, tenant, Allocation or Claim-device epoch
// fails closed, including across restarts and concurrent writers.
//
// This is a repository-level persistence primitive, NOT a trusted ingest
// adapter: independent source-key custody, receipt transport authentication,
// and driver RPC result provenance remain mandatory production gates.
func (j *DRAUnprepareReceiptJournal) VerifyAndRecord(
	ctx context.Context, verifier *DRAUnprepareVerifier,
	candidate ReleaseEvidenceCandidate, signed SignedDRAUnprepare, asOf time.Time,
) (bool, error) {
	if j == nil || j.db == nil {
		return false, ErrDRAReceiptJournalUnavailable
	}
	if verifier == nil {
		return false, ErrUncorrelatedDRAUnprepare
	}
	var session draJournalSQL = j.db
	var tx *sql.Tx
	if j.restricted {
		conn, err := j.db.Conn(ctx)
		if err != nil {
			return false, fmt.Errorf("%w: open restricted session: %v", ErrDRAReceiptJournalUnavailable, err)
		}
		defer conn.Close()
		// Keep the source row's FOR UPDATE lock until the receipt journal
		// INSERT (or replay decision) commits. A DBA revocation needs a
		// conflicting FK KEY SHARE lock on that same source identity.
		tx, err = conn.BeginTx(ctx, nil)
		if err != nil {
			return false, fmt.Errorf("%w: begin restricted write transaction: %v", ErrDRAReceiptJournalUnavailable, err)
		}
		defer tx.Rollback()
		if err := verifyDRAJournalSchema(ctx, tx); err != nil {
			return false, err
		}
		if err := verifyDRAJournalRole(ctx, tx); err != nil {
			return false, err
		}
		session = tx
	}
	commit := func() error {
		if tx == nil {
			return nil
		}
		if err := tx.Commit(); err != nil {
			return fmt.Errorf("%w: commit registered DRA receipt: %v", ErrDRAReceiptJournalUnavailable, err)
		}
		return nil
	}
	correlated, err := verifier.Correlate(candidate, signed, asOf)
	if err != nil {
		return false, err
	}
	if j.restricted {
		if err := verifyRegisteredDRAUnprepareSource(ctx, session, verifier, correlated.Release, signed, asOf); err != nil {
			return false, err
		}
	}
	recordJSON, err := json.Marshal(draReceiptJournalRecord{
		Release:       correlated.Release,
		Event:         correlated.Event,
		PayloadSHA256: correlated.PayloadSHA256,
		KeySHA256:     correlated.KeySHA256,
		Signature:     correlated.Signature[:],
	})
	if err != nil {
		return false, fmt.Errorf("%w: encode verified receipt: %v", ErrDRAReceiptJournalUnavailable, err)
	}
	deviceJSON, err := json.Marshal(correlated.Event.AllocatedDevice)
	if err != nil {
		return false, fmt.Errorf("%w: encode allocated device: %v", ErrDRAReceiptJournalUnavailable, err)
	}
	event := correlated.Event
	release := correlated.Release
	recordDigest := draJournalDigest("CCP:DRA:journal-record:v1\n", recordJSON)
	deviceDigest := draJournalDigest("CCP:DRA:device-epoch:v1\n", deviceJSON)

	// PostgreSQL uniqueness serializes same-event concurrent attempts and
	// different event/source IDs for the same historical allocated device
	// epoch. Restricted writes commit this statement together with the
	// independently registered source row lock; unchecked test-only writes
	// continue using the single INSERT statement's implicit transaction.
	// No INSERT/UPDATE paths on Allocation or billable evidence are present.
	args := []any{
		event.ClusterID, event.SourceID, event.EventID, event.Driver,
		release.TenantID, release.ProviderID, release.AllocationID,
		event.ClaimUID, event.PodUID, event.AllocatedClaimResourceVersion,
		deviceDigest, recordDigest, recordJSON, event.CompletedAt,
	}
	insertRows := `VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13::jsonb,$14)`
	if j.restricted {
		// PostgreSQL acquires the journal relation's INSERT lock before
		// evaluating this SELECT. An expiry that occurs while waiting for
		// that lock therefore cannot use an earlier source-key snapshot.
		// The previously locked source row also fences concurrent DBA
		// revocation via its foreign-key KEY SHARE lock.
		insertRows = `SELECT $1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13::jsonb,$14::timestamptz
			FROM public.dra_unprepare_source_keys k
			WHERE k.cluster_id=$1 AND k.source_id=$2 AND k.key_id=$15
			  AND k.tenant_id=$5 AND k.provider_id=$6 AND k.driver=$4
			  AND k.public_key=$16
			  AND k.valid_from <= $14::timestamptz
			  AND k.valid_until > $14::timestamptz
			  AND k.valid_from <= pg_catalog.clock_timestamp()
			  AND k.valid_until > pg_catalog.clock_timestamp()
			  AND $14::timestamptz >= pg_catalog.clock_timestamp() -
			    ($17::bigint * INTERVAL '1 microsecond')
			  AND $14::timestamptz <= pg_catalog.clock_timestamp() +
			    ($18::bigint * INTERVAL '1 microsecond')
			  AND NOT EXISTS (
			    SELECT 1 FROM public.dra_unprepare_source_revocations r
			    WHERE r.cluster_id=k.cluster_id AND r.source_id=k.source_id
			      AND r.key_id=k.key_id
			  )`
		args = append(args, event.KeyID, verifier.authority.PublicKey,
			verifier.maximumAge.Microseconds(), verifier.futureSkew.Microseconds())
	}
	result, err := session.ExecContext(ctx, `INSERT INTO public.dra_unprepare_receipt_journal (
		cluster_id, source_id, event_id, driver, tenant_id, provider_id,
		allocation_id, claim_uid, pod_uid, allocated_claim_resource_version,
		device_digest, record_digest, record_payload, completed_at
	) `+insertRows+` ON CONFLICT DO NOTHING`, args...)
	if err != nil {
		return false, fmt.Errorf("%w: append signed receipt: %v", ErrDRAReceiptJournalUnavailable, err)
	}
	n, err := result.RowsAffected()
	if err != nil {
		return false, fmt.Errorf("%w: inspect insert result: %v", ErrDRAReceiptJournalUnavailable, err)
	}
	switch n {
	case 1:
		if err := commit(); err != nil {
			return false, err
		}
		return true, nil
	case 0:
		// The final INSERT's live-clock predicate might have refused a
		// signer that expired while waiting on the journal relation lock.
		// Never report that case as an idempotent replay or success.
		if j.restricted {
			if err := verifyRegisteredDRAUnprepareSource(ctx, session, verifier, release, signed, asOf); err != nil {
				return false, err
			}
		}
		// Otherwise a conflict may be on the global source event identity
		// or the cross-source device epoch; accept only exact stored replay.
	default:
		return false, ErrDRAReceiptJournalUnavailable
	}
	var storedDigest string
	var payloadMatches bool
	err = session.QueryRowContext(ctx, `SELECT record_digest, record_payload=$4::jsonb
		FROM public.dra_unprepare_receipt_journal
		WHERE cluster_id=$1 AND source_id=$2 AND event_id=$3`,
		event.ClusterID, event.SourceID, event.EventID, recordJSON,
	).Scan(&storedDigest, &payloadMatches)
	if errors.Is(err, sql.ErrNoRows) {
		// The conflicting uniqueness constraint was the Claim/device epoch
		// with a different source/event identity: never accept as replay.
		return false, ErrDRAReceiptReplayConflict
	}
	if err != nil {
		return false, fmt.Errorf("%w: inspect signed receipt replay: %v", ErrDRAReceiptJournalUnavailable, err)
	}
	if storedDigest != recordDigest || !payloadMatches {
		return false, ErrDRAReceiptReplayConflict
	}
	if err := commit(); err != nil {
		return false, err
	}
	return false, nil
}
