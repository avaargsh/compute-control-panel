package provider

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"time"
)

// ErrDRAReceiptNotRecorded is a missing or nonmatching signed journal
// record. It never implies that a GPU is present or released.
var ErrDRAReceiptNotRecorded = errors.New("signed DRA Unprepare receipt not present in immutable journal")

// RequireRecordedDRAUnprepareReceipt is an evidence-only, read-only
// prerequisite for a *future* trusted release decision. The caller provides
// its own transaction and retains both the registered-source row lock and
// any separate Workload/Allocation/Operation locks until that transaction
// ends. Never validate in one transaction and mutate release state in another.
//
// A successful return means:
//  1. the signature and exact historical Claim/Pod/device were correlated by
//     the existing verifier;
//  2. an independently DBA-registered source key is live, unrevoked and
//     in-scope at PostgreSQL's actual time, with its row locked;
//  3. the immutable journal contains the precise signed event, digest,
//     source identity, tenant, Provider and historical Allocation.
//
// This does NOT authenticate driver transport, prove NodeUnprepareResources
// actually ran, establish GPU deallocation, or authorize RELEASED/Cost FINAL.
// It provisions no keys, performs no INSERT/UPDATE/DELETE, and makes no
// claim that the caller's database role is production-ready.
func RequireRecordedDRAUnprepareReceipt(
	ctx context.Context, tx *sql.Tx, verifier *DRAUnprepareVerifier,
	candidate ReleaseEvidenceCandidate, signed SignedDRAUnprepare, asOf time.Time,
) error {
	if ctx == nil || tx == nil {
		return ErrDRAReceiptJournalUnavailable
	}
	if verifier == nil {
		return ErrUncorrelatedDRAUnprepare
	}
	correlated, err := verifier.Correlate(candidate, signed, asOf)
	if err != nil {
		return err
	}
	// Reuse the existing source-key scope, PG live-clock and revocation
	// logic under the exact caller transaction, including FOR UPDATE.
	if err := verifyRegisteredDRAUnprepareSource(
		ctx, tx, verifier, correlated.Release, signed, asOf,
	); err != nil {
		return err
	}

	recordJSON, err := json.Marshal(draReceiptJournalRecord{
		Release:       correlated.Release,
		Event:         correlated.Event,
		PayloadSHA256: correlated.PayloadSHA256,
		KeySHA256:     correlated.KeySHA256,
		Signature:     correlated.Signature[:],
	})
	if err != nil {
		return fmt.Errorf("%w: encode expected receipt: %v", ErrDRAReceiptJournalUnavailable, err)
	}
	deviceJSON, err := json.Marshal(correlated.Event.AllocatedDevice)
	if err != nil {
		return fmt.Errorf("%w: encode allocated device: %v", ErrDRAReceiptJournalUnavailable, err)
	}
	event, release := correlated.Event, correlated.Release
	recordDigest := draJournalDigest("CCP:DRA:journal-record:v1\n", recordJSON)
	deviceDigest := draJournalDigest("CCP:DRA:device-epoch:v1\n", deviceJSON)

	// Journal rows have immutable UPDATE/DELETE/TRUNCATE triggers and exact
	// source/device uniqueness constraints. Compare the full record JSON and
	// canonical digest, not just its forgeable event ID. The source key row
	// remains locked in the same transaction, fencing concurrent DBA
	// revocation until this read-only decision completes.
	var persistedDigest string
	var exactPayload bool
	err = tx.QueryRowContext(ctx, `
		SELECT record_digest, record_payload=$4::jsonb
		FROM public.dra_unprepare_receipt_journal
		WHERE cluster_id=$1 AND source_id=$2 AND event_id=$3
		  AND driver=$5 AND tenant_id=$6 AND provider_id=$7
		  AND allocation_id=$8 AND claim_uid=$9 AND pod_uid=$10
		  AND allocated_claim_resource_version=$11 AND device_digest=$12
		  AND completed_at=$13
	`,
		event.ClusterID, event.SourceID, event.EventID, recordJSON,
		event.Driver, release.TenantID, release.ProviderID, release.AllocationID,
		event.ClaimUID, event.PodUID, event.AllocatedClaimResourceVersion,
		deviceDigest, event.CompletedAt,
	).Scan(&persistedDigest, &exactPayload)
	if errors.Is(err, sql.ErrNoRows) {
		return ErrDRAReceiptNotRecorded
	}
	if err != nil {
		return fmt.Errorf("%w: read immutable signed receipt: %v", ErrDRAReceiptJournalUnavailable, err)
	}
	if persistedDigest != recordDigest || !exactPayload {
		return ErrDRAReceiptNotRecorded
	}
	return nil
}
