package postgres

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"strings"

	"github.com/jackc/pgx/v5/pgconn"
)

var (
	ErrInboxInvalid    = errors.New("invalid inbox event")
	ErrInboxConflict   = errors.New("inbox event identity or content conflict")
	ErrInboxGap        = errors.New("inbox aggregate version gap")
	ErrInboxUnresolved = errors.New("inbox event is not durably applied")
)

// InboxEvent is an internal, immutable broker event envelope. Its origin and
// tenant scope MUST be authenticated by a separate trusted subscriber adapter.
// An untrusted caller must never invoke ApplyInboxEvent.
type InboxEvent struct {
	ConsumerName     string
	EventID          string
	Producer         string
	EventType        string
	SchemaVersion    int
	TenantID         string
	AggregateType    string
	AggregateID      string
	AggregateVersion int64
	Payload          json.RawMessage
}

// InboxApplyFunc is a DATABASE-ONLY effect, executed on the same PostgreSQL
// transaction as the inbox dedup record and cursor. Never call a provider,
// broker, HTTP endpoint or other nontransactional side effect from this hook.
type InboxApplyFunc func(context.Context, *sql.Tx, InboxEvent) error

func validateInboxEvent(e InboxEvent) error {
	for _, s := range []string{e.ConsumerName, e.EventID, e.Producer, e.EventType,
		e.TenantID, e.AggregateType, e.AggregateID} {
		if s == "" || strings.TrimSpace(s) != s || len(s) > 256 {
			return ErrInboxInvalid
		}
	}
	if e.SchemaVersion < 1 || e.AggregateVersion < 1 ||
		len(e.Payload) == 0 || len(e.Payload) > 1<<20 || !json.Valid(e.Payload) {
		return ErrInboxInvalid
	}
	var object map[string]json.RawMessage
	if err := json.Unmarshal(e.Payload, &object); err != nil || object == nil {
		return ErrInboxInvalid
	}
	return nil
}

func inboxUniqueConflict(err error) error {
	var pgErr *pgconn.PgError
	if errors.As(err, &pgErr) && pgErr.Code == "23505" {
		return ErrInboxConflict
	}
	return err
}

// ApplyInboxEvent atomically applies a single inbound event, a database-only
// effect, the APPLIED inbox record, and the tenant/aggregate delivery cursor.
// A committed duplicate returns applied=false without repeating the effect.
// Out-of-order event versions never skip a missing event. This is a receiver
// transaction boundary, not broker authentication or external exactly-once I/O.
func (r *Repository) ApplyInboxEvent(
	ctx context.Context, e InboxEvent, apply InboxApplyFunc,
) (applied bool, err error) {
	if r == nil || r.db == nil || apply == nil {
		return false, ErrInboxInvalid
	}
	if err := validateInboxEvent(e); err != nil {
		return false, err
	}
	tx, err := r.db.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelReadCommitted})
	if err != nil {
		return false, fmt.Errorf("begin inbox apply: %w", err)
	}
	defer func() { _ = tx.Rollback() }()

	// Lock order: one tenant+consumer+aggregate cursor, then event identity.
	// Concurrent redeliveries of the same version cannot both invoke apply.
	_, err = tx.ExecContext(ctx, `INSERT INTO compute_inbox_cursors(
		consumer_name,tenant_id,aggregate_type,aggregate_id,last_applied_version)
		VALUES ($1,$2,$3,$4,0) ON CONFLICT DO NOTHING`,
		e.ConsumerName, e.TenantID, e.AggregateType, e.AggregateID)
	if err != nil {
		return false, fmt.Errorf("initialize inbox cursor: %w", err)
	}
	var lastApplied int64
	err = tx.QueryRowContext(ctx, `SELECT last_applied_version
		FROM compute_inbox_cursors
		WHERE consumer_name=$1 AND tenant_id=$2 AND aggregate_type=$3
		  AND aggregate_id=$4 FOR UPDATE`,
		e.ConsumerName, e.TenantID, e.AggregateType, e.AggregateID).Scan(&lastApplied)
	if err != nil {
		return false, fmt.Errorf("lock inbox cursor: %w", err)
	}

	var (
		tenantID, producer, eventType, aggregateType, aggregateID, state string
		schemaVersion                                                    int
		aggregateVersion                                                 int64
		payloadMatches                                                   bool
	)
	err = tx.QueryRowContext(ctx, `SELECT tenant_id,producer,event_type,schema_version,
		aggregate_type,aggregate_id,aggregate_version,payload=$3::jsonb,state
		FROM compute_inbox_events WHERE consumer_name=$1 AND event_id=$2 FOR UPDATE`,
		e.ConsumerName, e.EventID, e.Payload).Scan(
		&tenantID, &producer, &eventType, &schemaVersion,
		&aggregateType, &aggregateID, &aggregateVersion, &payloadMatches, &state,
	)
	switch {
	case err == nil:
		if tenantID != e.TenantID || producer != e.Producer || eventType != e.EventType ||
			schemaVersion != e.SchemaVersion || aggregateType != e.AggregateType ||
			aggregateID != e.AggregateID || aggregateVersion != e.AggregateVersion || !payloadMatches {
			return false, ErrInboxConflict
		}
		if state != "APPLIED" || lastApplied < e.AggregateVersion {
			return false, ErrInboxUnresolved
		}
		return false, nil
	case !errors.Is(err, sql.ErrNoRows):
		return false, fmt.Errorf("inspect inbound event id: %w", err)
	}
	if e.AggregateVersion-1 != lastApplied {
		if e.AggregateVersion <= lastApplied {
			return false, ErrInboxConflict
		}
		return false, ErrInboxGap
	}
	_, err = tx.ExecContext(ctx, `INSERT INTO compute_inbox_events(
		consumer_name,event_id,producer,event_type,schema_version,tenant_id,
		aggregate_type,aggregate_id,aggregate_version,payload,state
	) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10::jsonb,'RECEIVED')`,
		e.ConsumerName, e.EventID, e.Producer, e.EventType, e.SchemaVersion,
		e.TenantID, e.AggregateType, e.AggregateID, e.AggregateVersion, e.Payload)
	if err != nil {
		return false, fmt.Errorf("persist inbox event: %w", inboxUniqueConflict(err))
	}

	if err := apply(ctx, tx, e); err != nil {
		// Roll back BOTH the domain mutation and the inbox receipt. A retry
		// will see the original cursor and may safely apply the same event.
		return false, fmt.Errorf("apply inbox event: %w", err)
	}
	receipt, err := tx.ExecContext(ctx, `UPDATE compute_inbox_events SET
		state='APPLIED',applied_at=clock_timestamp()
		WHERE consumer_name=$1 AND event_id=$2 AND state='RECEIVED'`,
		e.ConsumerName, e.EventID)
	if err != nil {
		return false, fmt.Errorf("record applied inbox receipt: %w", err)
	}
	receiptRows, err := receipt.RowsAffected()
	if err != nil {
		return false, fmt.Errorf("verify applied inbox receipt: %w", err)
	}
	if receiptRows != 1 {
		return false, ErrInboxUnresolved
	}
	cursorUpdate, err := tx.ExecContext(ctx, `UPDATE compute_inbox_cursors
		SET last_applied_version=$5,updated_at=clock_timestamp()
		WHERE consumer_name=$1 AND tenant_id=$2 AND aggregate_type=$3
		  AND aggregate_id=$4 AND last_applied_version=$6`,
		e.ConsumerName, e.TenantID, e.AggregateType, e.AggregateID,
		e.AggregateVersion, lastApplied)
	if err != nil {
		return false, fmt.Errorf("advance inbox cursor: %w", err)
	}
	cursorRows, err := cursorUpdate.RowsAffected()
	if err != nil {
		return false, fmt.Errorf("verify applied inbox cursor: %w", err)
	}
	if cursorRows != 1 {
		return false, ErrInboxUnresolved
	}
	if err := tx.Commit(); err != nil {
		// A lost commit ACK is ambiguous. Caller must reread durable inbox
		// status using the SAME event identity, never mint a new event ID.
		return false, fmt.Errorf("commit inbox event (outcome may be unknown): %w", err)
	}
	return true, nil
}
