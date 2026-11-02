package postgres

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
)

var (
	ErrInboxInvalid = errors.New("invalid compute Inbox event or handler")
	ErrInboxConflict = errors.New("conflicting compute Inbox event identity or version")
	ErrInboxGap = errors.New("compute Inbox event has a missing preceding version")
)

// InboxEvent is a trusted, already-authenticated broker event. This type is
// deliberately not an HTTP request DTO. A producer must never be inferred
// from user-controlled JSON fields.
type InboxEvent struct {
	ConsumerName string
	EventID string
	Producer string
	EventType string
	SchemaVersion int
	TenantID string
	AggregateType string
	AggregateID string
	AggregateVersion int64
	Payload json.RawMessage
}

// InboxHandler is only allowed to make transactional changes via tx. The
// caller must not make non-transactional or external side effects here.
type InboxHandler func(context.Context, *sql.Tx, InboxEvent) error

func validateInbox(e InboxEvent, handle InboxHandler) error {
	if handle == nil || e.SchemaVersion < 1 || e.AggregateVersion < 1 {
		return ErrInboxInvalid
	}
	for _, label := range []string{e.ConsumerName, e.EventID, e.Producer, e.EventType,
		e.TenantID, e.AggregateType, e.AggregateID} {
		if !outboxLabel(label) {
			return ErrInboxInvalid
		}
	}
	var object map[string]json.RawMessage
	if err := json.Unmarshal(e.Payload, &object); err != nil || object == nil {
		return ErrInboxInvalid
	}
	return nil
}

// ApplyInbox commits Inbox deduplication, business DB effects and an aggregate
// cursor in one PostgreSQL transaction. It serializes attempts on the
// (consumer, tenant, aggregate type, aggregate id) cursor row. Strict version
// sequencing prevents an unexpected v3 from skipping an absent v2.
// Exact replay of an APPLIED event is a no-op. Changed fields/JSONB payload,
// even under the same event_id or aggregate_version, fail closed.
// Returns applied=true only if this transaction executed the handler.
func (r *Repository) ApplyInbox(ctx context.Context, e InboxEvent, handle InboxHandler) (bool, error) {
	if err := validateInbox(e, handle); err != nil {
		return false, err
	}
	tx, err := r.db.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelReadCommitted})
	if err != nil {
		return false, fmt.Errorf("begin Inbox transaction: %w", err)
	}
	defer func() { _ = tx.Rollback() }()

	_, err = tx.ExecContext(ctx, `INSERT INTO compute_inbox_cursors(
		consumer_name, tenant_id, aggregate_type, aggregate_id, last_applied_version)
		VALUES($1,$2,$3,$4,0)
		ON CONFLICT(consumer_name,tenant_id,aggregate_type,aggregate_id) DO NOTHING`,
		e.ConsumerName, e.TenantID, e.AggregateType, e.AggregateID)
	if err != nil {
		return false, fmt.Errorf("initialize Inbox cursor: %w", err)
	}
	var cursor int64
	if err := tx.QueryRowContext(ctx, `SELECT last_applied_version
		FROM compute_inbox_cursors
		WHERE consumer_name=$1 AND tenant_id=$2 AND aggregate_type=$3 AND aggregate_id=$4
		FOR UPDATE`,
		e.ConsumerName, e.TenantID, e.AggregateType, e.AggregateID).Scan(&cursor); err != nil {
		return false, fmt.Errorf("lock Inbox cursor: %w", err)
	}
	// The global per-consumer event ID prevents a replay from switching
	// tenant or aggregate, even when that other aggregate has an empty cursor.
	var producer, eventType, tenantID, aggType, aggID, state string
	var schemaVersion int
	var aggVersion int64
	var payloadMatches bool
	err = tx.QueryRowContext(ctx, `SELECT producer,event_type,schema_version,tenant_id,
		aggregate_type,aggregate_id,aggregate_version,state,payload=$3::jsonb
		FROM compute_inbox_events WHERE consumer_name=$1 AND event_id=$2`,
		e.ConsumerName, e.EventID, []byte(e.Payload)).Scan(&producer, &eventType,
		&schemaVersion, &tenantID, &aggType, &aggID, &aggVersion, &state, &payloadMatches)
	if err == nil {
		if producer != e.Producer || eventType != e.EventType || schemaVersion != e.SchemaVersion ||
			tenantID != e.TenantID || aggType != e.AggregateType || aggID != e.AggregateID ||
			aggVersion != e.AggregateVersion || !payloadMatches || state != "APPLIED" ||
			cursor < e.AggregateVersion {
			return false, ErrInboxConflict
		}
		if err := tx.Commit(); err != nil {
			return false, fmt.Errorf("commit Inbox replay: %w", err)
		}
		return false, nil
	}
	if !errors.Is(err, sql.ErrNoRows) {
		return false, fmt.Errorf("check Inbox replay: %w", err)
	}
	// A different event ID must not claim an already-applied version.
	if e.AggregateVersion <= cursor {
		return false, ErrInboxConflict
	}
	if e.AggregateVersion != cursor+1 {
		return false, ErrInboxGap
	}
	_, err = tx.ExecContext(ctx, `INSERT INTO compute_inbox_events(
		consumer_name,event_id,producer,event_type,schema_version,tenant_id,
		aggregate_type,aggregate_id,aggregate_version,payload,state)
		VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$10::jsonb,'RECEIVED')`,
		e.ConsumerName, e.EventID, e.Producer, e.EventType, e.SchemaVersion,
		e.TenantID, e.AggregateType, e.AggregateID, e.AggregateVersion, []byte(e.Payload))
	if err != nil {
		return false, fmt.Errorf("persist received Inbox event: %w", err)
	}
	if err := handle(ctx, tx, e); err != nil {
		return false, fmt.Errorf("Inbox business handler: %w", err)
	}
	_, err = tx.ExecContext(ctx, `UPDATE compute_inbox_events
		SET state='APPLIED',applied_at=clock_timestamp()
		WHERE consumer_name=$1 AND event_id=$2 AND state='RECEIVED'`,
		e.ConsumerName, e.EventID)
	if err != nil {
		return false, fmt.Errorf("apply Inbox event: %w", err)
	}
	result, err := tx.ExecContext(ctx, `UPDATE compute_inbox_cursors SET
		last_applied_version=$5,updated_at=clock_timestamp()
		WHERE consumer_name=$1 AND tenant_id=$2 AND aggregate_type=$3
		AND aggregate_id=$4 AND last_applied_version=$6`,
		e.ConsumerName, e.TenantID, e.AggregateType, e.AggregateID,
		e.AggregateVersion, cursor)
	if err != nil {
		return false, fmt.Errorf("advance Inbox cursor: %w", err)
	}
	rows, err := result.RowsAffected()
	if err != nil {
		return false, err
	}
	if rows != 1 {
		return false, ErrInboxConflict
	}
	if err := tx.Commit(); err != nil {
		return false, fmt.Errorf("commit Inbox transaction: %w", err)
	}
	return true, nil
}
