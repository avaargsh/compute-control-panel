package postgres

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"
)

var ErrOutboxLeaseLost = errors.New("outbox lease is no longer held by this attempt")
var ErrOutboxInvalid = errors.New("invalid outbox dispatch request")

// OutboxEvent is a committed event plus its current delivery lease.
// Attempts is the persisted fencing token. A previous attempt can never
// acknowledge a lease that has been reclaimed by another publisher.
type OutboxEvent struct {
	ID               string
	TenantID         string
	Producer         string
	EventType        string
	AggregateType    string
	AggregateID      string
	AggregateVersion int64
	Attempts         int64
	Payload          json.RawMessage
	CorrelationID    string
	OccurredAt       time.Time
	LeaseExpiresAt   time.Time
}

type outboxScanner interface{ Scan(...any) error }

func scanOutbox(row outboxScanner) (OutboxEvent, error) {
	var e OutboxEvent
	err := row.Scan(&e.ID, &e.TenantID, &e.Producer, &e.EventType,
		&e.AggregateType, &e.AggregateID, &e.AggregateVersion, &e.Attempts,
		&e.Payload, &e.CorrelationID, &e.OccurredAt, &e.LeaseExpiresAt)
	return e, err
}

func outboxLabel(value string) bool {
	return value != "" && value == strings.TrimSpace(value) && len(value) <= 256
}

// ClaimOutbox leases one tenant-scoped event with FOR UPDATE SKIP LOCKED.
// Events within an aggregate cannot overtake earlier unpublished versions;
// unrelated aggregates and other tenants can be dispatched concurrently.
// PostgreSQL is authoritative for every lease and retry timestamp.
func (r *Repository) ClaimOutbox(ctx context.Context, tenantID, owner string, ttl time.Duration) (OutboxEvent, error) {
	if !outboxLabel(tenantID) || !outboxLabel(owner) || ttl < time.Millisecond || ttl > 24*time.Hour {
		return OutboxEvent{}, ErrOutboxInvalid
	}
	const query = `WITH next_event AS (
        SELECT e.event_id FROM compute_outbox_events e
        WHERE e.tenant_id=$1
          AND ((e.delivery_state='PENDING' AND e.next_attempt_at <= clock_timestamp())
            OR (e.delivery_state='PUBLISHING' AND e.lease_expires_at <= clock_timestamp()))
          AND NOT EXISTS (
            SELECT 1 FROM compute_outbox_events older
            WHERE older.producer=e.producer AND older.tenant_id=e.tenant_id
              AND older.aggregate_type=e.aggregate_type AND older.aggregate_id=e.aggregate_id
              AND older.aggregate_version < e.aggregate_version
              AND older.delivery_state <> 'PUBLISHED')
        ORDER BY e.occurred_at, e.event_id
        FOR UPDATE OF e SKIP LOCKED LIMIT 1
    )
    UPDATE compute_outbox_events e SET
        delivery_state='PUBLISHING', attempts=e.attempts+1,
        lease_owner=$2,
        lease_expires_at=clock_timestamp()+($3::bigint * INTERVAL '1 microsecond')
    FROM next_event
    WHERE e.event_id=next_event.event_id
    RETURNING e.event_id,e.tenant_id,e.producer,e.event_type,e.aggregate_type,
        e.aggregate_id,e.aggregate_version,e.attempts,e.payload,e.correlation_id,
        e.occurred_at,e.lease_expires_at`
	event, err := scanOutbox(r.db.QueryRowContext(ctx, query, tenantID, owner, ttl.Microseconds()))
	if err != nil {
		return OutboxEvent{}, err
	}
	return event, nil
}

// MarkOutboxPublished requires a current owner/attempt and an unexpired lease.
// A lost broker ACK may lead to redelivery. Consumers must deduplicate by the
// stable event_id: this boundary guarantees at-least-once, not exactly-once.
func (r *Repository) MarkOutboxPublished(ctx context.Context, tenantID, eventID, owner string, attempt int64) error {
	if !outboxLabel(tenantID) || !outboxLabel(eventID) || !outboxLabel(owner) || attempt <= 0 {
		return ErrOutboxInvalid
	}
	result, err := r.db.ExecContext(ctx, `UPDATE compute_outbox_events SET
        delivery_state='PUBLISHED', published_at=clock_timestamp(),
        lease_owner=NULL, lease_expires_at=NULL, last_error_code=NULL
        WHERE tenant_id=$1 AND event_id=$2 AND lease_owner=$3 AND attempts=$4
          AND delivery_state='PUBLISHING' AND lease_expires_at > clock_timestamp()`,
		tenantID, eventID, owner, attempt)
	if err != nil {
		return fmt.Errorf("acknowledge outbox: %w", err)
	}
	affected, err := result.RowsAffected()
	if err != nil {
		return err
	}
	if affected != 1 {
		return ErrOutboxLeaseLost
	}
	return nil
}

// RetryOutbox atomically releases a failed delivery attempt and schedules a
// persisted retry. A retry is only valid while the caller still owns the
// exact attempt. Poison-event quarantine needs a separate policy.
func (r *Repository) RetryOutbox(ctx context.Context, tenantID, eventID, owner string, attempt int64, backoff time.Duration, reason string) error {
	if !outboxLabel(tenantID) || !outboxLabel(eventID) || !outboxLabel(owner) || attempt <= 0 ||
		backoff < 0 || backoff > 24*time.Hour || !outboxLabel(reason) {
		return ErrOutboxInvalid
	}
	result, err := r.db.ExecContext(ctx, `UPDATE compute_outbox_events SET
        delivery_state='PENDING', lease_owner=NULL, lease_expires_at=NULL,
        next_attempt_at=clock_timestamp()+($5::bigint * INTERVAL '1 microsecond'),
        last_error_code=$6
        WHERE tenant_id=$1 AND event_id=$2 AND lease_owner=$3 AND attempts=$4
          AND delivery_state='PUBLISHING' AND lease_expires_at > clock_timestamp()`,
		tenantID, eventID, owner, attempt, backoff.Microseconds(), reason)
	if err != nil {
		return fmt.Errorf("schedule outbox retry: %w", err)
	}
	affected, err := result.RowsAffected()
	if err != nil {
		return err
	}
	if affected != 1 {
		return ErrOutboxLeaseLost
	}
	return nil
}
