// Package outbox implements at-least-once delivery of committed Compute events.
// Real message-broker authentication and subscriber deduplication remain
// downstream adapters, not properties of this in-process coordinator.
package outbox

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"time"

	"github.com/avaargsh/compute-control-panel/internal/store/postgres"
)

var ErrInvalidDispatcher = errors.New("outbox dispatcher requires a journal and publisher")

// Journal persists event claims, owner/attempt fencing, ACK and retry
// decisions. It does not publish to an external broker.
type Journal interface {
	ClaimOutbox(context.Context, string, string, time.Duration) (postgres.OutboxEvent, error)
	MarkOutboxPublished(context.Context, string, string, string, int64) error
	RetryOutbox(context.Context, string, string, string, int64, time.Duration, string) error
}

// Publisher is an acknowledged external message send boundary. Return nil
// only after receiving a success ACK from the target message service.
type Publisher interface {
	Publish(context.Context, postgres.OutboxEvent) error
}

type Dispatcher struct {
	journal Journal
	publisher Publisher
	retryBackoff time.Duration
}

func NewDispatcher(journal Journal, publisher Publisher, retryBackoff time.Duration) (*Dispatcher, error) {
	if journal == nil || publisher == nil || retryBackoff < 0 || retryBackoff > 24*time.Hour {
		return nil, ErrInvalidDispatcher
	}
	return &Dispatcher{journal:journal,publisher:publisher,retryBackoff:retryBackoff},nil
}

// DispatchOnce leases exactly one event for one tenant. It sends the committed
// bytes without recomputing the payload, then ACKs using the exact attempt
// token. A broker success followed by a database failure intentionally leaves
// the lease until expiration, after which the event may be re-sent.
// No caller may infer exactly-once delivery from a successful broker request.
func (d *Dispatcher) DispatchOnce(ctx context.Context, tenantID, owner string, ttl time.Duration) (bool, error) {
	e,err:=d.journal.ClaimOutbox(ctx,tenantID,owner,ttl)
	if errors.Is(err,sql.ErrNoRows) { return false,nil }
	if err!=nil{return false,fmt.Errorf("claim outbox: %w",err)}
	if err:=d.publisher.Publish(ctx,e);err!=nil {
		// A broker timeout can mean the event was already received.
		// Persist a retry using the SAME event_id: consumers deduplicate.
		retryErr:=d.journal.RetryOutbox(ctx,e.TenantID,e.ID,owner,e.Attempts,d.retryBackoff,"PUBLISH_FAILED")
		if retryErr!=nil {
			return true,errors.Join(fmt.Errorf("publish outbox: %w",err),
				fmt.Errorf("persist outbox retry: %w",retryErr))
		}
		return true,fmt.Errorf("publish outbox: %w",err)
	}
	if err:=d.journal.MarkOutboxPublished(ctx,e.TenantID,e.ID,owner,e.Attempts);err!=nil{
		// Do NOT issue an immediate second Publish; lease fencing and
		// at-least-once retry protocol owns the recovery.
		return true,fmt.Errorf("acknowledge outbox delivery: %w",err)
	}
	return true,nil
}

var _ Journal = (*postgres.Repository)(nil)
