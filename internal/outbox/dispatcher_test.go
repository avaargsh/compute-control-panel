package outbox

import (
	"context"
	"database/sql"
	"errors"
	"testing"
	"time"

	"github.com/avaargsh/compute-control-panel/internal/store/postgres"
)

type journalStub struct {
	event       postgres.OutboxEvent
	claimErr    error
	ackErr      error
	retryErr    error
	claimed     int
	acked       int
	retried     int
	retryReason string
}

func (j *journalStub) ClaimOutbox(_ context.Context, tenant, owner string, _ time.Duration) (postgres.OutboxEvent, error) {
	j.claimed++
	if j.claimErr != nil {
		return postgres.OutboxEvent{}, j.claimErr
	}
	if j.event.TenantID != tenant {
		return postgres.OutboxEvent{}, errors.New("wrong tenant")
	}
	return j.event, nil
}
func (j *journalStub) MarkOutboxPublished(_ context.Context, tenant, event, owner string, attempt int64) error {
	j.acked++
	if tenant != j.event.TenantID || event != j.event.ID || attempt != j.event.Attempts {
		return errors.New("ACK wrong lease")
	}
	return j.ackErr
}
func (j *journalStub) RetryOutbox(_ context.Context, tenant, event, owner string, attempt int64, backoff time.Duration, reason string) error {
	j.retried++
	j.retryReason = reason
	if tenant != j.event.TenantID || event != j.event.ID || attempt != j.event.Attempts {
		return errors.New("retry wrong lease")
	}
	return j.retryErr
}

type publisherStub struct {
	calls    int
	received postgres.OutboxEvent
	err      error
}

func (p *publisherStub) Publish(_ context.Context, event postgres.OutboxEvent) error {
	p.calls++
	p.received = event
	return p.err
}

func testEvent() postgres.OutboxEvent {
	return postgres.OutboxEvent{ID: "event-1", TenantID: "tenant-1", Attempts: 2, AggregateVersion: 1, Producer: "compute-control-panel", Payload: []byte(`{"ok":true}`)}
}

func TestDispatcherPersistsACKOnlyAfterPublisherSuccess(t *testing.T) {
	j := &journalStub{event: testEvent()}
	p := &publisherStub{}
	d, err := NewDispatcher(j, p, 0)
	if err != nil {
		t.Fatal(err)
	}
	found, err := d.DispatchOnce(context.Background(), "tenant-1", "sender", time.Minute)
	if err != nil || !found || j.claimed != 1 || p.calls != 1 || j.acked != 1 || j.retried != 0 || p.received.ID != "event-1" {
		t.Fatalf("success path found=%v err=%v claims=%d publish=%d ack=%d retry=%d", found, err, j.claimed, p.calls, j.acked, j.retried)
	}
}
func TestDispatcherPublisherErrorSchedulesAtLeastOnceRetry(t *testing.T) {
	j := &journalStub{event: testEvent()}
	p := &publisherStub{err: errors.New("broker write ACK lost")}
	d, _ := NewDispatcher(j, p, time.Second)
	found, err := d.DispatchOnce(context.Background(), "tenant-1", "sender", time.Minute)
	if !found || err == nil || p.calls != 1 || j.acked != 0 || j.retried != 1 || j.retryReason != "PUBLISH_FAILED" {
		t.Fatalf("lost broker ACK improperly marked complete: found=%v err=%v publish=%d ack=%d retry=%d reason=%q", found, err, p.calls, j.acked, j.retried, j.retryReason)
	}
}
func TestDispatcherACKFailureDoesNotReleaseOrClaimSuccess(t *testing.T) {
	j := &journalStub{event: testEvent(), ackErr: postgres.ErrOutboxLeaseLost}
	p := &publisherStub{}
	d, _ := NewDispatcher(j, p, 0)
	found, err := d.DispatchOnce(context.Background(), "tenant-1", "sender", time.Minute)
	if !found || !errors.Is(err, postgres.ErrOutboxLeaseLost) || p.calls != 1 || j.retried != 0 {
		t.Fatalf("ACK loss hidden or incorrectly retried: found=%v err=%v calls=%d retry=%d", found, err, p.calls, j.retried)
	}
}
func TestDispatcherNoPendingWork(t *testing.T) {
	j := &journalStub{claimErr: sql.ErrNoRows}
	p := &publisherStub{}
	d, _ := NewDispatcher(j, p, 0)
	found, err := d.DispatchOnce(context.Background(), "tenant-1", "sender", time.Minute)
	if found || err != nil || p.calls != 0 || j.acked != 0 || j.retried != 0 {
		t.Fatalf("empty queue was dispatched: found=%v err=%v", found, err)
	}
}
func TestDispatcherRetryPersistenceErrorIsNotHidden(t *testing.T) {
	j := &journalStub{event: testEvent(), retryErr: postgres.ErrOutboxLeaseLost}
	p := &publisherStub{err: errors.New("publisher unavailable")}
	d, _ := NewDispatcher(j, p, time.Second)
	_, err := d.DispatchOnce(context.Background(), "tenant-1", "sender", time.Minute)
	if !errors.Is(err, postgres.ErrOutboxLeaseLost) || p.calls != 1 || j.acked != 0 {
		t.Fatalf("retry persistence failure hidden: %v", err)
	}
}
