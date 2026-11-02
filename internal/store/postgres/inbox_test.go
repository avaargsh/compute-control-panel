package postgres

import (
	"context"
	"database/sql"
	"errors"
	"sync"
	"testing"
	"time"
)

func inboxFixture(t *testing.T, tenant, aggregate string, version int64) InboxEvent {
	t.Helper()
	return InboxEvent{
		ConsumerName: "compute-observer", EventID: "inbound-" + randomSuffix(t),
		Producer: "agent-control-panel", EventType: "compute.workload.changed.v1",
		SchemaVersion: 1, TenantID: tenant, AggregateType: "compute_workload",
		AggregateID: aggregate, AggregateVersion: version,
		Payload: []byte(`{"operation_id":"op-1","generation":1}`),
	}
}

func inboxEffect(ctx context.Context, tx *sql.Tx, event InboxEvent) error {
	_, err := tx.ExecContext(ctx, `INSERT INTO compute_outbox_events(
		event_id,producer,event_type,schema_version,tenant_id,aggregate_type,
		aggregate_id,aggregate_version,occurred_at,correlation_id,payload)
		VALUES($1,'compute-control-panel','inbox.applied.v1',1,$2,'inbox_effect',
			$3,1,clock_timestamp(),$4,'{}'::jsonb)`,
		"effect-"+event.EventID, event.TenantID, event.EventID, "inbox-"+event.EventID)
	return err
}

func TestInboxIdempotentConcurrentRedelivery(t *testing.T) {
	db := openIntegrationDB(t)
	repo, _ := NewRepository(db)
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	e := inboxFixture(t, "tenant-inbox-"+randomSuffix(t), "aggregate-"+randomSuffix(t), 1)
	const workers = 12
	applied := make([]bool, workers)
	errs := make([]error, workers)
	var wg sync.WaitGroup
	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			applied[i], errs[i] = repo.ApplyInbox(ctx, e, inboxEffect)
		}(i)
	}
	wg.Wait()
	n := 0
	for i, err := range errs {
		if err != nil {
			t.Fatalf("worker %d: %v", i, err)
		}
		if applied[i] {
			n++
		}
	}
	if n != 1 {
		t.Fatalf("expected exactly one durable effect, got %d", n)
	}
	var count, cursor int64
	if err := db.QueryRowContext(ctx, `SELECT count(*) FROM compute_outbox_events WHERE tenant_id=$1 AND aggregate_type='inbox_effect' AND aggregate_id=$2`,
		e.TenantID, e.EventID).Scan(&count); err != nil {
		t.Fatal(err)
	}
	if err := db.QueryRowContext(ctx, `SELECT last_applied_version FROM compute_inbox_cursors
		WHERE consumer_name=$1 AND tenant_id=$2 AND aggregate_type=$3 AND aggregate_id=$4`,
		e.ConsumerName, e.TenantID, e.AggregateType, e.AggregateID).Scan(&cursor); err != nil {
		t.Fatal(err)
	}
	if count != 1 || cursor != 1 {
		t.Fatalf("effect=%d cursor=%d", count, cursor)
	}
}

func TestInboxRejectsGapAndConflictingIdentity(t *testing.T) {
	db := openIntegrationDB(t)
	repo, _ := NewRepository(db)
	ctx := context.Background()
	tenant := "tenant-inbox-" + randomSuffix(t)
	aggregate := "aggregate-" + randomSuffix(t)
	first := inboxFixture(t, tenant, aggregate, 1)
	second := inboxFixture(t, tenant, aggregate, 2)
	if _, err := repo.ApplyInbox(ctx, second, inboxEffect); !errors.Is(err, ErrInboxGap) {
		t.Fatalf("missing version 1 was accepted: %v", err)
	}
	if applied, err := repo.ApplyInbox(ctx, first, inboxEffect); err != nil || !applied {
		t.Fatalf("apply first: applied=%v err=%v", applied, err)
	}
	if applied, err := repo.ApplyInbox(ctx, second, inboxEffect); err != nil || !applied {
		t.Fatalf("apply second: applied=%v err=%v", applied, err)
	}
	if applied, err := repo.ApplyInbox(ctx, first, inboxEffect); err != nil || applied {
		t.Fatalf("exact replay was not a no-op: applied=%v err=%v", applied, err)
	}
	changedPayload := first
	changedPayload.Payload = []byte(`{"operation_id":"different"}`)
	if _, err := repo.ApplyInbox(ctx, changedPayload, inboxEffect); !errors.Is(err, ErrInboxConflict) {
		t.Fatalf("same event ID changed payload: %v", err)
	}
	changedEventID := first
	changedEventID.EventID = "another-event-" + randomSuffix(t)
	if _, err := repo.ApplyInbox(ctx, changedEventID, inboxEffect); !errors.Is(err, ErrInboxConflict) {
		t.Fatalf("same aggregate version changed event ID: %v", err)
	}
	changedTenant := first
	changedTenant.TenantID = "other-tenant-" + randomSuffix(t)
	if _, err := repo.ApplyInbox(ctx, changedTenant, inboxEffect); !errors.Is(err, ErrInboxConflict) {
		t.Fatalf("same event ID reused across tenants: %v", err)
	}
}

func TestInboxAtomicBusinessFailureAndRetry(t *testing.T) {
	db := openIntegrationDB(t)
	repo, _ := NewRepository(db)
	ctx := context.Background()
	e := inboxFixture(t, "tenant-failure-"+randomSuffix(t), "aggregate-"+randomSuffix(t), 1)
	boom := errors.New("business transaction failed")
	if _, err := repo.ApplyInbox(ctx, e, func(ctx context.Context, tx *sql.Tx, ev InboxEvent) error {
		if err := inboxEffect(ctx, tx, ev); err != nil {
			return err
		}
		return boom
	}); !errors.Is(err, boom) {
		t.Fatalf("handler error was ignored: %v", err)
	}
	var count int
	if err := db.QueryRowContext(ctx, "SELECT count(*) FROM compute_inbox_events WHERE event_id=$1", e.EventID).Scan(&count); err != nil || count != 0 {
		t.Fatalf("failed handler stored Inbox: %d %v", count, err)
	}
	if err := db.QueryRowContext(ctx, "SELECT count(*) FROM compute_outbox_events WHERE aggregate_type='inbox_effect' AND aggregate_id=$1", e.EventID).Scan(&count); err != nil || count != 0 {
		t.Fatalf("failed handler committed effect: %d %v", count, err)
	}
	if applied, err := repo.ApplyInbox(ctx, e, inboxEffect); err != nil || !applied {
		t.Fatalf("retry after rollback: %v %v", applied, err)
	}
}

func TestInboxTenantAndConsumerCursorsAreIndependent(t *testing.T) {
	db := openIntegrationDB(t)
	repo, _ := NewRepository(db)
	ctx := context.Background()
	aggregate := "shared-" + randomSuffix(t)
	first := inboxFixture(t, "tenant-a-"+randomSuffix(t), aggregate, 1)
	second := inboxFixture(t, "tenant-b-"+randomSuffix(t), aggregate, 1)
	third := inboxFixture(t, first.TenantID, aggregate, 1)
	third.ConsumerName = "second-observer"
	for i, e := range []InboxEvent{first, second, third} {
		if applied, err := repo.ApplyInbox(ctx, e, inboxEffect); err != nil || !applied {
			t.Fatalf("independent consumer/tenant %d: %v %v", i, applied, err)
		}
	}
}

func TestInboxRejectsInvalidInput(t *testing.T) {
	db := openIntegrationDB(t)
	repo, _ := NewRepository(db)
	e := inboxFixture(t, "tenant-"+randomSuffix(t), "agg-"+randomSuffix(t), 1)
	e.Payload = []byte("[]")
	if _, err := repo.ApplyInbox(context.Background(), e, inboxEffect); !errors.Is(err, ErrInboxInvalid) {
		t.Fatalf("array payload accepted: %v", err)
	}
	e.Payload = []byte(`{"ok":true}`)
	if _, err := repo.ApplyInbox(context.Background(), e, nil); !errors.Is(err, ErrInboxInvalid) {
		t.Fatalf("nil business handler accepted: %v", err)
	}
}
