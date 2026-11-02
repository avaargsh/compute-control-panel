package postgres

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func inboxFixture(t *testing.T, tenant, aggregate string, version int64) InboxEvent {
	t.Helper()
	return InboxEvent{
		ConsumerName: "compute-inbox-test", EventID: "incoming-" + randomSuffix(t),
		Producer: "agent-control-panel", EventType: "compute.requested.v1",
		SchemaVersion: 1, TenantID: tenant, AggregateType: "run_intent",
		AggregateID: aggregate, AggregateVersion: version,
		Payload: json.RawMessage(`{"kind":"ComputeWorkload","operation_id":"op-1"}`),
	}
}

func persistInboxEffect(t *testing.T, ctx context.Context, tx *sql.Tx, e InboxEvent, eventID string) error {
	t.Helper()
	_, err := tx.ExecContext(ctx, `INSERT INTO compute_outbox_events (
		event_id,producer,event_type,schema_version,tenant_id,
		aggregate_type,aggregate_id,aggregate_version,occurred_at,
		correlation_id,payload
	) VALUES ($1,'compute-control-panel','compute.inbox.applied.v1',1,$2,
		'compute_inbox_effect',$3,1,clock_timestamp(),$4,'{}'::jsonb)`,
		eventID, e.TenantID, e.EventID, "correlation-"+eventID)
	return err
}

func TestInboxConcurrentRedeliveryAppliesEffectExactlyOnce(t *testing.T) {
	db := openIntegrationDB(t)
	repo, _ := NewRepository(db)
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	tenant := "inbox-tenant-" + randomSuffix(t)
	e := inboxFixture(t, tenant, "run-"+randomSuffix(t), 1)
	effectID := "out-effect-" + randomSuffix(t)
	var calls atomic.Int32
	const workers = 12
	created := make([]bool, workers)
	errs := make([]error, workers)
	var wg sync.WaitGroup
	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			created[i], errs[i] = repo.ApplyInboxEvent(ctx, e, func(ctx context.Context, tx *sql.Tx, event InboxEvent) error {
				calls.Add(1)
				return persistInboxEffect(t, ctx, tx, event, effectID)
			})
		}(i)
	}
	wg.Wait()
	successes := 0
	for i, err := range errs {
		if err != nil {
			t.Fatalf("redelivery[%d]: %v", i, err)
		}
		if created[i] {
			successes++
		}
	}
	if successes != 1 || calls.Load() != 1 {
		t.Fatalf("event applied %d times, callback invoked %d times", successes, calls.Load())
	}
	var applied, outbox int
	var cursor int64
	if err := db.QueryRowContext(ctx, `SELECT count(*) FROM compute_inbox_events
		WHERE consumer_name=$1 AND event_id=$2 AND state='APPLIED'`, e.ConsumerName, e.EventID).Scan(&applied); err != nil { t.Fatal(err) }
	if err := db.QueryRowContext(ctx, `SELECT count(*) FROM compute_outbox_events WHERE event_id=$1`, effectID).Scan(&outbox); err != nil {
		t.Fatal(err)
	}
	if err := db.QueryRowContext(ctx, `SELECT last_applied_version FROM compute_inbox_cursors
		WHERE consumer_name=$1 AND tenant_id=$2 AND aggregate_type=$3 AND aggregate_id=$4`,
		e.ConsumerName, e.TenantID, e.AggregateType, e.AggregateID).Scan(&cursor); err != nil { t.Fatal(err) }
	if applied != 1 || outbox != 1 || cursor != 1 {
		t.Fatalf("transaction not atomic: inbox=%d outbox=%d cursor=%d", applied, outbox, cursor)
	}
	restarted, _ := NewRepository(db)
	ok, err := restarted.ApplyInboxEvent(ctx, e, func(context.Context, *sql.Tx, InboxEvent) error {
		t.Fatal("restarted consumer repeated a committed effect")
		return nil
	})
	if err != nil || ok {
		t.Fatalf("restart redelivery: created=%v err=%v", ok, err)
	}
}

func TestInboxGapCollisionAndTenantIsolationFailClosed(t *testing.T) {
	db := openIntegrationDB(t)
	repo, _ := NewRepository(db)
	ctx := context.Background()
	tenant := "inbox-tenant-" + randomSuffix(t)
	aggregate := "run-" + randomSuffix(t)
	apply := func(e InboxEvent) (bool, error) {
		return repo.ApplyInboxEvent(ctx, e, func(context.Context, *sql.Tx, InboxEvent) error { return nil })
	}
	first := inboxFixture(t, tenant, aggregate, 1)
	gap := inboxFixture(t, tenant, aggregate, 3)
	if applied, err := apply(gap); applied || !errors.Is(err, ErrInboxGap) {
		t.Fatalf("out-of-order event accepted: %v %v", applied, err)
	}
	if applied, err := apply(first); !applied || err != nil {
		t.Fatalf("first event: %v %v", applied, err)
	}
	versionCollision := inboxFixture(t, tenant, aggregate, 1)
	if applied, err := apply(versionCollision); applied || !errors.Is(err, ErrInboxConflict) {
		t.Fatalf("different event id reused version: %v %v", applied, err)
	}
	changedPayload := first
	changedPayload.Payload = json.RawMessage(`{"kind":"ComputeWorkload","operation_id":"wrong"}`)
	if applied, err := apply(changedPayload); applied || !errors.Is(err, ErrInboxConflict) {
		t.Fatalf("event id reused with changed payload: %v %v", applied, err)
	}
	otherTenant := first
	otherTenant.TenantID = "other-" + tenant
	if applied, err := apply(otherTenant); applied || !errors.Is(err, ErrInboxConflict) {
		t.Fatalf("event id crossed tenant: %v %v", applied, err)
	}
	second := inboxFixture(t, tenant, aggregate, 2)
	if applied, err := apply(second); !applied || err != nil {
		t.Fatalf("second event: %v %v", applied, err)
	}
	if applied, err := apply(gap); !applied || err != nil {
		t.Fatalf("third event: %v %v", applied, err)
	}
	tenantB := inboxFixture(t, "isolated-"+tenant, aggregate, 1)
	if applied, err := apply(tenantB); !applied || err != nil {
		t.Fatalf("other tenant's independent sequence: %v %v", applied, err)
	}
}

func TestInboxHandlerRollbackRetainsUnappliedMessage(t *testing.T) {
	db := openIntegrationDB(t)
	repo, _ := NewRepository(db)
	ctx := context.Background()
	tenant := "inbox-tenant-" + randomSuffix(t)
	e := inboxFixture(t, tenant, "run-"+randomSuffix(t), 1)
	effectID := "out-effect-" + randomSuffix(t)
	reject := errors.New("domain transaction refused event")
	applied, err := repo.ApplyInboxEvent(ctx, e, func(ctx context.Context, tx *sql.Tx, e InboxEvent) error {
		if err := persistInboxEffect(t, ctx, tx, e, effectID); err != nil {
			return err
		}
		return reject
	})
	if applied || !errors.Is(err, reject) {
		t.Fatalf("failed handler marked event applied: %v %v", applied, err)
	}
	var count int
	if err := db.QueryRowContext(ctx, `SELECT count(*) FROM compute_outbox_events WHERE event_id=$1`, effectID).Scan(&count); err != nil {
		t.Fatal(err)
	}
	if count != 0 {
		t.Fatalf("failed handler committed side effect: %d", count)
	}
	applied, err = repo.ApplyInboxEvent(ctx, e, func(ctx context.Context, tx *sql.Tx, e InboxEvent) error {
		return persistInboxEffect(t, ctx, tx, e, effectID)
	})
	if !applied || err != nil {
		t.Fatalf("retry after rollback: %v %v", applied, err)
	}
}

func TestInboxMalformedPayloadCannotAcknowledge(t *testing.T) {
	db := openIntegrationDB(t)
	repo, _ := NewRepository(db)
	tenant := "inbox-tenant-" + randomSuffix(t)
	e := inboxFixture(t, tenant, "run-"+randomSuffix(t), 1)
	for _, raw := range []string{"null", "[]", "{", fmt.Sprintf("{%q:%q}", "x", "y")} {
		bad := e
		bad.Payload = json.RawMessage(raw)
		if raw[0] == '{' && raw != "{" {
			bad.SchemaVersion = 0
		}
		applied, err := repo.ApplyInboxEvent(context.Background(), bad,
			func(context.Context, *sql.Tx, InboxEvent) error { t.Fatal("invalid envelope dispatched"); return nil })
		if applied || !errors.Is(err, ErrInboxInvalid) {
			t.Fatalf("invalid inbound event accepted: %q: %v %v", raw, applied, err)
		}
	}
}
