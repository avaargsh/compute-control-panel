package postgres

import (
	"context"
	"database/sql"
	"errors"
	"testing"
	"time"
)

// Every dispatcher lease must come from the durable public Outbox, not from
// a caller/session-local table with a matching name and forged payload.
func TestOutboxRejectsTempOnlyDispatchEvent(t *testing.T) {
	db := openIntegrationDB(t)
	db.SetMaxOpenConns(1)
	db.SetMaxIdleConns(1)
	repo, err := NewRepository(db)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	if _, err := db.ExecContext(ctx, `CREATE TEMP TABLE compute_outbox_events
		(LIKE public.compute_outbox_events INCLUDING DEFAULTS)
		ON COMMIT PRESERVE ROWS`); err != nil {
		t.Fatal(err)
	}
	tenant := "tenant-temp-dispatch-" + randomSuffix(t)
	id := insertOutboxFixture(t, db, tenant, "aggregate-"+randomSuffix(t), 1)
	var real, shadow int
	if err := db.QueryRowContext(ctx, "SELECT count(*) FROM public.compute_outbox_events WHERE event_id=$1", id).Scan(&real); err != nil {
		t.Fatal(err)
	}
	if err := db.QueryRowContext(ctx, "SELECT count(*) FROM pg_temp.compute_outbox_events WHERE event_id=$1", id).Scan(&shadow); err != nil {
		t.Fatal(err)
	}
	if real != 0 || shadow != 1 {
		t.Fatalf("test setup: permanent=%d temp=%d", real, shadow)
	}
	if claimed, err := repo.ClaimOutbox(ctx, tenant, "publisher", time.Minute); !errors.Is(err, sql.ErrNoRows) {
		t.Fatalf("TEMP-only event can be published: claim=%+v err=%v", claimed, err)
	}
}

func shadowPermanentOutbox(t *testing.T, ctx context.Context, db *sql.DB, eventID string) {
	t.Helper()
	if _, err := db.ExecContext(ctx, `CREATE TEMP TABLE compute_outbox_events
		(LIKE public.compute_outbox_events INCLUDING DEFAULTS)
		ON COMMIT PRESERVE ROWS`); err != nil {
		t.Fatal(err)
	}
	if _, err := db.ExecContext(ctx, `INSERT INTO pg_temp.compute_outbox_events
		SELECT * FROM public.compute_outbox_events WHERE event_id=$1`, eventID); err != nil {
		t.Fatal(err)
	}
}

func requireOutboxState(t *testing.T, ctx context.Context, db *sql.DB, eventID, schema, want string, attempts int64) {
	t.Helper()
	var state string
	var gotAttempts int64
	query := "SELECT delivery_state,attempts FROM " + schema + ".compute_outbox_events WHERE event_id=$1"
	if err := db.QueryRowContext(ctx, query, eventID).Scan(&state, &gotAttempts); err != nil {
		t.Fatal(err)
	}
	if state != want || gotAttempts != attempts {
		t.Fatalf("%s Outbox state=%s attempts=%d, want %s/%d", schema, state, gotAttempts, want, attempts)
	}
}

func TestOutboxClaimFencesPermanentRowInsteadOfTempShadow(t *testing.T) {
	db := openIntegrationDB(t)
	db.SetMaxOpenConns(1)
	db.SetMaxIdleConns(1)
	repo, err := NewRepository(db)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	tenant := "tenant-claim-shadow-" + randomSuffix(t)
	id := insertOutboxFixture(t, db, tenant, "aggregate-"+randomSuffix(t), 1)
	shadowPermanentOutbox(t, ctx, db, id)
	got, err := repo.ClaimOutbox(ctx, tenant, "publisher", time.Minute)
	if err != nil || got.ID != id || got.Attempts != 1 {
		t.Fatalf("claim authoritative Outbox: event=%+v err=%v", got, err)
	}
	requireOutboxState(t, ctx, db, id, "public", "PUBLISHING", 1)
	requireOutboxState(t, ctx, db, id, "pg_temp", "PENDING", 0)
}

func TestOutboxACKAndRetryCannotAlterTempShadowInsteadOfPermanentLease(t *testing.T) {
	for _, tc := range []struct {
		name string
		ack  bool
		want string
	}{
		{name: "published acknowledgement", ack: true, want: "PUBLISHED"},
		{name: "scheduled retry", ack: false, want: "PENDING"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			db := openIntegrationDB(t)
			db.SetMaxOpenConns(1)
			db.SetMaxIdleConns(1)
			repo, err := NewRepository(db)
			if err != nil {
				t.Fatal(err)
			}
			ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
			defer cancel()
			tenant := "tenant-outbox-finish-" + randomSuffix(t)
			id := insertOutboxFixture(t, db, tenant, "aggregate-"+randomSuffix(t), 1)
			leased, err := repo.ClaimOutbox(ctx, tenant, "publisher", time.Minute)
			if err != nil || leased.ID != id {
				t.Fatalf("seed durable lease: event=%+v err=%v", leased, err)
			}
			shadowPermanentOutbox(t, ctx, db, id)
			if tc.ack {
				err = repo.MarkOutboxPublished(ctx, tenant, id, "publisher", leased.Attempts)
			} else {
				err = repo.RetryOutbox(ctx, tenant, id, "publisher", leased.Attempts, 0, "BROKER_TIMEOUT")
			}
			if err != nil {
				t.Fatalf("complete durable lease: %v", err)
			}
			requireOutboxState(t, ctx, db, id, "public", tc.want, leased.Attempts)
			requireOutboxState(t, ctx, db, id, "pg_temp", "PUBLISHING", leased.Attempts)
		})
	}
}
