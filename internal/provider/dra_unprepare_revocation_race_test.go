package provider

import (
	"context"
	"errors"
	"net/url"
	"testing"
	"time"
)

// A DBA revocation must not commit between a restricted write's source
// validation and its journal INSERT. An ACCESS EXCLUSIVE lock on the journal
// deliberately parks the write at INSERT, while the source key remains the
// independently maintained row whose FK lock serializes revocation.
func TestRestrictedDRAJournalSerializesConcurrentSourceRevocation(t *testing.T) {
	admin := openProviderPostgresDB(t)
	if !postgresCurrentUserIsSuperuser(t, admin) {
		t.Skip("requires disposable PG16 superuser for deterministic lock orchestration")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 25*time.Second)
	defer cancel()

	verifier, receipt, asOf := liveJournalReceiptFixture(t)
	candidate := releaseCandidateFixture()
	seedTrustedDRAReceiptSource(t, admin, verifier, receipt, candidate)

	dsn, err := url.Parse(kueueBindingDSN(t, draReceiptIngestRole, draReceiptIngestCITestPassword, ""))
	if err != nil {
		t.Fatal(err)
	}
	appName := kueueTestIdent(t, "ccp_dra_revocation_race_")
	q := dsn.Query()
	q.Set("application_name", appName)
	dsn.RawQuery = q.Encode()
	journal, db, err := OpenRestrictedDRAUnprepareReceiptJournal(ctx, dsn.String())
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	db.SetMaxOpenConns(1)

	// This lock is disposable test orchestration only: it forces the
	// restricted journal writer to pause after source verification but
	// before completing the INSERT, without a production test hook.
	hold, err := admin.BeginTx(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer hold.Rollback()
	if _, err := hold.ExecContext(ctx, `LOCK TABLE public.dra_unprepare_receipt_journal IN ACCESS EXCLUSIVE MODE`); err != nil {
		t.Fatal(err)
	}
	type outcome struct {
		added bool
		err   error
	}
	done := make(chan outcome, 1)

	go func() {
		added, err := journal.VerifyAndRecord(ctx, verifier, candidate, receipt, asOf)
		done <- outcome{added: added, err: err}
	}()

	// Synchronize on PostgreSQL's actual INSERT waiting for the journal
	// relation lock, not on arbitrary sleep/scheduler timing.
	blocked := false
	poll := time.NewTicker(30 * time.Millisecond)
	defer poll.Stop()
	for !blocked {
		var count int
		if err := admin.QueryRowContext(ctx, `
			SELECT count(*)::integer FROM pg_catalog.pg_stat_activity
			WHERE application_name=$1 AND wait_event_type='Lock'
			  AND query ILIKE '%INSERT INTO public.dra_unprepare_receipt_journal%'
		`, appName).Scan(&count); err != nil {
			t.Fatal(err)
		}
		if count > 0 {
			blocked = true
			break
		}
		select {
		case result := <-done:
			t.Fatalf("journal write completed before blocked INSERT: new=%t err=%v", result.added, result.err)
		case <-ctx.Done():
			t.Fatalf("journal INSERT never reached lock wait: %v", ctx.Err())
		case <-poll.C:
		}
	}

	revoke, err := admin.BeginTx(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer revoke.Rollback()
	if _, err := revoke.ExecContext(ctx, `SELECT pg_catalog.set_config('lock_timeout','500ms',true)`); err != nil {
		t.Fatal(err)
	}
	_, revokeErr := revoke.ExecContext(ctx, `INSERT INTO public.dra_unprepare_source_revocations
		(cluster_id,source_id,key_id,reason) VALUES ($1,$2,$3,'concurrent DBA test revocation')`,
		receipt.Event.ClusterID, receipt.Event.SourceID, receipt.Event.KeyID)
	// A successful revocation INSERT here would expose a TOCTOU gap. The
	// trusted key's FOR UPDATE lock must make the FK's KEY SHARE lock wait
	// until the journal transaction commits (or rolls back) first.
	if postgresSQLState(revokeErr) != "55P03" {
		t.Fatalf("revocation could overtake pending signed receipt: state=%s err=%v",
			postgresSQLState(revokeErr), revokeErr)
	}
	if err := revoke.Rollback(); err != nil {
		t.Fatal(err)
	}
	if err := hold.Rollback(); err != nil {
		t.Fatal(err)
	}
	select {
	case result := <-done:
		if result.err != nil || !result.added {
			t.Fatalf("unrevoked original source failed after lock release: new=%t err=%v", result.added, result.err)
		}
	case <-ctx.Done():
		t.Fatalf("journal never resumed after releasing DB locks: %v", ctx.Err())
	}

	_, err = admin.ExecContext(ctx, `INSERT INTO public.dra_unprepare_source_revocations
		(cluster_id,source_id,key_id,reason) VALUES ($1,$2,$3,'revoked after journal commit')`,
		receipt.Event.ClusterID, receipt.Event.SourceID, receipt.Event.KeyID)
	if err != nil {
		t.Fatal(err)
	}
	added, err := journal.VerifyAndRecord(ctx, verifier, candidate, receipt, asOf)
	if !errors.Is(err, ErrDRAReceiptSourceUntrusted) || added {
		t.Fatalf("post-commit revocation allowed event replay: new=%t err=%v", added, err)
	}
}
