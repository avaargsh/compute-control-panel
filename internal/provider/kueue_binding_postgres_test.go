package provider

import (
	"context"
	"crypto/rand"
	"database/sql"
	"encoding/hex"
	"errors"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/avaargsh/compute-control-panel/internal/operation"
	"github.com/jackc/pgx/v5/pgconn"
	_ "github.com/jackc/pgx/v5/stdlib"
)

func uniqueKueueRequest(t *testing.T) Request {
	t.Helper()
	var buf [8]byte
	if _, err := rand.Read(buf[:]); err != nil {
		t.Fatal(err)
	}
	suffix := hex.EncodeToString(buf[:])
	req := kueueAdapterRequest()
	req.OperationID = "op-" + suffix
	req.IdempotencyKey = "idem-" + suffix
	req.WorkloadID = "workload-" + suffix
	return req
}

func postgresDeleteRequest(create Request, observed operation.Observation) Request {
	req := create
	req.OperationID = "del-" + create.OperationID
	req.IdempotencyKey = "idem-" + req.OperationID
	req.Kind = operation.Delete
	req.TargetResourceID = observed.ResourceID
	req.TargetResourceUID = observed.ResourceUID
	return req
}

func assertKueueJobBindingStoreConformance(t *testing.T, store KueueJobBindingStore) {
	t.Helper()
	req := uniqueKueueRequest(t)
	const namespace = "tenant-a"
	suffix := strings.TrimPrefix(req.OperationID, "op-")
	jobName := "job-" + suffix
	uid := "uid-" + suffix
	if err := store.PutCreatePin("Not_A_Namespace", req, jobName, uid); !errors.Is(err, ErrInvalidKueueObservation) {
		t.Fatalf("invalid namespace: %v", err)
	}
	if err := store.PutCreatePin(namespace, req, jobName, "has space"); !errors.Is(err, ErrInvalidKueueObservation) {
		t.Fatalf("invalid uid: %v", err)
	}
	if err := store.PutCreatePin(namespace, req, jobName, uid); err != nil {
		t.Fatal(err)
	}
	if err := store.PutCreatePin(namespace, req, jobName, uid); err != nil {
		t.Fatal(err)
	}
	if err := store.PutCreatePin(namespace, req, jobName, uid+"-other"); !errors.Is(err, ErrOperationConflict) {
		t.Fatalf("pin uid rewrite: %v", err)
	}
	if err := store.PutCreatePin(namespace, req, "alt-"+jobName, uid); !errors.Is(err, ErrOperationConflict) {
		t.Fatalf("pin name rewrite: %v", err)
	}
	changed := req
	changed.WorkloadID = "workload-other-" + suffix
	if err := store.PutCreatePin(namespace, changed, jobName, uid); !errors.Is(err, ErrOperationConflict) {
		t.Fatalf("pin request rewrite: %v", err)
	}
	got, ok, err := store.GetCreatePin(namespace, req)
	if err != nil || !ok || got.UID != uid || got.Name != jobName || got.Namespace != namespace || !sameRequest(got.Request, req) {
		t.Fatalf("pin %+v ok=%v err=%v", got, ok, err)
	}
	if _, ok, err := store.GetCreatePin("other-ns", req); err != nil || ok {
		t.Fatalf("pin crossed namespace: ok=%v err=%v", ok, err)
	}
	missing := req
	missing.OperationID = "missing-" + suffix
	if _, ok, err := store.GetCreatePin(namespace, missing); err != nil || ok {
		t.Fatalf("missing pin: ok=%v err=%v", ok, err)
	}
	found, ok, err := store.CreatePinUID(namespace, jobName)
	if err != nil || !ok || found != uid {
		t.Fatalf("pin uid %q ok=%v err=%v", found, ok, err)
	}
	if _, ok, err := store.CreatePinUID("other-ns", jobName); err != nil || ok {
		t.Fatalf("name scan crossed namespace: ok=%v err=%v", ok, err)
	}

	remove := req
	remove.Kind = operation.Delete
	remove.OperationID = "del-" + suffix
	remove.IdempotencyKey = "idem-del-" + suffix
	remove.TargetResourceID = namespace + "/" + jobName
	remove.TargetResourceUID = uid
	if err := store.PutDeleteTombstone(namespace, remove, jobName, uid, false); err != nil {
		t.Fatal(err)
	}
	if err := store.PutDeleteTombstone(namespace, remove, jobName, uid, true); err != nil {
		t.Fatal(err)
	}
	if err := store.PutDeleteTombstone(namespace, remove, jobName, uid, false); err != nil {
		t.Fatal(err)
	}
	if err := store.PutDeleteTombstone(namespace, remove, jobName, uid+"-other", true); !errors.Is(err, ErrOperationConflict) {
		t.Fatalf("tombstone uid rewrite: %v", err)
	}
	if err := store.PutDeleteTombstone(namespace, remove, "alt-"+jobName, uid, true); !errors.Is(err, ErrOperationConflict) {
		t.Fatalf("tombstone name rewrite: %v", err)
	}
	changedDelete := remove
	changedDelete.RequestDigest = "sha256:" + strings.Repeat("cd", 32)
	if err := store.PutDeleteTombstone(namespace, changedDelete, jobName, uid, true); !errors.Is(err, ErrOperationConflict) {
		t.Fatalf("tombstone request rewrite: %v", err)
	}
	tomb, ok, err := store.GetDeleteTombstone(namespace, remove)
	if err != nil || !ok || !tomb.Acknowledged || tomb.UID != uid || tomb.Name != jobName || !sameRequest(tomb.Request, remove) {
		t.Fatalf("tombstone %+v ok=%v err=%v", tomb, ok, err)
	}
	if _, ok, err := store.GetDeleteTombstone("other-ns", remove); err != nil || ok {
		t.Fatalf("tombstone crossed namespace: ok=%v err=%v", ok, err)
	}
}

func openProviderPostgresDB(t *testing.T) *sql.DB {
	t.Helper()
	dsn := os.Getenv("DATABASE_URL")
	if dsn == "" {
		t.Skip("DATABASE_URL is not configured; PostgreSQL integration test skipped")
	}
	db, err := sql.Open("pgx", dsn)
	if err != nil {
		t.Fatalf("open PostgreSQL: %v", err)
	}
	db.SetMaxOpenConns(4)
	t.Cleanup(func() { _ = db.Close() })
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := db.PingContext(ctx); err != nil {
		t.Fatalf("ping PostgreSQL: %v", err)
	}
	var pins, tombs bool
	var migration string
	if err := db.QueryRowContext(ctx, `
		SELECT to_regclass('public.kueue_job_create_pins') IS NOT NULL,
		       to_regclass('public.kueue_job_delete_tombstones') IS NOT NULL,
		       COALESCE((SELECT name FROM schema_migrations WHERE version = 14), '')`).Scan(&pins, &tombs, &migration); err != nil {
		t.Fatalf("read kueue binding schema: %v", err)
	}
	if !pins || !tombs || migration != "0014_kueue_job_binding.sql" {
		t.Fatalf("migration 0014 is not applied: pins=%t tombs=%t migration=%q", pins, tombs, migration)
	}
	return db
}

func postgresSQLState(err error) string {
	var pgErr *pgconn.PgError
	if errors.As(err, &pgErr) {
		return pgErr.Code
	}
	return ""
}

func TestPostgresKueueJobBindingSQLRejectsRewrite(t *testing.T) {
	db := openProviderPostgresDB(t)
	store, err := NewPostgresKueueJobBindingStore(db)
	if err != nil {
		t.Fatal(err)
	}
	req := uniqueKueueRequest(t)
	const namespace = "tenant-a"
	suffix := strings.TrimPrefix(req.OperationID, "op-")
	jobName := "job-" + suffix
	uid := "uid-" + suffix
	if err := store.PutCreatePin(namespace, req, jobName, uid); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	var created, updated time.Time
	if err := db.QueryRowContext(ctx, `
		SELECT created_at, updated_at FROM public.kueue_job_create_pins
		WHERE namespace=$1 AND tenant_id=$2 AND operation_id=$3`,
		namespace, req.TenantID, req.OperationID).Scan(&created, &updated); err != nil {
		t.Fatal(err)
	}
	if err := store.PutCreatePin(namespace, req, jobName, uid); err != nil {
		t.Fatal(err)
	}
	var createdAgain, updatedAgain time.Time
	if err := db.QueryRowContext(ctx, `
		SELECT created_at, updated_at FROM public.kueue_job_create_pins
		WHERE namespace=$1 AND tenant_id=$2 AND operation_id=$3`,
		namespace, req.TenantID, req.OperationID).Scan(&createdAgain, &updatedAgain); err != nil {
		t.Fatal(err)
	}
	if !createdAgain.Equal(created) || !updatedAgain.Equal(updated) {
		t.Fatalf("identical pin put rewrote timestamps: created %s->%s updated %s->%s", created, createdAgain, updated, updatedAgain)
	}
	_, err = db.ExecContext(ctx, `
		UPDATE public.kueue_job_create_pins SET job_uid='uid-rewritten'
		WHERE namespace=$1 AND tenant_id=$2 AND operation_id=$3`,
		namespace, req.TenantID, req.OperationID)
	if postgresSQLState(err) != "55000" {
		t.Fatalf("pin uid rewrite state=%s err=%v", postgresSQLState(err), err)
	}
	_, err = db.ExecContext(ctx, `
		INSERT INTO public.kueue_job_create_pins (
			namespace, tenant_id, operation_id, job_name, job_uid,
			workload_id, provider_id, idempotency_key, request_digest,
			desired_generation, operation_kind, target_resource_id, target_resource_uid
		) VALUES ($1,$2,$3,$4,'other-uid',$5,$6,$7,$8,$9,$10,$11,$12)`,
		namespace, req.TenantID, req.OperationID, jobName,
		req.WorkloadID, req.ProviderID, req.IdempotencyKey, req.RequestDigest,
		req.DesiredGeneration, string(req.Kind), req.TargetResourceID, req.TargetResourceUID)
	if postgresSQLState(err) != "23505" {
		t.Fatalf("duplicate pin state=%s err=%v", postgresSQLState(err), err)
	}
	_, err = db.ExecContext(ctx, `
		INSERT INTO public.kueue_job_create_pins (
			namespace, tenant_id, operation_id, job_name, job_uid,
			workload_id, provider_id, idempotency_key, request_digest,
			desired_generation, operation_kind, target_resource_id, target_resource_uid
		) VALUES ($1,$2,$3,'empty-uid-job','',$4,$5,$6,$7,7,'CREATE','','')`,
		namespace, "empty-"+req.TenantID, "empty-"+req.OperationID,
		req.WorkloadID, req.ProviderID, req.IdempotencyKey, req.RequestDigest)
	if postgresSQLState(err) != "23514" {
		t.Fatalf("empty uid state=%s err=%v", postgresSQLState(err), err)
	}
	_, err = db.ExecContext(ctx, `
		DELETE FROM public.kueue_job_create_pins
		WHERE namespace=$1 AND tenant_id=$2 AND operation_id=$3`,
		namespace, req.TenantID, req.OperationID)
	if postgresSQLState(err) != "55000" {
		t.Fatalf("pin delete state=%s err=%v", postgresSQLState(err), err)
	}
	if _, err := db.ExecContext(ctx, `TRUNCATE public.kueue_job_create_pins`); postgresSQLState(err) != "55000" {
		t.Fatalf("pin truncate state=%s err=%v", postgresSQLState(err), err)
	}
	var storedUID string
	if err := db.QueryRowContext(ctx, `
		SELECT job_uid FROM public.kueue_job_create_pins
		WHERE namespace=$1 AND tenant_id=$2 AND operation_id=$3`,
		namespace, req.TenantID, req.OperationID).Scan(&storedUID); err != nil || storedUID != uid {
		t.Fatalf("pin after rejected rewrite uid=%q err=%v", storedUID, err)
	}

	remove := req
	remove.Kind = operation.Delete
	remove.OperationID = "del-" + suffix
	remove.IdempotencyKey = "idem-del-" + suffix
	remove.TargetResourceID = namespace + "/" + jobName
	remove.TargetResourceUID = uid
	if err := store.PutDeleteTombstone(namespace, remove, jobName, uid, false); err != nil {
		t.Fatal(err)
	}
	var tombCreated time.Time
	if err := db.QueryRowContext(ctx, `
		SELECT created_at FROM public.kueue_job_delete_tombstones
		WHERE namespace=$1 AND tenant_id=$2 AND operation_id=$3`,
		namespace, remove.TenantID, remove.OperationID).Scan(&tombCreated); err != nil {
		t.Fatal(err)
	}
	if err := store.PutDeleteTombstone(namespace, remove, jobName, uid, true); err != nil {
		t.Fatal(err)
	}
	var tombCreatedAgain, tombUpdated time.Time
	var acknowledged bool
	if err := db.QueryRowContext(ctx, `
		SELECT created_at, updated_at, acknowledged FROM public.kueue_job_delete_tombstones
		WHERE namespace=$1 AND tenant_id=$2 AND operation_id=$3`,
		namespace, remove.TenantID, remove.OperationID).Scan(&tombCreatedAgain, &tombUpdated, &acknowledged); err != nil {
		t.Fatal(err)
	}
	if !acknowledged || !tombCreatedAgain.Equal(tombCreated) || tombUpdated.Before(tombCreated) {
		t.Fatalf("acknowledgement created=%s updated=%s ack=%t", tombCreatedAgain, tombUpdated, acknowledged)
	}
	_, err = db.ExecContext(ctx, `
		UPDATE public.kueue_job_delete_tombstones SET acknowledged=FALSE
		WHERE namespace=$1 AND tenant_id=$2 AND operation_id=$3`,
		namespace, remove.TenantID, remove.OperationID)
	if postgresSQLState(err) != "55000" {
		t.Fatalf("clear acknowledgement state=%s err=%v", postgresSQLState(err), err)
	}
	_, err = db.ExecContext(ctx, `
		UPDATE public.kueue_job_delete_tombstones SET job_uid='uid-rewritten'
		WHERE namespace=$1 AND tenant_id=$2 AND operation_id=$3`,
		namespace, remove.TenantID, remove.OperationID)
	if postgresSQLState(err) != "55000" {
		t.Fatalf("tombstone uid rewrite state=%s err=%v", postgresSQLState(err), err)
	}
	_, err = db.ExecContext(ctx, `
		DELETE FROM public.kueue_job_delete_tombstones
		WHERE namespace=$1 AND tenant_id=$2 AND operation_id=$3`,
		namespace, remove.TenantID, remove.OperationID)
	if postgresSQLState(err) != "55000" {
		t.Fatalf("tombstone delete state=%s err=%v", postgresSQLState(err), err)
	}
	tomb, ok, err := store.GetDeleteTombstone(namespace, remove)
	if err != nil || !ok || !tomb.Acknowledged || tomb.UID != uid {
		t.Fatalf("tombstone after rejected rewrite %+v ok=%v err=%v", tomb, ok, err)
	}
}

// TestPostgresKueueJobBindingSurvivesNewConnection discards the first
// connection, store, and adapter, then opens new ones in this same test
// process. It does not SIGKILL a separate operating-system process.
func TestPostgresKueueJobBindingSurvivesNewConnection(t *testing.T) {
	if os.Getenv("DATABASE_URL") == "" {
		t.Skip("DATABASE_URL is not configured; PostgreSQL integration test skipped")
	}
	ctx := context.Background()

	t.Run("create replay and inspect", func(t *testing.T) {
		req := uniqueKueueRequest(t)
		api := &kueueAPI{}
		db, _, adapter, server := startPostgresKueueFixture(t, api)
		defer server.Close()
		if _, err := adapter.Apply(ctx, req); err != nil {
			t.Fatal(err)
		}
		assertSuspendedCreate(t, api, req)
		db, _, adapter = reconnectPostgresKueueAdapter(t, server, db)
		var uid string
		if err := db.QueryRowContext(ctx, `
			SELECT job_uid FROM public.kueue_job_create_pins
			WHERE namespace='tenant-a' AND tenant_id=$1 AND operation_id=$2`,
			req.TenantID, req.OperationID).Scan(&uid); err != nil || uid != "job-uid-created" {
			t.Fatalf("new connection pin uid=%q err=%v", uid, err)
		}
		again, err := adapter.Apply(ctx, req)
		if err != nil || api.postCount() != 1 || api.deleteCount() != 0 {
			t.Fatalf("restarted create %+v posts=%d deletes=%d err=%v", again, api.postCount(), api.deleteCount(), err)
		}
		observed, err := adapter.Inspect(ctx, req)
		name := kueueCreateJobName(req.TenantID, req.OperationID)
		if err != nil || observed.Result != operation.ResourcePresent || observed.ResourceUID != "job-uid-created" ||
			observed.ResourceID != "tenant-a/"+name || observed.ResourceOperationID != req.OperationID {
			t.Fatalf("restarted inspect %+v %v", observed, err)
		}
	})

	t.Run("successor left in place", func(t *testing.T) {
		req := uniqueKueueRequest(t)
		api := &kueueAPI{}
		db, _, adapter, server := startPostgresKueueFixture(t, api)
		defer server.Close()
		if _, err := adapter.Apply(ctx, req); err != nil {
			t.Fatal(err)
		}
		_, _, adapter = reconnectPostgresKueueAdapter(t, server, db)
		api.setReplacement("job-uid-successor", "")
		observed, err := adapter.Inspect(ctx, req)
		if err != nil || observed.Result != operation.ObservationConflict || observed.ReasonCode != "JOB_UID_REPLACED" ||
			observed.ResourceUID != "" || observed.Result == operation.ResourcePresent {
			t.Fatalf("successor inspect %+v %v", observed, err)
		}
		if _, err := adapter.Apply(ctx, req); !errors.Is(err, ErrOperationConflict) || api.postCount() != 1 ||
			api.deleteCount() != 0 || api.storedJobs() != 1 {
			t.Fatalf("successor apply posts=%d deletes=%d stored=%d err=%v", api.postCount(), api.deleteCount(), api.storedJobs(), err)
		}
		name := kueueCreateJobName(req.TenantID, req.OperationID)
		api.mu.Lock()
		storedUID := api.jobs[name].Metadata.UID
		api.mu.Unlock()
		if storedUID != "job-uid-created" {
			t.Fatalf("successor rewrote stored job uid %s", storedUID)
		}
	})

	t.Run("acknowledged delete", func(t *testing.T) {
		req := uniqueKueueRequest(t)
		api := &kueueAPI{}
		db, _, adapter, server := startPostgresKueueFixture(t, api)
		defer server.Close()
		if _, err := adapter.Apply(ctx, req); err != nil {
			t.Fatal(err)
		}
		observed, err := adapter.Inspect(ctx, req)
		if err != nil || observed.Result != operation.ResourcePresent || observed.ResourceUID != "job-uid-created" {
			t.Fatalf("create inspect %+v %v", observed, err)
		}
		remove := postgresDeleteRequest(req, observed)
		if _, err := adapter.Apply(ctx, remove); err != nil || api.deleteCount() != 1 || api.storedJobs() != 0 {
			t.Fatalf("delete deletes=%d stored=%d err=%v", api.deleteCount(), api.storedJobs(), err)
		}
		_, _, adapter = reconnectPostgresKueueAdapter(t, server, db)
		absent, err := adapter.Inspect(ctx, remove)
		if err != nil || absent.Result != operation.TargetAbsent || absent.TargetResourceUID != observed.ResourceUID ||
			absent.TargetResourceID != remove.TargetResourceID || absent.ResourceUID != "" ||
			absent.Result == operation.OperationNotAppliedFinal {
			t.Fatalf("restarted absence %+v %v", absent, err)
		}
		if _, err := adapter.Apply(ctx, remove); err != nil || api.deleteCount() != 1 || api.postCount() != 1 {
			t.Fatalf("restarted replay deletes=%d posts=%d err=%v", api.deleteCount(), api.postCount(), err)
		}
	})

	t.Run("unacknowledged tombstone", func(t *testing.T) {
		req := uniqueKueueRequest(t)
		api := &kueueAPI{deleteStatus: http.StatusInternalServerError}
		db, _, adapter, server := startPostgresKueueFixture(t, api)
		defer server.Close()
		if _, err := adapter.Apply(ctx, req); err != nil {
			t.Fatal(err)
		}
		observed, err := adapter.Inspect(ctx, req)
		if err != nil || observed.ResourceUID != "job-uid-created" {
			t.Fatalf("create inspect %+v %v", observed, err)
		}
		remove := postgresDeleteRequest(req, observed)
		if _, err := adapter.Apply(ctx, remove); !errors.Is(err, ErrAcknowledgementLost) || api.deleteCount() != 1 || api.storedJobs() != 1 {
			t.Fatalf("ambiguous delete deletes=%d stored=%d err=%v", api.deleteCount(), api.storedJobs(), err)
		}
		_, store, adapter := reconnectPostgresKueueAdapter(t, server, db)
		tomb, ok, err := store.GetDeleteTombstone("tenant-a", remove)
		if err != nil || !ok || tomb.Acknowledged || tomb.UID != observed.ResourceUID {
			t.Fatalf("reopened tombstone %+v ok=%v err=%v", tomb, ok, err)
		}
		api.setDeleteStatus(0)
		api.setReplacement("job-uid-successor", "")
		gets := api.methodCount(http.MethodGet)
		if _, err := adapter.Apply(ctx, remove); !errors.Is(err, ErrOperationConflict) || api.deleteCount() != 1 || api.storedJobs() != 1 {
			t.Fatalf("unacknowledged tombstone deleted successor deletes=%d stored=%d err=%v", api.deleteCount(), api.storedJobs(), err)
		}
		if api.methodCount(http.MethodGet) <= gets {
			t.Fatal("reopened adapter did not re-GET")
		}
		seen, err := adapter.Inspect(ctx, remove)
		if err != nil || seen.Result != operation.ObservationConflict || seen.ReasonCode != "JOB_UID_REPLACED" ||
			seen.Result == operation.TargetAbsent || seen.Result == operation.OperationNotAppliedFinal {
			t.Fatalf("successor after unacknowledged delete %+v %v", seen, err)
		}
	})

	t.Run("lost create acknowledgement", func(t *testing.T) {
		req := uniqueKueueRequest(t)
		api := &kueueAPI{loseCreateBody: true}
		db, _, adapter, server := startPostgresKueueFixture(t, api)
		defer server.Close()
		if _, err := adapter.Apply(ctx, req); !errors.Is(err, ErrAcknowledgementLost) || api.postCount() != 1 || api.storedJobs() != 1 {
			t.Fatalf("lost create posts=%d stored=%d err=%v", api.postCount(), api.storedJobs(), err)
		}
		db, store, adapter := reconnectPostgresKueueAdapter(t, server, db)
		if _, ok, err := store.GetCreatePin("tenant-a", req); err != nil || ok {
			t.Fatalf("lost create wrote a pin: ok=%v err=%v", ok, err)
		}
		var n int
		if err := db.QueryRowContext(ctx, `
			SELECT count(*) FROM public.kueue_job_create_pins
			WHERE namespace='tenant-a' AND tenant_id=$1 AND operation_id=$2`,
			req.TenantID, req.OperationID).Scan(&n); err != nil || n != 0 {
			t.Fatalf("lost create row count=%d err=%v", n, err)
		}
		observed, err := adapter.Inspect(ctx, req)
		if err != nil || observed.Result != operation.OperationInProgress || observed.ResourceUID != "" ||
			observed.Result == operation.ResourcePresent || observed.Result == operation.OperationNotAppliedFinal {
			t.Fatalf("lost create became trusted: %+v %v", observed, err)
		}
		if _, err := adapter.Apply(ctx, req); !errors.Is(err, ErrAcknowledgementLost) || api.postCount() != 1 {
			t.Fatalf("lost create replay posts=%d err=%v", api.postCount(), err)
		}
	})

	t.Run("database failure", func(t *testing.T) {
		req := uniqueKueueRequest(t)
		api := &kueueAPI{}
		db, _, adapter, server := startPostgresKueueFixture(t, api)
		defer server.Close()
		if _, err := adapter.Apply(ctx, req); err != nil || api.postCount() != 1 {
			t.Fatalf("create posts=%d err=%v", api.postCount(), err)
		}
		_ = db.Close()
		closed, err := sql.Open("pgx", os.Getenv("DATABASE_URL"))
		if err != nil {
			t.Fatal(err)
		}
		if err := closed.Close(); err != nil {
			t.Fatal(err)
		}
		blind, err := NewPostgresKueueJobBindingStore(closed)
		if err != nil {
			t.Fatal(err)
		}
		if _, ok, err := blind.GetCreatePin("tenant-a", req); err == nil || ok || errors.Is(err, sql.ErrNoRows) {
			t.Fatalf("closed database looked like a missing pin: ok=%v err=%v", ok, err)
		}
		adapter = reopenKueueAdapter(t, server, blind)
		if _, err := adapter.Apply(ctx, req); err == nil || api.postCount() != 1 || api.deleteCount() != 0 {
			t.Fatalf("closed database posted or adopted: posts=%d deletes=%d err=%v", api.postCount(), api.deleteCount(), err)
		}
		observed, err := adapter.Inspect(ctx, req)
		if err == nil || observed.Result == operation.ResourcePresent ||
			observed.Result == operation.OperationNotAppliedFinal || observed.Result == operation.TargetAbsent {
			t.Fatalf("closed database inspect %+v %v", observed, err)
		}
		remove := req
		remove.Kind = operation.Delete
		remove.OperationID = "del-" + req.OperationID
		remove.IdempotencyKey = "idem-" + remove.OperationID
		remove.TargetResourceID = "tenant-a/" + kueueCreateJobName(req.TenantID, req.OperationID)
		remove.TargetResourceUID = "job-uid-created"
		name := kueueCreateJobName(req.TenantID, req.OperationID)
		api.mu.Lock()
		delete(api.jobs, name)
		api.mu.Unlock()
		absent, err := adapter.Inspect(ctx, remove)
		if err == nil || absent.Result == operation.TargetAbsent || absent.Result == operation.OperationNotAppliedFinal {
			t.Fatalf("closed database 404 became absence %+v %v", absent, err)
		}
		if _, err := adapter.Apply(ctx, remove); err == nil || api.deleteCount() != 0 {
			t.Fatalf("closed database deleted: deletes=%d err=%v", api.deleteCount(), err)
		}
	})
}

func startPostgresKueueFixture(t *testing.T, api *kueueAPI) (*sql.DB, *PostgresKueueJobBindingStore, *KueueJobAdapter, *httptest.Server) {
	t.Helper()
	db := openProviderPostgresDB(t)
	store, err := NewPostgresKueueJobBindingStore(db)
	if err != nil {
		t.Fatal(err)
	}
	adapter, _, server := newKueueAdapterFixtureWithStore(t, api, store)
	return db, store, adapter, server
}

// reconnectPostgresKueueAdapter closes the previous pool so later reads cannot
// reuse its connections, then opens a new pool, store, and adapter.
func reconnectPostgresKueueAdapter(t *testing.T, server *httptest.Server, previous *sql.DB) (*sql.DB, *PostgresKueueJobBindingStore, *KueueJobAdapter) {
	t.Helper()
	if previous != nil {
		_ = previous.Close()
	}
	db := openProviderPostgresDB(t)
	store, err := NewPostgresKueueJobBindingStore(db)
	if err != nil {
		t.Fatal(err)
	}
	return db, store, reopenKueueAdapter(t, server, store)
}
