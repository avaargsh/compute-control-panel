package provider

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"math"
	"time"

	"github.com/avaargsh/compute-control-panel/internal/operation"
	"github.com/jackc/pgx/v5/pgconn"
)

const kueueBindingQueryTimeout = 10 * time.Second

const kueueCreatePinColumns = `
	namespace, tenant_id, operation_id, job_name, job_uid,
	workload_id, provider_id, idempotency_key, request_digest,
	desired_generation, operation_kind, target_resource_id, target_resource_uid`

const selectKueueCreatePinSQL = `
	SELECT ` + kueueCreatePinColumns + `
	FROM public.kueue_job_create_pins
	WHERE namespace = $1 AND tenant_id = $2 AND operation_id = $3`

const insertKueueCreatePinSQL = `
	INSERT INTO public.kueue_job_create_pins (` + kueueCreatePinColumns + `)
	VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13)`

const kueueDeleteTombstoneColumns = kueueCreatePinColumns + `, acknowledged`

const selectKueueDeleteTombstoneSQL = `
	SELECT ` + kueueDeleteTombstoneColumns + `
	FROM public.kueue_job_delete_tombstones
	WHERE namespace = $1 AND tenant_id = $2 AND operation_id = $3`

const insertKueueDeleteTombstoneSQL = `
	INSERT INTO public.kueue_job_delete_tombstones (` + kueueDeleteTombstoneColumns + `)
	VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14)`

const acknowledgeKueueDeleteTombstoneSQL = `
	UPDATE public.kueue_job_delete_tombstones
	SET acknowledged = TRUE, updated_at = pg_catalog.clock_timestamp()
	WHERE namespace = $1 AND tenant_id = $2 AND operation_id = $3
	  AND acknowledged = FALSE`

// PostgresKueueJobBindingStore is the PostgreSQL KueueJobBindingStore.
// A later process loads the same CREATE pin and DELETE tombstone by opening
// a new connection to this database. A missing row is not a stored record.
// Any database error is returned so the adapter fails closed instead of
// treating the read as absent, OPERATION_NOT_APPLIED_FINAL, or TARGET_ABSENT.
//
// The tables are migration 0014. They are not
// compute_operations.provider_resource_uid, Allocation RELEASED, or Kueue
// quota release. desired_generation is a BIGINT, matching the operation
// schema; a value above math.MaxInt64 is returned as an error.
type PostgresKueueJobBindingStore struct {
	db *sql.DB
}

// NewPostgresKueueJobBindingStore reads and writes the migration 0014 tables
// through db. The caller opens db, registers the PostgreSQL driver, and
// closes db. A nil database is rejected.
func NewPostgresKueueJobBindingStore(db *sql.DB) (*PostgresKueueJobBindingStore, error) {
	if db == nil {
		return nil, ErrInvalidKueueObservation
	}
	return &PostgresKueueJobBindingStore{db: db}, nil
}

func (s *PostgresKueueJobBindingStore) usable() error {
	if s == nil || s.db == nil {
		return ErrInvalidKueueObservation
	}
	return nil
}

// GetCreatePin returns the stored pin. A missing row is false with a nil
// error. A database error is returned and is not reported as a missing row.
func (s *PostgresKueueJobBindingStore) GetCreatePin(namespace string, req Request) (KueueCreatePin, bool, error) {
	if err := s.usable(); err != nil || !validKubeName(namespace) {
		return KueueCreatePin{}, false, ErrInvalidKueueObservation
	}
	ctx, cancel := context.WithTimeout(context.Background(), kueueBindingQueryTimeout)
	defer cancel()
	pin, ok, err := queryKueueCreatePin(ctx, s.db, selectKueueCreatePinSQL, namespace, req.TenantID, req.OperationID)
	if err != nil {
		return KueueCreatePin{}, false, fmt.Errorf("read kueue create pin: %w", err)
	}
	return pin, ok, nil
}

// PutCreatePin stores a pin. An identical pin is idempotent. A different UID,
// name, or request fingerprint for the same namespace and operation conflicts.
func (s *PostgresKueueJobBindingStore) PutCreatePin(namespace string, req Request, name, uid string) error {
	if err := s.usable(); err != nil || !validKubeName(namespace) || !validKubeName(name) || !acceptableJobUID(uid) {
		return ErrInvalidKueueObservation
	}
	generation, err := kueueBindingGeneration(req.DesiredGeneration)
	if err != nil {
		return err
	}
	var last error
	for attempt := 0; attempt < 2; attempt++ {
		last = s.putCreatePinOnce(namespace, req, name, uid, generation)
		if !postgresUniqueViolation(last) {
			return last
		}
	}
	return fmt.Errorf("write kueue create pin: %w", last)
}

func (s *PostgresKueueJobBindingStore) putCreatePinOnce(namespace string, req Request, name, uid string, generation int64) error {
	ctx, cancel := context.WithTimeout(context.Background(), kueueBindingQueryTimeout)
	defer cancel()
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback() }()

	pin, ok, err := queryKueueCreatePin(ctx, tx, selectKueueCreatePinSQL+" FOR UPDATE", namespace, req.TenantID, req.OperationID)
	if err != nil {
		return err
	}
	if ok {
		if !sameStoredCreatePin(namespace, name, uid, req, pin) {
			return ErrOperationConflict
		}
		return tx.Commit()
	}
	_, err = tx.ExecContext(ctx, insertKueueCreatePinSQL,
		namespace, req.TenantID, req.OperationID, name, uid,
		req.WorkloadID, req.ProviderID, req.IdempotencyKey, req.RequestDigest,
		generation, string(req.Kind), req.TargetResourceID, req.TargetResourceUID,
	)
	if err != nil {
		return err
	}
	return tx.Commit()
}

// GetDeleteTombstone returns the stored tombstone. A missing row is false
// with a nil error. A database error is returned and is not a missing row.
func (s *PostgresKueueJobBindingStore) GetDeleteTombstone(namespace string, req Request) (KueueDeleteTombstone, bool, error) {
	if err := s.usable(); err != nil || !validKubeName(namespace) {
		return KueueDeleteTombstone{}, false, ErrInvalidKueueObservation
	}
	ctx, cancel := context.WithTimeout(context.Background(), kueueBindingQueryTimeout)
	defer cancel()
	tomb, ok, err := queryKueueDeleteTombstone(ctx, s.db, selectKueueDeleteTombstoneSQL, namespace, req.TenantID, req.OperationID)
	if err != nil {
		return KueueDeleteTombstone{}, false, fmt.Errorf("read kueue delete tombstone: %w", err)
	}
	return tomb, ok, nil
}

// PutDeleteTombstone stores a DELETE intent. Once acknowledged, the tombstone
// stays acknowledged. A different UID, name, or request fingerprint conflicts.
func (s *PostgresKueueJobBindingStore) PutDeleteTombstone(namespace string, req Request, name, uid string, acknowledged bool) error {
	if err := s.usable(); err != nil || !validKubeName(namespace) || !validKubeName(name) || !acceptableJobUID(uid) {
		return ErrInvalidKueueObservation
	}
	generation, err := kueueBindingGeneration(req.DesiredGeneration)
	if err != nil {
		return err
	}
	var last error
	for attempt := 0; attempt < 2; attempt++ {
		last = s.putDeleteTombstoneOnce(namespace, req, name, uid, generation, acknowledged)
		if !postgresUniqueViolation(last) {
			return last
		}
	}
	return fmt.Errorf("write kueue delete tombstone: %w", last)
}

func (s *PostgresKueueJobBindingStore) putDeleteTombstoneOnce(namespace string, req Request, name, uid string, generation int64, acknowledged bool) error {
	ctx, cancel := context.WithTimeout(context.Background(), kueueBindingQueryTimeout)
	defer cancel()
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback() }()

	tomb, ok, err := queryKueueDeleteTombstone(ctx, tx, selectKueueDeleteTombstoneSQL+" FOR UPDATE", namespace, req.TenantID, req.OperationID)
	if err != nil {
		return err
	}
	if ok {
		if !sameStoredDeleteTombstone(namespace, name, uid, req, tomb) {
			return ErrOperationConflict
		}
		if tomb.Acknowledged || !acknowledged {
			return tx.Commit()
		}
		result, err := tx.ExecContext(ctx, acknowledgeKueueDeleteTombstoneSQL, namespace, req.TenantID, req.OperationID)
		if err != nil {
			return err
		}
		n, err := result.RowsAffected()
		if err != nil {
			return err
		}
		if n != 1 {
			return errors.New("kueue delete tombstone acknowledgement was not stored")
		}
		return tx.Commit()
	}
	_, err = tx.ExecContext(ctx, insertKueueDeleteTombstoneSQL,
		namespace, req.TenantID, req.OperationID, name, uid,
		req.WorkloadID, req.ProviderID, req.IdempotencyKey, req.RequestDigest,
		generation, string(req.Kind), req.TargetResourceID, req.TargetResourceUID,
		acknowledged,
	)
	if err != nil {
		return err
	}
	return tx.Commit()
}

// CreatePinUID returns the trusted UID recorded for a Job name in this
// namespace. It does not read Kubernetes. Distinct UIDs for that name are an
// error so the caller does not pick one. A missing name is false with a nil
// error. A database error is returned.
func (s *PostgresKueueJobBindingStore) CreatePinUID(namespace, jobName string) (string, bool, error) {
	if err := s.usable(); err != nil || !validKubeName(namespace) || !validKubeName(jobName) {
		return "", false, ErrInvalidKueueObservation
	}
	ctx, cancel := context.WithTimeout(context.Background(), kueueBindingQueryTimeout)
	defer cancel()
	rows, err := s.db.QueryContext(ctx, `
		SELECT job_uid FROM public.kueue_job_create_pins
		WHERE namespace = $1 AND job_name = $2`, namespace, jobName)
	if err != nil {
		return "", false, fmt.Errorf("read kueue create pin uid: %w", err)
	}
	defer rows.Close()
	var uid string
	found := false
	for rows.Next() {
		var next string
		if err := rows.Scan(&next); err != nil {
			return "", false, fmt.Errorf("read kueue create pin uid: %w", err)
		}
		if !found {
			uid = next
			found = true
			continue
		}
		if next != uid {
			return "", false, fmt.Errorf("ambiguous kueue create pin for namespace %s job %s", namespace, jobName)
		}
	}
	if err := rows.Err(); err != nil {
		return "", false, fmt.Errorf("read kueue create pin uid: %w", err)
	}
	if !found {
		return "", false, nil
	}
	return uid, true, nil
}

type rowQuery interface {
	QueryRowContext(ctx context.Context, query string, args ...any) *sql.Row
}

func queryKueueCreatePin(ctx context.Context, q rowQuery, query, namespace, tenantID, operationID string) (KueueCreatePin, bool, error) {
	pin, err := scanKueueCreatePin(q.QueryRowContext(ctx, query, namespace, tenantID, operationID))
	if errors.Is(err, sql.ErrNoRows) {
		return KueueCreatePin{}, false, nil
	}
	if err != nil {
		return KueueCreatePin{}, false, err
	}
	return pin, true, nil
}

func queryKueueDeleteTombstone(ctx context.Context, q rowQuery, query, namespace, tenantID, operationID string) (KueueDeleteTombstone, bool, error) {
	tomb, err := scanKueueDeleteTombstone(q.QueryRowContext(ctx, query, namespace, tenantID, operationID))
	if errors.Is(err, sql.ErrNoRows) {
		return KueueDeleteTombstone{}, false, nil
	}
	if err != nil {
		return KueueDeleteTombstone{}, false, err
	}
	return tomb, true, nil
}

type rowScanner interface {
	Scan(dest ...any) error
}

func scanKueueCreatePin(row rowScanner) (KueueCreatePin, error) {
	var pin KueueCreatePin
	var generation int64
	var kind string
	err := row.Scan(
		&pin.Namespace, &pin.Request.TenantID, &pin.Request.OperationID,
		&pin.Name, &pin.UID,
		&pin.Request.WorkloadID, &pin.Request.ProviderID, &pin.Request.IdempotencyKey,
		&pin.Request.RequestDigest, &generation, &kind,
		&pin.Request.TargetResourceID, &pin.Request.TargetResourceUID,
	)
	if err != nil {
		return KueueCreatePin{}, err
	}
	if generation < 0 {
		return KueueCreatePin{}, errors.New("kueue create pin desired generation is corrupt")
	}
	pin.Request.DesiredGeneration = uint64(generation)
	pin.Request.Kind = operation.Kind(kind)
	return pin, nil
}

func scanKueueDeleteTombstone(row rowScanner) (KueueDeleteTombstone, error) {
	var tomb KueueDeleteTombstone
	var generation int64
	var kind string
	err := row.Scan(
		&tomb.Namespace, &tomb.Request.TenantID, &tomb.Request.OperationID,
		&tomb.Name, &tomb.UID,
		&tomb.Request.WorkloadID, &tomb.Request.ProviderID, &tomb.Request.IdempotencyKey,
		&tomb.Request.RequestDigest, &generation, &kind,
		&tomb.Request.TargetResourceID, &tomb.Request.TargetResourceUID,
		&tomb.Acknowledged,
	)
	if err != nil {
		return KueueDeleteTombstone{}, err
	}
	if generation < 0 {
		return KueueDeleteTombstone{}, errors.New("kueue delete tombstone desired generation is corrupt")
	}
	tomb.Request.DesiredGeneration = uint64(generation)
	tomb.Request.Kind = operation.Kind(kind)
	return tomb, nil
}

func sameStoredCreatePin(namespace, name, uid string, req Request, got KueueCreatePin) bool {
	return got.Namespace == namespace && got.Name == name && got.UID == uid && sameRequest(got.Request, req)
}

func sameStoredDeleteTombstone(namespace, name, uid string, req Request, got KueueDeleteTombstone) bool {
	return got.Namespace == namespace && got.Name == name && got.UID == uid && sameRequest(got.Request, req)
}

func kueueBindingGeneration(v uint64) (int64, error) {
	if v > uint64(math.MaxInt64) {
		return 0, fmt.Errorf("kueue binding desired generation %d exceeds bigint", v)
	}
	return int64(v), nil
}

func postgresUniqueViolation(err error) bool {
	var pgErr *pgconn.PgError
	return errors.As(err, &pgErr) && pgErr.Code == "23505"
}

var _ KueueJobBindingStore = (*PostgresKueueJobBindingStore)(nil)
