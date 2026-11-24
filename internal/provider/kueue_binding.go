package provider

import (
	"sync"

	"github.com/avaargsh/compute-control-panel/internal/operation"
)

// KueueCreatePin is the trusted UID for one CREATE. It is written only from
// an acknowledged Job UID. Callers must not fill it from a Job name or
// annotations.
type KueueCreatePin struct {
	Namespace string
	Name      string
	UID       string
	Request   Request
}

// KueueDeleteTombstone records a UID-fenced DELETE intent. Acknowledged means
// the API accepted that exact UID or then reported it gone. An unacknowledged
// tombstone is a lost response: later calls must re-GET and must not delete a
// different UID. Presence of a tombstone is not Allocation RELEASED, Kueue
// quota release, or device or claim deallocation.
type KueueDeleteTombstone struct {
	Namespace    string
	Name         string
	UID          string
	Request      Request
	Acknowledged bool
}

// KueueJobBindingStore is the trusted lineage for a CREATE pin and a DELETE
// tombstone. A later KueueJobAdapter resumes those operations only when it
// loads the same records. A missing record is not proof the Kubernetes call
// never happened, and a read error is not a missing record.
//
// MemoryKueueJobBindingStore keeps the records in process memory. It is the
// explicit test and fake store. It outlives one adapter value only when that
// same store is passed to NewKueueJobAdapterWithBindings.
// PostgresKueueJobBindingStore keeps the same records in PostgreSQL, so a
// later process can load them from a new connection. In-cluster construction
// opens that store with OpenDurablePostgresKueueJobBindingStore and refuses
// a login that is broader than the binding DML. That is the Job UID pin and
// the DELETE tombstone only. It is
// not Allocation RELEASED, Kueue quota release, or a signed receipt.
// compute_operations.provider_resource_uid is not this store: it is recorded
// only after SUCCEEDED, and that success already required this pin.
type KueueJobBindingStore interface {
	GetCreatePin(namespace string, req Request) (KueueCreatePin, bool, error)
	GetCreatePinByJob(namespace, tenantID, jobName string) (KueueCreatePin, bool, error)
	PutCreatePin(namespace string, req Request, name, uid string) error
	GetDeleteTombstone(namespace string, req Request) (KueueDeleteTombstone, bool, error)
	PutDeleteTombstone(namespace string, req Request, name, uid string, acknowledged bool) error
	PutPreDeleteDRASnapshot(namespace string, req Request, snapshot KueuePreDeleteDRASnapshot) (KueuePreDeleteDRASnapshot, error)
	GetPreDeleteDRASnapshot(namespace string, req Request) (KueuePreDeleteDRASnapshot, bool, error)
	CreatePinUID(namespace, jobName string) (string, bool, error)
}

// MemoryKueueJobBindingStore is the in-memory KueueJobBindingStore. Share one
// value across adapter instances to simulate a process that can load its
// bindings again. A new process that cannot load this value stays fail-closed.
type MemoryKueueJobBindingStore struct {
	mu        sync.Mutex
	pins      map[string]KueueCreatePin
	deletes   map[string]KueueDeleteTombstone
	snapshots map[string]KueuePreDeleteDRASnapshot
}

// NewMemoryKueueJobBindingStore returns an empty process-local store.
func NewMemoryKueueJobBindingStore() *MemoryKueueJobBindingStore {
	return &MemoryKueueJobBindingStore{
		pins:      make(map[string]KueueCreatePin),
		deletes:   make(map[string]KueueDeleteTombstone),
		snapshots: make(map[string]KueuePreDeleteDRASnapshot),
	}
}

func kueueBindingKey(namespace string, req Request) string {
	return namespace + "\x00" + req.TenantID + "\x00" + req.OperationID
}

func (s *MemoryKueueJobBindingStore) usable() error {
	if s == nil || s.pins == nil || s.deletes == nil || s.snapshots == nil {
		return ErrInvalidKueueObservation
	}
	return nil
}

// GetCreatePin returns the pin for this namespace and operation, if one was stored.
func (s *MemoryKueueJobBindingStore) GetCreatePin(namespace string, req Request) (KueueCreatePin, bool, error) {
	if err := s.usable(); err != nil || !validKubeName(namespace) {
		return KueueCreatePin{}, false, ErrInvalidKueueObservation
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	pin, ok := s.pins[kueueBindingKey(namespace, req)]
	return pin, ok, nil
}

// GetCreatePinByJob resolves the immutable CREATE pin by its unique namespace/Job name.
func (s *MemoryKueueJobBindingStore) GetCreatePinByJob(namespace, tenantID, jobName string) (KueueCreatePin, bool, error) {
	if err := s.usable(); err != nil || !validKubeName(namespace) || tenantID == "" ||
		tenantID != strings.TrimSpace(tenantID) || !validKubeName(jobName) {
		return KueueCreatePin{}, false, ErrInvalidKueueObservation
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	var found KueueCreatePin
	ok := false
	for _, pin := range s.pins {
		if pin.Namespace != namespace || pin.Request.TenantID != tenantID || pin.Name != jobName {
			continue
		}
		if ok {
			return KueueCreatePin{}, false, ErrOperationConflict
		}
		found, ok = pin, true
	}
	return found, ok, nil
}

// PutCreatePin stores a pin. An identical pin is idempotent. A different UID,
// name, or request fingerprint for the same operation is a conflict.
func (s *MemoryKueueJobBindingStore) PutCreatePin(namespace string, req Request, name, uid string) error {
	if err := s.usable(); err != nil || !validKubeName(namespace) || !validKubeName(name) || !acceptableJobUID(uid) {
		return ErrInvalidKueueObservation
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	key := kueueBindingKey(namespace, req)
	if prev, ok := s.pins[key]; ok {
		if prev.UID != uid || prev.Name != name || prev.Namespace != namespace || !sameRequest(prev.Request, req) {
			return ErrOperationConflict
		}
		return nil
	}
	// CreatePinUID looks up by namespace and name, not by operation. Enforce
	// one owner while holding the same mutex as the insert; otherwise its
	// result would depend on map iteration order and could return a successor
	// UID to a DELETE fence.
	for _, prev := range s.pins {
		if prev.Namespace == namespace && prev.Name == name {
			return ErrOperationConflict
		}
	}
	s.pins[key] = KueueCreatePin{Namespace: namespace, Name: name, UID: uid, Request: req}
	return nil
}

// GetDeleteTombstone returns the tombstone for this namespace and operation.
func (s *MemoryKueueJobBindingStore) GetDeleteTombstone(namespace string, req Request) (KueueDeleteTombstone, bool, error) {
	if err := s.usable(); err != nil || !validKubeName(namespace) {
		return KueueDeleteTombstone{}, false, ErrInvalidKueueObservation
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	intent, ok := s.deletes[kueueBindingKey(namespace, req)]
	return intent, ok, nil
}

// PutDeleteTombstone stores a DELETE intent. Once acknowledged, the tombstone
// stays acknowledged. A different UID, name, or request fingerprint conflicts.
func (s *MemoryKueueJobBindingStore) PutDeleteTombstone(namespace string, req Request, name, uid string, acknowledged bool) error {
	if err := s.usable(); err != nil || !validKubeName(namespace) || !validKubeName(name) || !acceptableJobUID(uid) ||
		req.Validate() != nil || req.Kind != operation.Delete ||
		req.TargetResourceID != namespace+"/"+name || req.TargetResourceUID != uid {
		return ErrInvalidKueueObservation
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	// Snapshot capture and DELETE intent share this mutex. A pin mismatch
	// cannot create a tombstone that could later be associated with a successor.
	if pin, ok := memoryCreatePinByJobLocked(s, namespace, req.TenantID, name); ok &&
		(pin.UID != uid || pin.Request.WorkloadID != req.WorkloadID || pin.Request.ProviderID != req.ProviderID) {
		return ErrOperationConflict
	}
	key := kueueBindingKey(namespace, req)
	if prev, ok := s.deletes[key]; ok {
		if prev.UID != uid || prev.Name != name || prev.Namespace != namespace || !sameRequest(prev.Request, req) {
			return ErrOperationConflict
		}
		if prev.Acknowledged {
			acknowledged = true
		}
	}
	s.deletes[key] = KueueDeleteTombstone{
		Namespace: namespace, Name: name, UID: uid, Request: req, Acknowledged: acknowledged,
	}
	return nil
}

// CreatePinUID returns the trusted UID recorded for a Job name in this
// namespace. It does not read Kubernetes.
func (s *MemoryKueueJobBindingStore) CreatePinUID(namespace, jobName string) (string, bool, error) {
	if err := s.usable(); err != nil || !validKubeName(namespace) || !validKubeName(jobName) {
		return "", false, ErrInvalidKueueObservation
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, pin := range s.pins {
		if pin.Namespace == namespace && pin.Name == jobName {
			return pin.UID, true, nil
		}
	}
	return "", false, nil
}

func memoryCreatePinByJobLocked(s *MemoryKueueJobBindingStore, namespace, tenantID, jobName string) (KueueCreatePin, bool) {
	for _, pin := range s.pins {
		if pin.Namespace == namespace && pin.Request.TenantID == tenantID && pin.Name == jobName {
			return pin, true
		}
	}
	return KueueCreatePin{}, false
}

func (s *MemoryKueueJobBindingStore) PutPreDeleteDRASnapshot(
	namespace string, req Request, snapshot KueuePreDeleteDRASnapshot,
) (KueuePreDeleteDRASnapshot, error) {
	if err := s.usable(); err != nil || !validKubeName(namespace) {
		return KueuePreDeleteDRASnapshot{}, ErrInvalidKueueObservation
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	pin, ok := memoryCreatePinByJobLocked(s, namespace, req.TenantID, snapshot.Job.Name)
	if !ok {
		return KueuePreDeleteDRASnapshot{}, ErrOperationConflict
	}
	canonical, err := validateKueuePreDeleteDRASnapshot(namespace, req, pin, snapshot)
	if err != nil {
		return KueuePreDeleteDRASnapshot{}, err
	}
	key := kueueBindingKey(namespace, req)
	if previous, exists := s.snapshots[key]; exists {
		if previous.Digest != canonical.Digest {
			return KueuePreDeleteDRASnapshot{}, ErrOperationConflict
		}
		return cloneKueuePreDeleteDRASnapshot(previous), nil
	}
	if _, exists := s.deletes[key]; exists {
		return KueuePreDeleteDRASnapshot{}, ErrOperationConflict
	}
	for _, previous := range s.snapshots {
		if previous.Namespace == namespace && previous.TenantID == req.TenantID &&
			previous.CreateOperationID == canonical.CreateOperationID {
			return KueuePreDeleteDRASnapshot{}, ErrOperationConflict
		}
	}
	s.snapshots[key] = cloneKueuePreDeleteDRASnapshot(canonical)
	return canonical, nil
}

func (s *MemoryKueueJobBindingStore) GetPreDeleteDRASnapshot(
	namespace string, req Request,
) (KueuePreDeleteDRASnapshot, bool, error) {
	if err := s.usable(); err != nil || !validKubeName(namespace) ||
		req.Validate() != nil || req.Kind != operation.Delete {
		return KueuePreDeleteDRASnapshot{}, false, ErrInvalidKueueObservation
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	snapshot, ok := s.snapshots[kueueBindingKey(namespace, req)]
	if !ok {
		return KueuePreDeleteDRASnapshot{}, false, nil
	}
	return cloneKueuePreDeleteDRASnapshot(snapshot), true, nil
}

var _ KueueJobBindingStore = (*MemoryKueueJobBindingStore)(nil)
