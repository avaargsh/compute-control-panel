package provider

import (
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/avaargsh/compute-control-panel/internal/operation"
)

func preDeleteSnapshotFixture(create, remove Request, namespace, jobName, jobUID string) KueuePreDeleteDRASnapshot {
	suffix := strings.TrimPrefix(create.OperationID, "op-")
	return KueuePreDeleteDRASnapshot{
		Namespace: namespace, TenantID: create.TenantID, WorkloadID: create.WorkloadID,
		ProviderID: create.ProviderID, CreateOperationID: create.OperationID,
		DeleteOperationID: remove.OperationID, DesiredGeneration: create.DesiredGeneration,
		Job: KubernetesResourceRef{Namespace: namespace, Name: jobName, UID: jobUID, ResourceVersion: "11"},
		KueueWorkload: KubernetesResourceRef{Namespace: namespace, Name: "kueue-" + suffix, UID: "kueue-uid-" + suffix, ResourceVersion: "12"},
		Pod: KubernetesResourceRef{Namespace: namespace, Name: "pod-" + suffix, UID: "pod-uid-" + suffix, ResourceVersion: "13"},
		NodeName: "node-" + suffix,
		Claim: KubernetesResourceRef{Namespace: namespace, Name: "claim-" + suffix, UID: "claim-uid-" + suffix, ResourceVersion: "14"},
		ClaimTemplateName: "gpu-template", PodClaimAlias: "gpu", ConsumerContainer: "worker",
		Devices: []DRAAllocatedDevice{{Request: "gpu", Driver: "vendor.example", Pool: "pool-a", Device: "gpu-uuid-" + suffix}},
		Source: "kubernetes-api", AdapterVersion: "kueue-dra-v1",
		ObservedAt: time.Date(2026, 11, 24, 10, 0, 0, 0, time.UTC),
	}
}

func preDeleteDeleteRequest(create Request, namespace, jobName, jobUID string) Request {
	remove := create
	remove.Kind = operation.Delete
	remove.OperationID = "del-" + create.OperationID
	remove.IdempotencyKey = "idem-" + remove.OperationID
	remove.TargetResourceID = namespace + "/" + jobName
	remove.TargetResourceUID = jobUID
	return remove
}

func TestKueuePreDeleteDRASnapshotStoreConformance(t *testing.T) {
	for _, tc := range []struct {
		name string
		open func(*testing.T) KueueJobBindingStore
	}{
		{name: "memory", open: func(*testing.T) KueueJobBindingStore { return NewMemoryKueueJobBindingStore() }},
		{name: "postgres", open: func(t *testing.T) KueueJobBindingStore {
			db := openProviderPostgresDB(t)
			store, err := NewPostgresKueueJobBindingStore(db)
			if err != nil { t.Fatal(err) }
			return store
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			store := tc.open(t)
			create := uniqueKueueRequest(t)
			const namespace = "tenant-a"
			suffix := strings.TrimPrefix(create.OperationID, "op-")
			jobName, jobUID := "job-"+suffix, "job-uid-"+suffix
			remove := preDeleteDeleteRequest(create, namespace, jobName, jobUID)
			if err := store.PutCreatePin(namespace, create, jobName, jobUID); err != nil { t.Fatal(err) }

			pin, ok, err := store.GetCreatePinByJob(namespace, create.TenantID, jobName)
			if err != nil || !ok || pin.UID != jobUID || pin.Name != jobName ||
				pin.Request.OperationID != create.OperationID || !sameRequest(pin.Request, create) {
				t.Fatalf("create pin lookup %+v ok=%t err=%v", pin, ok, err)
			}

			want := preDeleteSnapshotFixture(create, remove, namespace, jobName, jobUID)
			stored, err := store.PutPreDeleteDRASnapshot(namespace, remove, want)
			if err != nil { t.Fatalf("store pre-delete snapshot: %v", err) }
			if stored.Digest == "" || !strings.HasPrefix(stored.Digest, "sha256:") {
				t.Fatalf("snapshot digest missing: %+v", stored)
			}
			got, ok, err := store.GetPreDeleteDRASnapshot(namespace, remove)
			if err != nil || !ok || got.Digest != stored.Digest ||
				got.CreateOperationID != create.OperationID || got.DeleteOperationID != remove.OperationID ||
				got.Job != want.Job || got.KueueWorkload != want.KueueWorkload ||
				got.Pod != want.Pod || got.Claim != want.Claim ||
				got.NodeName != want.NodeName || len(got.Devices) != 1 || got.Devices[0] != want.Devices[0] {
				t.Fatalf("snapshot readback %+v ok=%t err=%v", got, ok, err)
			}

			if _, err := store.PutPreDeleteDRASnapshot(namespace, remove, want); err != nil {
				t.Fatalf("identical snapshot replay was not idempotent: %v", err)
			}
			conflict := want
			conflict.Claim.UID = "successor-claim-uid"
			if _, err := store.PutPreDeleteDRASnapshot(namespace, remove, conflict); !errors.Is(err, ErrOperationConflict) {
				t.Fatalf("claim UID rewrite was accepted: %v", err)
			}
			conflict = want
			conflict.Job.UID = "successor-job-uid"
			if _, err := store.PutPreDeleteDRASnapshot(namespace, remove, conflict); !errors.Is(err, ErrOperationConflict) {
				t.Fatalf("successor Job UID was accepted: %v", err)
			}
			conflict = want
			conflict.TenantID = "tenant-other"
			if _, err := store.PutPreDeleteDRASnapshot(namespace, remove, conflict); !errors.Is(err, ErrOperationConflict) {
				t.Fatalf("cross-tenant snapshot was accepted: %v", err)
			}

			if err := store.PutDeleteTombstone(namespace, remove, jobName, jobUID, false); err != nil {
				t.Fatalf("delete intent after snapshot: %v", err)
			}
			// Once DELETE intent exists, only the exact already-persisted snapshot
			// may be replayed; a new snapshot for this operation cannot be minted.
			if _, err := store.PutPreDeleteDRASnapshot(namespace, remove, want); err != nil {
				t.Fatalf("identical pre-delete snapshot replay after tombstone: %v", err)
			}
			anotherDelete := preDeleteDeleteRequest(create, namespace, jobName, jobUID)
			anotherDelete.OperationID += "-successor"
			anotherDelete.IdempotencyKey += "-successor"
			other := preDeleteSnapshotFixture(create, anotherDelete, namespace, jobName, jobUID)
			if _, err := store.PutPreDeleteDRASnapshot(namespace, anotherDelete, other); !errors.Is(err, ErrOperationConflict) {
				t.Fatalf("second delete snapshot for one CREATE UID was accepted: %v", err)
			}
		})
	}
}

func TestKueuePreDeleteDRASnapshotRejectsSnapshotAfterDeleteIntent(t *testing.T) {
	for _, tc := range []struct {
		name string
		open func(*testing.T) KueueJobBindingStore
	}{
		{name: "memory", open: func(*testing.T) KueueJobBindingStore { return NewMemoryKueueJobBindingStore() }},
		{name: "postgres", open: func(t *testing.T) KueueJobBindingStore {
			db := openProviderPostgresDB(t)
			store, err := NewPostgresKueueJobBindingStore(db)
			if err != nil { t.Fatal(err) }
			return store
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			store := tc.open(t)
			create := uniqueKueueRequest(t)
			const namespace = "tenant-a"
			suffix := strings.TrimPrefix(create.OperationID, "op-")
			jobName, jobUID := "job-"+suffix, "job-uid-"+suffix
			remove := preDeleteDeleteRequest(create, namespace, jobName, jobUID)
			if err := store.PutCreatePin(namespace, create, jobName, jobUID); err != nil { t.Fatal(err) }
			if err := store.PutDeleteTombstone(namespace, remove, jobName, jobUID, false); err != nil { t.Fatal(err) }
			snapshot := preDeleteSnapshotFixture(create, remove, namespace, jobName, jobUID)
			if _, err := store.PutPreDeleteDRASnapshot(namespace, remove, snapshot); !errors.Is(err, ErrOperationConflict) {
				t.Fatalf("snapshot was created after DELETE intent: %v", err)
			}
			if _, ok, err := store.GetPreDeleteDRASnapshot(namespace, remove); err != nil || ok {
				t.Fatalf("late snapshot persisted: ok=%t err=%v", ok, err)
			}
		})
	}
}

func TestPostgresKueuePreDeleteSnapshotSerializesWithDeleteIntent(t *testing.T) {
	db := openProviderPostgresDB(t)
	store, err := NewPostgresKueueJobBindingStore(db)
	if err != nil { t.Fatal(err) }
	create := uniqueKueueRequest(t)
	const namespace = "tenant-a"
	suffix := strings.TrimPrefix(create.OperationID, "op-")
	jobName, jobUID := "job-"+suffix, "job-uid-"+suffix
	remove := preDeleteDeleteRequest(create, namespace, jobName, jobUID)
	if err := store.PutCreatePin(namespace, create, jobName, jobUID); err != nil { t.Fatal(err) }
	snapshot := preDeleteSnapshotFixture(create, remove, namespace, jobName, jobUID)

	start := make(chan struct{})
	type outcome struct { snapshot bool; err error }
	results := make(chan outcome, 2)
	go func() { <-start; _, err := store.PutPreDeleteDRASnapshot(namespace, remove, snapshot); results <- outcome{snapshot: true, err: err} }()
	go func() { <-start; err := store.PutDeleteTombstone(namespace, remove, jobName, jobUID, false); results <- outcome{err: err} }()
	close(start)

	var snapshotErr, tombstoneErr error
	for i := 0; i < 2; i++ {
		got := <-results
		if got.snapshot { snapshotErr = got.err } else { tombstoneErr = got.err }
	}
	if tombstoneErr != nil { t.Fatalf("DELETE intent failed: %v", tombstoneErr) }
	if snapshotErr == nil {
		if _, ok, err := store.GetPreDeleteDRASnapshot(namespace, remove); err != nil || !ok {
			t.Fatalf("snapshot won lock but is missing: ok=%t err=%v", ok, err)
		}
	} else if !errors.Is(snapshotErr, ErrOperationConflict) {
		t.Fatalf("snapshot/tombstone race returned unexpected error: %v", snapshotErr)
	} else if _, ok, err := store.GetPreDeleteDRASnapshot(namespace, remove); err != nil || ok {
		t.Fatalf("tombstone won lock but a late snapshot exists: ok=%t err=%v", ok, err)
	}
}
