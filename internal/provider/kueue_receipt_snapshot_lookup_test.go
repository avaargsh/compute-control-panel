package provider

import (
	"errors"
	"testing"
)

func receiptSnapshotLookupEvent(snapshot KueuePreDeleteDRASnapshot) DRAUnprepareEvent {
	return DRAUnprepareEvent{
		ClaimNamespace:                snapshot.Claim.Namespace,
		ClaimName:                     snapshot.Claim.Name,
		ClaimUID:                      snapshot.Claim.UID,
		PodUID:                        snapshot.Pod.UID,
		AllocatedClaimResourceVersion: snapshot.Claim.ResourceVersion,
		NodeName:                      snapshot.NodeName,
		AllocatedDevice:               snapshot.Devices[0],
	}
}

func TestGetPreDeleteDRASnapshotForReceiptIsTenantAndIdentityScoped(t *testing.T) {
	for _, tc := range []struct {
		name string
		open func(*testing.T) KueueJobBindingStore
	}{
		{name: "memory", open: func(*testing.T) KueueJobBindingStore { return NewMemoryKueueJobBindingStore() }},
		{name: "postgres", open: func(t *testing.T) KueueJobBindingStore {
			db := openProviderPostgresDB(t)
			store, err := NewPostgresKueueJobBindingStore(db)
			if err != nil {
				t.Fatal(err)
			}
			return store
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			store := tc.open(t)
			create := uniqueKueueRequest(t)
			const namespace = "tenant-a"
			const jobName, jobUID = "job-a", "job-uid-a"
			remove := preDeleteDeleteRequest(create, namespace, jobName, jobUID)
			if err := store.PutCreatePin(namespace, create, jobName, jobUID); err != nil {
				t.Fatal(err)
			}
			// A valid DELETE request may target the same desired generation as
			// its pinned CREATE. Receipt lookup must use the durable snapshot
			// contract, not invent an increment-only generation requirement.
			if create.DesiredGeneration != remove.DesiredGeneration {
				t.Fatal("fixture must cover equal CREATE/DELETE desired generations")
			}
			want, err := store.PutPreDeleteDRASnapshot(namespace, remove,
				preDeleteSnapshotFixture(create, remove, namespace, jobName, jobUID))
			if err != nil {
				t.Fatal(err)
			}
			event := receiptSnapshotLookupEvent(want)

			got, ok, err := store.GetPreDeleteDRASnapshotForReceipt(create.TenantID, event)
			if err != nil || !ok || got.Digest != want.Digest || got.DeleteOperationID != remove.OperationID {
				t.Fatalf("exact receipt lookup got=%+v ok=%t err=%v", got, ok, err)
			}

			if _, ok, err := store.GetPreDeleteDRASnapshotForReceipt("tenant-other", event); err != nil || ok {
				t.Fatalf("foreign tenant resolved snapshot: ok=%t err=%v", ok, err)
			}
			mutations := []struct {
				name   string
				mutate func(*DRAUnprepareEvent)
			}{
				{name: "claim namespace", mutate: func(e *DRAUnprepareEvent) { e.ClaimNamespace = "tenant-other" }},
				{name: "claim name", mutate: func(e *DRAUnprepareEvent) { e.ClaimName = "successor-claim" }},
				{name: "claim UID", mutate: func(e *DRAUnprepareEvent) { e.ClaimUID = "successor-claim-uid" }},
				{name: "claim resourceVersion", mutate: func(e *DRAUnprepareEvent) { e.AllocatedClaimResourceVersion = "999" }},
				{name: "Pod UID", mutate: func(e *DRAUnprepareEvent) { e.PodUID = "successor-pod-uid" }},
				{name: "Node", mutate: func(e *DRAUnprepareEvent) { e.NodeName = "successor-node" }},
				{name: "device", mutate: func(e *DRAUnprepareEvent) { e.AllocatedDevice.Device = "successor-device" }},
			}
			for _, mutation := range mutations {
				t.Run(mutation.name, func(t *testing.T) {
					changed := event
					mutation.mutate(&changed)
					if _, ok, err := store.GetPreDeleteDRASnapshotForReceipt(create.TenantID, changed); err != nil || ok {
						t.Fatalf("mismatched receipt resolved snapshot: ok=%t err=%v", ok, err)
					}
				})
			}
		})
	}
}

func TestMemoryPreDeleteReceiptSnapshotLookupRejectsAmbiguousIdentity(t *testing.T) {
	store := NewMemoryKueueJobBindingStore()
	createA := uniqueKueueRequest(t)
	createB := uniqueKueueRequest(t)
	const namespace = "tenant-a"
	const jobA, uidA = "job-a", "job-uid-a"
	const jobB, uidB = "job-b", "job-uid-b"
	removeA := preDeleteDeleteRequest(createA, namespace, jobA, uidA)
	removeB := preDeleteDeleteRequest(createB, namespace, jobB, uidB)
	for _, item := range []struct {
		create Request
		remove Request
		job    string
		uid    string
	}{{createA, removeA, jobA, uidA}, {createB, removeB, jobB, uidB}} {
		if err := store.PutCreatePin(namespace, item.create, item.job, item.uid); err != nil {
			t.Fatal(err)
		}
		snapshot := preDeleteSnapshotFixture(item.create, item.remove, namespace, item.job, item.uid)
		if item.job == jobB {
			snapshot.Claim = preDeleteSnapshotFixture(createA, removeA, namespace, jobA, uidA).Claim
			snapshot.Pod = preDeleteSnapshotFixture(createA, removeA, namespace, jobA, uidA).Pod
			snapshot.NodeName = preDeleteSnapshotFixture(createA, removeA, namespace, jobA, uidA).NodeName
			snapshot.Devices = preDeleteSnapshotFixture(createA, removeA, namespace, jobA, uidA).Devices
		}
		if _, err := store.PutPreDeleteDRASnapshot(namespace, item.remove, snapshot); err != nil {
			t.Fatal(err)
		}
	}
	snapshotA, ok, err := store.GetPreDeleteDRASnapshot(namespace, removeA)
	if err != nil || !ok {
		t.Fatalf("read first snapshot: ok=%t err=%v", ok, err)
	}
	if _, ok, err := store.GetPreDeleteDRASnapshotForReceipt(createA.TenantID, receiptSnapshotLookupEvent(snapshotA)); !errors.Is(err, ErrOperationConflict) || ok {
		t.Fatalf("ambiguous claim identity did not fail closed: ok=%t err=%v", ok, err)
	}
}
