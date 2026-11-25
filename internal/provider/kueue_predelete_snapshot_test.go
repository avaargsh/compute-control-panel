package provider

import (
	"context"
	"encoding/json"
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
		DeleteOperationID: remove.OperationID, CreateGeneration: create.DesiredGeneration, DeleteGeneration: remove.DesiredGeneration,
		Job:               KubernetesResourceRef{Namespace: namespace, Name: jobName, UID: jobUID, ResourceVersion: "11"},
		KueueWorkload:     KubernetesResourceRef{Namespace: namespace, Name: "kueue-" + suffix, UID: "kueue-uid-" + suffix, ResourceVersion: "12"},
		Pod:               KubernetesResourceRef{Namespace: namespace, Name: "pod-" + suffix, UID: "pod-uid-" + suffix, ResourceVersion: "13"},
		NodeName:          "node-" + suffix,
		Claim:             KubernetesResourceRef{Namespace: namespace, Name: "claim-" + suffix, UID: "claim-uid-" + suffix, ResourceVersion: "14"},
		ClaimTemplateName: "gpu-template", PodClaimAlias: "gpu", ConsumerContainer: "worker",
		Devices: []DRAAllocatedDevice{{Request: "gpu", Driver: "vendor.example", Pool: "pool-a", Device: "gpu-uuid-" + suffix}},
		Source:  "kubernetes-api", AdapterVersion: "kueue-dra-v1",
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
			suffix := strings.TrimPrefix(create.OperationID, "op-")
			jobName, jobUID := "job-"+suffix, "job-uid-"+suffix
			remove := preDeleteDeleteRequest(create, namespace, jobName, jobUID)
			if err := store.PutCreatePin(namespace, create, jobName, jobUID); err != nil {
				t.Fatal(err)
			}

			pin, ok, err := store.GetCreatePinByJob(namespace, create.TenantID, jobName)
			if err != nil || !ok || pin.UID != jobUID || pin.Name != jobName ||
				pin.Request.OperationID != create.OperationID || !sameRequest(pin.Request, create) {
				t.Fatalf("create pin lookup %+v ok=%t err=%v", pin, ok, err)
			}

			want := preDeleteSnapshotFixture(create, remove, namespace, jobName, jobUID)
			stored, err := store.PutPreDeleteDRASnapshot(namespace, remove, want)
			if err != nil {
				t.Fatalf("store pre-delete snapshot: %v", err)
			}
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
			suffix := strings.TrimPrefix(create.OperationID, "op-")
			jobName, jobUID := "job-"+suffix, "job-uid-"+suffix
			remove := preDeleteDeleteRequest(create, namespace, jobName, jobUID)
			if err := store.PutCreatePin(namespace, create, jobName, jobUID); err != nil {
				t.Fatal(err)
			}
			if err := store.PutDeleteTombstone(namespace, remove, jobName, jobUID, false); err != nil {
				t.Fatal(err)
			}
			snapshot := preDeleteSnapshotFixture(create, remove, namespace, jobName, jobUID)
			if _, err := store.PutPreDeleteDRASnapshot(namespace, remove, snapshot); !errors.Is(err, ErrOperationConflict) {
				t.Fatalf("snapshot was created after DELETE intent: %v", err)
			}
			if _, ok, err := store.GetPreDeleteDRASnapshot(namespace, remove); err != nil || ok {
				t.Fatalf("late snapshot persisted: ok=%t err=%v", ok, err)
			}
			another := remove
			another.OperationID += "-retry"
			another.IdempotencyKey += "-retry"
			late := preDeleteSnapshotFixture(create, another, namespace, jobName, jobUID)
			if _, err := store.PutPreDeleteDRASnapshot(namespace, another, late); !errors.Is(err, ErrOperationConflict) {
				t.Fatalf("new DELETE operation minted late history for the same Job UID: %v", err)
			}
			if _, ok, err := store.GetPreDeleteDRASnapshot(namespace, another); err != nil || ok {
				t.Fatalf("late retry snapshot persisted: ok=%t err=%v", ok, err)
			}
		})
	}
}

func TestPostgresKueuePreDeleteSnapshotSerializesWithDeleteIntent(t *testing.T) {
	db := openProviderPostgresDB(t)
	store, err := NewPostgresKueueJobBindingStore(db)
	if err != nil {
		t.Fatal(err)
	}
	create := uniqueKueueRequest(t)
	const namespace = "tenant-a"
	suffix := strings.TrimPrefix(create.OperationID, "op-")
	jobName, jobUID := "job-"+suffix, "job-uid-"+suffix
	remove := preDeleteDeleteRequest(create, namespace, jobName, jobUID)
	if err := store.PutCreatePin(namespace, create, jobName, jobUID); err != nil {
		t.Fatal(err)
	}
	snapshot := preDeleteSnapshotFixture(create, remove, namespace, jobName, jobUID)

	start := make(chan struct{})
	type outcome struct {
		snapshot bool
		err      error
	}
	results := make(chan outcome, 2)
	go func() {
		<-start
		_, err := store.PutPreDeleteDRASnapshot(namespace, remove, snapshot)
		results <- outcome{snapshot: true, err: err}
	}()
	go func() {
		<-start
		err := store.PutDeleteTombstone(namespace, remove, jobName, jobUID, false)
		results <- outcome{err: err}
	}()
	close(start)

	var snapshotErr, tombstoneErr error
	for i := 0; i < 2; i++ {
		got := <-results
		if got.snapshot {
			snapshotErr = got.err
		} else {
			tombstoneErr = got.err
		}
	}
	if tombstoneErr != nil {
		t.Fatalf("DELETE intent failed: %v", tombstoneErr)
	}
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

func TestKueueJobAdapterRecordsAndRequiresPreDeleteDRASnapshot(t *testing.T) {
	store := NewMemoryKueueJobBindingStore()
	create := uniqueKueueRequest(t)
	const namespace = "tenant-a"
	suffix := strings.TrimPrefix(create.OperationID, "op-")
	jobName, jobUID := "job-"+suffix, "job-uid-"+suffix
	remove := preDeleteDeleteRequest(create, namespace, jobName, jobUID)
	if err := store.PutCreatePin(namespace, create, jobName, jobUID); err != nil {
		t.Fatal(err)
	}
	adapter := &KueueJobAdapter{
		namespace: namespace, bindings: store, resourceClaimTemplateName: "gpu-template",
	}
	if err := adapter.requirePreDeleteDRASnapshot(remove); !errors.Is(err, ErrPreDeleteDRASnapshotRequired) {
		t.Fatalf("DRA delete without pre-delete snapshot was not blocked: %v", err)
	}

	pod := KubernetesResourceRef{Namespace: namespace, Name: "pod-" + suffix, UID: "pod-uid-" + suffix, ResourceVersion: "13"}
	evidence := KueueJobEvidence{
		TenantID: create.TenantID, WorkloadID: create.WorkloadID, DesiredGeneration: create.DesiredGeneration,
		Job:         KubernetesResourceRef{Namespace: namespace, Name: jobName, UID: jobUID, ResourceVersion: "11"},
		JobTenantID: create.TenantID, JobWorkloadID: create.WorkloadID,
		JobDesiredGeneration: create.DesiredGeneration,
		KueueWorkload:        KubernetesResourceRef{Namespace: namespace, Name: "kueue-" + suffix, UID: "kueue-uid-" + suffix, ResourceVersion: "12"},
		KueueOwnerJobUID:     jobUID, KueueQuotaReserved: true, KueueClusterQueue: "cq-a", KueueAdmitted: true,
		KueueQuotaCondition:        KueueConditionEvidence{Status: "True"},
		KueueAdmissionCondition:    KueueConditionEvidence{Status: "True"},
		KueueEvictedCondition:      KueueConditionEvidence{Status: "MISSING"},
		KueuePreemptedCondition:    KueueConditionEvidence{Status: "MISSING"},
		KueuePodSetAssignmentsJSON: "[]",
		Pod:                        pod, PodOwnerJobUID: jobUID, PodScheduled: true, PodNodeName: "node-" + suffix,
	}
	collection := KueueJobCollection{
		Evidence: evidence,
		Provenance: KueueEvidenceProvenance{
			Source: "kubernetes-api", AdapterVersion: "kueue-dra-v1", ObservedAt: time.Now().UTC(),
		},
		Stage: KueuePodPlaced,
	}
	deviceSet := DRADeviceSetObservation{
		Pod: pod, NodeName: evidence.PodNodeName, ClaimTemplateName: "gpu-template",
		PodClaimAlias: "gpu", ConsumerContainer: "worker",
		Claim:   KubernetesResourceRef{Namespace: namespace, Name: "claim-" + suffix, UID: "claim-uid-" + suffix, ResourceVersion: "14"},
		Devices: []DRAAllocatedDevice{{Request: "gpu", Driver: "vendor.example", Pool: "pool-a", Device: "gpu-uuid-" + suffix}},
	}
	snapshot, err := adapter.RecordPreDeleteDRASnapshot(context.Background(), remove, collection, deviceSet)
	if err != nil || snapshot.Digest == "" {
		t.Fatalf("record pre-delete snapshot %+v err=%v", snapshot, err)
	}
	if err := adapter.requirePreDeleteDRASnapshot(remove); err != nil {
		t.Fatalf("exact pre-delete snapshot did not unlock DELETE preflight: %v", err)
	}
	successor := remove
	successor.TargetResourceUID = "successor-job-uid"
	if err := adapter.requirePreDeleteDRASnapshot(successor); !errors.Is(err, ErrPreDeleteDRASnapshotRequired) {
		t.Fatalf("successor Job UID reused historical snapshot: %v", err)
	}
	badCollection := collection
	badCollection.Evidence.Pod.UID = "successor-pod-uid"
	if _, err := adapter.RecordPreDeleteDRASnapshot(context.Background(), remove, badCollection, deviceSet); !errors.Is(err, ErrInvalidPreDeleteDRASnapshot) {
		t.Fatalf("mismatched Pod UID was recorded: %v", err)
	}
}

func TestPostgresKueuePreDeleteDRASnapshotIsImmutable(t *testing.T) {
	db := openProviderPostgresDB(t)
	store, err := NewPostgresKueueJobBindingStore(db)
	if err != nil {
		t.Fatal(err)
	}
	create := uniqueKueueRequest(t)
	const namespace = "tenant-a"
	suffix := strings.TrimPrefix(create.OperationID, "op-")
	jobName, jobUID := "job-"+suffix, "job-uid-"+suffix
	remove := preDeleteDeleteRequest(create, namespace, jobName, jobUID)
	if err := store.PutCreatePin(namespace, create, jobName, jobUID); err != nil {
		t.Fatal(err)
	}
	snapshot := preDeleteSnapshotFixture(create, remove, namespace, jobName, jobUID)
	if _, err := store.PutPreDeleteDRASnapshot(namespace, remove, snapshot); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	for _, tc := range []struct {
		name  string
		query string
	}{
		{name: "update", query: `UPDATE public.kueue_job_pre_delete_dra_snapshots SET claim_uid='rewritten' WHERE namespace=$1 AND tenant_id=$2 AND delete_operation_id=$3`},
		{name: "delete", query: `DELETE FROM public.kueue_job_pre_delete_dra_snapshots WHERE namespace=$1 AND tenant_id=$2 AND delete_operation_id=$3`},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if _, err := db.ExecContext(ctx, tc.query, namespace, remove.TenantID, remove.OperationID); postgresSQLState(err) != "55000" {
				t.Fatalf("immutable snapshot %s state=%s err=%v", tc.name, postgresSQLState(err), err)
			}
		})
	}
	if _, err := db.ExecContext(ctx, `TRUNCATE public.kueue_job_pre_delete_dra_snapshots`); postgresSQLState(err) != "55000" {
		t.Fatalf("immutable snapshot truncate state=%s err=%v", postgresSQLState(err), err)
	}
}

func TestKueueJobAdapterBlocksDRADeleteBeforeExternalSideEffectWithoutSnapshot(t *testing.T) {
	adapter, api, server := newKueueAdapterFixture(t, &kueueAPI{})
	defer server.Close()
	adapter.queueName = "queue-a"
	if err := adapter.UseQueuedResourceClaimTemplate("gpu-template"); err != nil {
		t.Fatal(err)
	}
	create := kueueAdapterRequest()
	if _, err := adapter.Apply(context.Background(), create); err != nil {
		t.Fatal(err)
	}
	jobName := kueueCreateJobName(create.TenantID, create.OperationID)
	api.mu.Lock()
	job := api.jobs[jobName]
	api.mu.Unlock()
	if job.Metadata.UID == "" {
		t.Fatal("fake API did not persist created Job UID")
	}
	remove := create
	remove.OperationID = "op-delete-snapshot-gate"
	remove.IdempotencyKey = "idem-delete-snapshot-gate"
	remove.Kind = operation.Delete
	remove.TargetResourceID = adapter.namespace + "/" + jobName
	remove.TargetResourceUID = job.Metadata.UID
	if _, err := adapter.Apply(context.Background(), remove); !errors.Is(err, ErrPreDeleteDRASnapshotRequired) {
		t.Fatalf("DRA DELETE without snapshot was not blocked: %v", err)
	}
	if api.deleteCount() != 0 || api.storedJobs() != 1 {
		t.Fatalf("missing snapshot caused external DELETE: deletes=%d stored=%d", api.deleteCount(), api.storedJobs())
	}
	if _, ok, err := adapter.bindings.GetDeleteTombstone(adapter.namespace, remove); err != nil || ok {
		t.Fatalf("DELETE intent was persisted without pre-delete snapshot: exists=%t err=%v", ok, err)
	}
}

func TestPostgresKueuePreDeleteSnapshotInsertGuardsRejectTenantAndLateWrites(t *testing.T) {
	db := openProviderPostgresDB(t)
	store, err := NewPostgresKueueJobBindingStore(db)
	if err != nil {
		t.Fatal(err)
	}
	create := uniqueKueueRequest(t)
	const namespace = "tenant-a"
	suffix := strings.TrimPrefix(create.OperationID, "op-")
	jobName, jobUID := "job-"+suffix, "job-uid-"+suffix
	remove := preDeleteDeleteRequest(create, namespace, jobName, jobUID)
	if err := store.PutCreatePin(namespace, create, jobName, jobUID); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	// Direct SQL cannot write a DELETE tombstone for another tenant's pinned Job.
	_, err = db.ExecContext(ctx, `
		INSERT INTO public.kueue_job_delete_tombstones(
			namespace,tenant_id,operation_id,job_name,job_uid,workload_id,provider_id,
			idempotency_key,request_digest,desired_generation,operation_kind,
			target_resource_id,target_resource_uid,acknowledged
		) VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,'DELETE',$11,$5,FALSE)`,
		namespace, "tenant-other", "delete-cross-tenant-"+suffix, jobName, jobUID,
		create.WorkloadID, create.ProviderID, "idem-cross-tenant-"+suffix, create.RequestDigest,
		int64(remove.DesiredGeneration), namespace+"/"+jobName)
	if postgresSQLState(err) != "23514" {
		t.Fatalf("cross-tenant direct tombstone state=%s err=%v", postgresSQLState(err), err)
	}

	if err := store.PutDeleteTombstone(namespace, remove, jobName, jobUID, false); err != nil {
		t.Fatal(err)
	}
	pin, ok, err := store.GetCreatePinByJob(namespace, create.TenantID, jobName)
	if err != nil || !ok {
		t.Fatalf("read CREATE pin for direct snapshot insert: ok=%t err=%v", ok, err)
	}
	another := remove
	another.OperationID += "-retry"
	another.IdempotencyKey += "-retry"
	snapshot := preDeleteSnapshotFixture(create, another, namespace, jobName, jobUID)
	canonical, err := validateKueuePreDeleteDRASnapshot(namespace, another, pin, snapshot)
	if err != nil {
		t.Fatal(err)
	}
	createGeneration, err := kueueBindingGeneration(canonical.CreateGeneration)
	if err != nil {
		t.Fatal(err)
	}
	deleteGeneration, err := kueueBindingGeneration(canonical.DeleteGeneration)
	if err != nil {
		t.Fatal(err)
	}
	devices, err := json.Marshal(canonical.Devices)
	if err != nil {
		t.Fatal(err)
	}
	_, err = db.ExecContext(ctx, insertKueuePreDeleteSnapshotSQL,
		canonical.Namespace, canonical.TenantID, canonical.DeleteOperationID, canonical.CreateOperationID,
		canonical.WorkloadID, canonical.ProviderID, createGeneration, deleteGeneration,
		canonical.Job.Name, canonical.Job.UID, canonical.Job.ResourceVersion,
		canonical.KueueWorkload.Name, canonical.KueueWorkload.UID, canonical.KueueWorkload.ResourceVersion,
		canonical.Pod.Name, canonical.Pod.UID, canonical.Pod.ResourceVersion, canonical.NodeName,
		canonical.Claim.Name, canonical.Claim.UID, canonical.Claim.ResourceVersion, canonical.ClaimTemplateName,
		canonical.PodClaimAlias, canonical.ConsumerContainer, devices, canonical.Source, canonical.AdapterVersion,
		canonical.ObservedAt, canonical.Digest,
	)
	if postgresSQLState(err) != "55000" {
		t.Fatalf("late direct snapshot insert state=%s err=%v", postgresSQLState(err), err)
	}
}

type preDeleteSnapshotReadOverride struct {
	KueueJobBindingStore
	snapshot KueuePreDeleteDRASnapshot
}

func (s preDeleteSnapshotReadOverride) GetPreDeleteDRASnapshot(string, Request) (KueuePreDeleteDRASnapshot, bool, error) {
	return s.snapshot, true, nil
}

func TestKueuePreDeleteSnapshotReadRevalidatesCreatePinAndStructure(t *testing.T) {
	store := NewMemoryKueueJobBindingStore()
	create := uniqueKueueRequest(t)
	const namespace = "tenant-a"
	const jobName, jobUID = "job-a", "job-uid-a"
	remove := preDeleteDeleteRequest(create, namespace, jobName, jobUID)
	if err := store.PutCreatePin(namespace, create, jobName, jobUID); err != nil {
		t.Fatal(err)
	}
	stored, err := store.PutPreDeleteDRASnapshot(namespace, remove,
		preDeleteSnapshotFixture(create, remove, namespace, jobName, jobUID))
	if err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct {
		name   string
		mutate func(*KueuePreDeleteDRASnapshot)
	}{
		{name: "wrong CREATE generation", mutate: func(s *KueuePreDeleteDRASnapshot) { s.CreateGeneration++ }},
		{name: "foreign Claim namespace", mutate: func(s *KueuePreDeleteDRASnapshot) { s.Claim.Namespace = "tenant-other" }},
		{name: "missing Pod UID", mutate: func(s *KueuePreDeleteDRASnapshot) { s.Pod.UID = "" }},
		{name: "missing provenance", mutate: func(s *KueuePreDeleteDRASnapshot) { s.Source = "" }},
		{name: "missing observation time", mutate: func(s *KueuePreDeleteDRASnapshot) { s.ObservedAt = time.Time{} }},
		{name: "unserializable observation time", mutate: func(s *KueuePreDeleteDRASnapshot) {
			s.ObservedAt = time.Date(10000, 1, 1, 0, 0, 0, 0, time.UTC)
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			altered := cloneKueuePreDeleteDRASnapshot(stored)
			tc.mutate(&altered)
			altered.Digest = kueuePreDeleteSnapshotDigest(altered)
			adapter := &KueueJobAdapter{
				namespace: namespace, bindings: preDeleteSnapshotReadOverride{store, altered},
				resourceClaimTemplateName: "gpu-template",
			}
			if err := adapter.requirePreDeleteDRASnapshot(remove); !errors.Is(err, ErrPreDeleteDRASnapshotRequired) {
				t.Fatalf("invalid stored snapshot unlocked DELETE: %v", err)
			}
		})
	}
	adapter := &KueueJobAdapter{
		namespace: namespace, bindings: preDeleteSnapshotReadOverride{store, stored},
		resourceClaimTemplateName: "gpu-template",
	}
	if err := adapter.requirePreDeleteDRASnapshot(remove); err != nil {
		t.Fatalf("valid immutable snapshot rejected: %v", err)
	}
}
