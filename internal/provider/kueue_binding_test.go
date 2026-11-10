package provider

import (
	"context"
	"errors"
	"net/http"
	"testing"

	"github.com/avaargsh/compute-control-panel/internal/operation"
)

func TestMemoryKueueJobBindingStoreKeepsPinAndAcknowledgedTombstone(t *testing.T) {
	store := NewMemoryKueueJobBindingStore()
	req := kueueAdapterRequest()
	if err := store.PutCreatePin("tenant-a", req, "ccp-job", "uid-1"); err != nil {
		t.Fatal(err)
	}
	if err := store.PutCreatePin("tenant-a", req, "ccp-job", "uid-1"); err != nil {
		t.Fatal(err)
	}
	if err := store.PutCreatePin("tenant-a", req, "ccp-job", "uid-2"); !errors.Is(err, ErrOperationConflict) {
		t.Fatalf("pin rewrite: %v", err)
	}
	got, ok, err := store.GetCreatePin("tenant-a", req)
	if err != nil || !ok || got.UID != "uid-1" || got.Name != "ccp-job" || got.Namespace != "tenant-a" {
		t.Fatalf("pin %+v ok=%v err=%v", got, ok, err)
	}
	if _, ok, err := store.GetCreatePin("other-ns", req); err != nil || ok {
		t.Fatalf("pin crossed namespace: ok=%v err=%v", ok, err)
	}
	uid, ok, err := store.CreatePinUID("tenant-a", "ccp-job")
	if err != nil || !ok || uid != "uid-1" {
		t.Fatalf("pin uid %q ok=%v err=%v", uid, ok, err)
	}
	if _, ok, err := store.CreatePinUID("other-ns", "ccp-job"); err != nil || ok {
		t.Fatalf("name scan crossed namespace: ok=%v err=%v", ok, err)
	}

	remove := req
	remove.Kind = operation.Delete
	remove.OperationID = "op-delete-1"
	remove.IdempotencyKey = "idem-delete-1"
	remove.TargetResourceID = "tenant-a/ccp-job"
	remove.TargetResourceUID = "uid-1"
	if err := store.PutDeleteTombstone("tenant-a", remove, "ccp-job", "uid-1", false); err != nil {
		t.Fatal(err)
	}
	if err := store.PutDeleteTombstone("tenant-a", remove, "ccp-job", "uid-1", true); err != nil {
		t.Fatal(err)
	}
	if err := store.PutDeleteTombstone("tenant-a", remove, "ccp-job", "uid-1", false); err != nil {
		t.Fatal(err)
	}
	if err := store.PutDeleteTombstone("tenant-a", remove, "ccp-job", "uid-2", true); !errors.Is(err, ErrOperationConflict) {
		t.Fatalf("tombstone rewrite: %v", err)
	}
	tomb, ok, err := store.GetDeleteTombstone("tenant-a", remove)
	if err != nil || !ok || !tomb.Acknowledged || tomb.UID != "uid-1" {
		t.Fatalf("tombstone %+v ok=%v err=%v", tomb, ok, err)
	}
	if _, err := NewKueueJobAdapterWithBindings("https://kubernetes.default.svc", "tenant-a", nil, nil); !errors.Is(err, ErrInvalidKueueObservation) {
		t.Fatalf("nil store: %v", err)
	}
}

func TestKueueJobBindingStoreResumesCreateAcrossAdapterRestart(t *testing.T) {
	store := NewMemoryKueueJobBindingStore()
	adapter, api, server := newKueueAdapterFixtureWithStore(t, &kueueAPI{}, store)
	defer server.Close()
	ctx := context.Background()
	req := kueueAdapterRequest()
	if _, err := adapter.Apply(ctx, req); err != nil {
		t.Fatal(err)
	}

	restarted := reopenKueueAdapter(t, server, store)
	again, err := restarted.Apply(ctx, req)
	if err != nil || api.postCount() != 1 || api.deleteCount() != 0 {
		t.Fatalf("restarted create %+v posts=%d deletes=%d err=%v", again, api.postCount(), api.deleteCount(), err)
	}
	observed, err := restarted.Inspect(ctx, req)
	if err != nil || observed.Result != operation.ResourcePresent || observed.ResourceUID != "job-uid-created" ||
		observed.ResourceID != "tenant-a/"+kueueCreateJobName(req.TenantID, req.OperationID) ||
		observed.ResourceOperationID != req.OperationID {
		t.Fatalf("restarted inspect %+v %v", observed, err)
	}

	bare, err := NewKueueJobAdapter(server.URL, "tenant-a", server.Client())
	if err != nil {
		t.Fatal(err)
	}
	if _, err := bare.Apply(ctx, req); !errors.Is(err, ErrAcknowledgementLost) || api.postCount() != 1 {
		t.Fatalf("adapter without store adopted the Job: %v posts=%d", err, api.postCount())
	}
	unbound, err := bare.Inspect(ctx, req)
	if err != nil || unbound.Result != operation.OperationInProgress || unbound.ResourceUID != "" ||
		unbound.ResourceID != "" || unbound.Result == operation.OperationNotAppliedFinal ||
		unbound.Result == operation.ResourcePresent {
		t.Fatalf("adapter without store finalized CREATE: %+v %v", unbound, err)
	}
}

func TestKueueJobLostCreateACKDoesNotWriteBinding(t *testing.T) {
	store := NewMemoryKueueJobBindingStore()
	adapter, api, server := newKueueAdapterFixtureWithStore(t, &kueueAPI{loseCreateBody: true}, store)
	defer server.Close()
	ctx := context.Background()
	req := kueueAdapterRequest()
	if _, err := adapter.Apply(ctx, req); !errors.Is(err, ErrAcknowledgementLost) || api.postCount() != 1 || api.storedJobs() != 1 {
		t.Fatalf("lost create posts=%d stored=%d err=%v", api.postCount(), api.storedJobs(), err)
	}
	if _, ok, err := store.GetCreatePin("tenant-a", req); err != nil || ok {
		t.Fatalf("lost create wrote a pin: ok=%v err=%v", ok, err)
	}
	restarted := reopenKueueAdapter(t, server, store)
	observed, err := restarted.Inspect(ctx, req)
	if err != nil || observed.Result != operation.OperationInProgress || observed.ResourceUID != "" ||
		observed.Result == operation.ResourcePresent || observed.Result == operation.OperationNotAppliedFinal {
		t.Fatalf("lost create became trusted: %+v %v", observed, err)
	}
	if _, err := restarted.Apply(ctx, req); !errors.Is(err, ErrAcknowledgementLost) || api.postCount() != 1 {
		t.Fatalf("lost create replay posts=%d err=%v", api.postCount(), err)
	}
}

func TestKueueJobBindingStoreRejectsSuccessorAndChangedSpec(t *testing.T) {
	store := NewMemoryKueueJobBindingStore()
	adapter, api, server := newKueueAdapterFixtureWithStore(t, &kueueAPI{}, store)
	defer server.Close()
	ctx := context.Background()
	req := kueueAdapterRequest()
	if _, err := adapter.Apply(ctx, req); err != nil {
		t.Fatal(err)
	}
	api.setReplacement("job-uid-successor", "")
	restarted := reopenKueueAdapter(t, server, store)
	observed, err := restarted.Inspect(ctx, req)
	if err != nil || observed.Result != operation.ObservationConflict || observed.ReasonCode != "JOB_UID_REPLACED" ||
		observed.ResourceUID != "" || observed.Result == operation.ResourcePresent {
		t.Fatalf("successor inspect %+v %v", observed, err)
	}
	if _, err := restarted.Apply(ctx, req); !errors.Is(err, ErrOperationConflict) || api.postCount() != 1 ||
		api.deleteCount() != 0 || api.storedJobs() != 1 {
		t.Fatalf("successor apply posts=%d deletes=%d stored=%d err=%v", api.postCount(), api.deleteCount(), api.storedJobs(), err)
	}

	api.setReplacement("", "")
	name := kueueCreateJobName(req.TenantID, req.OperationID)
	api.mu.Lock()
	obj := api.jobs[name]
	suspended := false
	obj.Spec.Suspend = &suspended
	api.jobs[name] = obj
	api.mu.Unlock()
	if _, err := restarted.Apply(ctx, req); !errors.Is(err, ErrOperationConflict) || api.postCount() != 1 || api.storedJobs() != 1 {
		t.Fatalf("unsuspended replay posts=%d stored=%d err=%v", api.postCount(), api.storedJobs(), err)
	}
	unsafe, err := restarted.Inspect(ctx, req)
	if err != nil || unsafe.Result != operation.ObservationConflict || unsafe.ReasonCode != "JOB_SPEC_MISMATCH" ||
		unsafe.ResourceUID != "" {
		t.Fatalf("unsuspended inspect %+v %v", unsafe, err)
	}
}

func TestKueueJobBindingStoreResumesDeleteAcrossAdapterRestart(t *testing.T) {
	store := NewMemoryKueueJobBindingStore()
	adapter, api, server := newKueueAdapterFixtureWithStore(t, &kueueAPI{}, store)
	defer server.Close()
	ctx := context.Background()
	create, observed := createSuspendedJob(t, adapter, api)
	remove := kueueDeleteRequest(create, observed)
	ack, err := adapter.Apply(ctx, remove)
	if err != nil || api.deleteCount() != 1 || api.storedJobs() != 0 {
		t.Fatalf("delete %+v deletes=%d stored=%d err=%v", ack, api.deleteCount(), api.storedJobs(), err)
	}

	restarted := reopenKueueAdapter(t, server, store)
	absent, err := restarted.Inspect(ctx, remove)
	if err != nil || absent.Result != operation.TargetAbsent || absent.TargetResourceUID != observed.ResourceUID ||
		absent.TargetResourceID != remove.TargetResourceID || absent.ResourceUID != "" ||
		absent.Result == operation.OperationNotAppliedFinal {
		t.Fatalf("restarted absence %+v %v", absent, err)
	}
	if _, err := restarted.Apply(ctx, remove); err != nil || api.deleteCount() != 1 || api.postCount() != 1 {
		t.Fatalf("restarted replay deletes=%d posts=%d err=%v", api.deleteCount(), api.postCount(), err)
	}
	api.plantJob(kueueCreateJobName(create.TenantID, create.OperationID), "job-uid-successor")
	if _, err := restarted.Apply(ctx, remove); !errors.Is(err, ErrOperationConflict) || api.deleteCount() != 1 || api.storedJobs() != 1 {
		t.Fatalf("restarted replay deleted successor: deletes=%d stored=%d err=%v", api.deleteCount(), api.storedJobs(), err)
	}
	blocked, err := restarted.Inspect(ctx, remove)
	if err != nil || blocked.Result != operation.ObservationConflict || blocked.ReasonCode != "JOB_UID_REPLACED" ||
		blocked.Result == operation.TargetAbsent || blocked.Result == operation.OperationNotAppliedFinal {
		t.Fatalf("successor after tombstone %+v %v", blocked, err)
	}
}

func TestKueueJobRestartWithoutBindingLeaves404Ambiguous(t *testing.T) {
	adapter, api, server := newKueueAdapterFixture(t, &kueueAPI{})
	defer server.Close()
	ctx := context.Background()
	create, observed := createSuspendedJob(t, adapter, api)
	remove := kueueDeleteRequest(create, observed)
	if _, err := adapter.Apply(ctx, remove); err != nil || api.storedJobs() != 0 {
		t.Fatalf("delete err=%v stored=%d", err, api.storedJobs())
	}
	restarted, err := NewKueueJobAdapter(server.URL, "tenant-a", server.Client())
	if err != nil {
		t.Fatal(err)
	}
	absent, err := restarted.Inspect(ctx, remove)
	if err != nil || absent.Result != operation.ObservationConflict ||
		absent.ReasonCode != "MISSING_AUTHORITATIVE_OPERATION" ||
		absent.Result == operation.TargetAbsent || absent.Result == operation.OperationNotAppliedFinal {
		t.Fatalf("bare 404 after restart %+v %v", absent, err)
	}
	if _, err := restarted.Apply(ctx, remove); !errors.Is(err, ErrOperationConflict) || api.deleteCount() != 1 || api.postCount() != 1 {
		t.Fatalf("bare 404 was deleted again: deletes=%d posts=%d err=%v", api.deleteCount(), api.postCount(), err)
	}
	stale, err := restarted.Inspect(ctx, create)
	if err != nil || stale.Result != operation.ObservationConflict ||
		stale.ReasonCode != "MISSING_AUTHORITATIVE_OPERATION" ||
		stale.Result == operation.OperationNotAppliedFinal || stale.Result == operation.ResourcePresent {
		t.Fatalf("create 404 after restart %+v %v", stale, err)
	}
}

func TestKueueJobUnacknowledgedDeleteSurvivesRestartWithoutDeletingSuccessor(t *testing.T) {
	t.Run("lost response", func(t *testing.T) {
		store := NewMemoryKueueJobBindingStore()
		adapter, api, server := newKueueAdapterFixtureWithStore(t, &kueueAPI{loseDeleteBody: true}, store)
		defer server.Close()
		ctx := context.Background()
		create, observed := createSuspendedJob(t, adapter, api)
		remove := kueueDeleteRequest(create, observed)
		if _, err := adapter.Apply(ctx, remove); !errors.Is(err, ErrAcknowledgementLost) || api.deleteCount() != 1 || api.storedJobs() != 0 {
			t.Fatalf("lost delete ack deletes=%d stored=%d err=%v", api.deleteCount(), api.storedJobs(), err)
		}
		restarted := reopenKueueAdapter(t, server, store)
		absent, err := restarted.Inspect(ctx, remove)
		if err != nil || absent.Result != operation.TargetAbsent || absent.TargetResourceUID != observed.ResourceUID ||
			absent.Result == operation.OperationNotAppliedFinal {
			t.Fatalf("lost ack absence %+v %v", absent, err)
		}
		api.plantJob(kueueCreateJobName(create.TenantID, create.OperationID), "job-uid-successor")
		if _, err := restarted.Apply(ctx, remove); !errors.Is(err, ErrOperationConflict) || api.deleteCount() != 1 || api.storedJobs() != 1 {
			t.Fatalf("lost ack deleted successor deletes=%d stored=%d err=%v", api.deleteCount(), api.storedJobs(), err)
		}
	})

	t.Run("server error then same uid", func(t *testing.T) {
		store := NewMemoryKueueJobBindingStore()
		adapter, api, server := newKueueAdapterFixtureWithStore(t, &kueueAPI{deleteStatus: http.StatusInternalServerError}, store)
		defer server.Close()
		ctx := context.Background()
		_, observed := createSuspendedJob(t, adapter, api)
		remove := kueueDeleteRequest(kueueAdapterRequest(), observed)
		if _, err := adapter.Apply(ctx, remove); !errors.Is(err, ErrAcknowledgementLost) || api.deleteCount() != 1 || api.storedJobs() != 1 {
			t.Fatalf("ambiguous delete deletes=%d stored=%d err=%v", api.deleteCount(), api.storedJobs(), err)
		}
		api.setDeleteStatus(0)
		restarted := reopenKueueAdapter(t, server, store)
		if _, err := restarted.Apply(ctx, remove); err != nil || api.deleteCount() != 2 || api.storedJobs() != 0 {
			t.Fatalf("retry after restart deletes=%d stored=%d err=%v", api.deleteCount(), api.storedJobs(), err)
		}
		assertUIDPreconditions(t, api, observed.ResourceUID)
		absent, err := restarted.Inspect(ctx, remove)
		if err != nil || absent.Result != operation.TargetAbsent || absent.TargetResourceUID != observed.ResourceUID {
			t.Fatalf("retry inspect %+v %v", absent, err)
		}
	})

	t.Run("server error then successor", func(t *testing.T) {
		store := NewMemoryKueueJobBindingStore()
		adapter, api, server := newKueueAdapterFixtureWithStore(t, &kueueAPI{deleteStatus: http.StatusInternalServerError}, store)
		defer server.Close()
		ctx := context.Background()
		create, observed := createSuspendedJob(t, adapter, api)
		remove := kueueDeleteRequest(create, observed)
		if _, err := adapter.Apply(ctx, remove); !errors.Is(err, ErrAcknowledgementLost) || api.storedJobs() != 1 {
			t.Fatalf("ambiguous delete stored=%d err=%v", api.storedJobs(), err)
		}
		api.setDeleteStatus(0)
		api.setReplacement("job-uid-successor", "")
		restarted := reopenKueueAdapter(t, server, store)
		if _, err := restarted.Apply(ctx, remove); !errors.Is(err, ErrOperationConflict) || api.deleteCount() != 1 || api.storedJobs() != 1 {
			t.Fatalf("unacknowledged intent deleted successor deletes=%d stored=%d err=%v", api.deleteCount(), api.storedJobs(), err)
		}
		seen, err := restarted.Inspect(ctx, remove)
		if err != nil || seen.Result != operation.ObservationConflict || seen.ReasonCode != "JOB_UID_REPLACED" ||
			seen.Result == operation.TargetAbsent || seen.Result == operation.OperationNotAppliedFinal {
			t.Fatalf("successor after unacknowledged delete %+v %v", seen, err)
		}
	})
}

type failingBindingStore struct{}

func (failingBindingStore) GetCreatePin(string, Request) (KueueCreatePin, bool, error) {
	return KueueCreatePin{}, false, errBindingUnreadable
}

func (failingBindingStore) PutCreatePin(string, Request, string, string) error {
	return errBindingUnreadable
}

func (failingBindingStore) GetDeleteTombstone(string, Request) (KueueDeleteTombstone, bool, error) {
	return KueueDeleteTombstone{}, false, errBindingUnreadable
}

func (failingBindingStore) PutDeleteTombstone(string, Request, string, string, bool) error {
	return errBindingUnreadable
}

func (failingBindingStore) CreatePinUID(string, string) (string, bool, error) {
	return "", false, errBindingUnreadable
}

var errBindingUnreadable = errors.New("binding store unreadable")

func TestKueueJobUnreadableBindingStoreDoesNotAdoptOrDelete(t *testing.T) {
	store := NewMemoryKueueJobBindingStore()
	adapter, api, server := newKueueAdapterFixtureWithStore(t, &kueueAPI{}, store)
	defer server.Close()
	ctx := context.Background()
	req := kueueAdapterRequest()
	if _, err := adapter.Apply(ctx, req); err != nil {
		t.Fatal(err)
	}
	blind := reopenKueueAdapter(t, server, failingBindingStore{})
	if _, err := blind.Apply(ctx, req); !errors.Is(err, errBindingUnreadable) || api.postCount() != 1 {
		t.Fatalf("unreadable store posted or adopted: posts=%d err=%v", api.postCount(), err)
	}
	observed, err := blind.Inspect(ctx, req)
	if err == nil || !errors.Is(err, errBindingUnreadable) || observed.Result == operation.ResourcePresent ||
		observed.Result == operation.OperationNotAppliedFinal {
		t.Fatalf("unreadable store inspect %+v %v", observed, err)
	}

	fresh, freshAPI, freshServer := newKueueAdapterFixtureWithStore(t, &kueueAPI{}, failingBindingStore{})
	defer freshServer.Close()
	if _, err := fresh.Apply(ctx, req); !errors.Is(err, errBindingUnreadable) || freshAPI.postCount() != 0 || freshAPI.deleteCount() != 0 {
		t.Fatalf("unreadable store created a Job: posts=%d deletes=%d err=%v", freshAPI.postCount(), freshAPI.deleteCount(), err)
	}
}

func TestServiceAccountJobAdapterInjectsTrustedBindingStore(t *testing.T) {
	server, _, _ := serviceAccountTestServer(t)
	defer server.Close()
	ca, token := serviceAccountTestFiles(t, server)
	bindings := NewMemoryKueueJobBindingStore()

	adapter, err := NewServiceAccountKueueJobAdapterWithBindings(
		server.URL, ca, token, "tenant-a", bindings,
	)
	if err != nil || adapter.bindings != bindings {
		t.Fatalf("shared trusted binding not injected: adapter=%v err=%v", adapter != nil, err)
	}
	if _, err := NewServiceAccountKueueJobAdapterWithBindings(
		server.URL, ca, token, "tenant-a", nil,
	); !errors.Is(err, ErrInvalidKueueObservation) {
		t.Fatalf("nil binding accepted for ServiceAccount writer: %v", err)
	}
}
