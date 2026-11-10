package recovery_test

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/avaargsh/compute-control-panel/internal/operation"
	"github.com/avaargsh/compute-control-panel/internal/provider"
	"github.com/avaargsh/compute-control-panel/internal/recovery"
	"github.com/avaargsh/compute-control-panel/internal/store/postgres"
)

const (
	journalKueueNamespace = "ccp-recovery-test"
	journalKueueJobUID    = "job-uid-created"
	journalKueueSource    = "kubernetes-https-readonly-unattested"
	journalKueueVersion   = "kueue-v1beta1-single-pod-v1"
)

// journalKueueAPI simulates only the Kubernetes API surface for a suspended,
// unqueued Job. The real Postgres repository, recovery Worker, observer,
// adapter and binding store are used on both sides of the restart boundary.
// This is not a kind/Kueue/GPU acceptance test.
type journalKueueAPI struct {
	mu            sync.Mutex
	job           map[string]any
	template      []byte
	name          string
	posts         int
	deletes       int
	gets          int
	loseCreateACK bool
}

func (s *journalKueueAPI) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	const jobs = "/apis/batch/v1/namespaces/" + journalKueueNamespace + "/jobs"
	const workloads = "/apis/kueue.x-k8s.io/v1beta1/namespaces/" + journalKueueNamespace + "/workloads"
	switch {
	case r.Method == http.MethodPost && r.URL.Path == jobs:
		body, err := io.ReadAll(io.LimitReader(r.Body, 1<<20))
		var posted map[string]any
		if err != nil || json.Unmarshal(body, &posted) != nil {
			http.Error(w, "invalid Job", http.StatusBadRequest)
			return
		}
		meta, ok := posted["metadata"].(map[string]any)
		if !ok || meta["name"] == "" || meta["namespace"] != journalKueueNamespace {
			http.Error(w, "invalid metadata", http.StatusBadRequest)
			return
		}
		s.mu.Lock()
		s.posts++
		if s.job != nil {
			s.mu.Unlock()
			http.Error(w, "already exists", http.StatusConflict)
			return
		}
		s.name, _ = meta["name"].(string)
		meta["uid"] = journalKueueJobUID
		meta["resourceVersion"] = "101"
		s.job = posted
		lost := s.loseCreateACK
		s.mu.Unlock()
		w.WriteHeader(http.StatusCreated)
		if lost {
			_, _ = io.WriteString(w, "{")
			return
		}
		_ = json.NewEncoder(w).Encode(posted)
	case r.Method == http.MethodGet && strings.HasPrefix(r.URL.Path, jobs+"/"):
		s.mu.Lock()
		s.gets++
		if s.job == nil || r.URL.Path != jobs+"/"+s.name {
			s.mu.Unlock()
			http.NotFound(w, r)
			return
		}
		raw, err := json.Marshal(s.job)
		s.mu.Unlock()
		if err != nil {
			http.Error(w, "marshal Job", http.StatusInternalServerError)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write(raw)
	case r.Method == http.MethodGet && r.URL.Path == workloads:
		w.Header().Set("Content-Type", "application/json")
		_, _ = io.WriteString(w, "{\"items\":[]}")
	case r.Method == http.MethodDelete && strings.HasPrefix(r.URL.Path, jobs+"/"):
		body, err := io.ReadAll(io.LimitReader(r.Body, 1<<20))
		var opts struct {
			Preconditions struct {
				UID string `json:"uid"`
			} `json:"preconditions"`
		}
		if err != nil || json.Unmarshal(body, &opts) != nil || opts.Preconditions.UID == "" {
			http.Error(w, "invalid DeleteOptions", http.StatusBadRequest)
			return
		}
		s.mu.Lock()
		s.deletes++
		live := ""
		if s.job != nil {
			live, _ = s.job["metadata"].(map[string]any)["uid"].(string)
		}
		match := s.job != nil && r.URL.Path == jobs+"/"+s.name && opts.Preconditions.UID == live
		if match {
			s.job = nil
		}
		s.mu.Unlock()
		if !match {
			if live == "" {
				http.NotFound(w, r)
				return
			}
			http.Error(w, "UID precondition failed", http.StatusConflict)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = io.WriteString(w, `{"kind":"Status","apiVersion":"v1","status":"Success"}`)
	default:
		http.Error(w, "unexpected Kubernetes endpoint", http.StatusNotFound)
	}
}

func (s *journalKueueAPI) rememberTemplate() {
	s.mu.Lock()
	defer s.mu.Unlock()
	raw, err := json.Marshal(s.job)
	if err != nil {
		s.template = nil
		return
	}
	s.template = raw
}

func (s *journalKueueAPI) restoreSuccessor(uid string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	var job map[string]any
	if err := json.Unmarshal(s.template, &job); err != nil {
		return err
	}
	meta, _ := job["metadata"].(map[string]any)
	if meta == nil {
		return errors.New("successor template has no metadata")
	}
	meta["uid"] = uid
	meta["resourceVersion"] = "103"
	s.job = job
	s.name, _ = meta["name"].(string)
	return nil
}

func (s *journalKueueAPI) jobName() string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.name
}

func (s *journalKueueAPI) liveUID() string {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.job == nil {
		return ""
	}
	uid, _ := s.job["metadata"].(map[string]any)["uid"].(string)
	return uid
}

func (s *journalKueueAPI) replaceUID(uid string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.job["metadata"].(map[string]any)["uid"] = uid
	s.job["metadata"].(map[string]any)["resourceVersion"] = "102"
}

func (s *journalKueueAPI) counts() (posts, deletes, gets int) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.posts, s.deletes, s.gets
}

func kueueJournalRequest(op operation.Operation) provider.Request {
	return provider.Request{
		OperationID: op.ID, TenantID: op.TenantID, WorkloadID: op.WorkloadID,
		ProviderID: op.ProviderID, IdempotencyKey: op.IdempotencyKey,
		RequestDigest: op.RequestDigest, DesiredGeneration: op.Generation,
		Kind: op.Kind, TargetResourceID: op.TargetResourceID, TargetResourceUID: op.TargetResourceUID,
	}
}

// This covers the missing conjunction: the original Kubernetes side effect is
// issued by an adapter with a real PG UID binding, while a separate journal
// repository and binding connection run the actual fenced Inspect-first Worker.
// A Job existing by matching annotations without a pin is never sufficient.
func TestKueuePostgresJournalInspectFirstRecovery(t *testing.T) {
	for _, tc := range []struct {
		name          string
		loseCreateACK bool
		successorUID  string
		wantStatus    operation.Status
		wantResult    operation.ObservationResult
		wantReason    string
	}{
		{
			name:       "durable original UID resolves UNKNOWN without a second POST",
			wantStatus: operation.Succeeded, wantResult: operation.ResourcePresent,
		},
		{
			name:          "lost CREATE ACK leaves durable UNKNOWN without adopting an unbound UID",
			loseCreateACK: true, wantStatus: operation.Unknown, wantResult: operation.OperationInProgress,
		},
		{
			name:         "same-name successor becomes manual review without rewriting original UID",
			successorUID: "job-uid-successor", wantStatus: operation.ManualReview,
			wantResult: operation.ObservationConflict, wantReason: "JOB_UID_REPLACED",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			db, repo, original := setupUnknownWithProvider(t, false)
			ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
			defer cancel()
			_, created, err := repo.RegisterProvider(ctx, postgres.ProviderRegistration{
				ProviderID: original.ProviderID, ClusterScope: journalKueueNamespace,
				AdapterSource: journalKueueSource, AdapterVersion: journalKueueVersion,
				CapabilityDigest: "sha256:" + strings.Repeat("a", 64),
			})
			if err != nil || !created {
				t.Fatalf("register durable Provider: created=%v err=%v", created, err)
			}
			api := &journalKueueAPI{loseCreateACK: tc.loseCreateACK}
			server := httptest.NewTLSServer(api)
			defer server.Close()
			client := server.Client()
			client.Timeout = 3 * time.Second

			bindings, err := provider.NewPostgresKueueJobBindingStore(db)
			if err != nil {
				t.Fatal(err)
			}
			writer, err := provider.NewKueueJobAdapterWithBindings(server.URL, journalKueueNamespace, client, bindings)
			if err != nil {
				t.Fatal(err)
			}
			req := kueueJournalRequest(original)
			if _, err := writer.Apply(ctx, req); tc.loseCreateACK {
				if !errors.Is(err, provider.ErrAcknowledgementLost) {
					t.Fatalf("lost CREATE ACK was not ambiguous: %v", err)
				}
			} else if err != nil {
				t.Fatalf("create original suspended Job: %v", err)
			}
			if tc.successorUID != "" {
				api.replaceUID(tc.successorUID)
			}

			// Reopen both the journal and the binding database. No memory pin,
			// acknowledgement or provider object crosses this boundary.
			freshDB, err := sql.Open("pgx", os.Getenv("DATABASE_URL"))
			if err != nil {
				t.Fatal(err)
			}
			defer freshDB.Close()
			freshJournal, err := postgres.NewRepository(freshDB)
			if err != nil {
				t.Fatal(err)
			}
			freshBindings, err := provider.NewPostgresKueueJobBindingStore(freshDB)
			if err != nil {
				t.Fatal(err)
			}
			inspector, err := provider.NewKueueJobAdapterWithBindings(server.URL, journalKueueNamespace, client, freshBindings)
			if err != nil {
				t.Fatal(err)
			}
			worker, err := recovery.NewWorker(freshJournal, map[string]recovery.RegisteredProvider{
				original.ProviderID: {
					Adapter: inspectOnly{Adapter: inspector}, ClusterScope: journalKueueNamespace,
					Source: journalKueueSource, AdapterVersion: journalKueueVersion,
					CapabilityDigest: "sha256:" + strings.Repeat("a", 64),
				},
			})
			if err != nil {
				t.Fatal(err)
			}
			got, err := worker.RecoverUnknown(ctx, original.TenantID, original.ID, "kueue-recovery", time.Minute)
			if err != nil || got.Status != tc.wantStatus || got.FenceToken <= original.FenceToken ||
				got.Version <= original.Version {
				t.Fatalf("fenced Kueue journal recovery: got=%+v err=%v", got, err)
			}
			if tc.wantStatus == operation.Succeeded {
				if got.ProviderResourceUID != journalKueueJobUID ||
					!strings.HasPrefix(got.ProviderResourceID, journalKueueNamespace+"/ccp-") {
					t.Fatalf("recovery accepted unexpected Job identity: %+v", got)
				}
			} else if got.ProviderResourceUID != "" || got.ProviderResourceID != "" ||
				got.ManualReviewCode != tc.wantReason {
				t.Fatalf("inconclusive Job identity became final: %+v", got)
			}
			persisted, err := repo.GetOperation(ctx, original.TenantID, original.ID)
			if err != nil || persisted.Status != got.Status || persisted.FenceToken != got.FenceToken {
				t.Fatalf("new process result did not persist: %+v err=%v", persisted, err)
			}
			var obsCount, eventCount int
			var result, source, adapterVersion string
			if err := db.QueryRowContext(ctx, "SELECT count(*) FROM public.compute_resource_observations WHERE tenant_id=$1 AND compute_operation_id=$2", original.TenantID, original.ID).Scan(&obsCount); err != nil {
				t.Fatal(err)
			}
			if err := db.QueryRowContext(ctx, "SELECT operation_result, source, adapter_version FROM public.compute_resource_observations WHERE tenant_id=$1 AND compute_operation_id=$2", original.TenantID, original.ID).Scan(&result, &source, &adapterVersion); err != nil {
				t.Fatal(err)
			}
			if err := db.QueryRowContext(ctx, "SELECT count(*) FROM public.compute_outbox_events WHERE tenant_id=$1 AND aggregate_type='compute_operation' AND aggregate_id=$2", original.TenantID, original.ID).Scan(&eventCount); err != nil {
				t.Fatal(err)
			}
			if obsCount != 1 || eventCount != 2 || result != string(tc.wantResult) ||
				source != journalKueueSource || adapterVersion != journalKueueVersion {
				t.Fatalf("inspection not atomically journaled: observations=%d events=%d result=%s source=%s version=%s",
					obsCount, eventCount, result, source, adapterVersion)
			}
			posts, deletes, gets := api.counts()
			if posts != 1 || deletes != 0 || gets < 2 {
				t.Fatalf("recovery dispatched instead of inspecting: posts=%d deletes=%d gets=%d", posts, deletes, gets)
			}
		})
	}
}

// TestKueuePostgresJournalDeleteInspectFirstRecovery is the DELETE counterpart
// of the CREATE journal recovery above. Process A deletes through the adapter
// and leaves the ComputeOperation UNKNOWN. Process B is a new repository and
// adapter and may only Inspect. TARGET_ABSENT is not Allocation RELEASED.
func TestKueuePostgresJournalDeleteInspectFirstRecovery(t *testing.T) {
	for _, tc := range []struct {
		name         string
		successorUID string
		unreachable  bool
		wantStatus   operation.Status
		wantResult   operation.ObservationResult
		wantReason   string
	}{
		{
			name:       "acknowledged delete resolves UNKNOWN to exact-UID TARGET_ABSENT",
			wantStatus: operation.Succeeded, wantResult: operation.TargetAbsent,
		},
		{
			name:         "same-name successor stays MANUAL_REVIEW and is not deleted",
			successorUID: "job-uid-successor",
			wantStatus:   operation.ManualReview, wantResult: operation.ObservationConflict,
			wantReason: "JOB_UID_REPLACED",
		},
		{
			name: "unreachable DSN fails closed without a second DELETE", unreachable: true,
			wantStatus: operation.Unknown,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			db, repo, original := setupUnknownWithProvider(t, false)
			ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
			defer cancel()
			_, created, err := repo.RegisterProvider(ctx, postgres.ProviderRegistration{
				ProviderID: original.ProviderID, ClusterScope: journalKueueNamespace,
				AdapterSource: journalKueueSource, AdapterVersion: journalKueueVersion,
				CapabilityDigest: "sha256:" + strings.Repeat("a", 64),
			})
			if err != nil || !created {
				t.Fatalf("register durable Provider: created=%v err=%v", created, err)
			}
			api := &journalKueueAPI{}
			server := httptest.NewTLSServer(api)
			defer server.Close()
			client := server.Client()
			client.Timeout = 3 * time.Second
			bindings, err := provider.NewPostgresKueueJobBindingStore(db)
			if err != nil {
				t.Fatal(err)
			}
			writer, err := provider.NewKueueJobAdapterWithBindings(server.URL, journalKueueNamespace, client, bindings)
			if err != nil {
				t.Fatal(err)
			}
			if _, err := writer.Apply(ctx, kueueJournalRequest(original)); err != nil {
				t.Fatalf("create suspended Job: %v", err)
			}
			api.rememberTemplate()
			if api.jobName() == "" {
				t.Fatal("create did not record a Job name")
			}
			remove := provider.Request{
				OperationID: "operation-delete-" + uniqueID(t), TenantID: original.TenantID,
				WorkloadID: original.WorkloadID, ProviderID: original.ProviderID,
				IdempotencyKey:    "delete-idem-" + uniqueID(t),
				RequestDigest:     "sha256:" + strings.Repeat("b", 64),
				DesiredGeneration: original.Generation, Kind: operation.Delete,
				TargetResourceID:  journalKueueNamespace + "/" + api.jobName(),
				TargetResourceUID: journalKueueJobUID,
			}
			pending, created, err := repo.CreateOperation(ctx, operation.Operation{
				ID: remove.OperationID, TenantID: remove.TenantID, WorkloadID: remove.WorkloadID,
				ProviderID: remove.ProviderID, IdempotencyKey: remove.IdempotencyKey,
				RequestDigest: remove.RequestDigest, Generation: remove.DesiredGeneration,
				Version: 1, Kind: operation.Delete, Status: operation.Pending,
				TargetResourceID: remove.TargetResourceID, TargetResourceUID: remove.TargetResourceUID,
			}, "delete-event-"+uniqueID(t), "delete-corr")
			if err != nil || !created {
				t.Fatalf("record DELETE: created=%v err=%v", created, err)
			}
			claimed, err := repo.ClaimOperation(ctx, remove.TenantID, remove.OperationID, pending.Version, "delete-writer", time.Minute)
			if err != nil {
				t.Fatal(err)
			}
			started, err := repo.StartOperation(ctx, remove.TenantID, remove.OperationID, claimed.Version, "delete-writer", claimed.FenceToken)
			if err != nil {
				t.Fatal(err)
			}
			if _, err := writer.Apply(ctx, remove); err != nil {
				t.Fatalf("uid-fenced delete: %v", err)
			}
			tomb, ok, err := bindings.GetDeleteTombstone(journalKueueNamespace, remove)
			if err != nil || !ok || !tomb.Acknowledged || tomb.UID != journalKueueJobUID {
				t.Fatalf("acknowledged tombstone missing: %+v present=%t err=%v", tomb, ok, err)
			}
			unknown, err := repo.MarkOperationUnknown(ctx, remove.TenantID, remove.OperationID, started.Version, "delete-writer", started.FenceToken)
			if err != nil || unknown.Status != operation.Unknown || unknown.ProviderResourceUID != "" {
				t.Fatalf("DELETE was not UNKNOWN: %+v err=%v", unknown, err)
			}
			if tc.successorUID != "" {
				if err := api.restoreSuccessor(tc.successorUID); err != nil {
					t.Fatal(err)
				}
			}
			_, deletesBefore, _ := api.counts()

			if tc.unreachable {
				bad, err := sql.Open("pgx", "postgres://postgres:compute-ci@127.0.0.1:1/postgres?sslmode=disable&connect_timeout=2")
				if err != nil {
					t.Fatal(err)
				}
				defer bad.Close()
				badRepo, err := postgres.NewRepository(bad)
				if err != nil {
					t.Fatal(err)
				}
				badWorker, err := recovery.NewWorker(badRepo, map[string]recovery.RegisteredProvider{
					original.ProviderID: {
						Adapter: inspectOnly{Adapter: writer}, ClusterScope: journalKueueNamespace,
						Source: journalKueueSource, AdapterVersion: journalKueueVersion,
						CapabilityDigest: "sha256:" + strings.Repeat("a", 64),
					},
				})
				if err != nil {
					t.Fatal(err)
				}
				badCtx, badCancel := context.WithTimeout(context.Background(), 8*time.Second)
				defer badCancel()
				if _, err := badWorker.RecoverUnknown(badCtx, remove.TenantID, remove.OperationID, "kueue-delete-recovery", time.Minute); err == nil {
					t.Fatal("unreachable DSN recovered a DELETE")
				}
				persisted, err := repo.GetOperation(ctx, remove.TenantID, remove.OperationID)
				if err != nil || persisted.Status != operation.Unknown || persisted.ProviderResourceUID != "" ||
					persisted.TargetResourceUID != journalKueueJobUID {
					t.Fatalf("unreachable DSN mutated the DELETE journal: %+v err=%v", persisted, err)
				}
				var rows int
				if err := db.QueryRowContext(ctx, "SELECT count(*) FROM public.compute_resource_observations WHERE tenant_id=$1 AND compute_operation_id=$2", remove.TenantID, remove.OperationID).Scan(&rows); err != nil {
					t.Fatal(err)
				}
				_, deletes, _ := api.counts()
				if rows != 0 || deletes != deletesBefore || api.liveUID() != "" {
					t.Fatalf("unreachable DSN deleted or observed: rows=%d deletes=%d live=%q", rows, deletes, api.liveUID())
				}
				return
			}

			freshDB, err := sql.Open("pgx", os.Getenv("DATABASE_URL"))
			if err != nil {
				t.Fatal(err)
			}
			defer freshDB.Close()
			freshJournal, err := postgres.NewRepository(freshDB)
			if err != nil {
				t.Fatal(err)
			}
			freshBindings, err := provider.NewPostgresKueueJobBindingStore(freshDB)
			if err != nil {
				t.Fatal(err)
			}
			inspector, err := provider.NewKueueJobAdapterWithBindings(server.URL, journalKueueNamespace, client, freshBindings)
			if err != nil {
				t.Fatal(err)
			}
			worker, err := recovery.NewWorker(freshJournal, map[string]recovery.RegisteredProvider{
				original.ProviderID: {
					Adapter: inspectOnly{Adapter: inspector}, ClusterScope: journalKueueNamespace,
					Source: journalKueueSource, AdapterVersion: journalKueueVersion,
					CapabilityDigest: "sha256:" + strings.Repeat("a", 64),
				},
			})
			if err != nil {
				t.Fatal(err)
			}
			got, err := worker.RecoverUnknown(ctx, remove.TenantID, remove.OperationID, "kueue-delete-recovery", time.Minute)
			if err != nil || got.Status != tc.wantStatus || got.FenceToken <= unknown.FenceToken ||
				got.Version <= unknown.Version || got.ProviderResourceUID != "" || got.ProviderResourceID != "" ||
				got.TargetResourceUID != journalKueueJobUID || got.ManualReviewCode != tc.wantReason {
				t.Fatalf("fenced DELETE recovery: got=%+v err=%v", got, err)
			}
			var obsCount, eventCount int
			var result, source, adapterVersion, resourceUID, targetUID string
			if err := db.QueryRowContext(ctx, "SELECT count(*) FROM public.compute_resource_observations WHERE tenant_id=$1 AND compute_operation_id=$2", remove.TenantID, remove.OperationID).Scan(&obsCount); err != nil {
				t.Fatal(err)
			}
			if err := db.QueryRowContext(ctx, `SELECT operation_result, source, adapter_version,
				coalesce(provider_resource_uid,''), coalesce(details->>'TargetResourceUID','')
				FROM public.compute_resource_observations WHERE tenant_id=$1 AND compute_operation_id=$2`,
				remove.TenantID, remove.OperationID).Scan(&result, &source, &adapterVersion, &resourceUID, &targetUID); err != nil {
				t.Fatal(err)
			}
			if err := db.QueryRowContext(ctx, "SELECT count(*) FROM public.compute_outbox_events WHERE tenant_id=$1 AND aggregate_type='compute_operation' AND aggregate_id=$2", remove.TenantID, remove.OperationID).Scan(&eventCount); err != nil {
				t.Fatal(err)
			}
			wantTarget := ""
			if tc.wantResult == operation.TargetAbsent {
				wantTarget = journalKueueJobUID
			}
			if obsCount != 1 || eventCount != 2 || result != string(tc.wantResult) ||
				source != journalKueueSource || adapterVersion != journalKueueVersion ||
				resourceUID != "" || targetUID != wantTarget {
				t.Fatalf("DELETE inspection not journaled for the exact UID: observations=%d events=%d result=%s source=%s version=%s resource=%s target=%s",
					obsCount, eventCount, result, source, adapterVersion, resourceUID, targetUID)
			}
			_, deletes, _ := api.counts()
			if deletes != deletesBefore {
				t.Fatalf("recovery issued another DELETE: before=%d after=%d", deletesBefore, deletes)
			}
			if tc.successorUID != "" {
				if api.liveUID() != tc.successorUID {
					t.Fatalf("recovery deleted or adopted the successor: live=%q", api.liveUID())
				}
			} else if api.liveUID() != "" {
				t.Fatalf("TARGET_ABSENT left a Job in place: %q", api.liveUID())
			}
		})
	}
}
