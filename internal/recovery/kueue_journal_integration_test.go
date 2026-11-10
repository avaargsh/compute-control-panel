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
		s.mu.Lock()
		s.deletes++
		s.mu.Unlock()
		http.Error(w, "unexpected DELETE in recovery", http.StatusForbidden)
	default:
		http.Error(w, "unexpected Kubernetes endpoint", http.StatusNotFound)
	}
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
