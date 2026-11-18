package provider

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

func TestObservePairedPDAdmission(t *testing.T) {
	jobset := `{"metadata":{"name":"pd-a","namespace":"tenant-a","uid":"js-uid","resourceVersion":"11","generation":2,"labels":{"kueue.x-k8s.io/queue-name":"local-a"},"annotations":{"compute-control-panel.io/tenant-id":"tenant-a","compute-control-panel.io/workload-id":"w-a","compute-control-panel.io/desired-generation":"2","compute-control-panel.io/pd-intent-id":"` + strings.Repeat("a", 64) + `"}},"spec":{"suspend":false,"replicatedJobs":[{"name":"prefill","replicas":2,"template":{"spec":{"parallelism":1,"completions":1}}},{"name":"decode","replicas":3,"template":{"spec":{"parallelism":1,"completions":1}}}]}}`
	workload := `{"metadata":{"name":"wl-a","namespace":"tenant-a","uid":"wl-uid","resourceVersion":"22","generation":3,"ownerReferences":[{"apiVersion":"jobset.x-k8s.io/v1alpha2","kind":"JobSet","uid":"js-uid","controller":true}]},"spec":{"queueName":"local-a","podSets":[{"name":"prefill","count":2},{"name":"decode","count":3}]},"status":{"admission":{"clusterQueue":"cq-a","podSetAssignments":[{"name":"prefill","count":2,"flavors":{"example.com/gpu":"gpu"},"resourceUsage":{"example.com/gpu":"4"}},{"name":"decode","count":3,"flavors":{"example.com/gpu":"gpu"},"resourceUsage":{"example.com/gpu":"3"}}]},"conditions":[{"type":"Admitted","status":"True","observedGeneration":3},{"type":"QuotaReserved","status":"True","observedGeneration":3}]}}`
	q := PDJobSetQuery{TenantID: "tenant-a", WorkloadID: "w-a", IntentID: strings.Repeat("a", 64), Generation: 2, Namespace: "tenant-a", JobSetName: "pd-a", JobSetUID: "js-uid", KueueName: "wl-a", KueueUID: "wl-uid", LocalQueue: "local-a", ClusterQueue: "cq-a", GPUResource: "example.com/gpu", Prefill: PDPhaseQuota{Replicas: 2, GPUsPerReplica: 2}, Decode: PDPhaseQuota{Replicas: 3, GPUsPerReplica: 1}}
	for _, tc := range []struct {
		name, job, wl string
		drift         bool
		ok            bool
	}{
		{"paired", jobset, workload, false, true},
		{"wrong owner", jobset, strings.Replace(workload, "\"uid\":\"js-uid\"", "\"uid\":\"other\"", 1), false, false},
		{"partial quota", jobset, strings.Replace(workload, "\"example.com/gpu\":\"4\"", "\"example.com/gpu\":\"2\"", 1), false, false},
		{"pending", jobset, strings.Replace(workload, "\"type\":\"Admitted\",\"status\":\"True\"", "\"type\":\"Admitted\",\"status\":\"False\"", 1), false, false},
		{"stale generation", jobset, strings.ReplaceAll(workload, "\"observedGeneration\":3", "\"observedGeneration\":2"), false, false},
		{"evicted", jobset, strings.Replace(workload, "\"conditions\":[", "\"conditions\":[{\"type\":\"Evicted\",\"status\":\"True\"},", 1), false, false},
		{"wrong phase count", strings.Replace(jobset, "\"replicas\":2", "\"replicas\":1", 1), workload, false, false},
		{"suspended", strings.Replace(jobset, "\"suspend\":false", "\"suspend\":true", 1), workload, false, false},
		{"drift", jobset, workload, true, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			reads := 0
			server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
				if req.Method != http.MethodGet {
					t.Error("unexpected write")
				}
				switch req.URL.Path {
				case "/apis/jobset.x-k8s.io/v1alpha2/namespaces/tenant-a/jobsets/pd-a":
					_, _ = w.Write([]byte(tc.job))
				case "/apis/kueue.x-k8s.io/v1beta1/namespaces/tenant-a/workloads/wl-a":
					reads++
					body := tc.wl
					if tc.drift && reads > 1 {
						body = strings.Replace(body, "\"resourceVersion\":\"22\"", "\"resourceVersion\":\"23\"", 1)
					}
					_, _ = w.Write([]byte(body))
				default:
					http.NotFound(w, req)
				}
			}))
			defer server.Close()
			client := server.Client()
			client.Timeout = time.Second
			reader, err := NewKubernetesObservationReader(server.URL, client)
			if err != nil {
				t.Fatal(err)
			}
			got, err := reader.ObservePairedPDAdmission(context.Background(), q)
			if (err == nil) != tc.ok {
				t.Fatalf("evidence=%+v err=%v", got, err)
			}
			if tc.ok {
				if got.JobSet.UID != q.JobSetUID || got.Workload.UID != q.KueueUID || got.ReservedGPUCount != 7 || got.Stage != "PAIRED_QUOTA_ADMITTED" || got.PhysicalReady {
					t.Fatalf("bad admitted projection: %+v", got)
				}
				if _, err := json.Marshal(got); err != nil {
					t.Fatal(err)
				}
			} else if got != (PDJobSetAdmission{}) {
				t.Fatal("failure returned evidence")
			}
		})
	}
}
