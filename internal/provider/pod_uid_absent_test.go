package provider

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

const absentPodList = `{"apiVersion":"v1","kind":"PodList","metadata":{"resourceVersion":"300"},"items":[]}`
const priorPodJSON = `{"metadata":{"namespace":"tenant-a","name":"gpu-pod-a","uid":"pod-uid-a","resourceVersion":"222"}}`

// List snapshots show only Kubernetes API object absence. They are not
// kubelet/driver deallocation, release authorization or accounting finality.
func TestObservePodUIDAbsentRejectsIncompleteAndReusedEvidence(t *testing.T) {
	tests := []struct {
		name   string
		first  string
		second string
		status int
		mutate func(*KubernetesResourceRef)
		valid  bool
	}{
		{name: "stable complete PodList without old UID", valid: true},
		{name: "prior Pod remains", first: strings.Replace(absentPodList, "[]", "["+priorPodJSON+"]", 1)},
		{name: "prior Pod reappears in second list", second: strings.Replace(absentPodList, "[]", "["+priorPodJSON+"]", 1)},
		{name: "same-name successor Pod", first: strings.Replace(absentPodList, "[]", `[{"metadata":{"namespace":"tenant-a","name":"gpu-pod-a","uid":"successor-uid","resourceVersion":"305"}}]`, 1)},
		{name: "old UID reused under another name", first: strings.Replace(absentPodList, "[]", `[{"metadata":{"namespace":"tenant-a","name":"other-pod","uid":"pod-uid-a","resourceVersion":"305"}}]`, 1)},
		{name: "wrong API kind", first: strings.Replace(absentPodList, "PodList", "ServiceList", 1)},
		{name: "missing list resourceVersion", first: strings.Replace(absentPodList, `"resourceVersion":"300"`, `"resourceVersion":""`, 1)},
		{name: "paginated list", first: strings.Replace(absentPodList, `"resourceVersion":"300"`, `"resourceVersion":"300","continue":"next"`, 1)},
		{name: "snapshot version drift", second: strings.Replace(absentPodList, `"resourceVersion":"300"`, `"resourceVersion":"301"`, 1)},
		{name: "snapshot items drift", second: strings.Replace(absentPodList, "[]", `[{"metadata":{"namespace":"tenant-a","name":"new-pod","uid":"new-uid","resourceVersion":"305"}}]`, 1)},
		{name: "malformed list item", first: strings.Replace(absentPodList, "[]", `[{"metadata":{"name":"other-pod","uid":"new-uid"}}]`, 1)},
		{name: "foreign namespace in namespace-scoped list", first: strings.Replace(absentPodList, "[]", `[{"metadata":{"namespace":"tenant-b","name":"other-pod","uid":"new-uid","resourceVersion":"305"}}]`, 1)},
		{name: "API forbidden", status: http.StatusForbidden},
		{name: "API unavailable", status: http.StatusServiceUnavailable},
		{name: "untrusted Pod namespace", mutate: func(ref *KubernetesResourceRef) { ref.Namespace = "../tenant-b" }},
		{name: "prior Pod UID missing", mutate: func(ref *KubernetesResourceRef) { ref.UID = "" }},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			var reads, writes atomic.Int32
			api := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.Method != http.MethodGet {
					writes.Add(1)
					http.Error(w, "read only", http.StatusMethodNotAllowed)
					return
				}
				if r.URL.RequestURI() != "/api/v1/namespaces/tenant-a/pods" {
					t.Errorf("unexpected API request: %s", r.URL)
					http.NotFound(w, r)
					return
				}
				n := reads.Add(1)
				if tc.status != 0 {
					http.Error(w, "unavailable", tc.status)
					return
				}
				w.Header().Set("Content-Type", "application/json")
				body := absentPodList
				if n == 1 && tc.first != "" {
					body = tc.first
				}
				if n == 2 && tc.second != "" {
					body = tc.second
				}
				_, _ = fmt.Fprint(w, body)
			}))
			defer api.Close()
			client := api.Client()
			client.Timeout = 3 * time.Second
			reader, err := NewKubernetesObservationReader(api.URL, client)
			if err != nil {
				t.Fatal(err)
			}
			prior := releaseCandidateFixture().PriorQueue.Job.Pod
			if tc.mutate != nil {
				tc.mutate(&prior)
			}
			got, err := reader.ObservePodUIDAbsent(context.Background(), prior)
			if tc.valid {
				if err != nil || got.PreviousPod != prior ||
					got.PodListResourceVersion != "300" || reads.Load() != 2 {
					t.Fatalf("stable Pod absence not observed: %+v err=%v reads=%d", got, err, reads.Load())
				}
			} else if err == nil || got != (KubernetesPodAbsentObservation{}) {
				t.Fatalf("unsafe Pod absence accepted: %+v err=%v", got, err)
			}
			if writes.Load() != 0 {
				t.Fatalf("observer issued %d Kubernetes writes", writes.Load())
			}
			if tc.mutate != nil && reads.Load() != 0 {
				t.Fatalf("invalid Pod identity caused Kubernetes reads: %d", reads.Load())
			}
		})
	}
}
