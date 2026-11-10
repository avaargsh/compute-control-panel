package provider

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

const queueLocalJSON = `{"metadata":{"name":"lq-a","namespace":"tenant-a","uid":"local-uid-a","resourceVersion":"51"},"spec":{"clusterQueue":"cq-a"}}`
const queueClusterJSON = `{"metadata":{"name":"cq-a","uid":"cluster-uid-a","resourceVersion":"61"}}`

func pinnedQueueQuery() KueueQueueBindingQuery {
	return KueueQueueBindingQuery{
		Job: observedJobQuery(), LocalQueueName: "lq-a", LocalQueueUID: "local-uid-a",
		ClusterQueueName: "cq-a", ClusterQueueUID: "cluster-uid-a",
	}
}

// All responses are from a read-only TLS Kubernetes API fake, never an
// authenticated production Provider or real GPU device observation.
func queueBindingReader(t *testing.T, localFirst, localSecond, clusterFirst, clusterSecond, workloadList string, queueStatus int) (*KubernetesObservationReader, *atomic.Int32, func()) {
	t.Helper()
	var lqReads, cqReads atomic.Int32
	var totalReads atomic.Int32
	var wl struct {
		Items []json.RawMessage `json:"items"`
	}
	if err := json.Unmarshal([]byte(workloadList), &wl); err != nil || len(wl.Items) != 1 {
		t.Fatalf("bad workload fixture: %v", err)
	}
	server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		totalReads.Add(1)
		if r.Method != http.MethodGet {
			t.Errorf("unexpected mutating method %s", r.Method)
			http.Error(w, "read only", http.StatusMethodNotAllowed)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		switch r.URL.Path {
		case "/apis/batch/v1/namespaces/tenant-a/jobs/job-a":
			fmt.Fprint(w, testKubeJob)
		case "/apis/kueue.x-k8s.io/v1beta1/namespaces/tenant-a/workloads":
			fmt.Fprint(w, workloadList)
		case "/apis/kueue.x-k8s.io/v1beta1/namespaces/tenant-a/workloads/kueue-a":
			fmt.Fprint(w, string(wl.Items[0]))
		case "/api/v1/namespaces/tenant-a/pods":
			fmt.Fprint(w, testKubePods)
		case "/apis/kueue.x-k8s.io/v1beta1/namespaces/tenant-a/localqueues/lq-a":
			if queueStatus != 0 {
				http.Error(w, "denied", queueStatus)
				return
			}
			if lqReads.Add(1) == 1 {
				fmt.Fprint(w, localFirst)
			} else {
				fmt.Fprint(w, localSecond)
			}
		case "/apis/kueue.x-k8s.io/v1beta1/clusterqueues/cq-a":
			if cqReads.Add(1) == 1 {
				fmt.Fprint(w, clusterFirst)
			} else {
				fmt.Fprint(w, clusterSecond)
			}
		default:
			t.Errorf("unexpected Kube API GET %s", r.URL.Path)
			http.NotFound(w, r)
		}
	}))
	client := server.Client()
	client.Timeout = 3 * time.Second
	reader, err := NewKubernetesObservationReader(server.URL, client)
	if err != nil {
		server.Close()
		t.Fatal(err)
	}
	return reader, &totalReads, server.Close
}

func queueWorkloadFixture() string {
	return strings.Replace(testKueueWorkloads, `"status":{`, `"spec":{"queueName":"lq-a"},"status":{`, 1)
}

func TestKueueQueueBindingPinsLocalAndClusterUID(t *testing.T) {
	reader, reads, closeServer := queueBindingReader(t, queueLocalJSON, queueLocalJSON, queueClusterJSON, queueClusterJSON, queueWorkloadFixture(), 0)
	defer closeServer()
	got, err := reader.ObserveQueueBinding(context.Background(), pinnedQueueQuery())
	if err != nil || got.Stage != KueuePodPlaced || got.Job.KueueClusterQueue != "cq-a" ||
		got.Job.KueueWorkload.UID != "kueue-uid-a" || got.LocalQueue.UID != "local-uid-a" ||
		got.LocalQueue.ResourceVersion != "51" || got.LocalQueue.Namespace != "tenant-a" ||
		got.ClusterQueue.UID != "cluster-uid-a" || got.ClusterQueue.ResourceVersion != "61" ||
		got.ClusterQueue.Namespace != "" || reads.Load() < 9 {
		t.Fatalf("missing exact queue evidence: %+v err=%v reads=%d", got, err, reads.Load())
	}
	// Queue identity does not prove GPU assignment, Allocation READY, or release.
	if got.Stage == "READY" || got.Stage == "RELEASED" {
		t.Fatalf("queue observation promoted Allocation: %s", got.Stage)
	}
}

func TestKueueQueueBindingRejectsIncompleteOrReplacedQueue(t *testing.T) {
	tests := []struct {
		name                      string
		q                         func(*KueueQueueBindingQuery)
		firstLocal, nextLocal     string
		firstCluster, nextCluster string
		workload                  string
		status                    int
	}{
		{"missing trusted LocalQueue UID", func(q *KueueQueueBindingQuery) { q.LocalQueueUID = "" }, queueLocalJSON, queueLocalJSON, queueClusterJSON, queueClusterJSON, queueWorkloadFixture(), 0},
		{"missing trusted ClusterQueue UID", func(q *KueueQueueBindingQuery) { q.ClusterQueueUID = "" }, queueLocalJSON, queueLocalJSON, queueClusterJSON, queueClusterJSON, queueWorkloadFixture(), 0},
		{"foreign localqueue mapping", nil, strings.Replace(queueLocalJSON, `"clusterQueue":"cq-a"`, `"clusterQueue":"cq-b"`, 1), queueLocalJSON, queueClusterJSON, queueClusterJSON, queueWorkloadFixture(), 0},
		{"localqueue successor UID", nil, strings.Replace(queueLocalJSON, "local-uid-a", "local-uid-b", 1), queueLocalJSON, queueClusterJSON, queueClusterJSON, queueWorkloadFixture(), 0},
		{"clusterqueue successor UID", nil, queueLocalJSON, queueLocalJSON, strings.Replace(queueClusterJSON, "cluster-uid-a", "cluster-uid-b", 1), queueClusterJSON, queueWorkloadFixture(), 0},
		{"localqueue missing RV", nil, strings.Replace(queueLocalJSON, `"resourceVersion":"51"`, `"resourceVersion":""`, 1), queueLocalJSON, queueClusterJSON, queueClusterJSON, queueWorkloadFixture(), 0},
		{"clusterqueue missing RV", nil, queueLocalJSON, queueLocalJSON, strings.Replace(queueClusterJSON, `"resourceVersion":"61"`, `"resourceVersion":""`, 1), queueClusterJSON, queueWorkloadFixture(), 0},
		{"localqueue changed after read", nil, queueLocalJSON, strings.Replace(queueLocalJSON, `"resourceVersion":"51"`, `"resourceVersion":"52"`, 1), queueClusterJSON, queueClusterJSON, queueWorkloadFixture(), 0},
		{"clusterqueue changed after read", nil, queueLocalJSON, queueLocalJSON, queueClusterJSON, strings.Replace(queueClusterJSON, `"resourceVersion":"61"`, `"resourceVersion":"62"`, 1), queueWorkloadFixture(), 0},
		{"workload points at another LocalQueue", nil, queueLocalJSON, queueLocalJSON, queueClusterJSON, queueClusterJSON, strings.Replace(queueWorkloadFixture(), `"queueName":"lq-a"`, `"queueName":"lq-b"`, 1), 0},
		{"workload has no queueName", nil, queueLocalJSON, queueLocalJSON, queueClusterJSON, queueClusterJSON, testKueueWorkloads, 0},
		{"reserved in wrong ClusterQueue", nil, queueLocalJSON, queueLocalJSON, queueClusterJSON, queueClusterJSON, strings.Replace(queueWorkloadFixture(), `"clusterQueue":"cq-a"`, `"clusterQueue":"cq-b"`, 1), 0},
		{"queue GET forbidden", nil, queueLocalJSON, queueLocalJSON, queueClusterJSON, queueClusterJSON, queueWorkloadFixture(), http.StatusForbidden},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			reader, reads, done := queueBindingReader(t, tc.firstLocal, tc.nextLocal, tc.firstCluster, tc.nextCluster, tc.workload, tc.status)
			defer done()
			query := pinnedQueueQuery()
			if tc.q != nil {
				tc.q(&query)
			}
			got, err := reader.ObserveQueueBinding(context.Background(), query)
			if err == nil || got != (KueueQueueBindingEvidence{}) {
				t.Fatalf("invalid queue proof accepted: %+v err=%v", got, err)
			}
			if tc.q != nil && reads.Load() != 0 {
				t.Fatalf("invalid query caused API I/O: %d", reads.Load())
			}
		})
	}
}
