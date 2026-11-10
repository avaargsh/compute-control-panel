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

const clearedWorkloadList = `{"metadata":{"resourceVersion":"70"},"items":[]}`
const clearedClusterQueue = `{"metadata":{"name":"cq-a","uid":"cluster-uid-a","resourceVersion":"65"},"status":{"reservingWorkloads":0,"admittedWorkloads":0}}`

func previousReservedQueue() KueueQueueBindingEvidence {
	return KueueQueueBindingEvidence{
		Job: KueueJobEvidence{
			Job:              KubernetesResourceRef{Namespace: "tenant-a", Name: "job-a", UID: "job-uid-a", ResourceVersion: "11"},
			KueueWorkload:    KubernetesResourceRef{Namespace: "tenant-a", Name: "kueue-a", UID: "kueue-uid-a", ResourceVersion: "22"},
			KueueOwnerJobUID: "job-uid-a", KueueClusterQueue: "cq-a", KueueQuotaReserved: true,
			KueueQuotaCondition: KueueConditionEvidence{Status: "True"},
		},
		Stage:        KueueAdmitted,
		LocalQueue:   KubernetesResourceRef{Namespace: "tenant-a", Name: "lq-a", UID: "local-uid-a", ResourceVersion: "51"},
		ClusterQueue: KubernetesResourceRef{Name: "cq-a", UID: "cluster-uid-a", ResourceVersion: "61"},
	}
}

// These fixtures prove only API-object observations, not Kueue finality,
// authorized release, physical device deallocation or cost-liability closeout.
func TestKueueQuotaClearedFailsClosedOnPartialOrReusedObjects(t *testing.T) {
	workloadForOldJob := `{"metadata":{"namespace":"tenant-a","name":"kueue-a","uid":"kueue-uid-a","resourceVersion":"72","ownerReferences":[{"apiVersion":"batch/v1","kind":"Job","uid":"job-uid-a","controller":true}]}}`
	cases := []struct {
		name     string
		firstWL  string
		secondWL string
		firstLQ  string
		secondLQ string
		firstCQ  string
		secondCQ string
		status   int
		change   func(*KueueQueueBindingEvidence)
		valid    bool
	}{
		{name: "stable absent Workload and zero ClusterQueue", valid: true},
		{name: "old Workload still exists", firstWL: strings.Replace(clearedWorkloadList, "[]", "["+workloadForOldJob+"]", 1)},
		{name: "successor Workload reused exact name", firstWL: strings.Replace(clearedWorkloadList, "[]", `[{"metadata":{"namespace":"tenant-a","name":"kueue-a","uid":"successor","resourceVersion":"72"}}]`, 1)},
		{name: "another Workload still owned by old Job", firstWL: strings.Replace(clearedWorkloadList, "[]", `[{"metadata":{"namespace":"tenant-a","name":"replacement","uid":"new-uid","resourceVersion":"72","ownerReferences":[{"apiVersion":"batch/v1","kind":"Job","uid":"job-uid-a","controller":true}]} }]`, 1)},
		{name: "list paginated", firstWL: strings.Replace(clearedWorkloadList, `"resourceVersion":"70"`, `"resourceVersion":"70","continue":"next"`, 1)},
		{name: "list missing resourceVersion", firstWL: strings.Replace(clearedWorkloadList, `"resourceVersion":"70"`, `"resourceVersion":""`, 1)},
		{name: "list version changed on second read", secondWL: strings.Replace(clearedWorkloadList, `"resourceVersion":"70"`, `"resourceVersion":"71"`, 1)},
		{name: "old Workload returns on second list", secondWL: strings.Replace(clearedWorkloadList, "[]", "["+workloadForOldJob+"]", 1)},
		{name: "ClusterQueue still reserves workloads", firstCQ: strings.Replace(clearedClusterQueue, `"reservingWorkloads":0`, `"reservingWorkloads":1`, 1)},
		{name: "ClusterQueue still admits workloads", firstCQ: strings.Replace(clearedClusterQueue, `"admittedWorkloads":0`, `"admittedWorkloads":1`, 1)},
		{name: "ClusterQueue counters unavailable", firstCQ: strings.Replace(clearedClusterQueue, `"reservingWorkloads":0,"admittedWorkloads":0`, "", 1)},
		{name: "ClusterQueue name reused with new UID", firstCQ: strings.Replace(clearedClusterQueue, "cluster-uid-a", "successor", 1)},
		{name: "ClusterQueue RV changed on second read", secondCQ: strings.Replace(clearedClusterQueue, `"resourceVersion":"65"`, `"resourceVersion":"66"`, 1)},
		{name: "LocalQueue UID changed", firstLQ: strings.Replace(queueLocalJSON, "local-uid-a", "successor", 1)},
		{name: "LocalQueue rerouted to a foreign ClusterQueue", firstLQ: strings.Replace(queueLocalJSON, `"clusterQueue":"cq-a"`, `"clusterQueue":"cq-b"`, 1)},
		{name: "LocalQueue RV changed on second read", secondLQ: strings.Replace(queueLocalJSON, `"resourceVersion":"51"`, `"resourceVersion":"52"`, 1)},
		{name: "ClusterQueue read forbidden", status: http.StatusForbidden},
		{name: "previous queue had no quota proof", change: func(p *KueueQueueBindingEvidence) { p.Job.KueueQuotaReserved = false }},
		{name: "previous Workload UID absent", change: func(p *KueueQueueBindingEvidence) { p.Job.KueueWorkload.UID = "" }},
		{name: "previous ClusterQueue scope forged", change: func(p *KueueQueueBindingEvidence) { p.ClusterQueue.Namespace = "tenant-a" }},
		{name: "untrusted LocalQueue path", change: func(p *KueueQueueBindingEvidence) { p.LocalQueue.Name = "../sibling" }},
		{name: "untrusted namespace path", change: func(p *KueueQueueBindingEvidence) {
			p.Job.Job.Namespace = "../other"
			p.Job.KueueWorkload.Namespace = "../other"
			p.LocalQueue.Namespace = "../other"
		}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			firstWL, secondWL := clearedWorkloadList, clearedWorkloadList
			firstLQ, secondLQ := queueLocalJSON, queueLocalJSON
			firstCQ, secondCQ := clearedClusterQueue, clearedClusterQueue
			if tc.firstWL != "" {
				firstWL = tc.firstWL
			}
			if tc.secondWL != "" {
				secondWL = tc.secondWL
			}
			if tc.firstLQ != "" {
				firstLQ = tc.firstLQ
			}
			if tc.secondLQ != "" {
				secondLQ = tc.secondLQ
			}
			if tc.firstCQ != "" {
				firstCQ = tc.firstCQ
			}
			if tc.secondCQ != "" {
				secondCQ = tc.secondCQ
			}
			var wlReads, lqReads, cqReads, writes atomic.Int32
			api := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.Method != http.MethodGet {
					writes.Add(1)
					http.Error(w, "read only", http.StatusMethodNotAllowed)
					return
				}
				w.Header().Set("Content-Type", "application/json")
				switch r.URL.RequestURI() {
				case "/apis/kueue.x-k8s.io/v1beta1/namespaces/tenant-a/workloads":
					if wlReads.Add(1) == 1 {
						fmt.Fprint(w, firstWL)
					} else {
						fmt.Fprint(w, secondWL)
					}
				case "/apis/kueue.x-k8s.io/v1beta1/namespaces/tenant-a/localqueues/lq-a":
					if lqReads.Add(1) == 1 {
						fmt.Fprint(w, firstLQ)
					} else {
						fmt.Fprint(w, secondLQ)
					}
				case "/apis/kueue.x-k8s.io/v1beta1/clusterqueues/cq-a":
					if tc.status != 0 {
						http.Error(w, "denied", tc.status)
						return
					}
					if cqReads.Add(1) == 1 {
						fmt.Fprint(w, firstCQ)
					} else {
						fmt.Fprint(w, secondCQ)
					}
				default:
					t.Errorf("unexpected Kubernetes API %s %s", r.Method, r.URL)
					http.NotFound(w, r)
				}
			}))
			defer api.Close()
			client := api.Client()
			client.Timeout = 3 * time.Second
			reader, err := NewKubernetesObservationReader(api.URL, client)
			if err != nil {
				t.Fatal(err)
			}
			prior := previousReservedQueue()
			if tc.change != nil {
				tc.change(&prior)
			}
			got, err := reader.ObserveKueueQuotaCleared(context.Background(), prior)
			if tc.valid {
				if err != nil || got.PreviousWorkload != prior.Job.KueueWorkload ||
					got.LocalQueue.UID != "local-uid-a" || got.ClusterQueue.UID != "cluster-uid-a" ||
					got.WorkloadListResourceVersion != "70" ||
					got.ClusterQueue.ResourceVersion != "65" ||
					wlReads.Load() != 2 || lqReads.Load() != 2 || cqReads.Load() != 2 {
					t.Fatalf("no bounded cleared-quota snapshot: %+v err=%v gets=%d/%d/%d", got, err, wlReads.Load(), lqReads.Load(), cqReads.Load())
				}
			} else if err == nil || got != (KueueQuotaClearedObservation{}) {
				t.Fatalf("untrusted quota clear promoted: %+v err=%v", got, err)
			}
			if writes.Load() != 0 {
				t.Fatal("observer mutated Kubernetes")
			}
			if tc.change != nil && wlReads.Load()+lqReads.Load()+cqReads.Load() != 0 {
				t.Fatal("invalid previous evidence caused API reads")
			}
		})
	}
}
