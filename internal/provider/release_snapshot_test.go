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

// The API fixture is not a driver or kubelet deallocation receipt. The
// production-facing observer must return no composite snapshot on any drift.
func TestReleaseKubernetesSnapshotRejectsCrossSourceDrift(t *testing.T) {
	cases := []struct {
		name       string
		afterPod   string
		afterClaim string
		afterQueue string
		// A required authority is unavailable from the very first sweep.
		initial string
		mutate  func(*KueueQueueBindingEvidence, *DRAPodClaimObservation)
		valid   bool
	}{
		{name: "two stable rounds of three structural source observations", valid: true},
		{name: "G06 Pod UID still present while other sources appear clear", initial: "pod"},
		{name: "G06 Kueue quota still reserved while other sources appear clear", initial: "quota"},
		{name: "G06 ResourceClaim still allocated while other sources appear clear", initial: "claim"},
		{name: "Pod returns between source rounds", afterPod: strings.Replace(absentPodList, "[]", "["+priorPodJSON+"]", 1)},
		{name: "Pod list resourceVersion changes between rounds", afterPod: strings.Replace(absentPodList, "300", "301", 1)},
		{name: "quota resumes between source rounds", afterQueue: strings.Replace(clearedClusterQueue, "\"reservingWorkloads\":0", "\"reservingWorkloads\":1", 1)},
		{name: "Claim status becomes allocated between rounds", afterClaim: "allocated"},
		{name: "Pod identity differs from the historical Kueue Pod", mutate: func(q *KueueQueueBindingEvidence, p *DRAPodClaimObservation) {
			p.Pod.UID = "successor"
		}},
		{name: "Pod node differs from historical placement", mutate: func(q *KueueQueueBindingEvidence, p *DRAPodClaimObservation) {
			p.NodeName = "other-node"
		}},
		{name: "Claim belongs to another Pod", mutate: func(q *KueueQueueBindingEvidence, p *DRAPodClaimObservation) {
			p.Claim.PodUID = "other-pod"
		}},
		{name: "prior Kueue stage not placed", mutate: func(q *KueueQueueBindingEvidence, p *DRAPodClaimObservation) {
			q.Stage = KueueAdmitted
		}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			var podReads, claimReads, cqReads, writes atomic.Int32
			claimCleared := clearedDRAClaimJSON(t, nil)
			claimAllocated := draModifiedFixture(t, nil)
			srv := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.Method != http.MethodGet {
					writes.Add(1)
					http.Error(w, "read-only", http.StatusMethodNotAllowed)
					return
				}
				w.Header().Set("Content-Type", "application/json")
				switch r.URL.RequestURI() {
				case "/api/v1/namespaces/tenant-a/pods":
					n := podReads.Add(1)
					value := absentPodList
					if tc.initial == "pod" {
						value = strings.Replace(absentPodList, "[]", "["+priorPodJSON+"]", 1)
					}
					if n > 2 && tc.afterPod != "" {
						value = tc.afterPod
					}
					_, _ = fmt.Fprint(w, value)
				case "/apis/kueue.x-k8s.io/v1beta1/namespaces/tenant-a/workloads":
					_, _ = fmt.Fprint(w, clearedWorkloadList)
				case "/apis/kueue.x-k8s.io/v1beta1/namespaces/tenant-a/localqueues/lq-a":
					_, _ = fmt.Fprint(w, queueLocalJSON)
				case "/apis/kueue.x-k8s.io/v1beta1/clusterqueues/cq-a":
					n := cqReads.Add(1)
					value := clearedClusterQueue
					if tc.initial == "quota" {
						value = strings.Replace(clearedClusterQueue, "\"reservingWorkloads\":0", "\"reservingWorkloads\":1", 1)
					}
					if n > 2 && tc.afterQueue != "" {
						value = tc.afterQueue
					}
					_, _ = fmt.Fprint(w, value)
				case "/apis/resource.k8s.io/v1/namespaces/tenant-a/resourceclaims/gpu-claim-a":
					n := claimReads.Add(1)
					value := claimCleared
					if tc.initial == "claim" {
						value = claimAllocated
					}
					if n > 2 && tc.afterClaim == "allocated" {
						value = claimAllocated
					}
					_, _ = fmt.Fprint(w, value)
				default:
					t.Errorf("unexpected Kubernetes API read %s", r.URL.RequestURI())
					http.NotFound(w, r)
				}
			}))
			defer srv.Close()
			client := srv.Client()
			client.Timeout = 3 * time.Second
			reader, err := NewKubernetesObservationReader(srv.URL, client)
			if err != nil {
				t.Fatal(err)
			}
			historical := releaseCandidateFixture()
			queue, podClaim := historical.PriorQueue, historical.PriorPodClaim
			if tc.mutate != nil {
				tc.mutate(&queue, &podClaim)
			}
			got, err := reader.ObserveKubernetesReleaseSnapshot(context.Background(), queue, podClaim)
			if tc.valid {
				if err != nil || got.PodAbsent.PreviousPod != queue.Job.Pod ||
					got.PodAbsent.PodListResourceVersion != "300" ||
					got.QuotaCleared.ClusterQueue.ResourceVersion != "65" ||
					got.ClaimUnallocated.CurrentClaim.ResourceVersion != "151" ||
					podReads.Load() != 4 || claimReads.Load() != 4 || cqReads.Load() != 4 {
					t.Fatalf("stable cross-source snapshot absent: %+v err=%v reads=%d/%d/%d", got, err, podReads.Load(), claimReads.Load(), cqReads.Load())
				}
			} else if err == nil || got != (KubernetesReleaseSnapshot{}) {
				t.Fatalf("incomplete or drifting structural evidence accepted: %+v err=%v", got, err)
			}
			if writes.Load() != 0 {
				t.Fatalf("observer issued %d mutating requests", writes.Load())
			}
			if tc.mutate != nil && (podReads.Load()+claimReads.Load()+cqReads.Load()) != 0 {
				t.Fatalf("untrusted historical identities reached Kubernetes API")
			}
		})
	}
}

func TestReleaseSnapshotDeletedClaimRequiresTwoCompleteStableLISTSweeps(t *testing.T) {
	// The Claim is already gone; GET by name cannot prove UID absence.
	// Only two complete LISTs per sweep may establish API-object absence.
	// This never proves NodeUnprepare or GPU deallocation.
	withItems := func(item string) string {
		return strings.Replace(absentClaimList, "[]", "["+item+"]", 1)
	}
	tests := []struct {
		name           string
		firstList      string
		afterList      string
		listHTTPStatus int
		reappear       bool
		valid          bool
	}{
		{name: "deleted historical Claim stable across two complete sweeps", valid: true},
		{name: "old UID remains in Claim LIST", firstList: withItems(oldClaimItem)},
		{name: "successor reuses historical Claim name", firstList: withItems(successorClaimItem)},
		{name: "old Claim reappears between source sweeps", afterList: withItems(oldClaimItem)},
		{name: "LIST resourceVersion changes between sweeps", afterList: strings.Replace(absentClaimList, `"440"`, `"441"`, 1)},
		{name: "Claim name GET reappears as unallocated between sweeps", reappear: true},
		{name: "LIST is forbidden", listHTTPStatus: http.StatusForbidden},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			var podReads, claimGETs, claimLISTs, cqReads, writes atomic.Int32
			srv := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.Method != http.MethodGet {
					writes.Add(1)
					http.Error(w, "read only", http.StatusMethodNotAllowed)
					return
				}
				w.Header().Set("Content-Type", "application/json")
				switch r.URL.RequestURI() {
				case "/api/v1/namespaces/tenant-a/pods":
					podReads.Add(1)
					_, _ = fmt.Fprint(w, absentPodList)
				case "/apis/kueue.x-k8s.io/v1beta1/namespaces/tenant-a/workloads":
					_, _ = fmt.Fprint(w, clearedWorkloadList)
				case "/apis/kueue.x-k8s.io/v1beta1/namespaces/tenant-a/localqueues/lq-a":
					_, _ = fmt.Fprint(w, queueLocalJSON)
				case "/apis/kueue.x-k8s.io/v1beta1/clusterqueues/cq-a":
					cqReads.Add(1)
					_, _ = fmt.Fprint(w, clearedClusterQueue)
				case "/apis/resource.k8s.io/v1/namespaces/tenant-a/resourceclaims/gpu-claim-a":
					n := claimGETs.Add(1)
					if tc.reappear && n > 1 {
						_, _ = fmt.Fprint(w, clearedDRAClaimJSON(t, nil))
					} else {
						http.NotFound(w, r)
					}
				case "/apis/resource.k8s.io/v1/namespaces/tenant-a/resourceclaims":
					n := claimLISTs.Add(1)
					if tc.listHTTPStatus != 0 {
						http.Error(w, "denied", tc.listHTTPStatus)
						return
					}
					body := absentClaimList
					if n == 1 && tc.firstList != "" {
						body = tc.firstList
					}
					if n > 2 && tc.afterList != "" {
						body = tc.afterList
					}
					_, _ = fmt.Fprint(w, body)
				default:
					t.Errorf("unexpected Kubernetes API read: %s", r.URL.RequestURI())
					http.NotFound(w, r)
				}
			}))
			defer srv.Close()
			client := srv.Client()
			client.Timeout = 3 * time.Second
			reader, err := NewKubernetesObservationReader(srv.URL, client)
			if err != nil {
				t.Fatal(err)
			}
			candidate := releaseCandidateFixture()
			got, err := reader.ObserveKubernetesReleaseSnapshot(
				context.Background(), candidate.PriorQueue, candidate.PriorPodClaim,
			)
			if tc.valid {
				if err != nil || got.PodAbsent.PreviousPod.UID != "pod-uid-a" ||
					got.QuotaCleared.PreviousWorkload.UID != candidate.PriorQueue.Job.KueueWorkload.UID ||
					got.ClaimAbsent.PreviousClaim.UID != "claim-uid-a" ||
					got.ClaimAbsent.ClaimListResourceVersion != "440" ||
					got.ClaimUnallocated != (DRAClaimUnallocatedObservation{}) ||
					podReads.Load() != 4 || cqReads.Load() != 4 ||
					claimGETs.Load() != 2 || claimLISTs.Load() != 4 {
					t.Fatalf("historical Claim absence not correlated: snapshot=%+v err=%v, reads pod=%d cq=%d get=%d list=%d",
						got, err, podReads.Load(), cqReads.Load(), claimGETs.Load(), claimLISTs.Load())
				}
			} else if err == nil || got != (KubernetesReleaseSnapshot{}) {
				t.Fatalf("unsafe or drifting deleted Claim snapshot was accepted: %+v err=%v", got, err)
			}
			if writes.Load() != 0 {
				t.Fatal("release observer made a mutating Kubernetes request")
			}
		})
	}
}
