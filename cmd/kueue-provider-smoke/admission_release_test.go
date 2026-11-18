package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/avaargsh/compute-control-panel/internal/operation"
	"github.com/avaargsh/compute-control-panel/internal/provider"
)

func admissionReleaseFixture() (provider.KueueCreatePin, admissionReleaseSeed, operation.Observation) {
	pin, placed := placedObservationFixture()
	seed := admissionReleaseSeed{Pin: pin, Queue: provider.KueueQueueBindingEvidence{
		Job: placed.Evidence, Stage: placed.Stage,
		LocalQueue:   provider.KubernetesResourceRef{Namespace: smokeNamespace, Name: "ccp-smoke", UID: "lq-uid", ResourceVersion: "10"},
		ClusterQueue: provider.KubernetesResourceRef{Name: "ccp-smoke", UID: "cq-uid", ResourceVersion: "20"},
	}, PodClaim: provider.DRAPodClaimObservation{
		Pod: placed.Evidence.Pod, NodeName: placed.Evidence.PodNodeName, PodClaimAlias: "gpu", ConsumerContainer: "ccp-queued",
		Claim: provider.DRAClaimObservation{Claim: provider.KubernetesResourceRef{Namespace: smokeNamespace, Name: "claim-a", UID: "claim-uid", ResourceVersion: "201"},
			PodUID: placed.Evidence.Pod.UID, Device: provider.DRAAllocatedDevice{Request: "gpu", Driver: "gpu.example.com", Pool: "pool-a", Device: "device-a"}},
	}}
	remove := admissionDeleteRequest(pin.Namespace+"/"+pin.Name, pin.UID)
	absence := operation.Observation{OperationID: remove.OperationID, TenantID: remove.TenantID, WorkloadID: remove.WorkloadID, ProviderID: remove.ProviderID,
		DesiredGeneration: remove.DesiredGeneration, TargetResourceID: remove.TargetResourceID, TargetResourceUID: remove.TargetResourceUID,
		Result: operation.TargetAbsent, ResourceVersion: "job-absence-1", ObservedAt: time.Now().UTC()}
	return pin, seed, absence
}

func TestAdmissionReleaseObservationsStayUnknown(t *testing.T) {
	for _, fault := range []string{"", "claim404", "claim404 list403", "claim404 successor", "claim403", "claim allocated", "claim deleting", "claim replaced", "pod present", "quota reserved", "claim RV drift"} {
		t.Run(fault, func(t *testing.T) {
			pin, seed, absence := admissionReleaseFixture()
			var claimReads, claimListReads, writes atomic.Int32
			server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.Method != http.MethodGet {
					writes.Add(1)
					w.WriteHeader(http.StatusMethodNotAllowed)
					return
				}
				w.Header().Set("Content-Type", "application/json")
				var response any
				switch r.URL.Path {
				case "/api/v1/namespaces/ccp-kueue-smoke/pods":
					items := []any{}
					if fault == "pod present" {
						items = append(items, map[string]any{"metadata": releaseTestMeta(seed.PodClaim.Pod)})
					}
					response = map[string]any{"apiVersion": "v1", "kind": "PodList", "metadata": map[string]any{"resourceVersion": "300"}, "items": items}
				case "/apis/kueue.x-k8s.io/v1beta1/namespaces/ccp-kueue-smoke/workloads":
					response = map[string]any{"metadata": map[string]any{"resourceVersion": "400"}, "items": []any{}}
				case "/apis/kueue.x-k8s.io/v1beta1/namespaces/ccp-kueue-smoke/localqueues/ccp-smoke":
					response = map[string]any{"metadata": releaseTestMeta(seed.Queue.LocalQueue), "spec": map[string]any{"clusterQueue": "ccp-smoke"}}
				case "/apis/kueue.x-k8s.io/v1beta1/clusterqueues/ccp-smoke":
					reserved := 0
					if fault == "quota reserved" {
						reserved = 1
					}
					response = map[string]any{"metadata": releaseTestMeta(seed.Queue.ClusterQueue), "status": map[string]any{"reservingWorkloads": reserved, "admittedWorkloads": 0}}
				case "/apis/resource.k8s.io/v1/namespaces/ccp-kueue-smoke/resourceclaims":
					claimListReads.Add(1)
					if fault == "claim403" || fault == "claim404 list403" {
						w.WriteHeader(http.StatusForbidden)
						return
					}
					items := []any{map[string]any{"metadata": releaseTestMeta(seed.PodClaim.Claim.Claim)}}
					if fault == "claim404" {
						items = []any{}
					}
					if fault == "claim404 successor" {
						ref := seed.PodClaim.Claim.Claim
						ref.UID = "successor-claim"
						items = []any{map[string]any{"metadata": releaseTestMeta(ref)}}
					}
					response = map[string]any{"apiVersion": "resource.k8s.io/v1", "kind": "ResourceClaimList", "metadata": map[string]any{"resourceVersion": "500"}, "items": items}
				case "/apis/resource.k8s.io/v1/namespaces/ccp-kueue-smoke/resourceclaims/claim-a":
					n := claimReads.Add(1)
					if strings.HasPrefix(fault, "claim404") {
						w.WriteHeader(http.StatusNotFound)
						return
					}
					if fault == "claim403" {
						w.WriteHeader(http.StatusForbidden)
						return
					}
					ref := seed.PodClaim.Claim.Claim
					ref.ResourceVersion = "202"
					if fault == "claim replaced" {
						ref.UID = "successor-claim"
					}
					if fault == "claim RV drift" {
						ref.ResourceVersion = fmt.Sprint(202 + n)
					}
					meta := releaseTestMeta(ref)
					status := map[string]any{"allocation": nil, "reservedFor": []any{}, "devices": []any{}}
					if fault == "claim allocated" {
						status["allocation"] = map[string]any{"devices": map[string]any{}}
					}
					if fault == "claim deleting" {
						meta["deletionTimestamp"] = "2026-11-18T00:00:00Z"
					}
					response = map[string]any{"apiVersion": "resource.k8s.io/v1", "kind": "ResourceClaim", "metadata": meta, "status": status}
				default:
					t.Errorf("unexpected read %s", r.URL.Path)
					http.NotFound(w, r)
					return
				}
				if err := json.NewEncoder(w).Encode(response); err != nil {
					t.Error(err)
				}
			}))
			defer server.Close()
			client := server.Client()
			client.Timeout = 3 * time.Second
			reader, err := provider.NewKubernetesObservationReader(server.URL, client)
			if err != nil {
				t.Fatal(err)
			}
			report, err := observeAdmissionRelease(context.Background(), reader, pin, seed, absence)
			if err != nil {
				t.Fatal(err)
			}
			if report["release_status"] != "UNKNOWN" || report["allocation_released"] != false || report["cost_final"] != false || report["physical_gpu_verified"] != false {
				t.Fatalf("unverified sources promoted release: %+v", report)
			}
			complete := fault == ""
			if report["api_snapshot_complete"] != complete {
				t.Fatalf("snapshot completeness mismatch: %+v", report)
			}
			if complete && (report["pod_absent"] != true || report["quota_cleared"] != true || report["claim_unallocated"] != true) {
				t.Fatalf("complete snapshot lost observations: %+v", report)
			}
			if fault == "pod present" && report["pod_absent"] != nil {
				t.Fatalf("present Pod counted absent: %+v", report)
			}
			if fault == "quota reserved" && report["quota_cleared"] != nil {
				t.Fatalf("reserved quota counted clear: %+v", report)
			}
			if len(fault) >= 5 && fault[:5] == "claim" && report["claim_unallocated"] != nil {
				t.Fatalf("unproven Claim counted unallocated: %+v", report)
			}
			if fault == "claim404" {
				versions, ok := report["resource_versions"].(map[string]string)
				if report["claim_uid_absent"] != true || !ok || versions["resourceclaim_list"] != "500" || claimListReads.Load() != 2 || report["claim_unallocated"] != nil || report["api_snapshot_complete"] != false {
					t.Fatalf("stable Claim LIST absence must not imply unallocation: %+v reads=%d", report, claimListReads.Load())
				}
			} else if report["claim_uid_absent"] != nil {
				t.Fatalf("Claim absence promoted without a complete UID-pinned LIST: %+v", report)
			}
			if writes.Load() != 0 {
				t.Fatalf("observer issued %d writes", writes.Load())
			}
		})
	}
}

func releaseTestMeta(ref provider.KubernetesResourceRef) map[string]any {
	return map[string]any{"namespace": ref.Namespace, "name": ref.Name, "uid": ref.UID, "resourceVersion": ref.ResourceVersion}
}

func TestAdmissionReleaseRejectsUncorrelatedHistoryBeforeReads(t *testing.T) {
	for _, fault := range []string{"job UID", "generation", "pod UID", "claim Pod", "provider target", "provider generation", "provider resource", "seed pin", "node"} {
		t.Run(fault, func(t *testing.T) {
			pin, seed, absence := admissionReleaseFixture()
			switch fault {
			case "job UID":
				seed.Queue.Job.Job.UID = "successor"
			case "generation":
				seed.Queue.Job.DesiredGeneration++
			case "pod UID":
				seed.PodClaim.Pod.UID = "foreign"
			case "claim Pod":
				seed.PodClaim.Claim.PodUID = "foreign"
			case "provider target":
				absence.TargetResourceUID = "foreign"
			case "provider generation":
				absence.DesiredGeneration++
			case "provider resource":
				absence.ResourceGeneration = 7
			case "seed pin":
				seed.Pin.UID = "foreign"
			case "node":
				seed.PodClaim.NodeName = "other-node"
			}
			var reads atomic.Int32
			server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { reads.Add(1); http.NotFound(w, r) }))
			defer server.Close()
			client := server.Client()
			client.Timeout = 3 * time.Second
			reader, err := provider.NewKubernetesObservationReader(server.URL, client)
			if err != nil {
				t.Fatal(err)
			}
			report, err := observeAdmissionRelease(context.Background(), reader, pin, seed, absence)
			if err == nil || report != nil || reads.Load() != 0 {
				t.Fatalf("uncorrelated history reached API: report=%+v err=%v reads=%d", report, err, reads.Load())
			}
		})
	}
}

func TestAdmissionReleaseCommandRequiresExplicitOptIn(t *testing.T) {
	phase, subject, err := parseAdmissionArgs([]string{"admission", "inspect", "release"})
	if err != nil || phase != "inspect" || subject != "release" {
		t.Fatalf("invalid release command: %q %q %v", phase, subject, err)
	}
	if _, _, err := parseAdmissionArgs([]string{"admission", "inspect", "release", "force"}); err == nil {
		t.Fatal("accepted unexpected release option")
	}
	if err := admissionInspectRelease(func(string) string { return "" }, nil); err == nil {
		t.Fatal("release inspection accepted missing disposable opt-in")
	}
}

func TestAdmissionReleaseChecksCaptureUIDPinsBeforeDatabase(t *testing.T) {
	for _, changed := range []string{"", "local queue", "cluster queue"} {
		t.Run(changed, func(t *testing.T) {
			_, seed, _ := admissionReleaseFixture()
			expectedLocal, expectedCluster := seed.Queue.LocalQueue.UID, seed.Queue.ClusterQueue.UID
			switch changed {
			case "local queue":
				seed.Queue.LocalQueue.UID = "successor-local"
			case "cluster queue":
				seed.Queue.ClusterQueue.UID = "successor-cluster"
			}
			data, err := json.Marshal(seed)
			if err != nil {
				t.Fatal(err)
			}
			path := filepath.Join(t.TempDir(), "seed.json")
			if err := os.WriteFile(path, data, 0600); err != nil {
				t.Fatal(err)
			}
			env := map[string]string{
				"CCP_DISPOSABLE_KIND_SMOKE": "1", "POD_NAMESPACE": smokeNamespace, "CCP_KUEUE_RELEASE_SEED": path,
				"CCP_KUEUE_DRA_CLAIM": seed.PodClaim.Claim.Claim.Name, "CCP_KUEUE_DRA_CLAIM_UID": seed.PodClaim.Claim.Claim.UID,
				"CCP_KUEUE_LOCAL_QUEUE_UID": expectedLocal, "CCP_KUEUE_CLUSTER_QUEUE_UID": expectedCluster,
			}
			err = admissionInspectRelease(func(key string) string { return env[key] }, nil)
			expected := provider.ErrUncorrelatedReleaseEvidence
			if changed == "" {
				expected = errEmptyBindingDSN
			}
			if !errors.Is(err, expected) {
				t.Fatalf("UID pins not checked before DB: got %v, want %v", err, expected)
			}
		})
	}
}
