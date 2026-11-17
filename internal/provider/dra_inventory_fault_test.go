package provider

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"
)

// Exercise faults during both sweeps of the real Pod/Claim/inventory reader.
// Recovery repeats the observation on the same reader and UID-pinned query.
func TestDRAInventoryAPIFaultsAndRecovery(t *testing.T) {
	const podPath = "/api/v1/namespaces/tenant-a/pods/gpu-pod-a"
	const claimPath = "/apis/resource.k8s.io/v1/namespaces/tenant-a/resourceclaims/gpu-claim-a"
	const slicePath = "/apis/resource.k8s.io/v1/resourceslices"
	stages := []struct {
		name string
		path string
		read int32
	}{
		{"initial Pod", podPath, 1},
		{"Pod recheck", podPath, 2},
		{"final sweep Pod", podPath, 3},
		{"final Pod recheck", podPath, 4},
		{"initial Claim", claimPath, 1},
		{"Claim recheck", claimPath, 2},
		{"final sweep Claim", claimPath, 3},
		{"final Claim recheck", claimPath, 4},
		{"initial Slice list", slicePath, 1},
		{"Slice list recheck", slicePath, 2},
	}
	faults := []struct {
		name   string
		status int
	}{{"403", http.StatusForbidden}, {"404", http.StatusNotFound}, {"timeout", 0}}
	for _, template := range []bool{false, true} {
		for _, stage := range stages {
			for _, fault := range faults {
				t.Run(fmt.Sprintf("template=%t/%s/%s", template, stage.name, fault.name), func(t *testing.T) {
					pod := podDRABindingFixture
					query := DRAPoolInventoryQuery{Pod: podDRAQuery()}
					if template {
						pod = modifiedPodDRAFixture(t, func(v map[string]any) {
							claim := podDRASpec(v)["resourceClaims"].([]any)[0].(map[string]any)
							delete(claim, "resourceClaimName")
							claim["resourceClaimTemplateName"] = "gpu-template"
							v["status"] = map[string]any{"resourceClaimStatuses": []any{map[string]any{"name": "gpu", "resourceClaimName": "gpu-claim-a"}}}
						})
						query.Pod.ClaimTemplateName = "gpu-template"
					}
					bodies := map[string]string{podPath: pod, claimPath: draClaimFixture, slicePath: draSliceList(t, nil)}
					var reads, faultHits, writes atomic.Int32
					var recovered atomic.Bool
					server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
						if r.Method != http.MethodGet {
							writes.Add(1)
							http.Error(w, "read-only", http.StatusMethodNotAllowed)
							return
						}
						if r.URL.Path == stage.path && reads.Add(1) == stage.read && !recovered.Load() {
							faultHits.Add(1)
							if fault.status != 0 {
								http.Error(w, "injected API fault", fault.status)
							} else {
								<-r.Context().Done()
							}
							return
						}
						body, ok := bodies[r.URL.Path]
						if !ok {
							t.Errorf("unexpected read %s", r.URL.Path)
							http.NotFound(w, r)
							return
						}
						w.Header().Set("Content-Type", "application/json")
						_, _ = fmt.Fprint(w, body)
					}))
					defer server.Close()
					client := server.Client()
					client.Timeout = 500 * time.Millisecond
					reader, err := NewKubernetesObservationReader(server.URL, client)
					if err != nil {
						t.Fatal(err)
					}
					got, err := reader.ObserveDRAPoolInventory(context.Background(), query)
					if !errors.Is(err, ErrInvalidDRAObservation) || got != (DRAPoolInventoryObservation{}) || faultHits.Load() != 1 {
						t.Fatalf("fault produced evidence: observation=%+v err=%v injected=%d", got, err, faultHits.Load())
					}
					recovered.Store(true)
					got, err = reader.ObserveDRAPoolInventory(context.Background(), query)
					if err != nil || got.PodClaim.Pod.UID != query.Pod.Claim.ExpectedPodUID ||
						got.PodClaim.Claim.Claim.UID != query.Pod.Claim.ExpectedClaimUID || got.Slice.UID != "slice-uid-a" {
						t.Fatalf("fresh observation did not recover the same identities: %+v err=%v", got, err)
					}
					if writes.Load() != 0 {
						t.Fatalf("observer attempted %d writes", writes.Load())
					}
				})
			}
		}
	}
}

func TestKueueObservationAPIFaultsAndRecovery(t *testing.T) {
	for _, status := range []int{http.StatusForbidden, http.StatusNotFound, 0} {
		t.Run(fmt.Sprintf("status-%d", status), func(t *testing.T) {
			var recovered atomic.Bool
			var faults atomic.Int32
			server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.Method != http.MethodGet {
					t.Errorf("unexpected mutation %s", r.Method)
					w.WriteHeader(http.StatusMethodNotAllowed)
					return
				}
				w.Header().Set("Content-Type", "application/json")
				switch r.URL.Path {
				case "/apis/batch/v1/namespaces/tenant-a/jobs/job-a":
					_, _ = fmt.Fprint(w, testKubeJob)
				case "/apis/kueue.x-k8s.io/v1beta1/namespaces/tenant-a/workloads":
					if !recovered.Load() {
						faults.Add(1)
						if status == 0 {
							<-r.Context().Done()
						} else {
							w.WriteHeader(status)
						}
						return
					}
					_, _ = fmt.Fprint(w, testKueueWorkloads)
				case "/api/v1/namespaces/tenant-a/pods":
					_, _ = fmt.Fprint(w, testKubePods)
				default:
					t.Errorf("unexpected read %s", r.URL.Path)
					http.NotFound(w, r)
				}
			}))
			defer server.Close()
			client := server.Client()
			client.Timeout = 500 * time.Millisecond
			reader, err := NewKubernetesObservationReader(server.URL, client)
			if err != nil {
				t.Fatal(err)
			}
			evidence, stage, err := reader.ObserveJob(context.Background(), observedJobQuery())
			if err == nil || stage != KueueUnproven || evidence != (KueueJobEvidence{}) || faults.Load() != 1 {
				t.Fatalf("Kueue fault produced evidence: %+v stage=%s err=%v injected=%d", evidence, stage, err, faults.Load())
			}
			recovered.Store(true)
			evidence, stage, err = reader.ObserveJob(context.Background(), observedJobQuery())
			if err != nil || stage != KueuePodPlaced || evidence.Job.UID != observedJobQuery().ExpectedJobUID {
				t.Fatalf("Kueue recovery failed: %+v stage=%s err=%v", evidence, stage, err)
			}
		})
	}
}

func TestDRAClaimTemplateRechecksGeneratedStatus(t *testing.T) {
	for _, field := range []string{"resourceClaimName", "name"} {
		t.Run(field, func(t *testing.T) {
			body := func(replace bool) string {
				return modifiedPodDRAFixture(t, func(v map[string]any) {
					claim := podDRASpec(v)["resourceClaims"].([]any)[0].(map[string]any)
					delete(claim, "resourceClaimName")
					claim["resourceClaimTemplateName"] = "gpu-template"
					status := map[string]any{"name": "gpu", "resourceClaimName": "gpu-claim-a"}
					if replace {
						status[field] = "foreign"
					}
					v["status"] = map[string]any{"resourceClaimStatuses": []any{status}}
				})
			}
			first, second := body(false), body(true)
			var podReads, claimReads atomic.Int32
			server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.Method != http.MethodGet {
					t.Errorf("unexpected mutation %s", r.Method)
					w.WriteHeader(http.StatusMethodNotAllowed)
					return
				}
				w.Header().Set("Content-Type", "application/json")
				switch r.URL.Path {
				case "/api/v1/namespaces/tenant-a/pods/gpu-pod-a":
					if podReads.Add(1) == 1 {
						_, _ = fmt.Fprint(w, first)
					} else {
						_, _ = fmt.Fprint(w, second)
					}
				case "/apis/resource.k8s.io/v1/namespaces/tenant-a/resourceclaims/gpu-claim-a":
					claimReads.Add(1)
					_, _ = fmt.Fprint(w, draClaimFixture)
				default:
					t.Errorf("unexpected read %s", r.URL.Path)
					http.NotFound(w, r)
				}
			}))
			defer server.Close()
			client := server.Client()
			client.Timeout = 3 * time.Second
			reader, err := NewKubernetesObservationReader(server.URL, client)
			if err != nil {
				t.Fatal(err)
			}
			query := podDRAQuery()
			query.ClaimTemplateName = "gpu-template"
			got, err := reader.ObserveDRAPodClaim(context.Background(), query)
			if !errors.Is(err, ErrInvalidDRAObservation) || got != (DRAPodClaimObservation{}) || podReads.Load() != 2 || claimReads.Load() != 2 {
				t.Fatalf("same UID/RV changed generated status accepted: %+v err=%v reads=%d/%d", got, err, podReads.Load(), claimReads.Load())
			}
		})
	}
}
