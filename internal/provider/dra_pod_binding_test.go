package provider

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"
)

const podDRABindingFixture = `{"apiVersion":"v1","kind":"Pod","metadata":{"name":"gpu-pod-a","namespace":"tenant-a","uid":"pod-uid-a","resourceVersion":"221","ownerReferences":[{"apiVersion":"batch/v1","kind":"Job","name":"gpu-job-a","uid":"job-uid-a","controller":true}]},"spec":{"nodeName":"gpu-node-a","resourceClaims":[{"name":"gpu","resourceClaimName":"gpu-claim-a"}],"containers":[{"name":"worker","resources":{"claims":[{"name":"gpu"}]}}]}}`

func podDRAQuery() DRAPodClaimQuery {
	return DRAPodClaimQuery{
		Claim: draTestQuery(), ExpectedJobUID: "job-uid-a",
		ExpectedNodeName: "gpu-node-a", PodClaimAlias: "gpu",
	}
}

func modifiedPodDRAFixture(t *testing.T, f func(map[string]any)) string {
	t.Helper()
	var v map[string]any
	if err := json.Unmarshal([]byte(podDRABindingFixture), &v); err != nil {
		t.Fatal(err)
	}
	if f != nil {
		f(v)
	}
	b, err := json.Marshal(v)
	if err != nil {
		t.Fatal(err)
	}
	return string(b)
}

func podDRASpec(v map[string]any) map[string]any {
	return v["spec"].(map[string]any)
}

func podDRAMeta(v map[string]any) map[string]any {
	return v["metadata"].(map[string]any)
}

func TestDRAClaimRequiresLivePodClaimAndContainerConsumer(t *testing.T) {
	cases := []struct {
		name            string
		changePod       func(map[string]any)
		changeQuery     func(*DRAPodClaimQuery)
		secondPodChange func(map[string]any)
		changeClaim     func(map[string]any)
		podHTTPStatus   int
		claimHTTPStatus int
		wantValid       bool
	}{
		{name: "exact Job owned placed Pod, Pod resourceClaim and container", wantValid: true},
		{name: "same-name replacement Pod UID", changePod: func(v map[string]any) { podDRAMeta(v)["uid"] = "successor-pod-uid" }},
		{name: "foreign Job owner", changePod: func(v map[string]any) {
			podDRAMeta(v)["ownerReferences"].([]any)[0].(map[string]any)["uid"] = "foreign-job"
		}},
		{name: "non-controller Job owner", changePod: func(v map[string]any) {
			podDRAMeta(v)["ownerReferences"].([]any)[0].(map[string]any)["controller"] = false
		}},
		{name: "Pod not node-bound", changePod: func(v map[string]any) { delete(podDRASpec(v), "nodeName") }},
		{name: "Pod moved to different node", changePod: func(v map[string]any) { podDRASpec(v)["nodeName"] = "other-node" }},
		{name: "Pod missing resourceVersion", changePod: func(v map[string]any) { podDRAMeta(v)["resourceVersion"] = "" }},
		{name: "Pod has no claim", changePod: func(v map[string]any) { delete(podDRASpec(v), "resourceClaims") }},
		{name: "Pod references other claim", changePod: func(v map[string]any) {
			podDRASpec(v)["resourceClaims"].([]any)[0].(map[string]any)["resourceClaimName"] = "other-claim"
		}},
		{name: "template claim needs separate resolution", changePod: func(v map[string]any) {
			c := podDRASpec(v)["resourceClaims"].([]any)[0].(map[string]any)
			delete(c, "resourceClaimName")
			c["resourceClaimTemplateName"] = "gpu-template"
		}},
		{name: "ambiguous second Pod claim", changePod: func(v map[string]any) {
			s := podDRASpec(v)
			s["resourceClaims"] = append(s["resourceClaims"].([]any), map[string]any{"name": "gpu2", "resourceClaimName": "other-claim"})
		}},
		{name: "container does not consume Claim", changePod: func(v map[string]any) {
			c := podDRASpec(v)["containers"].([]any)[0].(map[string]any)
			c["resources"] = map[string]any{}
		}},
		{name: "container consumes different alias", changePod: func(v map[string]any) {
			podDRASpec(v)["containers"].([]any)[0].(map[string]any)["resources"].(map[string]any)["claims"].([]any)[0].(map[string]any)["name"] = "other"
		}},
		{name: "two container consumers ambiguous", changePod: func(v map[string]any) {
			s := podDRASpec(v)
			s["containers"] = append(s["containers"].([]any), map[string]any{"name": "second", "resources": map[string]any{"claims": []any{map[string]any{"name": "gpu"}}}})
		}},
		{name: "Pod is forbidden", podHTTPStatus: http.StatusForbidden},
		{name: "Pod is absent, not GPU release", podHTTPStatus: http.StatusNotFound},
		{name: "Claim is forbidden", claimHTTPStatus: http.StatusForbidden},
		{name: "DRA reservation differs from Pod", changeClaim: func(v map[string]any) {
			v["status"].(map[string]any)["reservedFor"].([]any)[0].(map[string]any)["uid"] = "foreign-pod-uid"
		}},
		{name: "Pod UID replaced after Claim read", secondPodChange: func(v map[string]any) { podDRAMeta(v)["uid"] = "new-pod-uid" }},
		{name: "Pod resourceVersion changed after Claim read", secondPodChange: func(v map[string]any) { podDRAMeta(v)["resourceVersion"] = "222" }},
		{name: "invalid expected Job UID", changeQuery: func(q *DRAPodClaimQuery) { q.ExpectedJobUID = "" }},
		{name: "unsafe expected Pod claim alias", changeQuery: func(q *DRAPodClaimQuery) { q.PodClaimAlias = "gpu/../../other" }},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			first := modifiedPodDRAFixture(t, tc.changePod)
			second := first
			if tc.secondPodChange != nil {
				second = modifiedPodDRAFixture(t, tc.secondPodChange)
			}
			claim := draModifiedFixture(t, tc.changeClaim)
			var podGets, claimGets, writes atomic.Int32
			ts := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.Method != http.MethodGet {
					writes.Add(1)
					http.Error(w, "writes forbidden", http.StatusMethodNotAllowed)
					return
				}
				w.Header().Set("Content-Type", "application/json")
				switch r.URL.RequestURI() {
				case "/api/v1/namespaces/tenant-a/pods/gpu-pod-a":
					serial := podGets.Add(1)
					if tc.podHTTPStatus != 0 {
						w.WriteHeader(tc.podHTTPStatus)
						return
					}
					if serial == 1 {
						_, _ = fmt.Fprint(w, first)
					} else {
						_, _ = fmt.Fprint(w, second)
					}
				case "/apis/resource.k8s.io/v1/namespaces/tenant-a/resourceclaims/gpu-claim-a":
					claimGets.Add(1)
					if tc.claimHTTPStatus != 0 {
						w.WriteHeader(tc.claimHTTPStatus)
						return
					}
					_, _ = fmt.Fprint(w, claim)
				default:
					t.Errorf("unexpected API request %s", r.URL)
					http.NotFound(w, r)
				}
			}))
			defer ts.Close()
			client := ts.Client()
			client.Timeout = 3 * time.Second
			reader, err := NewKubernetesObservationReader(ts.URL, client)
			if err != nil {
				t.Fatal(err)
			}
			query := podDRAQuery()
			if tc.changeQuery != nil {
				tc.changeQuery(&query)
			}
			got, err := reader.ObserveDRAPodClaim(context.Background(), query)
			if tc.wantValid {
				if err != nil || got.Pod.UID != "pod-uid-a" || got.Pod.ResourceVersion != "221" ||
					got.NodeName != "gpu-node-a" || got.PodClaimAlias != "gpu" ||
					got.ConsumerContainer != "worker" || got.Claim.Claim.UID != "claim-uid-a" ||
					got.Claim.PodUID != got.Pod.UID || got.Claim.Device.Device != "device-a" ||
					podGets.Load() != 2 || claimGets.Load() < 1 {
					t.Fatalf("bound Pod+Claim not proven: %+v err=%v reads=%d/%d", got, err, podGets.Load(), claimGets.Load())
				}
			} else if err == nil || got != (DRAPodClaimObservation{}) {
				t.Fatalf("invalid Pod/Claim association accepted: %+v err=%v", got, err)
			}
			if writes.Load() != 0 {
				t.Fatalf("DRA reader mutated Kubernetes: %d writes", writes.Load())
			}
			if tc.changeQuery != nil && (podGets.Load() != 0 || claimGets.Load() != 0) {
				t.Fatalf("invalid caller authority caused reads: pod=%d claim=%d", podGets.Load(), claimGets.Load())
			}
			if tc.changePod != nil && claimGets.Load() != 0 {
				t.Fatalf("invalid Pod was not rejected before Claim GET: %d", claimGets.Load())
			}
		})
	}
}

// Kubernetes creates the Claim named in Pod.status.resourceClaimStatuses from
// the Job's ResourceClaimTemplate. A pre-created Claim has no such authoritative
// binding and Kueue v0.19 refuses its admission into a managed Workload.
func TestDRAClaimTemplateRequiresGeneratedPodClaimStatus(t *testing.T) {
	tests := []struct {
		name   string
		change func(map[string]any)
		query  func(*DRAPodClaimQuery)
		allow  bool
	}{
		{name: "exact template and generated Claim status", allow: true},
		{name: "missing generated status", change: func(v map[string]any) {
			delete(v, "status")
		}},
		{name: "status names another Claim", change: func(v map[string]any) {
			v["status"].(map[string]any)["resourceClaimStatuses"].([]any)[0].(map[string]any)["resourceClaimName"] = "claim-from-other-pod"
		}},
		{name: "status uses wrong Pod alias", change: func(v map[string]any) {
			v["status"].(map[string]any)["resourceClaimStatuses"].([]any)[0].(map[string]any)["name"] = "other"
		}},
		{name: "second generated status is ambiguous", change: func(v map[string]any) {
			statuses := v["status"].(map[string]any)["resourceClaimStatuses"].([]any)
			v["status"].(map[string]any)["resourceClaimStatuses"] = append(statuses, map[string]any{"name": "gpu2", "resourceClaimName": "foreign"})
		}},
		{name: "template name differs", change: func(v map[string]any) {
			podDRASpec(v)["resourceClaims"].([]any)[0].(map[string]any)["resourceClaimTemplateName"] = "other-template"
		}},
		{name: "pre-created direct Claim mixed with template", change: func(v map[string]any) {
			podDRASpec(v)["resourceClaims"].([]any)[0].(map[string]any)["resourceClaimName"] = "gpu-claim-a"
		}},
		{name: "query has no authenticated template name", query: func(q *DRAPodClaimQuery) {
			q.ClaimTemplateName = "../forbidden"
		}},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			body := modifiedPodDRAFixture(t, func(v map[string]any) {
				claim := podDRASpec(v)["resourceClaims"].([]any)[0].(map[string]any)
				delete(claim, "resourceClaimName")
				claim["resourceClaimTemplateName"] = "gpu-template"
				v["status"] = map[string]any{"resourceClaimStatuses": []any{
					map[string]any{"name": "gpu", "resourceClaimName": "gpu-claim-a"},
				}}
				if tc.change != nil {
					tc.change(v)
				}
			})
			var podReads, claimReads atomic.Int32
			ts := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.Method != http.MethodGet {
					t.Errorf("unexpected Kubernetes mutation: %s", r.Method)
					http.Error(w, "forbidden", http.StatusMethodNotAllowed)
					return
				}
				w.Header().Set("Content-Type", "application/json")
				switch r.URL.Path {
				case "/api/v1/namespaces/tenant-a/pods/gpu-pod-a":
					podReads.Add(1)
					_, _ = fmt.Fprint(w, body)
				case "/apis/resource.k8s.io/v1/namespaces/tenant-a/resourceclaims/gpu-claim-a":
					claimReads.Add(1)
					_, _ = fmt.Fprint(w, draClaimFixture)
				default:
					t.Errorf("unexpected read %s", r.URL.Path)
					http.NotFound(w, r)
				}
			}))
			defer ts.Close()
			client := ts.Client()
			client.Timeout = 3 * time.Second
			reader, err := NewKubernetesObservationReader(ts.URL, client)
			if err != nil {
				t.Fatal(err)
			}
			query := podDRAQuery()
			query.ClaimTemplateName = "gpu-template"
			if tc.query != nil {
				tc.query(&query)
			}
			got, err := reader.ObserveDRAPodClaim(context.Background(), query)
			if tc.allow {
				if err != nil || got.Pod.UID != "pod-uid-a" || got.Claim.Claim.UID != "claim-uid-a" ||
					podReads.Load() != 2 || claimReads.Load() != 2 {
					t.Fatalf("generated Claim binding not established: %+v err=%v reads=%d/%d", got, err, podReads.Load(), claimReads.Load())
				}
			} else if err == nil || got != (DRAPodClaimObservation{}) {
				t.Fatalf("unproven generated Claim accepted: %+v err=%v", got, err)
			}
			if !tc.allow && claimReads.Load() != 0 {
				t.Fatalf("untrusted template/status reached Claim reader: %d", claimReads.Load())
			}
		})
	}
}
