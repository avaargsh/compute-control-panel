package provider

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"
)

func previousDRAClaimAllocation() DRAClaimObservation {
	return DRAClaimObservation{
		Claim: KubernetesResourceRef{
			Namespace: "tenant-a", Name: "gpu-claim-a", UID: "claim-uid-a", ResourceVersion: "150",
		},
		PodUID: "pod-uid-a",
		Device: DRAAllocatedDevice{
			Request: "gpu", Driver: "gpu.example.com", Pool: "pool-a",
			Device: "device-a", ShareID: "share-a",
		},
	}
}

func clearedDRAClaimJSON(t *testing.T, change func(map[string]any)) string {
	t.Helper()
	return draModifiedFixture(t, func(v map[string]any) {
		meta := v["metadata"].(map[string]any)
		meta["resourceVersion"] = "151"
		status := v["status"].(map[string]any)
		status["allocation"] = nil
		status["reservedFor"] = []any{}
		status["devices"] = []any{}
		if change != nil {
			change(v)
		}
	})
}

// A still-existing, exact-UID ResourceClaim that has no scheduler reservation
// is necessary evidence in some DRA release paths, but NOT driver deallocation.
func TestDRAClaimClearObservationRequiresStableUnallocatedStatus(t *testing.T) {
	cases := []struct {
		name                      string
		first, second             func(map[string]any)
		prior                     func(*DRAClaimObservation)
		firstStatus, secondStatus int
		valid                     bool
	}{
		{name: "stable same UID unallocated Claim", valid: true},
		{name: "same-name successor UID", first: func(v map[string]any) {
			v["metadata"].(map[string]any)["uid"] = "claim-uid-b"
		}},
		{name: "Claim resourceVersion missing", first: func(v map[string]any) {
			v["metadata"].(map[string]any)["resourceVersion"] = ""
		}},
		{name: "no post-allocation resourceVersion transition", first: func(v map[string]any) {
			v["metadata"].(map[string]any)["resourceVersion"] = "150"
		}},
		{name: "Claim still reserved for Pod", first: func(v map[string]any) {
			v["status"].(map[string]any)["reservedFor"] = []any{map[string]any{
				"resource": "pods", "name": "gpu-pod-a", "uid": "pod-uid-a",
			}}
		}},
		{name: "Claim still allocated", first: func(v map[string]any) {
			v["status"].(map[string]any)["allocation"] = map[string]any{"devices": map[string]any{"results": []any{}}}
		}},
		{name: "driver still reports device state", first: func(v map[string]any) {
			v["status"].(map[string]any)["devices"] = []any{map[string]any{"driver": "gpu.example.com"}}
		}},
		{name: "Claim deletion is pending", first: func(v map[string]any) {
			v["metadata"].(map[string]any)["deletionTimestamp"] = "2026-11-10T01:00:00Z"
		}},
		{name: "status missing", first: func(v map[string]any) {
			delete(v, "status")
		}},
		{name: "foreign namespace", first: func(v map[string]any) {
			v["metadata"].(map[string]any)["namespace"] = "tenant-b"
		}},
		{name: "wrong API kind", first: func(v map[string]any) {
			v["kind"] = "ResourceClaimTemplate"
		}},
		{name: "Claim successor after first read", second: func(v map[string]any) {
			v["metadata"].(map[string]any)["uid"] = "claim-uid-b"
		}},
		{name: "resourceVersion drift after first read", second: func(v map[string]any) {
			v["metadata"].(map[string]any)["resourceVersion"] = "152"
		}},
		{name: "allocation reappeared on second GET", second: func(v map[string]any) {
			v["status"].(map[string]any)["allocation"] = map[string]any{"devices": map[string]any{"results": []any{}}}
		}},
		{name: "reservation reappeared on second GET", second: func(v map[string]any) {
			v["status"].(map[string]any)["reservedFor"] = []any{map[string]any{"resource": "pods", "uid": "pod-uid-a"}}
		}},
		{name: "second GET forbidden", secondStatus: http.StatusForbidden},
		{name: "Claim 404 not proof of device release", firstStatus: http.StatusNotFound},
		{name: "Claim API unavailable", firstStatus: http.StatusServiceUnavailable},
		{name: "untrusted prior UID", prior: func(p *DRAClaimObservation) {
			p.Claim.UID = ""
		}},
		{name: "untrusted prior device", prior: func(p *DRAClaimObservation) {
			p.Device.Driver = ""
		}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			first := clearedDRAClaimJSON(t, tc.first)
			second := clearedDRAClaimJSON(t, tc.second)
			var reads, writes atomic.Int32
			srv := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.Method != http.MethodGet {
					writes.Add(1)
					http.Error(w, "writes forbidden", http.StatusMethodNotAllowed)
					return
				}
				if r.URL.RequestURI() != "/apis/resource.k8s.io/v1/namespaces/tenant-a/resourceclaims/gpu-claim-a" {
					t.Errorf("unexpected resource path %s", r.URL)
					http.NotFound(w, r)
					return
				}
				n := reads.Add(1)
				w.Header().Set("Content-Type", "application/json")
				switch {
				case n == 1 && tc.firstStatus != 0:
					w.WriteHeader(tc.firstStatus)
				case n == 2 && tc.secondStatus != 0:
					w.WriteHeader(tc.secondStatus)
				case n == 1:
					_, _ = fmt.Fprint(w, first)
				default:
					_, _ = fmt.Fprint(w, second)
				}
			}))
			defer srv.Close()
			client := srv.Client()
			client.Timeout = 3 * time.Second
			reader, err := NewKubernetesObservationReader(srv.URL, client)
			if err != nil {
				t.Fatal(err)
			}
			prior := previousDRAClaimAllocation()
			if tc.prior != nil {
				tc.prior(&prior)
			}
			got, err := reader.ObserveDRAClaimUnallocated(context.Background(), prior)
			if tc.valid {
				if err != nil || got.PreviousClaim != prior.Claim ||
					got.CurrentClaim.UID != "claim-uid-a" ||
					got.CurrentClaim.ResourceVersion != "151" ||
					got.PreviousPodUID != prior.PodUID ||
					got.PreviousDevice != prior.Device ||
					reads.Load() != 2 {
					t.Fatalf("stable unallocated Claim not observed: %+v err=%v reads=%d", got, err, reads.Load())
				}
			} else if err == nil || got != (DRAClaimUnallocatedObservation{}) {
				t.Fatalf("unsafe Claim release observation accepted: %+v err=%v", got, err)
			}
			if writes.Load() != 0 {
				t.Fatalf("observer issued %d Kubernetes writes", writes.Load())
			}
			if tc.prior != nil && reads.Load() != 0 {
				t.Fatalf("untrusted prior evidence reached Kubernetes: reads=%d", reads.Load())
			}
		})
	}
}
