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

func draSliceList(t *testing.T, modify func(map[string]any)) string {
	t.Helper()
	v := map[string]any{
		"apiVersion": "resource.k8s.io/v1", "kind": "ResourceSliceList",
		"metadata": map[string]any{"resourceVersion": "500"},
		"items": []any{
			map[string]any{
				"apiVersion": "resource.k8s.io/v1", "kind": "ResourceSlice",
				"metadata": map[string]any{"name": "gpu-slice-a", "uid": "slice-uid-a", "resourceVersion": "450"},
				"spec": map[string]any{
					"driver": "gpu.example.com", "nodeName": "gpu-node-a",
					"pool":    map[string]any{"name": "pool-a", "generation": float64(4), "resourceSliceCount": float64(1)},
					"devices": []any{map[string]any{"name": "device-a"}},
				},
			},
		},
	}
	if modify != nil {
		modify(v)
	}
	b, err := json.Marshal(v)
	if err != nil {
		t.Fatal(err)
	}
	return string(b)
}

func draFirstSlice(list map[string]any) map[string]any {
	return list["items"].([]any)[0].(map[string]any)
}
func draSliceSpec(list map[string]any) map[string]any {
	return draFirstSlice(list)["spec"].(map[string]any)
}

// appendCompleteSecondSlice models a driver pool spanning multiple nodes.
// A successful read must see every slice of the highest generation.
func appendCompleteSecondSlice(v map[string]any) {
	draSliceSpec(v)["pool"].(map[string]any)["resourceSliceCount"] = float64(2)
	v["items"] = append(v["items"].([]any), map[string]any{
		"apiVersion": "resource.k8s.io/v1",
		"kind":       "ResourceSlice",
		"metadata":   map[string]any{"name": "gpu-slice-b", "uid": "slice-uid-b", "resourceVersion": "452"},
		"spec": map[string]any{
			"driver":   "gpu.example.com",
			"nodeName": "gpu-node-b",
			"pool":     map[string]any{"name": "pool-a", "generation": float64(4), "resourceSliceCount": float64(2)},
			"devices":  []any{map[string]any{"name": "device-b"}},
		},
	})
}

func TestDRAPoolInventoryRequiresCompleteCurrentGeneration(t *testing.T) {
	tests := []struct {
		name      string
		first     func(map[string]any)
		second    func(map[string]any)
		status    int
		want      bool
		wantCount int
	}{
		{name: "complete single-node pool with exact allocated device", want: true},
		{name: "complete two-slice pool with exact device and distinct nodes", first: appendCompleteSecondSlice, want: true, wantCount: 2},
		{name: "incomplete second read of formerly complete pool", first: appendCompleteSecondSlice},
		{name: "duplicate allocated device across same-generation slices", first: func(v map[string]any) {
			appendCompleteSecondSlice(v)
			v["items"].([]any)[1].(map[string]any)["spec"].(map[string]any)["devices"] = []any{map[string]any{"name": "device-a"}}
		}},
		{name: "missing allocated device in inventory", first: func(v map[string]any) { draSliceSpec(v)["devices"] = []any{map[string]any{"name": "other-device"}} }},
		{name: "device published for wrong node", first: func(v map[string]any) { draSliceSpec(v)["nodeName"] = "other-node" }},
		{name: "pool advertised two slices but only one seen", first: func(v map[string]any) { draSliceSpec(v)["pool"].(map[string]any)["resourceSliceCount"] = float64(2) }},
		{name: "old generation selected device but latest lacks it", first: func(v map[string]any) {
			copy := map[string]any{"apiVersion": "resource.k8s.io/v1", "kind": "ResourceSlice", "metadata": map[string]any{"name": "gpu-slice-new", "uid": "new-uid", "resourceVersion": "451"}, "spec": map[string]any{"driver": "gpu.example.com", "nodeName": "gpu-node-a", "pool": map[string]any{"name": "pool-a", "generation": float64(5), "resourceSliceCount": float64(1)}, "devices": []any{map[string]any{"name": "other-device"}}}}
			v["items"] = append(v["items"].([]any), copy)
		}},
		{name: "duplicate device in same current generation", first: func(v map[string]any) {
			s := draSliceSpec(v)
			s["devices"] = append(s["devices"].([]any), map[string]any{"name": "device-a"})
		}},
		{name: "incomplete slice list pagination", first: func(v map[string]any) { v["metadata"].(map[string]any)["continue"] = "next-page" }},
		{name: "missing slice UID", first: func(v map[string]any) { draFirstSlice(v)["metadata"].(map[string]any)["uid"] = "" }},
		{name: "missing resourceVersion", first: func(v map[string]any) { draFirstSlice(v)["metadata"].(map[string]any)["resourceVersion"] = "" }},
		{name: "driver differs from allocated Claim", first: func(v map[string]any) { draSliceSpec(v)["driver"] = "different.example.com" }},
		{name: "pool differs from Claim", first: func(v map[string]any) { draSliceSpec(v)["pool"].(map[string]any)["name"] = "different-pool" }},
		{name: "inventory RV changed after first list", second: func(v map[string]any) { draFirstSlice(v)["metadata"].(map[string]any)["resourceVersion"] = "460" }},
		{name: "inventory generation changed after first list", second: func(v map[string]any) { draSliceSpec(v)["pool"].(map[string]any)["generation"] = float64(5) }},
		{name: "list forbidden", status: http.StatusForbidden},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			first := draSliceList(t, tc.first)
			second := first
			if tc.second != nil {
				second = draSliceList(t, tc.second)
			}
			var podReads, claimReads, sliceReads, writes atomic.Int32
			ts := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.Method != http.MethodGet {
					writes.Add(1)
					http.Error(w, "read-only", http.StatusMethodNotAllowed)
					return
				}
				w.Header().Set("Content-Type", "application/json")
				switch r.URL.RequestURI() {
				case "/api/v1/namespaces/tenant-a/pods/gpu-pod-a":
					podReads.Add(1)
					_, _ = fmt.Fprint(w, podDRABindingFixture)
				case "/apis/resource.k8s.io/v1/namespaces/tenant-a/resourceclaims/gpu-claim-a":
					claimReads.Add(1)
					_, _ = fmt.Fprint(w, draClaimFixture)
				case "/apis/resource.k8s.io/v1/resourceslices":
					n := sliceReads.Add(1)
					if tc.status != 0 {
						w.WriteHeader(tc.status)
						return
					}
					if n == 1 {
						_, _ = fmt.Fprint(w, first)
					} else {
						_, _ = fmt.Fprint(w, second)
					}
				default:
					t.Errorf("unexpected request: %s %s", r.Method, r.URL.RequestURI())
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
			got, err := reader.ObserveDRAPoolInventory(context.Background(), DRAPoolInventoryQuery{Pod: podDRAQuery()})
			if tc.want {
				if err != nil || got.PodClaim.Pod.UID != "pod-uid-a" || got.PodClaim.Claim.Claim.UID != "claim-uid-a" ||
					got.Slice.UID != "slice-uid-a" || got.Slice.Name != "gpu-slice-a" ||
					got.PoolGeneration != 4 || got.PoolResourceSliceCount != func() int {
					if tc.wantCount > 0 {
						return tc.wantCount
					}
					return 1
				}() ||
					podReads.Load() != 4 || claimReads.Load() != 4 || sliceReads.Load() != 2 {
					t.Fatalf("no complete generation/UID-fenced inventory proof: %+v err=%v calls=%d/%d/%d", got, err, podReads.Load(), claimReads.Load(), sliceReads.Load())
				}
			} else if err == nil || got != (DRAPoolInventoryObservation{}) {
				t.Fatalf("promoted incomplete or changed inventory: %+v err=%v", got, err)
			}
			if writes.Load() != 0 {
				t.Fatalf("inventory reader mutated Kubernetes: %d", writes.Load())
			}
		})
	}
}
