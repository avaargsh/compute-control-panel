package provider

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

func TestObserveDRAMultiDeviceResourceSlicesAndNodeUID(t *testing.T) {
	q := draTestQuery()
	q.ShareID = ""
	expected := []DRAAllocatedDevice{
		{Request: "gpu", Driver: q.Driver, Pool: q.Pool, Device: "device-a"},
		{Request: "gpu", Driver: q.Driver, Pool: q.Pool, Device: "device-b"},
	}
	query := DRAMultiSliceQuery{
		Devices: DRADeviceSetQuery{
			Pod:     DRAPodClaimQuery{Claim: q, ExpectedJobUID: "job-a", ExpectedNodeName: "node-a", PodClaimAlias: "gpu", ClaimTemplateName: "gpu-template"},
			Devices: expected,
		},
		ExpectedNodeUID: "node-uid-a",
	}
	pod := `{"apiVersion":"v1","kind":"Pod","metadata":{"namespace":"tenant-a","name":"gpu-pod-a","uid":"pod-uid-a","resourceVersion":"40","ownerReferences":[{"apiVersion":"batch/v1","kind":"Job","uid":"job-a","controller":true}]},"spec":{"nodeName":"node-a","resourceClaims":[{"name":"gpu","resourceClaimTemplateName":"gpu-template"}],"containers":[{"name":"model","resources":{"claims":[{"name":"gpu"}]}}]},"status":{"resourceClaimStatuses":[{"name":"gpu","resourceClaimName":"gpu-claim-a"}]}}`
	claim := draModifiedFixture(t, func(v map[string]any) {
		status := v["status"].(map[string]any)
		allocation := status["allocation"].(map[string]any)["devices"].(map[string]any)
		allocation["results"] = []any{
			map[string]any{"request": "gpu", "driver": q.Driver, "pool": q.Pool, "device": "device-a"},
			map[string]any{"request": "gpu", "driver": q.Driver, "pool": q.Pool, "device": "device-b"},
		}
	})
	list := `{"apiVersion":"resource.k8s.io/v1","kind":"ResourceSliceList","metadata":{"resourceVersion":"slices-rv"},"items":[
		{"metadata":{"name":"slice-a","uid":"slice-uid-a","resourceVersion":"31"},"spec":{"driver":"gpu.example.com","nodeName":"node-a","pool":{"name":"pool-a","generation":5,"resourceSliceCount":2},"devices":[{"name":"device-a"}]}},
		{"metadata":{"name":"slice-b","uid":"slice-uid-b","resourceVersion":"32"},"spec":{"driver":"gpu.example.com","nodeName":"node-a","pool":{"name":"pool-a","generation":5,"resourceSliceCount":2},"devices":[{"name":"device-b"}]}}]}`
	node := `{"apiVersion":"v1","kind":"Node","metadata":{"name":"node-a","uid":"node-uid-a","resourceVersion":"22"}}`
	for _, tc := range []struct {
		name       string
		secondList string
		secondNode string
		want       bool
	}{
		{"complete", list, node, true},
		{"ResourceSlice RV drift", strings.Replace(list, `"resourceVersion":"32"`, `"resourceVersion":"33"`, 1), node, false},
		{"missing second device", strings.Replace(list, `{"name":"device-b"}`, `{"name":"other"}`, 1), node, false},
		{"second Node UID", list, strings.Replace(node, "node-uid-a", "node-successor", 1), false},
		{"missing slice", strings.Replace(list, `"resourceSliceCount":2`, `"resourceSliceCount":3`, 1), node, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var nodeReads, sliceReads, claimReads atomic.Int32
			server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.Method != http.MethodGet {
					t.Errorf("unexpected mutation %s", r.Method)
					http.Error(w, "no", 405)
					return
				}
				switch r.URL.Path {
				case "/api/v1/namespaces/tenant-a/pods/gpu-pod-a":
					_, _ = w.Write([]byte(pod))
				case "/apis/resource.k8s.io/v1/namespaces/tenant-a/resourceclaims/gpu-claim-a":
					claimReads.Add(1)
					_, _ = w.Write([]byte(claim))
				case "/apis/resource.k8s.io/v1/resourceslices":
					if sliceReads.Add(1) == 1 {
						_, _ = w.Write([]byte(list))
					} else {
						_, _ = w.Write([]byte(tc.secondList))
					}
				case "/api/v1/nodes/node-a":
					if nodeReads.Add(1) == 1 {
						_, _ = w.Write([]byte(node))
					} else {
						_, _ = w.Write([]byte(tc.secondNode))
					}
				default:
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
			got, err := reader.ObserveDRAMultiSlice(context.Background(), query)
			if tc.want {
				if err != nil || len(got.Devices) != 2 || got.Devices[0].Device != "device-a" ||
					got.Devices[0].SliceUID != "slice-uid-a" || got.Devices[1].SliceUID != "slice-uid-b" ||
					got.NodeUID != "node-uid-a" || got.PhysicalPrepared || nodeReads.Load() != 2 || sliceReads.Load() != 2 || claimReads.Load() != 4 {
					t.Fatalf("unexpected verified structural evidence %+v %v counts node=%d slices=%d claim=%d", got, err, nodeReads.Load(), sliceReads.Load(), claimReads.Load())
				}
			} else if err == nil || len(got.Devices) != 0 || got.NodeUID != "" {
				t.Fatalf("unproven source accepted %+v %v", got, err)
			}
		})
	}
}
