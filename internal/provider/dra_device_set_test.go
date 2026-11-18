package provider

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"reflect"
	"sync/atomic"
	"testing"
	"time"
)

func TestObserveDRAPodDeviceSet(t *testing.T) {
	base := draTestQuery()
	base.ShareID = ""
	a := DRAAllocatedDevice{Request: base.Request, Driver: base.Driver, Pool: base.Pool, Device: base.Device}
	b := a
	b.Device = "device-b"
	q := DRADeviceSetQuery{Pod: DRAPodClaimQuery{Claim: base, ExpectedJobUID: "job-a", ExpectedNodeName: "node-a", PodClaimAlias: "gpu", ClaimTemplateName: "template-a"}, Devices: []DRAAllocatedDevice{a, b}}
	pod := `{"apiVersion":"v1","kind":"Pod","metadata":{"namespace":"tenant-a","name":"gpu-pod-a","uid":"pod-uid-a","resourceVersion":"10","ownerReferences":[{"apiVersion":"batch/v1","kind":"Job","uid":"job-a","controller":true}]},"spec":{"nodeName":"node-a","resourceClaims":[{"name":"gpu","resourceClaimTemplateName":"template-a"}],"containers":[{"name":"model","resources":{"claims":[{"name":"gpu"}]}}]},"status":{"resourceClaimStatuses":[{"name":"gpu","resourceClaimName":"gpu-claim-a"}]}}`
	claim := func(devices []DRAAllocatedDevice, change func(map[string]any)) string {
		return draModifiedFixture(t, func(v map[string]any) {
			raw, err := json.Marshal(devices)
			if err != nil {
				t.Fatal(err)
			}
			var rows []any
			if err := json.Unmarshal(raw, &rows); err != nil {
				t.Fatal(err)
			}
			v["status"].(map[string]any)["allocation"].(map[string]any)["devices"].(map[string]any)["results"] = rows
			if change != nil {
				change(v)
			}
		})
	}
	normal := claim([]DRAAllocatedDevice{a, b}, nil)
	foreign := b
	foreign.Device = "foreign"
	shared := b
	shared.ShareID = "shared"
	for _, tc := range []struct {
		name, first, second string
		ok                  bool
	}{
		{"exact set", normal, normal, true},
		{"order independent", normal, claim([]DRAAllocatedDevice{b, a}, nil), true},
		{"partial", claim([]DRAAllocatedDevice{a}, nil), normal, false},
		{"extra", claim([]DRAAllocatedDevice{a, b, foreign}, nil), normal, false},
		{"duplicate", claim([]DRAAllocatedDevice{a, a}, nil), normal, false},
		{"shared", claim([]DRAAllocatedDevice{a, shared}, nil), normal, false},
		{"same RV device drift", normal, claim([]DRAAllocatedDevice{a, foreign}, nil), false},
		{"RV drift", normal, claim([]DRAAllocatedDevice{a, b}, func(v map[string]any) { v["metadata"].(map[string]any)["resourceVersion"] = "151" }), false},
		{"deleting claim", claim([]DRAAllocatedDevice{a, b}, func(v map[string]any) { v["metadata"].(map[string]any)["deletionTimestamp"] = "2026-11-18T00:00:00Z" }), normal, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var reads atomic.Int32
			s := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
				if req.Method != http.MethodGet {
					t.Error("unexpected write")
				}
				switch req.URL.Path {
				case "/api/v1/namespaces/tenant-a/pods/gpu-pod-a":
					_, _ = w.Write([]byte(pod))
				case "/apis/resource.k8s.io/v1/namespaces/tenant-a/resourceclaims/gpu-claim-a":
					if reads.Add(1) == 1 {
						_, _ = w.Write([]byte(tc.first))
					} else {
						_, _ = w.Write([]byte(tc.second))
					}
				default:
					http.NotFound(w, req)
				}
			}))
			defer s.Close()
			client := s.Client()
			client.Timeout = time.Second
			reader, err := NewKubernetesObservationReader(s.URL, client)
			if err != nil {
				t.Fatal(err)
			}
			got, err := reader.ObserveDRAPodDeviceSet(context.Background(), q)
			if tc.ok {
				if err != nil || got.Claim.UID != base.ExpectedClaimUID || got.Pod.UID != base.ExpectedPodUID || got.ConsumerContainer != "model" || !reflect.DeepEqual(got.Devices, []DRAAllocatedDevice{a, b}) || reads.Load() != 2 {
					t.Fatalf("valid exact set rejected: %+v %v", got, err)
				}
			} else if err == nil || !reflect.DeepEqual(got, DRADeviceSetObservation{}) {
				t.Fatalf("invalid evidence accepted: %+v %v", got, err)
			}
		})
	}
}

func TestPDReplicaDeviceSetRequiresExactCountAndUniqueAddresses(t *testing.T) {
	q := PDJobSetQuery{TenantID: "tenant", WorkloadID: "w", IntentID: "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", Generation: 2, Namespace: "tenant", JobSetName: "pd", JobSetUID: "js", KueueName: "wl", KueueUID: "wluid", LocalQueue: "lq", ClusterQueue: "cq", GPUResource: "example.com/gpu", Prefill: PDPhaseQuota{1, 2}, Decode: PDPhaseQuota{1, 1}}
	replica := func(phase string, count int) PDReplicaClaimQuery {
		c := DRAClaimQuery{Namespace: "tenant", ClaimName: phase + "-claim", ExpectedClaimUID: phase + "-claim-uid", PodName: phase + "-pod", ExpectedPodUID: phase + "-pod-uid", Request: "gpu", Driver: "driver", Pool: "pool", Device: phase + "-gpu"}
		d := DRAAllocatedDevice{Request: c.Request, Driver: c.Driver, Pool: c.Pool, Device: c.Device}
		r := PDReplicaClaimQuery{Phase: phase, JobUID: phase + "-job", Pod: DRAPodClaimQuery{Claim: c, ExpectedJobUID: phase + "-job", ExpectedNodeName: "node", PodClaimAlias: "gpu", ClaimTemplateName: "template"}, Devices: []DRAAllocatedDevice{d}}
		if count > 1 {
			d.Device += "-2"
			r.Devices = append(r.Devices, d)
		}
		return r
	}
	p, d := replica("prefill", 2), replica("decode", 1)
	if !validPDReplicaClaims(q, []PDReplicaClaimQuery{p, d}) {
		t.Fatal("exact multi-device pair rejected")
	}
	p.Devices[1] = d.Devices[0]
	if validPDReplicaClaims(q, []PDReplicaClaimQuery{p, d}) {
		t.Fatal("second device overlaps decode")
	}
	p = replica("prefill", 2)
	p.Devices = p.Devices[:1]
	if validPDReplicaClaims(q, []PDReplicaClaimQuery{p, d}) {
		t.Fatal("partial multi-device replica accepted")
	}
}
