package provider

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"reflect"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

func TestObservePairedPDClaimsMultiDevice(t *testing.T) {
	q := PDJobSetQuery{TenantID: "tenant", WorkloadID: "w", IntentID: strings.Repeat("a", 64), Generation: 2, Namespace: "tenant", JobSetName: "pd", JobSetUID: "js", KueueName: "wl", KueueUID: "wluid", LocalQueue: "lq", ClusterQueue: "cq", GPUResource: "example.com/gpu", Prefill: PDPhaseQuota{1, 2}, Decode: PDPhaseQuota{1, 1}}
	replica := func(phase string, count int) PDReplicaClaimQuery {
		name := phase + "-gpu"
		c := DRAClaimQuery{Namespace: q.Namespace, ClaimName: phase + "-claim", ExpectedClaimUID: phase + "-claim-uid", PodName: phase + "-pod", ExpectedPodUID: phase + "-pod-uid", Request: "gpu", Driver: "driver", Pool: "pool", Device: name}
		r := PDReplicaClaimQuery{Phase: phase, JobUID: phase + "-job-uid", Pod: DRAPodClaimQuery{Claim: c, ExpectedJobUID: phase + "-job-uid", ExpectedNodeName: "node", PodClaimAlias: "gpu", ClaimTemplateName: "template"}}
		for i := 0; i < count; i++ {
			d := expectedDevice(c)
			if i != 0 {
				d.Device = fmt.Sprintf("%s-%d", name, i)
			}
			r.Devices = append(r.Devices, d)
		}
		return r
	}
	pins := []PDReplicaClaimQuery{replica("prefill", 2), replica("decode", 1)}
	jobset := `{"metadata":{"name":"pd","namespace":"tenant","uid":"js","resourceVersion":"11","generation":2,"labels":{"kueue.x-k8s.io/queue-name":"lq"},"annotations":{"compute-control-panel.io/tenant-id":"tenant","compute-control-panel.io/workload-id":"w","compute-control-panel.io/desired-generation":"2","compute-control-panel.io/pd-intent-id":"` + q.IntentID + `"}},"spec":{"suspend":false,"replicatedJobs":[{"name":"prefill","replicas":1,"template":{"spec":{"parallelism":1,"completions":1}}},{"name":"decode","replicas":1,"template":{"spec":{"parallelism":1,"completions":1}}}]}}`
	workload := `{"metadata":{"name":"wl","namespace":"tenant","uid":"wluid","resourceVersion":"22","generation":3,"ownerReferences":[{"apiVersion":"jobset.x-k8s.io/v1alpha2","kind":"JobSet","uid":"js","controller":true}]},"spec":{"queueName":"lq","podSets":[{"name":"prefill","count":1},{"name":"decode","count":1}]},"status":{"admission":{"clusterQueue":"cq","podSetAssignments":[{"name":"prefill","count":1,"flavors":{"example.com/gpu":"gpu"},"resourceUsage":{"example.com/gpu":"2"}},{"name":"decode","count":1,"flavors":{"example.com/gpu":"gpu"},"resourceUsage":{"example.com/gpu":"1"}}]},"conditions":[{"type":"Admitted","status":"True","observedGeneration":3},{"type":"QuotaReserved","status":"True","observedGeneration":3}]}}`
	jsPath := "/apis/jobset.x-k8s.io/v1alpha2/namespaces/tenant/jobsets/pd"
	wlPath := "/apis/kueue.x-k8s.io/v1beta1/namespaces/tenant/workloads/wl"
	bodies := map[string]string{jsPath: jobset, wlPath: workload}
	for _, pin := range pins {
		c := pin.Pod.Claim
		jobName := "pd-" + pin.Phase + "-0"
		bodies["/apis/batch/v1/namespaces/tenant/jobs/"+jobName] = fmt.Sprintf(`{"metadata":{"name":%q,"namespace":"tenant","uid":%q,"resourceVersion":"30","generation":1,"labels":{"jobset.sigs.k8s.io/jobset-name":"pd","jobset.sigs.k8s.io/jobset-uid":"js","jobset.sigs.k8s.io/replicatedjob-name":%q,"jobset.sigs.k8s.io/job-index":"0"},"ownerReferences":[{"apiVersion":"jobset.x-k8s.io/v1alpha2","kind":"JobSet","uid":"js","controller":true}]},"spec":{"suspend":false,"parallelism":1,"completions":1}}`, jobName, pin.JobUID, pin.Phase)
		bodies["/api/v1/namespaces/tenant/pods/"+c.PodName] = fmt.Sprintf(`{"apiVersion":"v1","kind":"Pod","metadata":{"namespace":"tenant","name":%q,"uid":%q,"resourceVersion":"40","ownerReferences":[{"apiVersion":"batch/v1","kind":"Job","uid":%q,"controller":true}]},"spec":{"nodeName":"node","resourceClaims":[{"name":"gpu","resourceClaimTemplateName":"template"}],"containers":[{"name":"model","resources":{"claims":[{"name":"gpu"}]}}]},"status":{"resourceClaimStatuses":[{"name":"gpu","resourceClaimName":%q}]}}`, c.PodName, c.ExpectedPodUID, pin.JobUID, c.ClaimName)
		devices, err := json.Marshal(pin.Devices)
		if err != nil {
			t.Fatal(err)
		}
		bodies["/apis/resource.k8s.io/v1/namespaces/tenant/resourceclaims/"+c.ClaimName] = fmt.Sprintf(`{"apiVersion":"resource.k8s.io/v1","kind":"ResourceClaim","metadata":{"namespace":"tenant","name":%q,"uid":%q,"resourceVersion":"50"},"status":{"reservedFor":[{"resource":"pods","name":%q,"uid":%q}],"allocation":{"devices":{"results":%s}}}}`, c.ClaimName, c.ExpectedClaimUID, c.PodName, c.ExpectedPodUID, devices)
	}
	for _, mode := range []string{"exact", "second-sweep-claim-drift", "final-admission-drift"} {
		t.Run(mode, func(t *testing.T) {
			var claims, admissions atomic.Int32
			s := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				body, ok := bodies[r.URL.Path]
				if !ok || r.Method != http.MethodGet {
					http.NotFound(w, r)
					return
				}
				if r.URL.Path == "/apis/resource.k8s.io/v1/namespaces/tenant/resourceclaims/prefill-claim" {
					if claims.Add(1) > 2 && mode == "second-sweep-claim-drift" {
						body = strings.Replace(body, `"resourceVersion":"50"`, `"resourceVersion":"51"`, 1)
					}
				}
				if r.URL.Path == wlPath {
					if admissions.Add(1) > 2 && mode == "final-admission-drift" {
						body = strings.Replace(body, `"resourceVersion":"22"`, `"resourceVersion":"23"`, 1)
					}
				}
				_, _ = w.Write([]byte(body))
			}))
			defer s.Close()
			client := s.Client()
			client.Timeout = time.Second
			reader, err := NewKubernetesObservationReader(s.URL, client)
			if err != nil {
				t.Fatal(err)
			}
			got, err := reader.ObservePairedPDClaims(context.Background(), q, pins)
			if mode != "exact" {
				if err == nil || !reflect.DeepEqual(got, PDJobSetDRAEvidence{}) {
					t.Fatalf("unstable evidence accepted: %+v %v", got, err)
				}
				return
			}
			if err != nil || len(got.Replicas) != 2 || got.Admission.ReservedGPUCount != 3 || got.Admission.PhysicalReady ||
				!reflect.DeepEqual(got.Replicas[0].DeviceSet.Devices, pins[0].Devices) ||
				!reflect.DeepEqual(got.Replicas[1].DeviceSet.Devices, pins[1].Devices) ||
				got.Replicas[0].Pod != (DRAPodClaimObservation{}) || got.Replicas[1].Pod.Claim.Device != pins[1].Devices[0] ||
				claims.Load() != 4 || admissions.Load() != 4 {
				t.Fatalf("paired device proof missing: %+v %v", got, err)
			}
		})
	}
}
