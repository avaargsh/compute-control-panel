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

const draClaimFixture = "{\"apiVersion\":\"resource.k8s.io/v1\",\"kind\":\"ResourceClaim\",\"metadata\":{\"namespace\":\"tenant-a\",\"name\":\"gpu-claim-a\",\"uid\":\"claim-uid-a\",\"resourceVersion\":\"150\"},\"status\":{\"reservedFor\":[{\"resource\":\"pods\",\"name\":\"gpu-pod-a\",\"uid\":\"pod-uid-a\"}],\"allocation\":{\"devices\":{\"results\":[{\"request\":\"gpu\",\"driver\":\"gpu.example.com\",\"pool\":\"pool-a\",\"device\":\"device-a\",\"shareID\":\"share-a\"}]}}}}"

func draTestQuery() DRAClaimQuery {
	return DRAClaimQuery{
		Namespace: "tenant-a", ClaimName: "gpu-claim-a", ExpectedClaimUID: "claim-uid-a",
		PodName: "gpu-pod-a", ExpectedPodUID: "pod-uid-a",
		Request: "gpu", Driver: "gpu.example.com", Pool: "pool-a",
		Device: "device-a", ShareID: "share-a",
	}
}

func draModifiedFixture(t *testing.T, change func(map[string]any)) string {
	t.Helper()
	var v map[string]any
	if err := json.Unmarshal([]byte(draClaimFixture), &v); err != nil {
		t.Fatal(err)
	}
	if change != nil {
		change(v)
	}
	raw, err := json.Marshal(v)
	if err != nil {
		t.Fatal(err)
	}
	return string(raw)
}

func draFirstDevice(v map[string]any) map[string]any {
	status := v["status"].(map[string]any)
	return status["allocation"].(map[string]any)["devices"].(map[string]any)["results"].([]any)[0].(map[string]any)
}

// This reads a scheduler-side DRA v1 claim allocation, not proof of kubelet
// device preparation, GPU usage, GPU readiness or device release.
func TestDRAClaimReadOnlyUIDBoundObservation(t *testing.T) {
	tests := []struct {
		name   string
		change func(map[string]any)
		status int
		valid  bool
	}{
		{name: "exact claim, Pod reservation and allocated device", valid: true},
		{name: "successor Claim UID", change: func(v map[string]any) { v["metadata"].(map[string]any)["uid"] = "successor" }},
		{name: "missing resourceVersion", change: func(v map[string]any) { v["metadata"].(map[string]any)["resourceVersion"] = "" }},
		{name: "foreign reserved Pod", change: func(v map[string]any) {
			v["status"].(map[string]any)["reservedFor"].([]any)[0].(map[string]any)["uid"] = "other-uid"
		}},
		{name: "wrong consumer type", change: func(v map[string]any) {
			v["status"].(map[string]any)["reservedFor"].([]any)[0].(map[string]any)["resource"] = "jobs"
		}},
		{name: "no Pod reservation", change: func(v map[string]any) { v["status"].(map[string]any)["reservedFor"] = []any{} }},
		{name: "second Pod reservation", change: func(v map[string]any) {
			v["status"].(map[string]any)["reservedFor"] = []any{map[string]any{"resource": "pods", "name": "gpu-pod-a", "uid": "pod-uid-a"}, map[string]any{"resource": "pods", "name": "other", "uid": "other"}}
		}},
		{name: "unallocated Claim", change: func(v map[string]any) { v["status"].(map[string]any)["allocation"] = nil }},
		{name: "wrong driver", change: func(v map[string]any) { draFirstDevice(v)["driver"] = "other.example.com" }},
		{name: "wrong pool", change: func(v map[string]any) { draFirstDevice(v)["pool"] = "other-pool" }},
		{name: "wrong device", change: func(v map[string]any) { draFirstDevice(v)["device"] = "other-device" }},
		{name: "wrong request", change: func(v map[string]any) { draFirstDevice(v)["request"] = "wrong-request" }},
		{name: "missing ShareID", change: func(v map[string]any) { delete(draFirstDevice(v), "shareID") }},
		{name: "second allocated device", change: func(v map[string]any) {
			devices := v["status"].(map[string]any)["allocation"].(map[string]any)["devices"].(map[string]any)
			devices["results"] = append(devices["results"].([]any), map[string]any{"request": "gpu", "driver": "gpu.example.com", "pool": "pool-a", "device": "other"})
		}},
		{name: "unexpected API kind", change: func(v map[string]any) { v["kind"] = "ResourceClaimTemplate" }},
		{name: "Claim 404 not proof of GPU release", status: http.StatusNotFound},
		{name: "forbidden Claim read is not proof", status: http.StatusForbidden},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			var calls atomic.Int32
			body := draModifiedFixture(t, tc.change)
			ts := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				calls.Add(1)
				if r.Method != http.MethodGet || r.URL.RequestURI() != "/apis/resource.k8s.io/v1/namespaces/tenant-a/resourceclaims/gpu-claim-a" {
					t.Errorf("unexpected Kubernetes operation %s %s", r.Method, r.URL.RequestURI())
					http.Error(w, "invalid", http.StatusBadRequest)
					return
				}
				w.Header().Set("Content-Type", "application/json")
				if tc.status != 0 {
					w.WriteHeader(tc.status)
				}
				_, _ = fmt.Fprint(w, body)
			}))
			defer ts.Close()
			client := ts.Client()
			client.Timeout = 3 * time.Second
			reader, err := NewKubernetesObservationReader(ts.URL, client)
			if err != nil {
				t.Fatal(err)
			}
			observed, err := reader.ObserveDRAClaim(context.Background(), draTestQuery())
			if tc.valid {
				if err != nil || observed.Claim.UID != "claim-uid-a" ||
					observed.Claim.ResourceVersion != "150" || observed.PodUID != "pod-uid-a" ||
					observed.Device.Driver != "gpu.example.com" || observed.Device.Pool != "pool-a" ||
					observed.Device.Device != "device-a" || observed.Device.ShareID != "share-a" {
					t.Fatalf("expected UID-bound claim observation, got %+v err=%v", observed, err)
				}
			} else if err == nil || observed != (DRAClaimObservation{}) {
				t.Fatalf("unsafe DRA claim promoted: %+v err=%v", observed, err)
			}
			wantReads := int32(1)
			if tc.valid {
				wantReads = 2 // read back UID/resourceVersion before trusting the snapshot
			}
			if calls.Load() != wantReads {
				t.Fatalf("expected %d read-only requests, got %d", wantReads, calls.Load())
			}
		})
	}
}

func TestDRAClaimInvalidQueryRejectedBeforeHTTPS(t *testing.T) {
	var calls atomic.Int32
	ts := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		t.Errorf("unsafe query reached Kubernetes: %s", r.URL)
	}))
	defer ts.Close()
	client := ts.Client()
	client.Timeout = 3 * time.Second
	reader, err := NewKubernetesObservationReader(ts.URL, client)
	if err != nil {
		t.Fatal(err)
	}
	tests := []struct {
		name   string
		change func(*DRAClaimQuery)
	}{
		{"namespace traversal", func(q *DRAClaimQuery) { q.Namespace = "../default" }},
		{"claim path escape", func(q *DRAClaimQuery) { q.ClaimName = "claim/other" }},
		{"missing Claim UID", func(q *DRAClaimQuery) { q.ExpectedClaimUID = "" }},
		{"invalid Pod name", func(q *DRAClaimQuery) { q.PodName = "PodUppercase" }},
		{"missing Pod UID", func(q *DRAClaimQuery) { q.ExpectedPodUID = "" }},
		{"invalid pool", func(q *DRAClaimQuery) { q.Pool = "pool-a\nX-Fake:yes" }},
		{"missing driver", func(q *DRAClaimQuery) { q.Driver = "" }},
		{"invalid ShareID", func(q *DRAClaimQuery) { q.ShareID = "share-a\n" }},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			query := draTestQuery()
			tc.change(&query)
			got, err := reader.ObserveDRAClaim(context.Background(), query)
			if err == nil || got != (DRAClaimObservation{}) {
				t.Fatalf("invalid query accepted: %+v err=%v", got, err)
			}
		})
	}
	if calls.Load() != 0 {
		t.Fatalf("invalid queries reached Kubernetes: %d", calls.Load())
	}
}

func TestDRAClaimDetectsConcurrentReplacementAndResourceVersionDrift(t *testing.T) {
	tests := []struct {
		name      string
		second    func(map[string]any)
		status    int
		wantValid bool
	}{
		{name: "unchanged exact resource", wantValid: true},
		{name: "same-name successor UID", second: func(v map[string]any) {
			v["metadata"].(map[string]any)["uid"] = "claim-successor-uid"
		}},
		{name: "new resourceVersion with same UID", second: func(v map[string]any) {
			v["metadata"].(map[string]any)["resourceVersion"] = "151"
		}},
		{name: "claim removed after first GET", status: http.StatusNotFound},
		{name: "permissions revoked after first GET", status: http.StatusForbidden},
		{name: "missing UID on second GET", second: func(v map[string]any) {
			v["metadata"].(map[string]any)["uid"] = ""
		}},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			secondBody := draModifiedFixture(t, tc.second)
			var calls atomic.Int32
			ts := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				count := calls.Add(1)
				if r.Method != http.MethodGet ||
					r.URL.RequestURI() != "/apis/resource.k8s.io/v1/namespaces/tenant-a/resourceclaims/gpu-claim-a" {
					t.Errorf("unexpected mutation/path %s %s", r.Method, r.URL.RequestURI())
					http.Error(w, "forbidden", http.StatusMethodNotAllowed)
					return
				}
				w.Header().Set("Content-Type", "application/json")
				if count == 1 {
					_, _ = fmt.Fprint(w, draClaimFixture)
					return
				}
				if tc.status != 0 {
					w.WriteHeader(tc.status)
					return
				}
				_, _ = fmt.Fprint(w, secondBody)
			}))
			defer ts.Close()
			client := ts.Client()
			client.Timeout = 3 * time.Second
			reader, err := NewKubernetesObservationReader(ts.URL, client)
			if err != nil {
				t.Fatal(err)
			}
			got, err := reader.ObserveDRAClaim(context.Background(), draTestQuery())
			if tc.wantValid {
				if err != nil || got.Claim.UID != "claim-uid-a" ||
					got.Claim.ResourceVersion != "150" || got.PodUID != "pod-uid-a" {
					t.Fatalf("stable claim not observed: %+v err=%v", got, err)
				}
			} else if err == nil || got != (DRAClaimObservation{}) {
				t.Fatalf("changed or unavailable Claim accepted: %+v err=%v", got, err)
			}
			if calls.Load() != 2 {
				t.Fatalf("Claim double read was not performed: calls=%d", calls.Load())
			}
		})
	}
}
