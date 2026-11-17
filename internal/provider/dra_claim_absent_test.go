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

const absentClaimList = "{\"apiVersion\":\"resource.k8s.io/v1\",\"kind\":\"ResourceClaimList\",\"metadata\":{\"resourceVersion\":\"440\"},\"items\":[]}"
const oldClaimItem = "{\"metadata\":{\"namespace\":\"tenant-a\",\"name\":\"gpu-claim-a\",\"uid\":\"claim-uid-a\",\"resourceVersion\":\"151\"}}"
const successorClaimItem = "{\"metadata\":{\"namespace\":\"tenant-a\",\"name\":\"gpu-claim-a\",\"uid\":\"successor-uid\",\"resourceVersion\":\"151\"}}"
const unrelatedClaimItem = "{\"metadata\":{\"namespace\":\"tenant-a\",\"name\":\"other-claim\",\"uid\":\"other-uid\",\"resourceVersion\":\"151\"}}"

// A stable namespace ResourceClaim LIST can establish only API object absence.
// It is distinct from a same-UID, unallocated Claim and does NOT attest
// kubelet NodeUnprepare, device release, Allocation RELEASED, or cost FINAL.
func TestDRAClaimUIDAbsentRequiresStableCompleteNamespaceList(t *testing.T) {
	withItems := func(items string) string { return strings.Replace(absentClaimList, "[]", "["+items+"]", 1) }
	tests := []struct {
		name   string
		first  string
		second string
		status int
		mutate func(*DRAClaimObservation)
		valid  bool
	}{
		{name: "old Claim absent from two complete snapshots", valid: true},
		{name: "unrelated Claim allowed in complete snapshot", first: withItems(unrelatedClaimItem), second: withItems(unrelatedClaimItem), valid: true},
		{name: "old Claim UID still present", first: withItems(oldClaimItem)},
		{name: "same-name successor Claim", first: withItems(successorClaimItem)},
		{name: "old Claim UID under different name", first: withItems(strings.Replace(oldClaimItem, "gpu-claim-a", "another-claim", 1))},
		{name: "old Claim reappears second read", second: withItems(oldClaimItem)},
		{name: "successor appears second read", second: withItems(successorClaimItem)},
		{name: "different unrelated items between reads", second: withItems(unrelatedClaimItem)},
		{name: "list RV drift", second: strings.Replace(absentClaimList, "\"440\"", "\"441\"", 1)},
		{name: "wrong API kind", first: strings.Replace(absentClaimList, "ResourceClaimList", "ResourceClaimTemplateList", 1)},
		{name: "wrong API version", first: strings.Replace(absentClaimList, "resource.k8s.io/v1", "resource.k8s.io/v1beta1", 1)},
		{name: "missing list resourceVersion", first: strings.Replace(absentClaimList, "\"440\"", "\"\"", 1)},
		{name: "paginated LIST", first: strings.Replace(absentClaimList, "\"resourceVersion\":\"440\"", "\"resourceVersion\":\"440\",\"continue\":\"more\"", 1)},
		{name: "nil items not a complete LIST", first: strings.Replace(absentClaimList, "\"items\":[]", "\"items\":null", 1)},
		{name: "malformed item", first: withItems("{\"metadata\":{\"namespace\":\"tenant-a\",\"name\":\"other-claim\",\"uid\":\"other-uid\"}}")},
		{name: "foreign namespace in namespace LIST", first: withItems(strings.Replace(unrelatedClaimItem, "tenant-a", "tenant-b", 1))},
		{name: "duplicate identity", first: withItems(unrelatedClaimItem + "," + unrelatedClaimItem)},
		{name: "HTTP forbidden", status: http.StatusForbidden},
		{name: "HTTP 404 is not a complete list", status: http.StatusNotFound},
		{name: "malformed prior UID", mutate: func(p *DRAClaimObservation) { p.Claim.UID = "" }},
		{name: "malformed prior namespace", mutate: func(p *DRAClaimObservation) { p.Claim.Namespace = "../tenant-b" }},
		{name: "malformed prior device", mutate: func(p *DRAClaimObservation) { p.Device.Device = "" }},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			var reads, writes atomic.Int32
			server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.Method != http.MethodGet {
					writes.Add(1)
					http.Error(w, "read only", http.StatusMethodNotAllowed)
					return
				}
				if r.URL.RequestURI() != "/apis/resource.k8s.io/v1/namespaces/tenant-a/resourceclaims" {
					t.Errorf("unexpected API path %s", r.URL.RequestURI())
					http.NotFound(w, r)
					return
				}
				n := reads.Add(1)
				if tc.status != 0 {
					http.Error(w, "blocked", tc.status)
					return
				}
				w.Header().Set("Content-Type", "application/json")
				body := absentClaimList
				if n == 1 && tc.first != "" {
					body = tc.first
				}
				if n == 2 && tc.second != "" {
					body = tc.second
				}
				_, _ = fmt.Fprint(w, body)
			}))
			defer server.Close()
			client := server.Client()
			client.Timeout = 3 * time.Second
			reader, err := NewKubernetesObservationReader(server.URL, client)
			if err != nil {
				t.Fatal(err)
			}
			prior := previousDRAClaimAllocation()
			if tc.mutate != nil {
				tc.mutate(&prior)
			}
			got, err := reader.ObserveDRAClaimUIDAbsent(context.Background(), prior)
			if tc.valid {
				if err != nil || got.PreviousClaim != prior.Claim || got.ClaimListResourceVersion != "440" || reads.Load() != 2 {
					t.Fatalf("stable complete Claim API absence missing: got=%+v err=%v reads=%d", got, err, reads.Load())
				}
			} else if err == nil || got != (DRAClaimUIDAbsentObservation{}) {
				t.Fatalf("incomplete or reused Claim absence accepted: got=%+v err=%v", got, err)
			}
			if tc.mutate != nil && reads.Load() != 0 {
				t.Fatalf("invalid prior Claim caused API calls: %d", reads.Load())
			}
			if writes.Load() != 0 {
				t.Fatal("Claim observer issued a Kubernetes mutation")
			}
		})
	}
}
