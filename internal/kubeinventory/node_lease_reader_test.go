package kubeinventory

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

func TestObserveNodeLeasePinnedSourceRead(t *testing.T) {
	now := time.Date(2026, 11, 24, 0, 0, 30, 0, time.UTC)
	const node = `{"apiVersion":"v1","kind":"Node","metadata":{"name":"worker-a","uid":"node-uid-a","resourceVersion":"20"},"status":{"conditions":[{"type":"Ready","status":"True"}]}}`
	const lease = `{"apiVersion":"coordination.k8s.io/v1","kind":"Lease","metadata":{"name":"worker-a","namespace":"kube-node-lease","uid":"lease-uid-a","resourceVersion":"30","ownerReferences":[{"apiVersion":"v1","kind":"Node","name":"worker-a","uid":"node-uid-a"}]},"spec":{"holderIdentity":"worker-a","renewTime":"2026-11-24T00:00:20Z","leaseDurationSeconds":40}}`
	for _, tc := range []struct {
		name       string
		nodeSecond string
		leaseFirst string
		leaseLast  string
		wantFresh  bool
		wantError  error
	}{
		{name: "stable exact Node and Lease identities", wantFresh: true},
		{name: "Node successor UID between reads", nodeSecond: strings.Replace(node, "node-uid-a", "node-successor", 1)},
		{name: "Node resourceVersion changes", nodeSecond: strings.Replace(node, `"resourceVersion":"20"`, `"resourceVersion":"21"`, 1)},
		{name: "Lease replaced between reads", leaseLast: strings.Replace(lease, "lease-uid-a", "lease-successor", 1)},
		{name: "Lease renewed between reads", leaseLast: strings.Replace(lease, `"resourceVersion":"30"`, `"resourceVersion":"31"`, 1)},
		{name: "stale same-name Lease owner", leaseFirst: strings.Replace(lease, `"uid":"node-uid-a"`, `"uid":"old-node-uid"`, 1)},
		{name: "Node is terminating", nodeSecond: strings.Replace(node, `"resourceVersion":"20"`, `"resourceVersion":"20","deletionTimestamp":"2026-11-24T00:00:28Z"`, 1)},
		{name: "Lease forbidden", wantError: ErrForbidden},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var nodeReads, leaseReads, writes atomic.Int32
			reader, _ := fixtureReader(t, func(w http.ResponseWriter, r *http.Request) {
				if r.Method != http.MethodGet {
					writes.Add(1)
					w.WriteHeader(http.StatusMethodNotAllowed)
					return
				}
				switch r.URL.Path {
				case "/api/v1/nodes/worker-a":
					n := nodeReads.Add(1)
					if n == 2 && tc.nodeSecond != "" {
						_, _ = fmt.Fprint(w, tc.nodeSecond)
					} else {
						_, _ = fmt.Fprint(w, node)
					}
				case "/apis/coordination.k8s.io/v1/namespaces/kube-node-lease/leases/worker-a":
					n := leaseReads.Add(1)
					if tc.wantError != nil {
						w.WriteHeader(http.StatusForbidden)
						return
					}
					switch {
					case n == 1 && tc.leaseFirst != "":
						_, _ = fmt.Fprint(w, tc.leaseFirst)
					case n == 2 && tc.leaseLast != "":
						_, _ = fmt.Fprint(w, tc.leaseLast)
					default:
						_, _ = fmt.Fprint(w, lease)
					}
				default:
					t.Errorf("unexpected API request %s", r.URL.Path)
					http.NotFound(w, r)
				}
			})
			reader.now = func() time.Time { return now }
			got, err := reader.ObserveNodeLease(context.Background(), "tenant-a", []string{"project-a"}, "worker-a", "node-uid-a", time.Minute)
			if tc.wantError != nil {
				if !errors.Is(err, tc.wantError) || got.State == nodeLeaseFresh {
					t.Fatalf("failed to reject inaccessible source: assessment=%+v err=%v", got, err)
				}
			} else if err != nil || (got.State == nodeLeaseFresh) != tc.wantFresh {
				t.Fatalf("unexpected source identity result: %+v err=%v", got, err)
			}
			if nodeReads.Load() > 2 || leaseReads.Load() > 2 || writes.Load() != 0 {
				t.Fatalf("unbounded or mutating reads node=%d lease=%d writes=%d", nodeReads.Load(), leaseReads.Load(), writes.Load())
			}
			if tc.wantFresh && (nodeReads.Load() != 2 || leaseReads.Load() != 2) {
				t.Fatalf("positive result must reread both source UIDs: node=%d lease=%d", nodeReads.Load(), leaseReads.Load())
			}
		})
	}
}

func TestObserveNodeLeaseDeniesScopeAndInvalidNameBeforeNetwork(t *testing.T) {
	var calls atomic.Int32
	reader, _ := fixtureReader(t, func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		http.Error(w, "should not be called", http.StatusInternalServerError)
	})
	for _, tc := range []struct {
		tenant string
		project []string
		name string
		uid string
	}{
		{tenant: "outside-tenant", project: []string{"project-a"}, name: "worker-a", uid: "node-uid-a"},
		{tenant: "tenant-a", project: []string{"outside-project"}, name: "worker-a", uid: "node-uid-a"},
		{tenant: "tenant-a", project: []string{"project-a"}, name: "../leases", uid: "node-uid-a"},
		{tenant: "tenant-a", project: []string{"project-a"}, name: "worker-a", uid: ""},
	} {
		got, err := reader.ObserveNodeLease(context.Background(), tc.tenant, tc.project, tc.name, tc.uid, time.Minute)
		if err == nil || got.State == nodeLeaseFresh {
			t.Fatalf("invalid scope/identity accepted: %+v err=%v", got, err)
		}
	}
	if calls.Load() != 0 {
		t.Fatalf("untrusted request reached cluster API: %d", calls.Load())
	}
}
