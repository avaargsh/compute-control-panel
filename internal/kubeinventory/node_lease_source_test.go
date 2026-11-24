package kubeinventory

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"strings"
	"testing"
	"time"
)

const nodeLeaseSourceFixture = `{"apiVersion":"coordination.k8s.io/v1","kind":"Lease","metadata":{"namespace":"kube-node-lease","name":"worker-a","uid":"lease-uid-a","resourceVersion":"900","ownerReferences":[{"apiVersion":"v1","kind":"Node","name":"worker-a","uid":"node-uid-a"}]},"spec":{"holderIdentity":"worker-a","renewTime":"2026-11-21T07:59:55Z","leaseDurationSeconds":40}}`

func nodeLeaseSourceNodeList(uid string, terminating bool) string {
	deletion := ""
	if terminating {
		deletion = `,"deletionTimestamp":"2026-11-21T07:59:50Z"`
	}
	return fmt.Sprintf(`{"metadata":{"resourceVersion":"777"},"items":[{"metadata":{"name":"worker-a","uid":%q,"resourceVersion":"700"%s},"status":{"conditions":[{"type":"Ready","status":"True"}]}}]}`, uid, deletion)
}

func TestObserveNodeLeaseReadsExactKubernetesSourceAndFencesNode(t *testing.T) {
	nodeReads, leaseReads, writes := 0, 0, 0
	reader, _ := fixtureReader(t, func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			writes++
			w.WriteHeader(http.StatusMethodNotAllowed)
			return
		}
		switch r.URL.Path {
		case "/api/v1/nodes":
			nodeReads++
			_, _ = fmt.Fprint(w, nodeLeaseSourceNodeList("node-uid-a", false))
		case "/apis/coordination.k8s.io/v1/namespaces/kube-node-lease/leases/worker-a":
			leaseReads++
			if r.Header.Get("Authorization") != "Bearer node-reader-token" {
				w.WriteHeader(http.StatusUnauthorized)
				return
			}
			_, _ = fmt.Fprint(w, nodeLeaseSourceFixture)
		default:
			t.Errorf("unexpected request: %s %s", r.Method, r.URL.Path)
			http.NotFound(w, r)
		}
	})
	got, err := reader.observeNodeLease(context.Background(), "tenant-a", []string{"project-a"}, "worker-a", "node-uid-a", 20*time.Second)
	if err != nil || got.Assessment.State != nodeLeaseFresh || got.Assessment.ReasonCode != "" ||
		got.NodeUID != "node-uid-a" || got.NodeResourceVersion != "700" ||
		got.LeaseUID != "lease-uid-a" || got.LeaseResourceVersion != "900" ||
		!got.Assessment.ExpiresAt.Equal(time.Date(2026, 11, 21, 8, 0, 15, 0, time.UTC)) ||
		nodeReads != 2 || leaseReads != 1 || writes != 0 {
		t.Fatalf("no exact-source heartbeat: %+v err=%v node/lease/write=%d/%d/%d", got, err, nodeReads, leaseReads, writes)
	}
}

func TestObserveNodeLeaseRejectsNodeReplacementAndTermination(t *testing.T) {
	for _, tc := range []struct {
		name        string
		secondUID   string
		terminating bool
	}{
		{"same name successor", "node-uid-successor", false},
		{"terminating after lease read", "node-uid-a", true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			nodeReads := 0
			reader, _ := fixtureReader(t, func(w http.ResponseWriter, r *http.Request) {
				switch r.URL.Path {
				case "/api/v1/nodes":
					nodeReads++
					if nodeReads == 1 {
						_, _ = fmt.Fprint(w, nodeLeaseSourceNodeList("node-uid-a", false))
					} else {
						_, _ = fmt.Fprint(w, nodeLeaseSourceNodeList(tc.secondUID, tc.terminating))
					}
				case "/apis/coordination.k8s.io/v1/namespaces/kube-node-lease/leases/worker-a":
					_, _ = fmt.Fprint(w, nodeLeaseSourceFixture)
				default:
					http.NotFound(w, r)
				}
			})
			got, err := reader.observeNodeLease(context.Background(), "tenant-a", []string{"project-a"}, "worker-a", "node-uid-a", 20*time.Second)
			if err != nil || got.Assessment.State != nodeLeaseUnknown ||
				got.Assessment.ReasonCode != "NODE_CHANGED_DURING_LEASE_READ" ||
				!got.Assessment.ExpiresAt.IsZero() || nodeReads != 2 {
				t.Fatalf("Node successor/termination accepted: %+v err=%v nodes=%d", got, err, nodeReads)
			}
		})
	}
}

func TestObserveNodeLeaseMissingOrForbiddenCannotBeFresh(t *testing.T) {
	for _, tc := range []struct {
		name   string
		status int
		reason string
		err    error
	}{
		{"missing Lease", http.StatusNotFound, "LEASE_MISSING", nil},
		{"permission denied", http.StatusForbidden, "", ErrLeaseForbidden},
		{"unauthorized", http.StatusUnauthorized, "", ErrLeaseUnauthorized},
		{"redirect rejected", http.StatusFound, "", ErrLeaseUnavailable},
	} {
		t.Run(tc.name, func(t *testing.T) {
			reader, _ := fixtureReader(t, func(w http.ResponseWriter, r *http.Request) {
				if r.URL.Path == "/api/v1/nodes" {
					_, _ = fmt.Fprint(w, nodeLeaseSourceNodeList("node-uid-a", false))
					return
				}
				if tc.status == http.StatusFound {
					w.Header().Set("Location", "https://elsewhere.invalid/secrets")
				}
				w.WriteHeader(tc.status)
				_, _ = fmt.Fprint(w, "sensitive upstream details must never be reflected")
			})
			got, err := reader.observeNodeLease(context.Background(), "tenant-a", []string{"project-a"}, "worker-a", "node-uid-a", 20*time.Second)
			if got.Assessment.State == nodeLeaseFresh || !got.Assessment.ExpiresAt.IsZero() ||
				(tc.err != nil && !errors.Is(err, tc.err)) ||
				(tc.err == nil && (err != nil || got.Assessment.ReasonCode != tc.reason)) ||
				(err != nil && strings.Contains(err.Error(), "sensitive")) {
				t.Fatalf("untrusted Lease read accepted/leaked: %+v err=%v", got, err)
			}
		})
	}
}

func TestObserveNodeLeaseChecksScopeAndNameBeforeNetwork(t *testing.T) {
	calls := 0
	reader, _ := fixtureReader(t, func(w http.ResponseWriter, _ *http.Request) {
		calls++
		w.WriteHeader(http.StatusInternalServerError)
	})
	for _, tc := range []struct {
		tenant  string
		project string
		name    string
		uid     string
	}{
		{"different-tenant", "project-a", "worker-a", "node-uid-a"},
		{"tenant-a", "different-project", "worker-a", "node-uid-a"},
		{"tenant-a", "project-a", "worker-a/../../secrets", "node-uid-a"},
		{"tenant-a", "project-a", "worker-a", ""},
	} {
		got, err := reader.observeNodeLease(context.Background(), tc.tenant, []string{tc.project}, tc.name, tc.uid, time.Minute)
		if err == nil || got.Assessment.State == nodeLeaseFresh || calls != 0 {
			t.Fatalf("invalid source scope reached network: got=%+v err=%v calls=%d", got, err, calls)
		}
	}
}
