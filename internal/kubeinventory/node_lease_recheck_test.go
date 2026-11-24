package kubeinventory

import (
	"context"
	"fmt"
	"net/http"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

func TestNodeLeaseSourceRejectsChangedSecondLease(t *testing.T) {
	for _, tc := range []struct {
		name   string
		second string
		status int
	}{
		{"replacement UID", strings.Replace(nodeLeaseSourceFixture, "lease-uid-a", "lease-uid-b", 1), http.StatusOK},
		{"new resourceVersion", strings.Replace(nodeLeaseSourceFixture, `"resourceVersion":"900"`, `"resourceVersion":"901"`, 1), http.StatusOK},
		{"deleted Lease", "", http.StatusNotFound},
		{"second read denied", "", http.StatusForbidden},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var reads atomic.Int32
			reader, _ := fixtureReader(t, func(w http.ResponseWriter, r *http.Request) {
				if r.URL.Path == "/api/v1/nodes" {
					_, _ = fmt.Fprint(w, nodeLeaseSourceNodeList("node-uid-a", false))
					return
				}
				if reads.Add(1) == 1 {
					_, _ = fmt.Fprint(w, nodeLeaseSourceFixture)
					return
				}
				w.WriteHeader(tc.status)
				_, _ = fmt.Fprint(w, tc.second)
			})
			got, _ := reader.observeNodeLease(context.Background(), "tenant-a", []string{"project-a"}, "worker-a", "node-uid-a", 20*time.Second)
			if reads.Load() != 2 || got.Assessment.State != nodeLeaseUnknown || !got.Assessment.ExpiresAt.IsZero() {
				t.Fatalf("changed or inaccessible Lease accepted: reads=%d got=%+v", reads.Load(), got)
			}
		})
	}
}

func TestNodeLeaseSourceRejectsClockRollbackAndExpiredCollection(t *testing.T) {
	start := time.Date(2026, 11, 21, 8, 0, 0, 0, time.UTC)
	for _, tc := range []struct {
		name  string
		end   time.Time
		lease  string
		maxAge time.Duration
	}{
		{"clock rollback", start.Add(-time.Second), nodeLeaseSourceFixture, 20 * time.Second},
		{"collection exceeds TTL", start.Add(time.Minute), strings.Replace(nodeLeaseSourceFixture, `"leaseDurationSeconds":40`, `"leaseDurationSeconds":180`, 1), 2 * time.Minute},
	} {
		t.Run(tc.name, func(t *testing.T) {
			reader, _ := fixtureReader(t, func(w http.ResponseWriter, r *http.Request) {
				if r.URL.Path == "/api/v1/nodes" {
					_, _ = fmt.Fprint(w, nodeLeaseSourceNodeList("node-uid-a", false))
					return
				}
				_, _ = fmt.Fprint(w, tc.lease)
			})
			calls := 0
			reader.now = func() time.Time {
				calls++
				if calls == 1 {
					return start
				}
				return tc.end
			}
			got, _ := reader.observeNodeLease(context.Background(), "tenant-a", []string{"project-a"}, "worker-a", "node-uid-a", tc.maxAge)
			if calls < 2 || got.Assessment.State != nodeLeaseUnknown || !got.Assessment.ExpiresAt.IsZero() {
				t.Fatalf("invalid collection clock accepted: calls=%d got=%+v", calls, got)
			}
		})
	}
}
