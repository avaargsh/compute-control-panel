package kubeinventory

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"sync/atomic"
	"testing"
	"time"

	"github.com/avaargsh/compute-control-panel/internal/observation"
)

func TestInventoryCollectionDoesNotRenewSnapshotTTL(t *testing.T) {
	start := time.Date(2026, 11, 24, 1, 0, 0, 0, time.UTC)
	for _, tc := range []struct {
		name    string
		elapsed time.Duration
		wantErr bool
	}{
		{name: "within budget", elapsed: 20 * time.Second},
		{name: "exact expiry", elapsed: time.Minute, wantErr: true},
		{name: "expired during pagination", elapsed: time.Minute + time.Second, wantErr: true},
		{name: "clock moved backward", elapsed: -time.Second, wantErr: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var clock atomic.Int64
			clock.Store(start.UnixNano())
			reader, _ := fixtureReader(t, func(w http.ResponseWriter, r *http.Request) {
				switch r.URL.Query().Get("continue") {
				case "":
					_, _ = fmt.Fprint(w, `{"metadata":{"continue":"page-2","resourceVersion":"777"},"items":[{"metadata":{"name":"worker-a","uid":"node-uid-a","resourceVersion":"700"},"status":{"capacity":{"nvidia.com/gpu":"4"},"allocatable":{"nvidia.com/gpu":"3"},"conditions":[{"type":"Ready","status":"True"}]}}]}`)
				case "page-2":
					clock.Store(start.Add(tc.elapsed).UnixNano())
					_, _ = fmt.Fprint(w, `{"metadata":{"resourceVersion":"777"},"items":[{"metadata":{"name":"worker-b","uid":"node-uid-b","resourceVersion":"701"},"status":{"conditions":[{"type":"Ready","status":"True"}]}}]}`)
				default:
					t.Error("unexpected continuation")
					w.WriteHeader(http.StatusBadRequest)
				}
			})
			reader.now = func() time.Time { return time.Unix(0, clock.Load()).UTC() }
			snapshot, err := reader.ListObservations(context.Background(), "tenant-a", []string{"project-a"})
			if tc.wantErr {
				if !errors.Is(err, ErrInvalidResponse) || len(snapshot.Items) != 0 {
					t.Fatalf("stale or inconsistent collection must fail closed: items=%d err=%v", len(snapshot.Items), err)
				}
				return
			}
			if err != nil || len(snapshot.Items) != 3 {
				t.Fatalf("valid bounded collection: items=%d err=%v", len(snapshot.Items), err)
			}
			for _, record := range snapshot.Items {
				if !record.ObservedAt.Equal(start) || !record.ExpiresAt.Equal(start.Add(time.Minute)) {
					t.Fatalf("pagination renewed freshness: observed=%s expires=%s", record.ObservedAt, record.ExpiresAt)
				}
				view, err := observation.Project(record, start.Add(time.Minute))
				if err != nil || view.Status != observation.StatusStale {
					t.Fatalf("snapshot survived original expiry: status=%s err=%v", view.Status, err)
				}
			}
		})
	}
}
