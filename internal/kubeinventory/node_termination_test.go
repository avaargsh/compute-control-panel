package kubeinventory

import (
	"context"
	"fmt"
	"net/http"
	"testing"

	"github.com/avaargsh/compute-control-panel/internal/observation"
)

func TestTerminatingNodeNeverProjectsAvailable(t *testing.T) {
	cases := []struct {
		name     string
		deletion string
		want     observation.Status
		reason   string
	}{
		{name: "terminating Ready node", deletion: `,"deletionTimestamp":"2026-11-21T07:59:00Z"`, want: observation.StatusUnavailable, reason: "NODE_TERMINATING"},
		{name: "explicit null deletion timestamp", deletion: `,"deletionTimestamp":null`, want: observation.StatusAvailable},
		{name: "live Ready node", want: observation.StatusAvailable},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			reader, _ := fixtureReader(t, func(w http.ResponseWriter, r *http.Request) {
				_, _ = fmt.Fprintf(w, `{"metadata":{"resourceVersion":"777"},"items":[{"metadata":{"name":"worker-a","uid":"node-uid-a","resourceVersion":"700"%s},"status":{"capacity":{"nvidia.com/gpu":"4"},"allocatable":{"nvidia.com/gpu":"3"},"conditions":[{"type":"Ready","status":"True"}]}}]}`, tc.deletion)
			})
			snapshot, err := reader.ListObservations(context.Background(), "tenant-a", []string{"project-a"})
			if err != nil || len(snapshot.Items) != 2 {
				t.Fatalf("expected Node and raw GPU resource observation: items=%d err=%v", len(snapshot.Items), err)
			}
			node, gpu := snapshot.Items[0], snapshot.Items[1]
			view, err := observation.Project(node, node.ObservedAt)
			if err != nil || view.Status != tc.want || view.ReasonCode != tc.reason || view.Resource.UID != "node-uid-a" {
				t.Fatalf("unexpected Node lifecycle projection: %+v err=%v", view, err)
			}
			device, err := observation.Project(gpu, gpu.ObservedAt)
			if err != nil || device.Status != observation.StatusUnknown || device.Quantity == nil ||
				device.Quantity.Capacity != "4" || device.Quantity.Allocatable != "3" || device.Resource.UID != "" {
				t.Fatalf("must preserve only raw GPU quantities, not device availability: %+v err=%v", device, err)
			}
		})
	}
}
