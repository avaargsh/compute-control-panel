package kubeinventory

import (
	"context"
	"fmt"
	"net/http"
	"testing"

	"github.com/avaargsh/compute-control-panel/internal/observation"
)

func TestNodeInventoryRejectsAmbiguousReadyConditions(t *testing.T) {
	for _, tc := range []struct {
		name       string
		conditions string
		want       observation.Condition
		reason     string
	}{
		{"missing status before True", `[{"type":"Ready"},{"type":"Ready","status":"True"}]`, observation.ConditionUnknown, "READY_CONDITION_CONFLICT"},
		{"empty status before True", `[{"type":"Ready","status":""},{"type":"Ready","status":"True"}]`, observation.ConditionUnknown, "READY_CONDITION_CONFLICT"},
		{"duplicate True", `[{"type":"Ready","status":"True"},{"type":"Ready","status":"True"}]`, observation.ConditionUnknown, "READY_CONDITION_CONFLICT"},
		{"conflicting Ready", `[{"type":"Ready","status":"False"},{"type":"Ready","status":"True"}]`, observation.ConditionUnknown, "READY_CONDITION_CONFLICT"},
		{"single Ready", `[{"type":"Ready","status":"True"}]`, observation.ConditionAvailable, ""},
		{"unrelated condition", `[{"type":"DiskPressure","status":"False"},{"type":"Ready","status":"True"}]`, observation.ConditionAvailable, ""},
	} {
		t.Run(tc.name, func(t *testing.T) {
			reader, _ := fixtureReader(t, func(w http.ResponseWriter, _ *http.Request) {
				_, _ = fmt.Fprintf(w, `{"metadata":{"resourceVersion":"1"},"items":[{"metadata":{"name":"worker-a","uid":"u1","resourceVersion":"1"},"status":{"conditions":%s}}]}`, tc.conditions)
			})
			snapshot, err := reader.ListObservations(context.Background(), "tenant-a", []string{"project-a"})
			if err != nil || len(snapshot.Items) != 1 {
				t.Fatalf("unexpected inventory: %+v err=%v", snapshot, err)
			}
			got := snapshot.Items[0]
			if got.Condition != tc.want || got.ReasonCode != tc.reason {
				t.Fatalf("ambiguous Ready state: got %s/%s want %s/%s", got.Condition, got.ReasonCode, tc.want, tc.reason)
			}
		})
	}
}
