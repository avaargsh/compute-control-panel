package kubeinventory

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"testing"
)

func TestNodeInventoryRequiresExplicitItemsArray(t *testing.T) {
	for _, tc := range []struct {
		name    string
		payload string
		valid   bool
	}{
		{"missing items", `{"metadata":{"resourceVersion":"1"}}`, false},
		{"null items", `{"metadata":{"resourceVersion":"1"},"items":null}`, false},
		{"explicit empty array", `{"metadata":{"resourceVersion":"1"},"items":[]}`, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			reader, _ := fixtureReader(t, func(w http.ResponseWriter, _ *http.Request) {
				w.Header().Set("Content-Type", "application/json")
				_, _ = fmt.Fprint(w, tc.payload)
			})
			snapshot, err := reader.ListObservations(context.Background(), "tenant-a", []string{"project-a"})
			if tc.valid {
				if err != nil || len(snapshot.Items) != 0 {
					t.Fatalf("valid empty inventory rejected: %+v, %v", snapshot, err)
				}
				return
			}
			if !errors.Is(err, ErrInvalidResponse) || len(snapshot.Items) != 0 {
				t.Fatalf("malformed inventory accepted: %+v, %v", snapshot, err)
			}
		})
	}
}

func TestNodeInventoryRejectsMissingItemsOnLaterPage(t *testing.T) {
	reader, _ := fixtureReader(t, func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		if r.URL.Query().Get("continue") == "" {
			_, _ = fmt.Fprint(w, `{"metadata":{"resourceVersion":"1","continue":"next"},"items":[{"metadata":{"name":"worker-a","uid":"u1","resourceVersion":"1"}}]}`)
			return
		}
		_, _ = fmt.Fprint(w, `{"metadata":{"resourceVersion":"1"},"items":null}`)
	})
	snapshot, err := reader.ListObservations(context.Background(), "tenant-a", []string{"project-a"})
	if !errors.Is(err, ErrInvalidResponse) || len(snapshot.Items) != 0 {
		t.Fatalf("partial inventory escaped: %+v, %v", snapshot, err)
	}
}
