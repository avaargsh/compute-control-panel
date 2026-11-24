package kubeinventory

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"testing"
)

func TestReaderRejectsDuplicateNodeUIDAcrossPages(t *testing.T) {
	reader, _ := fixtureReader(t, func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Query().Get("continue") {
		case "":
			_, _ = fmt.Fprint(w, `{"metadata":{"continue":"page-2","resourceVersion":"777"},"items":[{"metadata":{"name":"worker-a","uid":"same-uid","resourceVersion":"700"},"status":{"capacity":{"nvidia.com/gpu":"4"},"allocatable":{"nvidia.com/gpu":"4"}}}]}`)
		case "page-2":
			_, _ = fmt.Fprint(w, `{"metadata":{"resourceVersion":"777"},"items":[{"metadata":{"name":"worker-b","uid":"same-uid","resourceVersion":"701"},"status":{"capacity":{"nvidia.com/gpu":"4"},"allocatable":{"nvidia.com/gpu":"4"}}}]}`)
		default:
			w.WriteHeader(http.StatusBadRequest)
		}
	})
	snapshot, err := reader.ListObservations(context.Background(), "tenant-a", []string{"project-a"})
	if !errors.Is(err, ErrInvalidResponse) || len(snapshot.Items) != 0 {
		t.Fatalf("duplicate UID must reject entire snapshot: items=%d err=%v", len(snapshot.Items), err)
	}
}
