package provider

import (
	"context"
	"errors"
	"net/http/httptest"
	"testing"

	"github.com/avaargsh/compute-control-panel/internal/operation"
)

// After a process restart there is no trustworthy original UID in the local
// adapter. Matching Job names and annotations are insufficient to distinguish
// the original Job from a successor that copied the same annotations.
func TestKueueJobRestartDoesNotAdoptUnpinnedSameNameJob(t *testing.T) {
	for _, tc := range []struct {
		name         string
		replacement  string
	}{
		{name: "original job still exists"},
		{name: "successor copied original annotations", replacement: "job-uid-successor"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			adapter, api, server := newKueueAdapterFixture(t, &kueueAPI{})
			defer server.Close()
			ctx := context.Background()
			req := kueueAdapterRequest()
			if _, err := adapter.Apply(ctx, req); err != nil {
				t.Fatal(err)
			}
			if tc.replacement != "" {
				api.setReplacement(tc.replacement, "")
			}
			// Reinstantiate without a durable expected UID; this is the same
			// state as recovering UNKNOWN before loading a journal binding.
			restarted, err := NewKueueJobAdapter(server.URL, "tenant-a", server.Client())
			if err != nil {
				t.Fatal(err)
			}
			observed, err := restarted.Inspect(ctx, req)
			if err != nil || observed.Result != operation.OperationInProgress ||
				observed.ResourceUID != "" || observed.ResourceID != "" {
				t.Fatalf("fresh adapter adopted an unpinned Job: observation=%+v err=%v", observed, err)
			}
			if _, err := restarted.Apply(ctx, req); !errors.Is(err, ErrAcknowledgementLost) {
				t.Fatalf("fresh adapter acknowledged an unbound Job: %v", err)
			}
			if api.postCount() != 1 {
				t.Fatalf("restarted adapter made duplicate Job CREATE: posts=%d", api.postCount())
			}
		})
	}
}

// A lost CREATE acknowledgement that never persisted a UID is UNKNOWN even
// if the API now reports a same-name object with matching annotations.
func TestKueueJobLostACKRestartKeepsUnboundJobInconclusive(t *testing.T) {
	adapter, api, server := newKueueAdapterFixture(t, &kueueAPI{loseCreateBody: true})
	defer server.Close()
	ctx := context.Background()
	req := kueueAdapterRequest()
	if _, err := adapter.Apply(ctx, req); !errors.Is(err, ErrAcknowledgementLost) {
		t.Fatalf("expected lost create ACK: %v", err)
	}
	restarted, err := NewKueueJobAdapter(server.URL, "tenant-a", server.Client())
	if err != nil {
		t.Fatal(err)
	}
	obs, err := restarted.Inspect(ctx, req)
	if err != nil || obs.Result != operation.OperationInProgress || obs.ResourceUID != "" {
		t.Fatalf("lost ACK without durable UID was treated final: %+v err=%v", obs, err)
	}
	if _, err := restarted.Apply(ctx, req); !errors.Is(err, ErrAcknowledgementLost) {
		t.Fatalf("lost ACK replay was falsely acknowledged: %v", err)
	}
	if api.postCount() != 1 {
		t.Fatalf("lost ACK replay posted second Job: %d", api.postCount())
	}
}
