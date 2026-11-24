package hostidentity

import (
	"math"
	"strings"
	"testing"
	"time"
)

func TestPrepareSessionStart(t *testing.T) {
	now := time.Date(2026, 11, 24, 12, 0, 0, 0, time.UTC)
	b := Binding{HostID: "host-a", ProviderID: "provider-a", ClusterScope: "cluster-a", NodeUID: "uid-a", Subject: "spiffe://ccp.example/host/a"}
	prior := SessionFence{Binding: b, Epoch: 3, SessionID: "old", LastSequence: 2, LastDigest: "sha256:" + strings.Repeat("a", 64), IssuedAt: now.Add(-2 * time.Minute), LastRecordedAt: now.Add(-time.Minute), ExpiresAt: now.Add(-time.Second)}
	for _, tc := range []struct {
		name   string
		change func(*Binding, **SessionFence, *int64, *string, *time.Time, *time.Time)
		want   int64
	}{
		{name: "expired predecessor can be replaced", want: 4},
		{name: "first epoch", change: func(_ *Binding, p **SessionFence, e *int64, _ *string, _, _ *time.Time) { *p = nil; *e = 0 }, want: 1},
		{name: "stale expected epoch", change: func(_ *Binding, _ **SessionFence, e *int64, _ *string, _, _ *time.Time) { *e = 2 }},
		{name: "future expected epoch", change: func(_ *Binding, _ **SessionFence, e *int64, _ *string, _, _ *time.Time) { *e = 4 }},
		{name: "foreign identity", change: func(b *Binding, _ **SessionFence, _ *int64, _ *string, _, _ *time.Time) { b.NodeUID = "successor" }},
		{name: "reuse session ID", change: func(_ *Binding, _ **SessionFence, _ *int64, s *string, _, _ *time.Time) { *s = "old" }},
		{name: "epoch overflow", change: func(_ *Binding, p **SessionFence, e *int64, _ *string, _, _ *time.Time) {
			(*p).Epoch = math.MaxInt64
			*e = math.MaxInt64
		}},
		{name: "clock rollback", change: func(_ *Binding, p **SessionFence, _ *int64, _ *string, n, _ *time.Time) {
			*n = (*p).LastRecordedAt.Add(-time.Nanosecond)
		}},
		{name: "invalid expiry", change: func(_ *Binding, _ **SessionFence, _ *int64, _ *string, n, x *time.Time) { *x = *n }},
		{name: "corrupt prior digest", change: func(_ *Binding, p **SessionFence, _ *int64, _ *string, _, _ *time.Time) { (*p).LastDigest = "bad" }},
		{name: "nonzero initial expected epoch", change: func(_ *Binding, p **SessionFence, _ *int64, _ *string, _, _ *time.Time) { *p = nil }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			binding, old, expected, id, issued, expiry := b, prior, int64(3), "new", now, now.Add(time.Minute)
			p := &old
			if tc.change != nil {
				tc.change(&binding, &p, &expected, &id, &issued, &expiry)
			}
			before := old
			got, ok := PrepareSessionStart(binding, p, expected, id, issued, expiry)
			if ok != (tc.want != 0) {
				t.Fatalf("accepted=%v, want epoch=%d", ok, tc.want)
			}
			if old != before {
				t.Fatal("predecessor changed")
			}
			if !ok {
				if got != (SessionFence{}) {
					t.Fatal("rejected start returned a fence")
				}
				return
			}
			want := SessionFence{Binding: binding, Epoch: tc.want, SessionID: id, IssuedAt: issued, ExpiresAt: expiry}
			if got != want {
				t.Fatalf("candidate=%+v, want %+v", got, want)
			}
		})
	}
}

func TestPrepareSessionStartRejectsUnexpiredPredecessor(t *testing.T) {
	now := time.Date(2026, 11, 24, 12, 0, 0, 0, time.UTC)
	binding := Binding{HostID: "host-active", ProviderID: "provider-active", ClusterScope: "cluster-active", NodeUID: "uid-active", Subject: "spiffe://ccp.example/host/active"}
	previous := SessionFence{
		Binding: binding, Epoch: 7, SessionID: "session-active",
		IssuedAt: now.Add(-time.Minute), ExpiresAt: now.Add(time.Second),
	}
	if candidate, ok := PrepareSessionStart(binding, &previous, 7, "session-next", now, now.Add(time.Minute)); ok {
		t.Fatalf("active session was replaced: candidate=%+v", candidate)
	}
}
