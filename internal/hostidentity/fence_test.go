package hostidentity

import (
	"strings"
	"testing"
	"time"
)

func TestHeartbeatFence(t *testing.T) {
	now := time.Date(2026, 11, 24, 2, 0, 0, 0, time.UTC)
	digest := "sha256:" + strings.Repeat("a", 64)
	otherDigest := "sha256:" + strings.Repeat("b", 64)
	binding := Binding{HostID: "host-a", ProviderID: "provider-a", ClusterScope: "cluster-a", NodeUID: "node-uid-a", Subject: "spiffe://ccp.example/host/a"}
	current := SessionFence{Binding: binding, Epoch: 3, SessionID: "session-3", LastSequence: 7, LastDigest: digest, ExpiresAt: now.Add(time.Minute)}
	heartbeat := Heartbeat{Binding: binding, Epoch: 3, SessionID: "session-3", Sequence: 8, Digest: digest}
	for _, tc := range []struct {
		name   string
		mutate func(*SessionFence, *Heartbeat, *time.Time)
		want   Disposition
	}{
		{name: "new current sequence", want: Accept},
		{name: "exact replay", mutate: func(_ *SessionFence, h *Heartbeat, _ *time.Time) { h.Sequence = 7 }, want: Replay},
		{name: "conflicting replay", mutate: func(_ *SessionFence, h *Heartbeat, _ *time.Time) { h.Sequence = 7; h.Digest = otherDigest }, want: Reject},
		{name: "old sequence", mutate: func(_ *SessionFence, h *Heartbeat, _ *time.Time) { h.Sequence = 6 }, want: Reject},
		{name: "old epoch with larger sequence", mutate: func(_ *SessionFence, h *Heartbeat, _ *time.Time) { h.Epoch = 2; h.Sequence = 999 }, want: Reject},
		{name: "future epoch", mutate: func(_ *SessionFence, h *Heartbeat, _ *time.Time) { h.Epoch = 4 }, want: Reject},
		{name: "wrong session", mutate: func(_ *SessionFence, h *Heartbeat, _ *time.Time) { h.SessionID = "session-other" }, want: Reject},
		{name: "successor Node UID", mutate: func(_ *SessionFence, h *Heartbeat, _ *time.Time) { h.Binding.NodeUID = "node-uid-b" }, want: Reject},
		{name: "foreign host", mutate: func(_ *SessionFence, h *Heartbeat, _ *time.Time) { h.Binding.HostID = "host-b" }, want: Reject},
		{name: "foreign provider", mutate: func(_ *SessionFence, h *Heartbeat, _ *time.Time) { h.Binding.ProviderID = "provider-b" }, want: Reject},
		{name: "foreign cluster", mutate: func(_ *SessionFence, h *Heartbeat, _ *time.Time) { h.Binding.ClusterScope = "cluster-b" }, want: Reject},
		{name: "foreign principal", mutate: func(_ *SessionFence, h *Heartbeat, _ *time.Time) { h.Binding.Subject = "spiffe://ccp.example/host/b" }, want: Reject},
		{name: "exact expiry", mutate: func(s *SessionFence, _ *Heartbeat, n *time.Time) { *n = s.ExpiresAt }, want: Reject},
		{name: "expired replay", mutate: func(s *SessionFence, h *Heartbeat, n *time.Time) { *n = s.ExpiresAt.Add(time.Second); h.Sequence = 7 }, want: Reject},
		{name: "invalid clock", mutate: func(_ *SessionFence, _ *Heartbeat, n *time.Time) { *n = time.Time{} }, want: Reject},
		{name: "invalid digest", mutate: func(_ *SessionFence, h *Heartbeat, _ *time.Time) { h.Digest = "not-a-digest" }, want: Reject},
		{name: "invalid current fence", mutate: func(s *SessionFence, _ *Heartbeat, _ *time.Time) { s.LastDigest = "" }, want: Reject},
		{name: "first valid heartbeat", mutate: func(s *SessionFence, h *Heartbeat, _ *time.Time) {
			s.LastSequence = 0
			s.LastDigest = ""
			h.Sequence = 1
		}, want: Accept},
		{name: "zero sequence", mutate: func(_ *SessionFence, h *Heartbeat, _ *time.Time) { h.Sequence = 0 }, want: Reject},
	} {
		t.Run(tc.name, func(t *testing.T) {
			s, h, n := current, heartbeat, now
			if tc.mutate != nil {
				tc.mutate(&s, &h, &n)
			}
			before := s
			if got := EvaluateHeartbeat(s, h, n); got != tc.want {
				t.Fatalf("disposition=%s, want %s", got, tc.want)
			}
			if s != before {
				t.Fatal("pure evaluation changed persisted session fence")
			}
		})
	}
}
