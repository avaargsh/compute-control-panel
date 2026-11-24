package kubeinventory

import (
	"encoding/json"
	"testing"
	"time"
)

func TestAssessNodeLeaseFreshness(t *testing.T) {
	now := time.Date(2026, 11, 24, 0, 0, 30, 0, time.UTC)
	const fixture = `{"apiVersion":"coordination.k8s.io/v1","kind":"Lease","metadata":{"namespace":"kube-node-lease","name":"worker-a","uid":"lease-uid-a","resourceVersion":"900","ownerReferences":[{"apiVersion":"v1","kind":"Node","name":"worker-a","uid":"node-uid-a"}]},"spec":{"holderIdentity":"worker-a","renewTime":"2026-11-24T00:00:20Z","leaseDurationSeconds":40}}`
	tests := []struct {
		name     string
		mutate   func(map[string]any)
		policy   time.Duration
		want     string
		reason   string
		deadline int
	}{
		{name: "fresh exact Node identity", policy: time.Minute, want: "FRESH", deadline: 60},
		{name: "policy caps lease duration", policy: 15 * time.Second, want: "FRESH", deadline: 35},
		{name: "exact expiry boundary", policy: 10 * time.Second, want: "EXPIRED", reason: "LEASE_EXPIRED", deadline: 30},
		{name: "expired source duration", policy: time.Minute, want: "EXPIRED", reason: "LEASE_EXPIRED", deadline: 25, mutate: func(v map[string]any) {
			v["spec"].(map[string]any)["leaseDurationSeconds"] = 5
		}},
		{name: "future renewal", policy: time.Minute, want: "UNKNOWN", reason: "LEASE_RENEWAL_INVALID", mutate: func(v map[string]any) {
			v["spec"].(map[string]any)["renewTime"] = "2026-11-24T00:00:31Z"
		}},
		{name: "missing renewal", policy: time.Minute, want: "UNKNOWN", reason: "LEASE_RENEWAL_INVALID", mutate: func(v map[string]any) {
			delete(v["spec"].(map[string]any), "renewTime")
		}},
		{name: "missing duration", policy: time.Minute, want: "UNKNOWN", reason: "LEASE_DURATION_INVALID", mutate: func(v map[string]any) {
			delete(v["spec"].(map[string]any), "leaseDurationSeconds")
		}},
		{name: "zero duration", policy: time.Minute, want: "UNKNOWN", reason: "LEASE_DURATION_INVALID", mutate: func(v map[string]any) {
			v["spec"].(map[string]any)["leaseDurationSeconds"] = 0
		}},
		{name: "old Node UID after same-name replacement", policy: time.Minute, want: "UNKNOWN", reason: "LEASE_OWNER_MISMATCH", mutate: func(v map[string]any) {
			v["metadata"].(map[string]any)["ownerReferences"].([]any)[0].(map[string]any)["uid"] = "old-node-uid"
		}},
		{name: "missing owner", policy: time.Minute, want: "UNKNOWN", reason: "LEASE_OWNER_MISMATCH", mutate: func(v map[string]any) {
			delete(v["metadata"].(map[string]any), "ownerReferences")
		}},
		{name: "ambiguous owners", policy: time.Minute, want: "UNKNOWN", reason: "LEASE_OWNER_MISMATCH", mutate: func(v map[string]any) {
			m := v["metadata"].(map[string]any)
			owners := m["ownerReferences"].([]any)
			m["ownerReferences"] = append(owners, owners[0])
		}},
		{name: "wrong namespace", policy: time.Minute, want: "UNKNOWN", reason: "LEASE_IDENTITY_INVALID", mutate: func(v map[string]any) {
			v["metadata"].(map[string]any)["namespace"] = "default"
		}},
		{name: "holder differs from Node", policy: time.Minute, want: "UNKNOWN", reason: "LEASE_IDENTITY_INVALID", mutate: func(v map[string]any) {
			v["spec"].(map[string]any)["holderIdentity"] = "other-worker"
		}},
		{name: "terminating Lease", policy: time.Minute, want: "UNKNOWN", reason: "LEASE_IDENTITY_INVALID", mutate: func(v map[string]any) {
			v["metadata"].(map[string]any)["deletionTimestamp"] = "2026-11-24T00:00:25Z"
		}},
		{name: "missing Lease UID", policy: time.Minute, want: "UNKNOWN", reason: "LEASE_IDENTITY_INVALID", mutate: func(v map[string]any) {
			delete(v["metadata"].(map[string]any), "uid")
		}},
		{name: "missing resource version", policy: time.Minute, want: "UNKNOWN", reason: "LEASE_IDENTITY_INVALID", mutate: func(v map[string]any) {
			delete(v["metadata"].(map[string]any), "resourceVersion")
		}},
		{name: "zero policy", want: "UNKNOWN", reason: "LEASE_POLICY_INVALID"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			var raw map[string]any
			if err := json.Unmarshal([]byte(fixture), &raw); err != nil {
				t.Fatal(err)
			}
			if tc.mutate != nil {
				tc.mutate(raw)
			}
			data, err := json.Marshal(raw)
			if err != nil {
				t.Fatal(err)
			}
			var lease nodeLease
			if err := json.Unmarshal(data, &lease); err != nil {
				t.Fatal(err)
			}
			got := assessNodeLease("worker-a", "node-uid-a", lease, now, tc.policy)
			if string(got.State) != tc.want || got.ReasonCode != tc.reason {
				t.Fatalf("heartbeat state=%q reason=%q; want %q %q", got.State, got.ReasonCode, tc.want, tc.reason)
			}
			if tc.deadline != 0 {
				wantDeadline := time.Date(2026, 11, 24, 0, 0, 0, 0, time.UTC).Add(time.Duration(tc.deadline) * time.Second)
				if !got.ExpiresAt.Equal(wantDeadline) {
					t.Fatalf("deadline=%v; want %v", got.ExpiresAt, wantDeadline)
				}
			} else if !got.ExpiresAt.IsZero() {
				t.Fatalf("untrusted heartbeat has nonzero deadline: %v", got.ExpiresAt)
			}
		})
	}
}
