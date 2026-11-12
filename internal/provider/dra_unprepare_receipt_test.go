package provider

import (
	"bytes"
	"crypto/ed25519"
	"strings"
	"testing"
	"time"
)

func unprepareTestAuthority() (DRAUnprepareAuthority, ed25519.PrivateKey) {
	private := ed25519.NewKeyFromSeed(bytes.Repeat([]byte{4}, ed25519.SeedSize))
	return DRAUnprepareAuthority{
		ClusterID: "cluster-a", SourceID: "node-plugin-a", KeyID: "driver-key-a",
		Driver: "gpu.example.com", PublicKey: private.Public().(ed25519.PublicKey),
	}, private
}

func unprepareTestReceipt(private ed25519.PrivateKey) SignedDRAUnprepare {
	event := DRAUnprepareEvent{
		Version: "ccp.dra.node-unprepare.v1", ClusterID: "cluster-a", SourceID: "node-plugin-a",
		KeyID: "driver-key-a", EventID: "unprepare-a", RPC: "NodeUnprepareResources",
		Result: "SUCCESS", Driver: "gpu.example.com", NodeName: "gpu-node-a",
		ClaimNamespace: "tenant-a", ClaimName: "gpu-claim-a", ClaimUID: "claim-uid-a",
		CompletedAt: time.Date(2026, 11, 10, 10, 0, 30, 0, time.UTC),
	}
	return SignedDRAUnprepare{Event: event, Signature: ed25519.Sign(private, draUnprepareSigningBytes(event))}
}

func TestDriverUnprepareReceiptSignedAndBoundToHistoricalAllocation(t *testing.T) {
	cases := []struct {
		name   string
		mutate func(*ReleaseEvidenceCandidate, *SignedDRAUnprepare, *DRAUnprepareAuthority, *time.Time)
		resign bool
		valid  bool
	}{
		{name: "exact signed Claim UID and prior Pod node", valid: true},
		{name: "tampered payload", mutate: func(_ *ReleaseEvidenceCandidate, s *SignedDRAUnprepare, _ *DRAUnprepareAuthority, _ *time.Time) {
			s.Event.ClaimUID = "other"
		}},
		{name: "signed successor Claim UID", resign: true, mutate: func(_ *ReleaseEvidenceCandidate, s *SignedDRAUnprepare, _ *DRAUnprepareAuthority, _ *time.Time) {
			s.Event.ClaimUID = "other"
		}},
		{name: "signed cross-tenant Claim", resign: true, mutate: func(_ *ReleaseEvidenceCandidate, s *SignedDRAUnprepare, _ *DRAUnprepareAuthority, _ *time.Time) {
			s.Event.ClaimNamespace = "tenant-b"
		}},
		{name: "signed wrong driver", resign: true, mutate: func(_ *ReleaseEvidenceCandidate, s *SignedDRAUnprepare, _ *DRAUnprepareAuthority, _ *time.Time) {
			s.Event.Driver = "other.example.com"
		}},
		{name: "signed wrong node", resign: true, mutate: func(_ *ReleaseEvidenceCandidate, s *SignedDRAUnprepare, _ *DRAUnprepareAuthority, _ *time.Time) {
			s.Event.NodeName = "other-node"
		}},
		{name: "signed wrong cluster", resign: true, mutate: func(_ *ReleaseEvidenceCandidate, s *SignedDRAUnprepare, _ *DRAUnprepareAuthority, _ *time.Time) {
			s.Event.ClusterID = "cluster-b"
		}},
		{name: "signed wrong source", resign: true, mutate: func(_ *ReleaseEvidenceCandidate, s *SignedDRAUnprepare, _ *DRAUnprepareAuthority, _ *time.Time) {
			s.Event.SourceID = "rogue-source"
		}},
		{name: "signed wrong signing key ID", resign: true, mutate: func(_ *ReleaseEvidenceCandidate, s *SignedDRAUnprepare, _ *DRAUnprepareAuthority, _ *time.Time) {
			s.Event.KeyID = "other-key"
		}},
		{name: "signed failed unprepare", resign: true, mutate: func(_ *ReleaseEvidenceCandidate, s *SignedDRAUnprepare, _ *DRAUnprepareAuthority, _ *time.Time) {
			s.Event.Result = "FAILED"
		}},
		{name: "signed wrong RPC", resign: true, mutate: func(_ *ReleaseEvidenceCandidate, s *SignedDRAUnprepare, _ *DRAUnprepareAuthority, _ *time.Time) {
			s.Event.RPC = "NodePrepareResources"
		}},
		{name: "signed unsupported schema", resign: true, mutate: func(_ *ReleaseEvidenceCandidate, s *SignedDRAUnprepare, _ *DRAUnprepareAuthority, _ *time.Time) {
			s.Event.Version = "v2"
		}},
		{name: "signed empty event ID", resign: true, mutate: func(_ *ReleaseEvidenceCandidate, s *SignedDRAUnprepare, _ *DRAUnprepareAuthority, _ *time.Time) {
			s.Event.EventID = ""
		}},
		{name: "signed malformed event ID", resign: true, mutate: func(_ *ReleaseEvidenceCandidate, s *SignedDRAUnprepare, _ *DRAUnprepareAuthority, _ *time.Time) {
			s.Event.EventID = "line\nbreak"
		}},
		{name: "signed stale completion", resign: true, mutate: func(_ *ReleaseEvidenceCandidate, s *SignedDRAUnprepare, _ *DRAUnprepareAuthority, _ *time.Time) {
			s.Event.CompletedAt = s.Event.CompletedAt.Add(-time.Hour)
		}},
		{name: "signed future completion", resign: true, mutate: func(_ *ReleaseEvidenceCandidate, s *SignedDRAUnprepare, _ *DRAUnprepareAuthority, _ *time.Time) {
			s.Event.CompletedAt = s.Event.CompletedAt.Add(2 * time.Minute)
		}},
		{name: "absent signature", mutate: func(_ *ReleaseEvidenceCandidate, s *SignedDRAUnprepare, _ *DRAUnprepareAuthority, _ *time.Time) {
			s.Signature = nil
		}},
		{name: "wrong signature key", mutate: func(_ *ReleaseEvidenceCandidate, s *SignedDRAUnprepare, _ *DRAUnprepareAuthority, _ *time.Time) {
			wrong := ed25519.NewKeyFromSeed(bytes.Repeat([]byte{8}, ed25519.SeedSize))
			s.Signature = ed25519.Sign(wrong, draUnprepareSigningBytes(s.Event))
		}},
		{name: "untrusted provisioned source", mutate: func(_ *ReleaseEvidenceCandidate, _ *SignedDRAUnprepare, a *DRAUnprepareAuthority, _ *time.Time) {
			a.SourceID = "other-source"
		}},
		{name: "untrusted provisioned key", mutate: func(_ *ReleaseEvidenceCandidate, _ *SignedDRAUnprepare, a *DRAUnprepareAuthority, _ *time.Time) {
			wrong := ed25519.NewKeyFromSeed(bytes.Repeat([]byte{8}, ed25519.SeedSize))
			a.PublicKey = wrong.Public().(ed25519.PublicKey)
		}},
		{name: "cross-allocation splice", mutate: func(e *ReleaseEvidenceCandidate, _ *SignedDRAUnprepare, _ *DRAUnprepareAuthority, _ *time.Time) {
			e.Create.AllocationID = "other-allocation"
		}},
		{name: "prior driver splice", mutate: func(e *ReleaseEvidenceCandidate, _ *SignedDRAUnprepare, _ *DRAUnprepareAuthority, _ *time.Time) {
			e.PriorPodClaim.Claim.Device.Driver = "other.example.com"
		}},
		{name: "no Pod absence evidence", mutate: func(e *ReleaseEvidenceCandidate, _ *SignedDRAUnprepare, _ *DRAUnprepareAuthority, _ *time.Time) {
			e.PodAbsent = KubernetesPodAbsentObservation{}
		}},
		{name: "untrusted clock", mutate: func(_ *ReleaseEvidenceCandidate, _ *SignedDRAUnprepare, _ *DRAUnprepareAuthority, now *time.Time) {
			*now = time.Time{}
		}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			a, private := unprepareTestAuthority()
			signed := unprepareTestReceipt(private)
			e := releaseCandidateFixture()
			now := time.Date(2026, 11, 10, 10, 1, 0, 0, time.UTC)
			if tc.mutate != nil {
				tc.mutate(&e, &signed, &a, &now)
			}
			if tc.resign {
				signed.Signature = ed25519.Sign(private, draUnprepareSigningBytes(signed.Event))
			}
			v, err := NewDRAUnprepareVerifier(a, 10*time.Minute, 30*time.Second)
			if err != nil {
				t.Fatal(err)
			}
			got, err := v.Correlate(e, signed, now)
			if tc.valid {
				if err != nil || got.Release.AllocationID != "allocation-a" ||
					got.Event.ClaimUID != "claim-uid-a" || got.Event.EventID != "unprepare-a" ||
					!strings.HasPrefix(got.PayloadSHA256, "sha256:") ||
					len(got.PayloadSHA256) != 71 {
					t.Fatalf("valid signed receipt not correlated: %+v err=%v", got, err)
				}
			} else if err == nil || got != (DRAUnprepareCorrelation{}) {
				t.Fatalf("unsafe receipt accepted: %+v err=%v", got, err)
			}
		})
	}
}

func TestDRAUnprepareVerifierRejectsCallerKeyMutationAndBadAuthority(t *testing.T) {
	a, private := unprepareTestAuthority()
	v, err := NewDRAUnprepareVerifier(a, 10*time.Minute, 30*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	for i := range a.PublicKey {
		a.PublicKey[i] ^= 0x7f
	}
	if _, err := v.Correlate(releaseCandidateFixture(), unprepareTestReceipt(private),
		time.Date(2026, 11, 10, 10, 1, 0, 0, time.UTC)); err != nil {
		t.Fatalf("caller mutated provisioned public key: %v", err)
	}
	a, _ = unprepareTestAuthority()
	tests := []struct {
		name string
		edit func(*DRAUnprepareAuthority, *time.Duration, *time.Duration)
	}{
		{"missing key", func(a *DRAUnprepareAuthority, _, _ *time.Duration) { a.PublicKey = nil }},
		{"short key", func(a *DRAUnprepareAuthority, _, _ *time.Duration) { a.PublicKey = []byte{1} }},
		{"missing cluster", func(a *DRAUnprepareAuthority, _, _ *time.Duration) { a.ClusterID = "" }},
		{"missing source", func(a *DRAUnprepareAuthority, _, _ *time.Duration) { a.SourceID = "" }},
		{"missing key ID", func(a *DRAUnprepareAuthority, _, _ *time.Duration) { a.KeyID = "" }},
		{"missing driver", func(a *DRAUnprepareAuthority, _, _ *time.Duration) { a.Driver = "" }},
		{"zero age", func(_ *DRAUnprepareAuthority, age, _ *time.Duration) { *age = 0 }},
		{"negative skew", func(_ *DRAUnprepareAuthority, _, skew *time.Duration) { *skew = -time.Second }},
		{"skew over age", func(_ *DRAUnprepareAuthority, _, skew *time.Duration) { *skew = 15 * time.Minute }},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			source := a
			age, skew := 10*time.Minute, 30*time.Second
			tc.edit(&source, &age, &skew)
			if verifier, err := NewDRAUnprepareVerifier(source, age, skew); err == nil || verifier != nil {
				t.Fatalf("bad verifier setup accepted: %+v", verifier)
			}
		})
	}
}
