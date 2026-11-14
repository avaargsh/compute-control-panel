package provider

import (
	"crypto/ed25519"
	"errors"
	"testing"
	"time"
)

// A time outside encoding/json's RFC3339 year range cannot be signed as
// a canonical event. The test deliberately signs the bytes produced by the
// current helper: an invalid marshaled payload must still fail closed.
func TestDRAUnprepareRejectsUnencodableSignedTimestamp(t *testing.T) {
	authority, private := unprepareTestAuthority()
	receipt := unprepareTestReceipt(private)
	receipt.Event.CompletedAt = time.Date(10000, time.November, 10, 10, 0, 30, 0, time.UTC)
	receipt.Signature = ed25519.Sign(private, draUnprepareSigningBytes(receipt.Event))

	verifier, err := NewDRAUnprepareVerifier(authority, 10*time.Minute, 30*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	now := receipt.Event.CompletedAt.Add(30 * time.Second)
	got, err := verifier.Correlate(releaseCandidateFixture(), receipt, now)
	if !errors.Is(err, ErrUncorrelatedDRAUnprepare) || got != (DRAUnprepareCorrelation{}) {
		t.Fatalf("unencodable signed event was correlated: %+v err=%v", got, err)
	}
}
