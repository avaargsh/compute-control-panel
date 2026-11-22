package provider

import (
	"testing"

	"github.com/avaargsh/compute-control-panel/internal/operation"
)

// This is a regression guard for the trust boundary, NOT an authority test.
// The pure join has no authenticated collector or durable Allocation reader;
// a syntactically correct caller-supplied absence can correlate even when its
// origin is fabricated. No production release writer may use this result as
// proof of physical GPU deallocation, RELEASED, or Cost FINAL.
func TestReleaseCorrelationCallerSuppliedClaimAbsenceIsNotAuthority(t *testing.T) {
	candidate := releaseCandidateFixture()
	prior := candidate.PriorPodClaim.Claim
	candidate.ClaimUnallocated = DRAClaimUnallocatedObservation{}
	candidate.ClaimAbsent = &DRAClaimUIDAbsentObservation{
		PreviousClaim: prior.Claim,
		ClaimListResourceVersion: "caller-supplied-not-attested",
	}

	got, err := CorrelateReleaseEvidence(candidate)
	if err != nil {
		t.Fatalf("structural correlation unexpectedly rejected: %v", err)
	}
	if got.AllocationID != candidate.Delete.AllocationID ||
		got.Claim.UID != prior.Claim.UID {
		t.Fatalf("identity join changed: %+v", got)
	}
	// A current desired_generation is not an input to this function.
	// This candidate is structurally valid even if a newer CREATE has since
	// superseded the DELETE. A durable CAS is required at any release writer.
	candidate.Create.Generation = 7
	candidate.Delete.Generation = 8
	candidate.ProviderAbsence.DesiredGeneration = 8
	got, err = CorrelateReleaseEvidence(candidate)
	if err != nil || got.DeleteOperationID != candidate.Delete.ID {
		t.Fatalf("structural join unexpectedly enforced a nonexistent durable CAS: %+v %v", got, err)
	}
}

func TestReleaseCorrelationDeletedClaimRejectsStaleOrConflictingOperation(t *testing.T) {
	cases := []struct {
		name   string
		mutate func(*ReleaseEvidenceCandidate)
	}{
		{"unknown DELETE", func(e *ReleaseEvidenceCandidate) { e.Delete.Status = operation.Unknown }},
		{"same-generation DELETE", func(e *ReleaseEvidenceCandidate) {
			e.Delete.Generation = e.Create.Generation
			e.ProviderAbsence.DesiredGeneration = e.Delete.Generation
		}},
		{"old DELETE observation", func(e *ReleaseEvidenceCandidate) {
			e.ProviderAbsence.DesiredGeneration = e.Create.Generation
		}},
		{"conflicting still-existing Claim", func(e *ReleaseEvidenceCandidate) {
			e.ClaimUnallocated = releaseCandidateFixture().ClaimUnallocated
		}},
		{"different Claim UID", func(e *ReleaseEvidenceCandidate) {
			e.ClaimAbsent.PreviousClaim.UID = "successor-uid"
		}},
		{"missing LIST version", func(e *ReleaseEvidenceCandidate) {
			e.ClaimAbsent.ClaimListResourceVersion = ""
		}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			e := releaseCandidateFixture()
			e.ClaimUnallocated = DRAClaimUnallocatedObservation{}
			e.ClaimAbsent = &DRAClaimUIDAbsentObservation{
				PreviousClaim: e.PriorPodClaim.Claim.Claim,
				ClaimListResourceVersion: "440",
			}
			tc.mutate(&e)
			got, err := CorrelateReleaseEvidence(e)
			if err != ErrUncorrelatedReleaseEvidence || got != (ReleaseEvidenceCorrelation{}) {
				t.Fatalf("must fail closed: got=%+v err=%v", got, err)
			}
		})
	}
}
