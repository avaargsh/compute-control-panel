package provider

import "testing"

// Migration 0003 requires a non-NULL Operation allocation_id to match that
// Allocation's desired_generation. A newer DELETE therefore cannot reuse the
// historical CREATE's non-NULL allocation FK.
func TestReleaseCorrelationAcceptsSchemaCompatibleNewerDelete(t *testing.T) {
	e := releaseCandidateFixture()
	e.Delete.AllocationID = "" // SQL NULL for newer-generation DELETE
	got, err := CorrelateReleaseEvidence(e)
	if err != nil || got.AllocationID != e.Create.AllocationID ||
		got.DeleteOperationID != e.Delete.ID {
		t.Fatalf("schema-compatible DELETE lost historical Allocation link: %+v err=%v", got, err)
	}
}

func TestReleaseCorrelationRejectsUnpersistableCrossGenerationAllocationFK(t *testing.T) {
	e := releaseCandidateFixture()
	// Recreate the old, unpersistable synthetic fixture shape explicitly:
	// newer DELETE falsely claims the CREATE's old-generation Allocation FK.
	e.Delete.AllocationID = e.Create.AllocationID
	if got, err := CorrelateReleaseEvidence(e); err == nil ||
		got != (ReleaseEvidenceCorrelation{}) {
		t.Fatalf("non-NULL cross-generation DELETE allocation FK was accepted: %+v err=%v", got, err)
	}
}
