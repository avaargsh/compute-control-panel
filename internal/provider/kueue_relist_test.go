package provider

import (
	"errors"
	"testing"
)

func relistAdmittedEvidence() KueueJobEvidence {
	e := kueueEvidenceFixture()
	e.PodScheduled = false
	e.PodNodeName = ""
	return e
}

func relistUnknownEvidence() KueueJobEvidence {
	e := kueueEvidenceFixture()
	e.KueueQuotaReserved = false
	e.KueueAdmitted = false
	e.KueueClusterQueue = ""
	e.KueuePodSetAssignmentsJSON = ""
	e.KueueQuotaCondition = KueueConditionEvidence{Status: "Unknown"}
	e.KueueAdmissionCondition = KueueConditionEvidence{Status: "Unknown"}
	e.Pod = KubernetesResourceRef{}
	e.PodOwnerJobUID = ""
	e.PodScheduled = false
	e.PodNodeName = ""
	return e
}

func TestProjectRelistAdmittedOrPlacedStaysNotReady(t *testing.T) {
	admitted, err := ProjectRelistSnapshot(relistAdmittedEvidence())
	if err != nil || admitted.Phase != "NOT_READY" || admitted.Inconclusive ||
		!admitted.Admitted || !admitted.QuotaReserved || admitted.PodScheduled ||
		admitted.WorkloadUID != "kueue-uid-a" || admitted.WorkloadResourceVersion != "102" ||
		admitted.JobUID != "job-uid-a" || admitted.JobResourceVersion != "101" ||
		admitted.PodUID != "pod-uid-a" || admitted.Allocated || admitted.Active ||
		admitted.Ready || admitted.Costable || admitted.Released {
		t.Fatalf("admitted relist: %+v err=%v", admitted, err)
	}
	placed, err := ProjectRelistSnapshot(kueueEvidenceFixture())
	if err != nil || placed.Phase != "NOT_READY" || !placed.PodScheduled || placed.NodeName != "node-1" ||
		placed.Ready || placed.Released || placed.Allocated || placed.Active || placed.Costable {
		t.Fatalf("placed relist after a controller restart was promoted: %+v err=%v", placed, err)
	}
}

func TestProjectRelistInconclusiveIsUnknown(t *testing.T) {
	unknown, err := ProjectRelistSnapshot(relistUnknownEvidence())
	if err != nil || unknown.Phase != "UNKNOWN" || !unknown.Inconclusive ||
		unknown.Admitted || unknown.QuotaReserved || unknown.Ready || unknown.Released ||
		unknown.Allocated || unknown.Active || unknown.Costable ||
		unknown.WorkloadUID != "kueue-uid-a" || unknown.JobUID != "job-uid-a" {
		t.Fatalf("unknown admission: %+v err=%v", unknown, err)
	}
	missing := relistUnknownEvidence()
	missing.KueueWorkload = KubernetesResourceRef{}
	missing.KueueOwnerJobUID = ""
	missing.KueueQuotaCondition = KueueConditionEvidence{}
	missing.KueueAdmissionCondition = KueueConditionEvidence{}
	got, err := ProjectRelistSnapshot(missing)
	if err != nil || got.Phase != "UNKNOWN" || got.WorkloadUID != "" || got.Released || got.Ready {
		t.Fatalf("missing workload: %+v err=%v", got, err)
	}
}

func TestProjectRelistRejectsCrossedIdentities(t *testing.T) {
	e := relistAdmittedEvidence()
	e.KueueWorkload.UID = e.Job.UID
	got, err := ProjectRelistSnapshot(e)
	if !errors.Is(err, ErrInvalidKueueObservation) || got != (RelistProjection{}) || got.Released || got.Ready {
		t.Fatalf("same uid projected: %+v err=%v", got, err)
	}
}

func TestRelistFenceRejectsEarlierReadAndDifferentUID(t *testing.T) {
	currentEvidence := kueueEvidenceFixture()
	currentEvidence.Job.ResourceVersion = "rv-110"
	currentEvidence.KueueWorkload.ResourceVersion = "rv-120"
	currentEvidence.Pod.ResourceVersion = "rv-130"
	current, err := ProjectRelistSnapshot(currentEvidence)
	if err != nil {
		t.Fatal(err)
	}
	current.ReadSequence = 2
	fence, err := AdvanceRelistFence(RelistFence{}, current)
	if err != nil || fence.JobResourceVersion != "rv-110" || fence.Phase != "NOT_READY" || fence.ReadSequence != 2 {
		t.Fatalf("initial fence: %+v err=%v", fence, err)
	}
	earlier := current
	earlier.ReadSequence = 1
	earlier.JobResourceVersion = "999"
	earlier.WorkloadResourceVersion = "999"
	kept, err := AdvanceRelistFence(fence, earlier)
	if !errors.Is(err, ErrStaleKueueRelist) || kept != fence || kept.JobResourceVersion != "rv-110" {
		t.Fatalf("earlier read overwrote the fence: %+v err=%v", kept, err)
	}
	sameName := current
	sameName.JobUID = "job-uid-successor"
	sameName.JobResourceVersion = "rv-200"
	sameName.WorkloadUID = "kueue-uid-successor"
	sameName.WorkloadResourceVersion = "rv-210"
	sameName.ReadSequence = 10
	kept, err = AdvanceRelistFence(fence, sameName)
	if !errors.Is(err, ErrCrossKueueRelist) || kept != fence || kept.JobUID != "job-uid-a" {
		t.Fatalf("successor uid projected onto the original: %+v err=%v", kept, err)
	}
	repeated, err := AdvanceRelistFence(fence, current)
	if err != nil || repeated != fence {
		t.Fatalf("duplicate observation was not de-duplicated: %+v err=%v", repeated, err)
	}
}

func TestRelistResourceVersionIsOpaque(t *testing.T) {
	evidence := kueueEvidenceFixture()
	evidence.Job.ResourceVersion = "job-rv"
	evidence.KueueWorkload.ResourceVersion = "wl-0"
	evidence.Pod.ResourceVersion = "pod/010"
	nonNumeric, err := ProjectRelistSnapshot(evidence)
	if err != nil || nonNumeric.JobResourceVersion != "job-rv" || nonNumeric.WorkloadResourceVersion != "wl-0" ||
		nonNumeric.PodResourceVersion != "pod/010" || nonNumeric.Ready || nonNumeric.Released {
		t.Fatalf("non-numeric resourceVersion rejected: %+v err=%v", nonNumeric, err)
	}
	evidence.Job.ResourceVersion = "200"
	evidence.KueueWorkload.ResourceVersion = "300"
	evidence.Pod.ResourceVersion = "400"
	current, err := ProjectRelistSnapshot(evidence)
	if err != nil {
		t.Fatal(err)
	}
	current.ReadSequence = 1
	fence, err := AdvanceRelistFence(RelistFence{}, current)
	if err != nil || fence.JobResourceVersion != "200" || fence.WorkloadResourceVersion != "300" {
		t.Fatalf("initial numeric resourceVersion rejected: %+v err=%v", fence, err)
	}
	later := current
	later.ReadSequence = 2
	later.JobResourceVersion = "10"
	later.WorkloadResourceVersion = "0"
	later.PodResourceVersion = "1"
	fence, err = AdvanceRelistFence(fence, later)
	if err != nil || fence.JobResourceVersion != "10" || fence.ReadSequence != 2 || fence.Phase != "NOT_READY" {
		t.Fatalf("later read with a numerically smaller resourceVersion was rejected: %+v err=%v", fence, err)
	}
	empty := later
	empty.JobResourceVersion = ""
	kept, err := AdvanceRelistFence(fence, empty)
	if !errors.Is(err, ErrInvalidKueueObservation) || kept != fence {
		t.Fatalf("empty resourceVersion was fenced: %+v err=%v", kept, err)
	}
	otherPod := later
	otherPod.ReadSequence = 1
	otherPod.PodUID = "pod-uid-other"
	kept, err = AdvanceRelistFence(fence, otherPod)
	if !errors.Is(err, ErrCrossKueueRelist) || kept != fence {
		t.Fatalf("earlier pod uid change was accepted: %+v err=%v", kept, err)
	}
	otherPod.ReadSequence = 3
	fence, err = AdvanceRelistFence(fence, otherPod)
	if err != nil || fence.PodUID != "pod-uid-other" || fence.ReadSequence != 3 || fence.Phase != "NOT_READY" {
		t.Fatalf("later pod uid change was rejected: %+v err=%v", fence, err)
	}
}

func TestRelistFenceMovesForwardAndKeepsInconclusiveOffReady(t *testing.T) {
	early, err := ProjectRelistSnapshot(relistUnknownEvidence())
	if err != nil {
		t.Fatal(err)
	}
	early.ReadSequence = 1
	fence, err := AdvanceRelistFence(RelistFence{}, early)
	if err != nil || fence.Phase != "UNKNOWN" {
		t.Fatalf("early fence: %+v err=%v", fence, err)
	}
	laterEvidence := kueueEvidenceFixture()
	laterEvidence.Job.ResourceVersion = "110"
	laterEvidence.KueueWorkload.ResourceVersion = "120"
	later, err := ProjectRelistSnapshot(laterEvidence)
	if err != nil {
		t.Fatal(err)
	}
	later.ReadSequence = 2
	fence, err = AdvanceRelistFence(fence, later)
	if err != nil || fence.Phase != "NOT_READY" || fence.JobResourceVersion != "110" ||
		fence.WorkloadResourceVersion != "120" || fence.PodUID != "pod-uid-a" {
		t.Fatalf("re-list did not advance: %+v err=%v", fence, err)
	}
	kept, err := AdvanceRelistFence(fence, early)
	if !errors.Is(err, ErrStaleKueueRelist) || kept != fence {
		t.Fatalf("inconclusive older observation replaced the fence: %+v err=%v", kept, err)
	}
	inconclusive := later
	inconclusive.Phase = "UNKNOWN"
	inconclusive.Inconclusive = true
	inconclusive.Admitted = false
	inconclusive.QuotaReserved = false
	inconclusive.ClusterQueue = ""
	inconclusive.PodUID = ""
	inconclusive.PodResourceVersion = ""
	inconclusive.PodScheduled = false
	inconclusive.NodeName = ""
	inconclusive.JobResourceVersion = "111"
	inconclusive.WorkloadResourceVersion = "121"
	inconclusive.ReadSequence = fence.ReadSequence + 1
	kept, err = AdvanceRelistFence(fence, inconclusive)
	if !errors.Is(err, ErrRelistInconclusive) || kept != fence || kept.Phase != "NOT_READY" {
		t.Fatalf("restart-window uncertainty replaced a conclusive fence: %+v err=%v", kept, err)
	}
}

func TestConvergeRelistUsesCurrentReadWithinBound(t *testing.T) {
	calls := 0
	newer := kueueEvidenceFixture()
	newer.Job.ResourceVersion = "140"
	newer.KueueWorkload.ResourceVersion = "150"
	newer.Pod.ResourceVersion = "160"
	fence, attempts, err := ConvergeRelist(5, "job-uid-a", 0, func() (KueueJobEvidence, error) {
		calls++
		switch calls {
		case 1:
			return KueueJobEvidence{}, ErrInvalidKueueObservation
		case 2:
			return relistUnknownEvidence(), nil
		default:
			return newer, nil
		}
	})
	if err != nil || attempts != 3 || calls != 3 || fence.Phase != "NOT_READY" ||
		fence.JobUID != "job-uid-a" || fence.JobResourceVersion != "140" ||
		fence.WorkloadResourceVersion != "150" {
		t.Fatalf("converge: %+v attempts=%d calls=%d err=%v", fence, attempts, calls, err)
	}
	stale, err := ProjectRelistSnapshot(relistAdmittedEvidence())
	if err != nil {
		t.Fatal(err)
	}
	stale.ReadSequence = 1
	if stale.ReadSequence >= fence.ReadSequence {
		t.Fatalf("recorded read is not earlier than the relist: stale=%d fence=%d", stale.ReadSequence, fence.ReadSequence)
	}
	kept, err := AdvanceRelistFence(fence, stale)
	if !errors.Is(err, ErrStaleKueueRelist) || kept != fence {
		t.Fatalf("converged fence kept the stale observation: %+v err=%v", kept, err)
	}
}

func TestConvergeRelistRejectsOtherJobUIDAndBoundsAttempts(t *testing.T) {
	other := kueueEvidenceFixture()
	other.Job.UID = "job-uid-other"
	other.KueueOwnerJobUID = other.Job.UID
	other.PodOwnerJobUID = other.Job.UID
	fence, attempts, err := ConvergeRelist(3, "job-uid-a", 0, func() (KueueJobEvidence, error) {
		return other, nil
	})
	if !errors.Is(err, ErrCrossKueueRelist) || attempts != 1 || !fence.empty() || fence.JobUID != "" {
		t.Fatalf("other uid was fenced: %+v attempts=%d err=%v", fence, attempts, err)
	}
	fence, attempts, err = ConvergeRelist(4, "job-uid-a", 0, func() (KueueJobEvidence, error) {
		return KueueJobEvidence{}, ErrInvalidKueueObservation
	})
	if !errors.Is(err, ErrRelistInconclusive) || attempts != 4 || !fence.empty() {
		t.Fatalf("exhausted relist: %+v attempts=%d err=%v", fence, attempts, err)
	}
	if _, _, err := ConvergeRelist(RelistAttemptLimit+1, "job-uid-a", 0, func() (KueueJobEvidence, error) {
		return newerOrEmpty(), nil
	}); !errors.Is(err, ErrInvalidKueueObservation) {
		t.Fatalf("unbounded relist was accepted: %v", err)
	}
}

func newerOrEmpty() KueueJobEvidence {
	return KueueJobEvidence{}
}
