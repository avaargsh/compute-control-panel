package observation

import (
	"testing"
	"time"
)

func sampleRecord() Record {
	at := time.Date(2026, 11, 21, 8, 0, 0, 0, time.UTC)
	return Record{
		TenantID: "tenant-a", ProjectID: "project-a",
		Resource:  ResourceRef{ClusterID: "cluster-a", Kind: "GPU", Name: "node-a/GPU-1", UID: "GPU-1"},
		Condition: ConditionAvailable, Source: "kubernetes/device-plugin", ObservedAt: at, ExpiresAt: at.Add(time.Minute),
		SourceRevision: "rv-6",
	}
}

func TestProjectValidObservation(t *testing.T) {
	r := sampleRecord()
	v, err := Project(r, r.ObservedAt.Add(30*time.Second))
	if err != nil {
		t.Fatal(err)
	}
	if v.Status != StatusAvailable || v.Source != "kubernetes/device-plugin" || !v.ObservedAt.Equal(r.ObservedAt) || v.ProjectID != "project-a" {
		t.Fatalf("unexpected view: %+v", v)
	}
}

func TestProjectFailsClosedOnTimestampOrSource(t *testing.T) {
	for _, tc := range []struct {
		name   string
		modify func(*Record)
		want   Status
	}{
		{"expired", func(r *Record) {}, StatusStale},
		{"missing source", func(r *Record) { r.Source = "" }, StatusUnknown},
		{"future observation", func(r *Record) {
			r.ObservedAt = r.ObservedAt.Add(2 * time.Minute)
			r.ExpiresAt = r.ExpiresAt.Add(2 * time.Minute)
		}, StatusUnknown},
		{"bad expiry", func(r *Record) { r.ExpiresAt = r.ObservedAt }, StatusUnknown},
		{"invalid condition", func(r *Record) { r.Condition = "READY" }, StatusUnknown},
	} {
		t.Run(tc.name, func(t *testing.T) {
			r := sampleRecord()
			tc.modify(&r)
			now := r.ObservedAt.Add(30 * time.Second)
			if tc.name == "expired" {
				now = r.ExpiresAt
			}
			if tc.name == "future observation" {
				now = sampleRecord().ObservedAt.Add(30 * time.Second)
			}
			v, err := Project(r, now)
			if err != nil {
				t.Fatal(err)
			}
			if v.Status != tc.want || v.ReasonCode == "" && tc.want != StatusAvailable {
				t.Fatalf("status=%s reason=%q want %s", v.Status, v.ReasonCode, tc.want)
			}
		})
	}
}

func TestProjectRejectsMissingIdentity(t *testing.T) {
	for _, tc := range []struct {
		name string
		mod  func(*Record)
	}{
		{"tenant", func(r *Record) { r.TenantID = "" }},
		{"project", func(r *Record) { r.ProjectID = "" }},
		{"cluster", func(r *Record) { r.Resource.ClusterID = "" }},
		{"kind", func(r *Record) { r.Resource.Kind = "" }},
		{"name", func(r *Record) { r.Resource.Name = "" }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			r := sampleRecord()
			tc.mod(&r)
			if _, err := Project(r, r.ObservedAt); err == nil {
				t.Fatal("invalid resource identity accepted")
			}
		})
	}
}

func TestSnapshotModeRequiresKnownValue(t *testing.T) {
	if err := ValidateMode(ModeLive); err != nil {
		t.Fatal(err)
	}
	if err := ValidateMode(ModeDemo); err != nil {
		t.Fatal(err)
	}
	if err := ValidateMode("other"); err == nil {
		t.Fatal("unsupported demo/live mode")
	}
}

func TestProjectPreservesSourceUnknownReason(t *testing.T) {
	r := sampleRecord()
	r.Condition = ConditionUnknown
	r.ReasonCode = "GPU_DRIVER_UNREACHABLE"
	v, err := Project(r, r.ObservedAt.Add(5*time.Second))
	if err != nil {
		t.Fatal(err)
	}
	if v.Status != StatusUnknown || v.ReasonCode != "GPU_DRIVER_UNREACHABLE" {
		t.Fatalf("source diagnosis lost: %+v", v)
	}
}
