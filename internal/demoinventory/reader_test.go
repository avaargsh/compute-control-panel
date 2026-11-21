package demoinventory

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/avaargsh/compute-control-panel/internal/observation"
)

func TestDemoReaderUsesSameObservationSnapshotAsKubernetes(t *testing.T) {
	now := time.Date(2026, 11, 21, 8, 30, 0, 0, time.UTC)
	reader, err := NewReader("demo-tenant", "demo-project", func() time.Time { return now })
	if err != nil {
		t.Fatal(err)
	}
	snapshot, err := reader.ListObservations(context.Background(), "demo-tenant", []string{"demo-project"})
	if err != nil {
		t.Fatal(err)
	}
	if snapshot.DataMode != observation.ModeDemo || len(snapshot.Items) != 5 {
		t.Fatalf("wrong mode or record count: %+v", snapshot)
	}
	nodeCount, gpuCount := 0, 0
	for _, record := range snapshot.Items {
		if record.TenantID != "demo-tenant" || record.ProjectID != "demo-project" ||
			record.Source != "synthetic/demo" || record.SourceRevision != "fixture-v1" ||
			record.EvidenceRef != "" || !record.ObservedAt.Equal(now) ||
			!record.ExpiresAt.After(now) {
			t.Fatalf("demo record is not clearly synthetic: %+v", record)
		}
		view, err := observation.Project(record, now)
		if err != nil {
			t.Fatal(err)
		}
		switch record.Resource.Kind {
		case "Node":
			nodeCount++
			if record.Resource.UID == "" || record.Quantity != nil {
				t.Fatalf("unexpected node shape: %+v", record)
			}
		case "NodeGPUResource":
			gpuCount++
			if record.Resource.UID != "" || record.Quantity == nil ||
				record.Quantity.ResourceName == "" || record.Quantity.Capacity == "" ||
				record.Quantity.Allocatable == "" ||
				view.Status != observation.StatusUnknown ||
				view.ReasonCode != "NODE_ALLOCATABLE_NOT_FREE_CAPACITY" {
				t.Fatalf("demo pretends GPU quantity is free or physically attested: %+v", record)
			}
		default:
			t.Fatalf("unexpected demo kind %q", record.Resource.Kind)
		}
	}
	if nodeCount != 3 || gpuCount != 2 {
		t.Fatalf("expected three nodes and two resource advertisements, got %d/%d", nodeCount, gpuCount)
	}
}

func TestDemoReaderDeniesForeignScopeBeforeReturningAnyData(t *testing.T) {
	reader, err := NewReader("demo-tenant", "demo-project", time.Now)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := reader.ListObservations(context.Background(), "foreign-tenant", []string{"demo-project"}); !errors.Is(err, ErrScope) {
		t.Fatalf("foreign tenant should fail closed: %v", err)
	}
	snapshot, err := reader.ListObservations(context.Background(), "demo-tenant", []string{"foreign-project"})
	if err != nil || snapshot.DataMode != observation.ModeDemo || len(snapshot.Items) != 0 {
		t.Fatalf("foreign project must receive no records, got %+v err=%v", snapshot, err)
	}
}

func TestDemoReaderRequiresExplicitScopeAndClock(t *testing.T) {
	for _, tc := range []struct {
		name string
		t, p string
		clock func() time.Time
	}{
		{"no tenant", "", "project", time.Now},
		{"no project", "tenant", "", time.Now},
		{"no clock", "tenant", "project", nil},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if _, err := NewReader(tc.t, tc.p, tc.clock); err == nil {
				t.Fatal("invalid demo source admitted")
			}
		})
	}
	reader, _ := NewReader("tenant", "project", func() time.Time { return time.Time{} })
	if _, err := reader.ListObservations(context.Background(), "tenant", []string{"project"}); err == nil {
		t.Fatal("zero observation clock admitted")
	}
}

func TestDemoReaderHonorsContextCancellation(t *testing.T) {
	reader, err := NewReader("tenant", "project", time.Now)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := reader.ListObservations(ctx, "tenant", []string{"project"}); !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled demo read returned %v", err)
	}
}
