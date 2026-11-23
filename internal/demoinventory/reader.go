// Package demoinventory produces synthetic observations compatible with the
// real Kubernetes inventory schema. Its facts are never trusted GPU evidence.
package demoinventory

import (
	"context"
	"errors"
	"strings"
	"time"

	"github.com/avaargsh/compute-control-panel/internal/observation"
)

var ErrScope = errors.New("demo inventory tenant scope denied")

const source = "synthetic/demo"

// Reader is intentionally separate from the live Kubernetes adapter.
// The tenant/project scope is fixed by trusted construction, never by a URL.
type Reader struct {
	tenantID  string
	projectID string
	now       func() time.Time
}

func NewReader(tenantID, projectID string, now func() time.Time) (*Reader, error) {
	if strings.TrimSpace(tenantID) == "" || strings.TrimSpace(projectID) == "" || now == nil {
		return nil, errors.New("demo reader requires an explicit tenant, project and clock")
	}
	return &Reader{tenantID: tenantID, projectID: projectID, now: now}, nil
}

func (r *Reader) ListObservations(ctx context.Context, tenantID string, projectIDs []string) (observation.Snapshot, error) {
	if r == nil || tenantID != r.tenantID {
		return observation.Snapshot{}, ErrScope
	}
	if err := ctx.Err(); err != nil {
		return observation.Snapshot{}, err
	}
	out := observation.Snapshot{DataMode: observation.ModeDemo, Items: []observation.Record{}}
	granted := false
	for _, project := range projectIDs {
		if project == r.projectID {
			granted = true
			break
		}
	}
	if !granted {
		return out, nil
	}
	now := r.now().UTC()
	if now.IsZero() {
		return observation.Snapshot{}, errors.New("demo clock is unavailable")
	}
	record := func(kind, name, uid string, condition observation.Condition, reason string, quantity *observation.Quantity) observation.Record {
		return observation.Record{
			TenantID: r.tenantID, ProjectID: r.projectID,
			Resource: observation.ResourceRef{
				ClusterID: "demo-cluster", Kind: kind, Name: name,
				UID: uid, ResourceVersion: "fixture-v1",
			},
			Condition: condition, ReasonCode: reason, Quantity: quantity,
			Source: source, SourceRevision: "fixture-v1",
			ObservedAt: now, ExpiresAt: now.Add(90 * time.Second),
		}
	}
	out.Items = []observation.Record{
		record("Node", "demo-cpu-01", "synthetic-node-cpu", observation.ConditionAvailable, "", nil),
		record("Node", "demo-gpu-nvidia-01", "synthetic-node-nvidia", observation.ConditionAvailable, "", nil),
		record("Node", "demo-gpu-ascend-01", "synthetic-node-ascend", observation.ConditionDegraded, "NODE_UNSCHEDULABLE", nil),
		record("NodeGPUResource", "demo-gpu-nvidia-01/nvidia.com/gpu", "", observation.ConditionUnknown,
			"NODE_ALLOCATABLE_NOT_FREE_CAPACITY", &observation.Quantity{
				ResourceName: "nvidia.com/gpu", Capacity: "4", Allocatable: "4",
			}),
		record("NodeGPUResource", "demo-gpu-ascend-01/huawei.com/Ascend910", "", observation.ConditionUnknown,
			"NODE_ALLOCATABLE_NOT_FREE_CAPACITY", &observation.Quantity{
				ResourceName: "huawei.com/Ascend910", Capacity: "8", Allocatable: "6",
			}),
	}
	return out, nil
}
