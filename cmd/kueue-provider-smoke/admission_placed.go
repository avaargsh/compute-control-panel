package main

import (
	"context"
	"errors"
	"fmt"
	"io"

	"github.com/avaargsh/compute-control-panel/internal/operation"
	"github.com/avaargsh/compute-control-panel/internal/provider"
)

// projectStablePinnedPlacement consumes two immutable Job/Kueue/Pod API
// snapshots. Kubernetes resourceVersions are opaque equality fences; a
// changed value, UID or admission state means no converged projection.
// Provenance is an unattested read label, never driver/device proof.
func projectStablePinnedPlacement(
	pin provider.KueueCreatePin,
	first, second provider.KueueJobCollection,
) (admissionReport, error) {
	reject := func() (admissionReport, error) {
		return admissionReport{}, provider.ErrInvalidKueueObservation
	}
	if pin.Namespace != smokeNamespace || pin.Name == "" || pin.UID == "" ||
		pin.Request != admissionCreateRequest() ||
		first.Stage != provider.KueuePodPlaced ||
		second.Stage != provider.KueuePodPlaced ||
		first.Evidence != second.Evidence ||
		first.Provenance.Source != "kubernetes-https-readonly-unattested" ||
		second.Provenance.Source != first.Provenance.Source ||
		first.Provenance.AdapterVersion == "" ||
		second.Provenance.AdapterVersion != first.Provenance.AdapterVersion ||
		first.Provenance.ObservedAt.IsZero() ||
		second.Provenance.ObservedAt.Before(first.Provenance.ObservedAt) {
		return reject()
	}
	e := first.Evidence
	if e.Job.Namespace != pin.Namespace || e.Job.Name != pin.Name ||
		e.Job.UID != pin.UID || e.TenantID != pin.Request.TenantID ||
		e.WorkloadID != pin.Request.WorkloadID ||
		e.DesiredGeneration != pin.Request.DesiredGeneration {
		return reject()
	}
	projected, err := provider.ProjectPlacementWithoutDeviceProof(e)
	if err != nil || projected.Phase != "NOT_READY" || !projected.NodeBound ||
		projected.Allocated || projected.Active || projected.Ready ||
		projected.Costable || projected.Released {
		return reject()
	}
	return admissionReport{
		Phase: "inspect-placed", Projection: projected.Phase, Stage: string(projected.Stage),
		Result: string(operation.ResourcePresent), QuotaReserved: projected.QuotaReserved,
		Admitted: projected.Admitted, ClusterQueue: projected.ClusterQueue,
		PodSetAssignments: projected.PodSetAssignmentsJSON,
		WorkloadNamespace: projected.WorkloadNamespace, WorkloadName: projected.WorkloadName,
		WorkloadUID: projected.WorkloadUID, WorkloadResourceVersion: projected.WorkloadResourceVersion,
		JobUID: projected.JobUID, JobName: projected.JobName, JobResourceVersion: projected.JobResourceVersion,
		PodUID: projected.PodUID, PodResourceVersion: projected.PodResourceVersion,
		PodScheduled: projected.PodScheduled, NodeBound: projected.NodeBound, NodeName: projected.NodeName,
		AbsenceObserved: false, Allocated: false, Active: false, Ready: false, Costable: false,
		Released: false, ReleaseAttested: false, OperationID: pin.Request.OperationID,
	}, nil
}

// admissionInspectPlaced is disposable-kind-only. It never issues CREATE,
// DELETE or a device mutation. The admitted Job's authority comes from the
// independently persisted pin, not Kubernetes name, user input or a Pod
// annotation. Inspect checks the provider-side Job UID again before report.
// Device state remains unknown even when the Pod is node-bound.
func admissionInspectPlaced(
	ctx context.Context, adapter *provider.KueueJobAdapter,
	pin provider.KueueCreatePin, out io.Writer,
) error {
	if adapter == nil {
		return provider.ErrInvalidKueueObservation
	}
	create := admissionCreateRequest()
	first, err := adapter.CollectCreate(ctx, create)
	if err != nil {
		return fmt.Errorf("first pinned placement read: %w", err)
	}
	second, err := adapter.CollectCreate(ctx, create)
	if err != nil {
		return fmt.Errorf("second pinned placement read: %w", err)
	}
	report, err := projectStablePinnedPlacement(pin, first, second)
	if err != nil {
		return fmt.Errorf("placed Pod evidence not stable and pinned: %w", err)
	}
	obs, err := stableJobObservation(ctx, func(ctx context.Context) (operation.Observation, error) {
		return adapter.Inspect(ctx, create)
	})
	if err != nil {
		return err
	}
	if obs.Result != operation.ResourcePresent || obs.ResourceUID != pin.UID ||
		obs.ResourceID != pin.Namespace+"/"+pin.Name {
		return errors.New("post-observation provider Inspect did not match durable Job UID")
	}
	return writeDurable(out, report)
}
