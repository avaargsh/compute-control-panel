package main

import (
	"context"
	"fmt"
	"io"
	"time"

	"github.com/avaargsh/compute-control-panel/internal/operation"
	"github.com/avaargsh/compute-control-panel/internal/provider"
)

// singleWorkloadDRAEvidence joins already validated reader observations. The
// surrounding collection rechecks the durable Job identity after DRA reads.
// Simulated inventory never promotes the CCP allocation lifecycle.
func singleWorkloadDRAEvidence(pin provider.KueueCreatePin, first, second provider.KueueJobCollection, inv provider.DRAPoolInventoryObservation) (map[string]any, error) {
	placed, err := projectStablePinnedPlacement(pin, first, second)
	p := inv.PodClaim
	if err != nil || p.Pod != first.Evidence.Pod || p.NodeName != placed.NodeName ||
		p.Claim.PodUID != p.Pod.UID || p.Claim.Claim.Namespace != p.Pod.Namespace ||
		p.Claim.Claim.UID == "" || p.Claim.Claim.ResourceVersion == "" ||
		p.Claim.Device.Driver == "" || p.Claim.Device.Pool == "" || p.Claim.Device.Device == "" ||
		inv.Slice.UID == "" || inv.Slice.ResourceVersion == "" {
		return nil, provider.ErrInvalidDRAObservation
	}
	req := pin.Request
	return map[string]any{
		"case_id": "M1-SINGLE-WORKLOAD-DRA", "environment": "DISPOSABLE_KIND", "device_authority": "SIMULATED_DEVICE_ONLY",
		"ccp_commit": nil, "tenant_id": req.TenantID, "workload_id": req.WorkloadID, "allocation_id": nil,
		"operation_id": req.OperationID, "operation_generation": req.DesiredGeneration,
		"job_uid": placed.JobUID, "kueue_workload_uid": placed.WorkloadUID,
		"pod_uid": p.Pod.UID, "pod_owner_job_uid": first.Evidence.PodOwnerJobUID, "pod_node": p.NodeName,
		"resourceclaim_uid": p.Claim.Claim.UID,
		"kueue_admitted":    placed.Admitted, "quota_reserved": placed.QuotaReserved,
		"pod_scheduled": placed.PodScheduled, "claim_allocated": true,
		"device":                map[string]any{"driver": p.Claim.Device.Driver, "pool": p.Claim.Device.Pool, "device": p.Claim.Device.Device, "resourceslice_uid": inv.Slice.UID},
		"resource_versions":     map[string]string{"job": placed.JobResourceVersion, "kueue_workload": placed.WorkloadResourceVersion, "pod": p.Pod.ResourceVersion, "resourceclaim": p.Claim.Claim.ResourceVersion, "resourceslice": inv.Slice.ResourceVersion},
		"physical_gpu_verified": false, "allocation_ready": false, "allocation_released": false, "cost_final": false,
		"quota_device_causality": "UNKNOWN",
	}, nil
}

func admissionInspectDRA(getenv func(string) string, out io.Writer) error {
	adapter, err := openAdmissionAdapter(getenv)
	if err != nil {
		return err
	}
	db, store, err := openAdmissionPinStore(getenv)
	if err != nil {
		return err
	}
	defer db.Close()
	pin, err := loadAdmissionPin(store)
	if err != nil {
		return err
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	first, err := adapter.CollectCreate(ctx, pin.Request)
	if err != nil {
		return err
	}
	endpoint, err := apiEndpoint(getenv)
	if err != nil {
		return err
	}
	ca, token := durableCredentialPaths(getenv)
	reader, err := provider.NewServiceAccountKubernetesObservationReader(endpoint, ca, token)
	if err != nil {
		return err
	}
	inv, err := reader.ObserveDRAPoolInventory(ctx, provider.DRAPoolInventoryQuery{Pod: provider.DRAPodClaimQuery{
		Claim: provider.DRAClaimQuery{Namespace: pin.Namespace, ClaimName: getenv("CCP_KUEUE_DRA_CLAIM"), ExpectedClaimUID: getenv("CCP_KUEUE_DRA_CLAIM_UID"),
			PodName: first.Evidence.Pod.Name, ExpectedPodUID: first.Evidence.Pod.UID, Request: "gpu", Driver: getenv("CCP_KUEUE_DRA_DRIVER"), Pool: getenv("CCP_KUEUE_DRA_POOL"), Device: getenv("CCP_KUEUE_DRA_DEVICE")},
		ExpectedJobUID: pin.UID, ExpectedNodeName: first.Evidence.PodNodeName, PodClaimAlias: "gpu",
		ClaimTemplateName: getenv("CCP_KUEUE_DRA_TEMPLATE"),
	}})
	if err != nil {
		return fmt.Errorf("same-workload DRA observation: %w", err)
	}
	second, err := adapter.CollectCreate(ctx, pin.Request)
	if err != nil {
		return err
	}
	evidence, err := singleWorkloadDRAEvidence(pin, first, second, inv)
	if err != nil {
		return err
	}
	obs, err := stableJobObservation(ctx, func(ctx context.Context) (operation.Observation, error) {
		return adapter.Inspect(ctx, pin.Request)
	})
	if err != nil {
		return err
	}
	if obs.Result != operation.ResourcePresent || obs.ResourceUID != pin.UID || obs.ResourceID != pin.Namespace+"/"+pin.Name {
		return provider.ErrInvalidDRAObservation
	}
	return writeDurable(out, evidence)
}
