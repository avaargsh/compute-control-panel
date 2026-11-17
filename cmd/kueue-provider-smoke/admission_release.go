package main

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"time"

	"github.com/avaargsh/compute-control-panel/internal/operation"
	"github.com/avaargsh/compute-control-panel/internal/provider"
)

// admissionReleaseSeed carries disposable API observations across the existing
// CREATE/DELETE smoke processes. Its Job identity is checked against the durable
// binding store. Source trust and Allocation identity require separate evidence.
type admissionReleaseSeed struct {
	Pin      provider.KueueCreatePin            `json:"pin"`
	Queue    provider.KueueQueueBindingEvidence `json:"queue"`
	PodClaim provider.DRAPodClaimObservation    `json:"pod_claim"`
}

func observeAdmissionRelease(ctx context.Context, reader *provider.KubernetesObservationReader, pin provider.KueueCreatePin, seed admissionReleaseSeed, absence operation.Observation) (map[string]any, error) {
	placed, err := provider.ProjectPlacementWithoutDeviceProof(seed.Queue.Job)
	job := seed.Queue.Job
	p := seed.PodClaim
	remove := admissionDeleteRequest(pin.Namespace+"/"+pin.Name, pin.UID)
	if err != nil || reader == nil || seed.Pin != pin || pin.Request != admissionCreateRequest() || pin.Namespace != smokeNamespace ||
		seed.Queue.Stage != provider.KueuePodPlaced || placed.JobUID != pin.UID || placed.JobName != pin.Name ||
		job.Job.Namespace != pin.Namespace || job.TenantID != pin.Request.TenantID || job.WorkloadID != pin.Request.WorkloadID || job.DesiredGeneration != pin.Request.DesiredGeneration ||
		seed.Queue.LocalQueue.Namespace != pin.Namespace || seed.Queue.LocalQueue.Name != admissionLocalQueue || seed.Queue.ClusterQueue.Name != admissionLocalQueue ||
		p.Pod != job.Pod || p.NodeName != job.PodNodeName || p.Claim.PodUID != p.Pod.UID || p.Claim.Claim.Namespace != pin.Namespace ||
		p.PodClaimAlias != "gpu" || p.ConsumerContainer != "ccp-queued" ||
		absence.OperationID != remove.OperationID || absence.TenantID != remove.TenantID || absence.WorkloadID != remove.WorkloadID ||
		absence.ProviderID != remove.ProviderID || absence.DesiredGeneration != remove.DesiredGeneration ||
		absence.Result != operation.TargetAbsent || absence.TargetResourceID != remove.TargetResourceID || absence.TargetResourceUID != remove.TargetResourceUID ||
		absence.ResourceID != "" || absence.ResourceUID != "" || absence.ResourceOperationID != "" || absence.ResourceGeneration != 0 || absence.ErrorCode != "" || absence.ReasonCode != "" || absence.ResourceVersion == "" || absence.ObservedAt.IsZero() {
		return nil, provider.ErrUncorrelatedReleaseEvidence
	}
	report := map[string]any{
		"case_id": "M2-SINGLE-WORKLOAD-RELEASE", "environment": "DISPOSABLE_KIND", "device_authority": "SIMULATED_DEVICE_ONLY",
		"evidence_authority": "KUBERNETES_READONLY_UNATTESTED", "release_status": "UNKNOWN",
		"ccp_commit": nil, "allocation_id": nil, "tenant_id": pin.Request.TenantID, "workload_id": pin.Request.WorkloadID,
		"operation_id": remove.OperationID, "operation_generation": remove.DesiredGeneration,
		"job_uid": pin.UID, "pod_uid": p.Pod.UID, "pod_owner_job_uid": job.PodOwnerJobUID, "pod_node": p.NodeName,
		"kueue_workload_uid": job.KueueWorkload.UID, "resourceclaim_uid": p.Claim.Claim.UID,
		"device": p.Claim.Device, "provider_target_absent": true, "provider_absence_verified": nil, "driver_unprepare_verified": nil,
		"pod_absent": nil, "quota_cleared": nil, "claim_unallocated": nil, "claim_uid_absent": nil, "api_snapshot_complete": false,
		"physical_gpu_verified": false, "allocation_ready": false, "allocation_released": false, "cost_final": false,
	}
	versions := map[string]string{"provider_observation": absence.ResourceVersion}
	failures := map[string]string{}
	snapshot, snapshotErr := reader.ObserveKubernetesReleaseSnapshot(ctx, seed.Queue, p)
	if snapshotErr == nil {
		report["api_snapshot_complete"] = true
		report["pod_absent"], report["quota_cleared"], report["claim_unallocated"] = true, true, true
		versions["pod_list"] = snapshot.PodAbsent.PodListResourceVersion
		versions["workload_list"] = snapshot.QuotaCleared.WorkloadListResourceVersion
		versions["cluster_queue"] = snapshot.QuotaCleared.ClusterQueue.ResourceVersion
		versions["local_queue"] = snapshot.QuotaCleared.LocalQueue.ResourceVersion
		versions["resourceclaim"] = snapshot.ClaimUnallocated.CurrentClaim.ResourceVersion
	} else {
		failures["snapshot"] = snapshotErr.Error()
		// Report each independent observation when a composite cannot be collected.
		// api_snapshot_complete stays false for this set of sequential probes.
		pod, podErr := reader.ObservePodUIDAbsent(ctx, p.Pod)
		if podErr == nil {
			report["pod_absent"] = true
			versions["pod_list"] = pod.PodListResourceVersion
		} else {
			failures["pod"] = podErr.Error()
		}
		quota, quotaErr := reader.ObserveKueueQuotaCleared(ctx, seed.Queue)
		if quotaErr == nil {
			report["quota_cleared"] = true
			versions["workload_list"] = quota.WorkloadListResourceVersion
			versions["cluster_queue"] = quota.ClusterQueue.ResourceVersion
			versions["local_queue"] = quota.LocalQueue.ResourceVersion
		} else {
			failures["quota"] = quotaErr.Error()
		}
		claim, claimErr := reader.ObserveDRAClaimUnallocated(ctx, p.Claim)
		if claimErr == nil {
			report["claim_unallocated"] = true
			versions["resourceclaim"] = claim.CurrentClaim.ResourceVersion
		} else {
			failures["claim"] = claimErr.Error()
			// Once a generated Claim is GC'd, GET 404 cannot prove it was
			// unallocated. A separate stable LIST may attest only API UID
			// absence; driver Unprepare and release finality remain UNKNOWN.
			absent, absentErr := reader.ObserveDRAClaimUIDAbsent(ctx, p.Claim)
			if absentErr == nil {
				report["claim_uid_absent"] = true
				versions["resourceclaim_list"] = absent.ClaimListResourceVersion
			} else {
				failures["claim_list"] = absentErr.Error()
			}
		}
	}
	report["resource_versions"] = versions
	report["evidence_errors"] = failures
	return report, nil
}

func admissionInspectRelease(getenv func(string) string, out io.Writer) error {
	if err := requireDisposableSmoke(getenv); err != nil {
		return err
	}
	file, err := os.Open(getenv("CCP_KUEUE_RELEASE_SEED"))
	if err != nil {
		return fmt.Errorf("read disposable release seed: %w", err)
	}
	defer file.Close()
	data, err := io.ReadAll(io.LimitReader(file, (1<<20)+1))
	var seed admissionReleaseSeed
	if err != nil || len(data) > 1<<20 || json.Unmarshal(data, &seed) != nil {
		return provider.ErrUncorrelatedReleaseEvidence
	}
	claimName, claimUID := getenv("CCP_KUEUE_DRA_CLAIM"), getenv("CCP_KUEUE_DRA_CLAIM_UID")
	localUID, clusterUID := getenv("CCP_KUEUE_LOCAL_QUEUE_UID"), getenv("CCP_KUEUE_CLUSTER_QUEUE_UID")
	if claimName == "" || claimUID == "" || localUID == "" || clusterUID == "" ||
		seed.PodClaim.Claim.Claim.Name != claimName || seed.PodClaim.Claim.Claim.UID != claimUID ||
		seed.Queue.LocalQueue.UID != localUID || seed.Queue.ClusterQueue.UID != clusterUID {
		return provider.ErrUncorrelatedReleaseEvidence
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
	adapter, err := openAdmissionAdapter(getenv)
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
	ctx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
	defer cancel()
	remove := admissionDeleteRequest(pin.Namespace+"/"+pin.Name, pin.UID)
	absence, err := adapter.Inspect(ctx, remove)
	if err != nil {
		return err
	}
	report, err := observeAdmissionRelease(ctx, reader, pin, seed, absence)
	if err != nil {
		return err
	}
	commit, err := checkedSmokeSourceCommit(getenv)
	if err != nil {
		return err
	}
	report["ccp_commit"] = commit
	report["ccp_commit_source"] = "CI_CHECKOUT_UNATTESTED"
	return writeDurable(out, report)
}
