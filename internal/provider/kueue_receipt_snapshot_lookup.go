package provider

import (
	"context"
	"fmt"
	"reflect"
)

func validDRAReceiptSnapshotLookup(tenantID, providerID string, event DRAUnprepareEvent) bool {
	if !validDRAIdentity(tenantID) || !validDRAIdentity(providerID) ||
		!validKubeName(event.ClaimNamespace) || !validKubeName(event.ClaimName) {
		return false
	}
	for _, value := range []string{
		event.ClaimUID, event.PodUID, event.AllocatedClaimResourceVersion,
		event.NodeName, event.AllocatedDevice.Request, event.AllocatedDevice.Driver,
		event.AllocatedDevice.Pool, event.AllocatedDevice.Device,
	} {
		if !validDRAIdentity(value) {
			return false
		}
	}
	return event.AllocatedDevice.ShareID == "" || validDRAIdentity(event.AllocatedDevice.ShareID)
}

func preDeleteSnapshotMatchesDRAReceipt(snapshot KueuePreDeleteDRASnapshot, tenantID, providerID string, event DRAUnprepareEvent) bool {
	if snapshot.Digest == "" || snapshot.Digest != kueuePreDeleteSnapshotDigest(snapshot) ||
		snapshot.Namespace != event.ClaimNamespace || snapshot.TenantID != tenantID || snapshot.ProviderID != providerID ||
		snapshot.TenantID == "" || snapshot.WorkloadID == "" || snapshot.ProviderID == "" ||
		snapshot.CreateOperationID == "" || snapshot.DeleteOperationID == "" ||
		snapshot.CreateOperationID == snapshot.DeleteOperationID ||
		snapshot.CreateGeneration == 0 || snapshot.DeleteGeneration == 0 || snapshot.DeleteGeneration < snapshot.CreateGeneration ||
		!validKubernetesRef(snapshot.Job) || snapshot.Job.Namespace != snapshot.Namespace ||
		!validKubernetesRef(snapshot.KueueWorkload) || snapshot.KueueWorkload.Namespace != snapshot.Namespace ||
		!validKubernetesRef(snapshot.Pod) || snapshot.Pod.Namespace != snapshot.Namespace ||
		!validKubernetesRef(snapshot.Claim) || snapshot.Claim.Namespace != snapshot.Namespace ||
		!validDRAIdentity(snapshot.NodeName) ||
		!validKubeName(snapshot.ClaimTemplateName) ||
		!validKubeName(snapshot.PodClaimAlias) ||
		!validKubeName(snapshot.ConsumerContainer) ||
		!validObservationLabel(snapshot.Source) ||
		!validObservationLabel(snapshot.AdapterVersion) ||
		snapshot.ObservedAt.IsZero() ||
		snapshot.Claim.Name != event.ClaimName ||
		snapshot.Claim.UID != event.ClaimUID ||
		snapshot.Claim.ResourceVersion != event.AllocatedClaimResourceVersion ||
		snapshot.Pod.UID != event.PodUID ||
		snapshot.NodeName != event.NodeName ||
		len(snapshot.Devices) == 0 || len(snapshot.Devices) > maxDRADeviceSet {
		return false
	}
	devices, ok := canonicalDRADeviceSet(snapshot.Devices, snapshot.Devices[0].Request)
	if !ok || !reflect.DeepEqual(devices, snapshot.Devices) {
		return false
	}
	for _, device := range devices {
		if device == event.AllocatedDevice {
			return true
		}
	}
	return false
}

// GetPreDeleteDRASnapshotForReceipt resolves one immutable snapshot using the
// authenticated tenant, independently registered signer Provider, and the
// signed event's exact Claim/Pod/device identity.
// The event is a lookup key only. Callers must still load and authenticate the
// CREATE/DELETE operations and independent Provider/Kueue/Pod/Claim evidence;
// this method does not establish receipt provenance, physical release,
// Allocation RELEASED, or cost finality.
func (s *MemoryKueueJobBindingStore) GetPreDeleteDRASnapshotForReceipt(
	tenantID, providerID string, event DRAUnprepareEvent,
) (KueuePreDeleteDRASnapshot, bool, error) {
	if err := s.usable(); err != nil {
		return KueuePreDeleteDRASnapshot{}, false, err
	}
	if !validDRAReceiptSnapshotLookup(tenantID, providerID, event) {
		return KueuePreDeleteDRASnapshot{}, false, ErrUncorrelatedDRAUnprepare
	}
	s.mu.Lock()
	defer s.mu.Unlock()

	var found KueuePreDeleteDRASnapshot
	matched := false
	for _, snapshot := range s.snapshots {
		if snapshot.Namespace != event.ClaimNamespace || snapshot.TenantID != tenantID || snapshot.ProviderID != providerID ||
			snapshot.Claim.Name != event.ClaimName || snapshot.Claim.UID != event.ClaimUID ||
			snapshot.Pod.UID != event.PodUID {
			continue
		}
		if matched {
			return KueuePreDeleteDRASnapshot{}, false, ErrOperationConflict
		}
		found, matched = snapshot, true
	}
	if !matched || !preDeleteSnapshotMatchesDRAReceipt(found, tenantID, providerID, event) {
		return KueuePreDeleteDRASnapshot{}, false, nil
	}
	return cloneKueuePreDeleteDRASnapshot(found), true, nil
}

// GetPreDeleteDRASnapshotForReceipt is the durable counterpart of the memory
// lookup. LIMIT 2 is deliberate: a duplicate historical Claim/Pod identity is
// ambiguous and must not be resolved by choosing an arbitrary operation row.
func (s *PostgresKueueJobBindingStore) GetPreDeleteDRASnapshotForReceipt(
	tenantID, providerID string, event DRAUnprepareEvent,
) (KueuePreDeleteDRASnapshot, bool, error) {
	if err := s.usable(); err != nil {
		return KueuePreDeleteDRASnapshot{}, false, err
	}
	if !validDRAReceiptSnapshotLookup(tenantID, providerID, event) {
		return KueuePreDeleteDRASnapshot{}, false, ErrUncorrelatedDRAUnprepare
	}
	ctx, cancel := context.WithTimeout(context.Background(), kueueBindingQueryTimeout)
	defer cancel()

	query := "SELECT " + kueuePreDeleteSnapshotColumns +
		"\nFROM public.kueue_job_pre_delete_dra_snapshots" +
		"\nWHERE namespace=$1 AND tenant_id=$2 AND provider_id=$3 AND claim_name=$4 AND claim_uid=$5 AND pod_uid=$6" +
		"\nORDER BY delete_operation_id LIMIT 2"
	rows, err := s.db.QueryContext(ctx, query,
		event.ClaimNamespace, tenantID, providerID, event.ClaimName, event.ClaimUID, event.PodUID)
	if err != nil {
		return KueuePreDeleteDRASnapshot{}, false, fmt.Errorf("lookup pre-delete DRA snapshot for receipt: %w", err)
	}
	defer rows.Close()

	var found KueuePreDeleteDRASnapshot
	matched := false
	for rows.Next() {
		snapshot, scanErr := scanKueuePreDeleteSnapshot(rows)
		if scanErr != nil {
			return KueuePreDeleteDRASnapshot{}, false, fmt.Errorf("scan pre-delete DRA receipt snapshot: %w", scanErr)
		}
		if matched {
			return KueuePreDeleteDRASnapshot{}, false, ErrOperationConflict
		}
		found, matched = snapshot, true
	}
	if err := rows.Err(); err != nil {
		return KueuePreDeleteDRASnapshot{}, false, fmt.Errorf("iterate pre-delete DRA receipt snapshots: %w", err)
	}
	if !matched || !preDeleteSnapshotMatchesDRAReceipt(found, tenantID, providerID, event) {
		return KueuePreDeleteDRASnapshot{}, false, nil
	}
	return cloneKueuePreDeleteDRASnapshot(found), true, nil
}

var _ KueueJobBindingStore = (*MemoryKueueJobBindingStore)(nil)
var _ KueueJobBindingStore = (*PostgresKueueJobBindingStore)(nil)
