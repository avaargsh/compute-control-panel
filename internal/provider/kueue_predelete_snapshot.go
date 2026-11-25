package provider

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"reflect"
	"strings"
	"time"

	"github.com/avaargsh/compute-control-panel/internal/operation"
)

var (
	ErrPreDeleteDRASnapshotRequired = errors.New("pre-delete DRA allocation snapshot is required")
	ErrInvalidPreDeleteDRASnapshot  = errors.New("invalid pre-delete DRA allocation snapshot")
)

// KueuePreDeleteDRASnapshot is immutable structural history captured while the
// exact CREATE Job, Kueue Workload, Pod and DRA ResourceClaim still exist.
// It is not an authenticated source attestation, driver RPC receipt, physical
// GPU release proof, Allocation RELEASED decision or cost-finality record.
type KueuePreDeleteDRASnapshot struct {
	Namespace         string
	TenantID          string
	WorkloadID        string
	ProviderID        string
	CreateOperationID string
	DeleteOperationID string
	CreateGeneration  uint64
	DeleteGeneration  uint64
	Job               KubernetesResourceRef
	KueueWorkload     KubernetesResourceRef
	Pod               KubernetesResourceRef
	NodeName          string
	Claim             KubernetesResourceRef
	ClaimTemplateName string
	PodClaimAlias     string
	ConsumerContainer string
	Devices           []DRAAllocatedDevice
	Source            string
	AdapterVersion    string
	ObservedAt        time.Time
	Digest            string
}

func cloneKueuePreDeleteDRASnapshot(snapshot KueuePreDeleteDRASnapshot) KueuePreDeleteDRASnapshot {
	snapshot.Devices = append([]DRAAllocatedDevice(nil), snapshot.Devices...)
	return snapshot
}

func kueuePreDeleteSnapshotDigest(snapshot KueuePreDeleteDRASnapshot) string {
	snapshot.Digest = ""
	raw, err := json.Marshal(snapshot)
	if err != nil {
		return ""
	}
	sum := sha256.Sum256(raw)
	return "sha256:" + hex.EncodeToString(sum[:])
}

func validateKueuePreDeleteDRASnapshot(
	namespace string, req Request, pin KueueCreatePin, snapshot KueuePreDeleteDRASnapshot,
) (KueuePreDeleteDRASnapshot, error) {
	reject := func() (KueuePreDeleteDRASnapshot, error) {
		return KueuePreDeleteDRASnapshot{}, ErrInvalidPreDeleteDRASnapshot
	}
	if !validKubeName(namespace) || req.Validate() != nil || req.Kind != operation.Delete ||
		pin.Request.Validate() != nil || pin.Request.Kind != operation.Create {
		return reject()
	}
	if snapshot.Namespace != namespace || snapshot.TenantID != req.TenantID ||
		snapshot.WorkloadID != req.WorkloadID || snapshot.ProviderID != req.ProviderID ||
		snapshot.CreateOperationID != pin.Request.OperationID ||
		snapshot.DeleteOperationID != req.OperationID ||
		snapshot.CreateGeneration != pin.Request.DesiredGeneration ||
		snapshot.DeleteGeneration != req.DesiredGeneration ||
		pin.Namespace != namespace || pin.Request.TenantID != req.TenantID ||
		pin.Request.WorkloadID != req.WorkloadID || pin.Request.ProviderID != req.ProviderID ||
		pin.Name == "" || pin.UID == "" ||
		req.TargetResourceID != namespace+"/"+pin.Name || req.TargetResourceUID != pin.UID ||
		snapshot.Job.Namespace != namespace || snapshot.Job.Name != pin.Name || snapshot.Job.UID != pin.UID {
		return KueuePreDeleteDRASnapshot{}, ErrOperationConflict
	}
	if !validKubernetesRef(snapshot.Job) ||
		!validKubernetesRef(snapshot.KueueWorkload) || snapshot.KueueWorkload.Namespace != namespace ||
		!validKubernetesRef(snapshot.Pod) || snapshot.Pod.Namespace != namespace ||
		!validKubernetesRef(snapshot.Claim) || snapshot.Claim.Namespace != namespace ||
		!validDRAIdentity(snapshot.NodeName) || !validKubeName(snapshot.ClaimTemplateName) ||
		!validKubeName(snapshot.PodClaimAlias) || !validKubeName(snapshot.ConsumerContainer) ||
		!validObservationLabel(snapshot.Source) || !validObservationLabel(snapshot.AdapterVersion) ||
		snapshot.ObservedAt.IsZero() || snapshot.CreateGeneration == 0 || snapshot.DeleteGeneration == 0 ||
		len(snapshot.Devices) == 0 || len(snapshot.Devices) > maxDRADeviceSet {
		return reject()
	}
	devices, ok := canonicalDRADeviceSet(snapshot.Devices, snapshot.Devices[0].Request)
	if !ok || len(devices) != len(snapshot.Devices) {
		return reject()
	}
	snapshot.Devices = devices
	snapshot.ObservedAt = snapshot.ObservedAt.UTC().Truncate(time.Microsecond)
	snapshot.Digest = kueuePreDeleteSnapshotDigest(snapshot)
	if !strings.HasPrefix(snapshot.Digest, "sha256:") {
		return reject()
	}
	return snapshot, nil
}

// RecordPreDeleteDRASnapshot binds already-collected, read-only Kueue and DRA
// observations to the durable CREATE Job UID pin and stores them before DELETE
// intent is written. The caller must obtain collection and deviceSet from the
// authenticated Kubernetes reader; these public Go values are not themselves
// cryptographic source attestations.
//
// The adapter only accepts the explicitly configured per-Pod ClaimTemplate
// path. Ambiguous/multi-claim observations, a successor Job UID, mismatched
// tenant/workload/provider, or a non-placed Pod fail closed.
func (a *KueueJobAdapter) RecordPreDeleteDRASnapshot(
	ctx context.Context, req Request, collection KueueJobCollection, deviceSet DRADeviceSetObservation,
) (KueuePreDeleteDRASnapshot, error) {
	reject := func(err error) (KueuePreDeleteDRASnapshot, error) {
		if err != nil {
			return KueuePreDeleteDRASnapshot{}, err
		}
		return KueuePreDeleteDRASnapshot{}, ErrInvalidPreDeleteDRASnapshot
	}
	if ctx == nil {
		return reject(nil)
	}
	if err := ctx.Err(); err != nil {
		return reject(err)
	}
	if a == nil || a.bindings == nil || a.resourceClaimTemplateName == "" ||
		req.Validate() != nil || req.Kind != operation.Delete {
		return reject(nil)
	}
	name, err := a.deleteJobName(req)
	if err != nil {
		return reject(err)
	}
	pin, ok, err := a.bindings.GetCreatePinByJob(a.namespace, req.TenantID, name)
	if err != nil {
		return reject(err)
	}
	if !ok || pin.UID != req.TargetResourceUID ||
		pin.Request.WorkloadID != req.WorkloadID || pin.Request.ProviderID != req.ProviderID {
		return reject(nil)
	}
	bound, err := bindKueueJobCollection(a.observationQuery(pin.Request, name, pin.UID),
		collection.Evidence, collection.Provenance)
	if err != nil || bound.Stage != KueuePodPlaced ||
		bound.Evidence.KueueWorkload == (KubernetesResourceRef{}) ||
		bound.Evidence.Pod == (KubernetesResourceRef{}) {
		return reject(err)
	}
	if len(deviceSet.Devices) == 0 {
		return reject(nil)
	}
	devices, valid := canonicalDRADeviceSet(deviceSet.Devices, deviceSet.Devices[0].Request)
	if !valid || deviceSet.Pod != bound.Evidence.Pod ||
		deviceSet.NodeName != bound.Evidence.PodNodeName ||
		deviceSet.Claim.Namespace != a.namespace ||
		deviceSet.ClaimTemplateName != a.resourceClaimTemplateName ||
		!validKubernetesRef(deviceSet.Claim) ||
		!validKubeName(deviceSet.PodClaimAlias) ||
		!validKubeName(deviceSet.ConsumerContainer) {
		return reject(nil)
	}
	snapshot := KueuePreDeleteDRASnapshot{
		Namespace: a.namespace, TenantID: req.TenantID, WorkloadID: req.WorkloadID,
		ProviderID: req.ProviderID, CreateOperationID: pin.Request.OperationID,
		DeleteOperationID: req.OperationID, CreateGeneration: pin.Request.DesiredGeneration,
		DeleteGeneration: req.DesiredGeneration,
		Job:              bound.Evidence.Job, KueueWorkload: bound.Evidence.KueueWorkload,
		Pod: deviceSet.Pod, NodeName: deviceSet.NodeName, Claim: deviceSet.Claim,
		ClaimTemplateName: deviceSet.ClaimTemplateName, PodClaimAlias: deviceSet.PodClaimAlias,
		ConsumerContainer: deviceSet.ConsumerContainer, Devices: devices,
		Source: bound.Provenance.Source, AdapterVersion: bound.Provenance.AdapterVersion,
		ObservedAt: bound.Provenance.ObservedAt,
	}
	return a.bindings.PutPreDeleteDRASnapshot(a.namespace, req, snapshot)
}

// preDeleteSnapshotMatchesReleaseCandidate binds the persisted pre-DELETE
// snapshot to the exact historical CREATE, DELETE, Job, Kueue Workload, Pod,
// Node, Claim and device identities used by release correlation. The digest
// detects mutation; it is not a signature or source-authentication mechanism.
func preDeleteSnapshotMatchesReleaseCandidate(
	snapshot KueuePreDeleteDRASnapshot, candidate ReleaseEvidenceCandidate,
) bool {
	prior := candidate.PriorPodClaim
	job := candidate.PriorQueue.Job
	if snapshot.Digest == "" || snapshot.Digest != kueuePreDeleteSnapshotDigest(snapshot) ||
		!validKubeName(snapshot.Namespace) ||
		snapshot.TenantID != candidate.Create.TenantID ||
		snapshot.WorkloadID != candidate.Create.WorkloadID ||
		snapshot.ProviderID != candidate.Create.ProviderID ||
		snapshot.CreateOperationID != candidate.Create.ID ||
		snapshot.DeleteOperationID != candidate.Delete.ID ||
		snapshot.CreateGeneration != candidate.Create.Generation ||
		snapshot.DeleteGeneration != candidate.Delete.Generation ||
		snapshot.Job != job.Job ||
		snapshot.KueueWorkload != job.KueueWorkload ||
		snapshot.Pod != prior.Pod || snapshot.Pod != job.Pod ||
		snapshot.NodeName != prior.NodeName || snapshot.NodeName != job.PodNodeName ||
		snapshot.Claim != prior.Claim.Claim ||
		snapshot.PodClaimAlias != prior.PodClaimAlias ||
		snapshot.ConsumerContainer != prior.ConsumerContainer ||
		snapshot.Job.Namespace != snapshot.Namespace ||
		snapshot.Job.UID != candidate.Create.ProviderResourceUID ||
		snapshot.Job.UID != candidate.Delete.TargetResourceUID ||
		candidate.Create.ProviderResourceID != snapshot.Namespace+"/"+snapshot.Job.Name ||
		candidate.Delete.TargetResourceID != snapshot.Namespace+"/"+snapshot.Job.Name ||
		!validKubernetesRef(snapshot.Job) ||
		!validKubernetesRef(snapshot.KueueWorkload) ||
		!validKubernetesRef(snapshot.Pod) ||
		!validKubernetesRef(snapshot.Claim) ||
		!validKubeName(snapshot.ClaimTemplateName) ||
		!validKubeName(snapshot.PodClaimAlias) ||
		!validKubeName(snapshot.ConsumerContainer) ||
		!validObservationLabel(snapshot.Source) ||
		!validObservationLabel(snapshot.AdapterVersion) ||
		snapshot.ObservedAt.IsZero() || len(snapshot.Devices) == 0 {
		return false
	}
	devices, ok := canonicalDRADeviceSet(snapshot.Devices, prior.Claim.Device.Request)
	if !ok || !reflect.DeepEqual(devices, snapshot.Devices) {
		return false
	}
	for _, device := range devices {
		if device == prior.Claim.Device {
			return true
		}
	}
	return false
}

func (a *KueueJobAdapter) requirePreDeleteDRASnapshot(req Request) error {
	if a == nil || a.bindings == nil {
		return ErrInvalidKueueObservation
	}
	if a.resourceClaimTemplateName == "" {
		return nil
	}
	snapshot, ok, err := a.bindings.GetPreDeleteDRASnapshot(a.namespace, req)
	if err != nil {
		return err
	}
	if !ok || !preDeleteSnapshotMatchesDelete(snapshot, a.namespace, req, a.resourceClaimTemplateName) {
		return ErrPreDeleteDRASnapshotRequired
	}
	pin, pinned, err := a.bindings.GetCreatePinByJob(a.namespace, req.TenantID, snapshot.Job.Name)
	if err != nil {
		return err
	}
	if !pinned || pin.UID != req.TargetResourceUID ||
		pin.Request.OperationID != snapshot.CreateOperationID ||
		pin.Request.WorkloadID != req.WorkloadID || pin.Request.ProviderID != req.ProviderID {
		return ErrPreDeleteDRASnapshotRequired
	}
	// Recheck the complete stored snapshot against the durable CREATE pin,
	// not just the DELETE target and digest. A digest is not validation of
	// the snapshot's generation, provenance or object identity fields.
	canonical, err := validateKueuePreDeleteDRASnapshot(a.namespace, req, pin, snapshot)
	if err != nil || canonical.Digest != snapshot.Digest {
		return ErrPreDeleteDRASnapshotRequired
	}
	return nil
}

func preDeleteSnapshotMatchesDelete(snapshot KueuePreDeleteDRASnapshot, namespace string, req Request, templateName string) bool {
	if len(snapshot.Devices) == 0 || snapshot.Digest == "" {
		return false
	}
	return snapshot.Namespace == namespace && snapshot.TenantID == req.TenantID &&
		snapshot.WorkloadID == req.WorkloadID && snapshot.ProviderID == req.ProviderID &&
		snapshot.DeleteOperationID == req.OperationID &&
		snapshot.DeleteGeneration == req.DesiredGeneration &&
		snapshot.Job.Namespace == namespace &&
		req.TargetResourceID == namespace+"/"+snapshot.Job.Name &&
		snapshot.Job.UID == req.TargetResourceUID &&
		snapshot.ClaimTemplateName == templateName &&
		snapshot.Digest == kueuePreDeleteSnapshotDigest(snapshot) &&
		reflect.DeepEqual(snapshot.Devices, func() []DRAAllocatedDevice {
			got, ok := canonicalDRADeviceSet(snapshot.Devices, snapshot.Devices[0].Request)
			if !ok {
				return nil
			}
			return got
		}())
}
