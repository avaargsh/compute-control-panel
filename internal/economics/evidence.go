package economics

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"strconv"
	"strings"
	"time"
)

const (
	AllocationUsageEvidenceSchemaVersion = "compute.economics.allocation-usage-evidence.v1"
	EvidenceStatusFinal                  = "FINAL"
	EvidenceStatusIncomplete             = "INCOMPLETE"
	DeviceEvidenceModeDRA                = "DRA"
	DeviceEvidenceModeCertifiedAdapter   = "CERTIFIED_DEVICE_ADAPTER"

	ReasonProviderBindingMissing   = "PROVIDER_BINDING_MISSING"
	ReasonProviderIdentityMismatch = "PROVIDER_IDENTITY_MISMATCH"
	ReasonKueueNotAdmitted         = "KUEUE_NOT_ADMITTED"
	ReasonKueueIdentityMismatch    = "KUEUE_IDENTITY_MISMATCH"
	ReasonPodNotScheduled          = "POD_NOT_SCHEDULED"
	ReasonDeviceAssignmentMissing  = "DEVICE_ASSIGNMENT_MISSING"
	ReasonDeviceCountMismatch      = "DEVICE_COUNT_MISMATCH"
	ReasonGenerationMismatch       = "GENERATION_MISMATCH"
	ReasonSourceStale              = "SOURCE_STALE"
	ReasonSourceConflict           = "SOURCE_CONFLICT"
	ReasonTimestampUntrusted       = "TIMESTAMP_UNTRUSTED"
	ReasonReleaseUnconfirmed       = "RELEASE_UNCONFIRMED"
)

var (
	ErrInvalidEvidence       = errors.New("invalid allocation usage evidence")
	ErrInvalidEvidencePolicy = errors.New("invalid allocation usage evidence policy")
)

// EvidenceRecord is an immutable reference to one source transition or
// observation. Trusted configuration, not the incoming record, decides whether
// SourceTimestamp is authoritative.
type EvidenceRecord struct {
	ObservationID         string
	TenantID              string
	ProjectID             string
	WorkloadID            string
	AllocationID          string
	AllocationGeneration  int64
	ProviderID            string
	ComputeOperationID    string
	Source                string
	AdapterVersion        string
	NativeUID             string
	NativeResourceVersion string
	ObservedAt            time.Time
	RecordedAt            time.Time
	SourceTimestamp       *time.Time
	ResponseDigest        string
	EvidenceRef           string
}

type ComputeOperationUsageEvidence struct {
	ID                string
	TenantID          string
	ProjectID         string
	WorkloadID        string
	AllocationID      string
	DesiredGeneration int64
	ProviderID        string
	Status            string
	RequestDigest     string
}

type ProviderUsageEvidence struct {
	Operation          ComputeOperationUsageEvidence
	ResourceID         string
	ResourceUID        string
	PoolID             string
	ProfileID          string
	GPUModel           string
	GPUCount           int64
	Binding            EvidenceRecord
	CostLiabilityStart EvidenceRecord
	CostLiabilityEnd   *EvidenceRecord
}

type KueueUsageEvidence struct {
	WorkloadUID      string
	JobUID           string
	ProfileID        string
	LocalQueue       string
	ClusterQueue     string
	ResourceFlavor   string
	GPUResourceName  string
	Admitted         bool
	AdmittedGPUCount int64
	Admission        EvidenceRecord
	Release          *EvidenceRecord
}

type PodUsageEvidence struct {
	PodUID          string
	JobUID          string
	NodeName        string
	Scheduled       bool
	ScheduledRecord EvidenceRecord
}

type DeviceUsageEvidence struct {
	Mode                string
	CapabilityProfileID string
	ClaimUID            string
	PodUID              string
	Driver              string
	PoolID              string
	ProfileID           string
	ResourceUID         string
	GPUCount            int64
	DeviceIDs           []string
	Assignment          EvidenceRecord
	Release             *EvidenceRecord
}

type AllocationUsageEvidence struct {
	SchemaVersion         string
	EvidenceSetID         string
	TenantID              string
	ProjectID             string
	WorkloadID            string
	AllocationID          string
	AllocationGeneration  int64
	ProviderID            string
	PoolID                string
	ProfileID             string
	CapabilityProfileID   string
	GPUModel              string
	GPUCount              string
	MeteringPolicyVersion string
	Provider              ProviderUsageEvidence
	Kueue                 KueueUsageEvidence
	Pod                   PodUsageEvidence
	Device                DeviceUsageEvidence
}

// EvidencePolicy is trusted, versioned platform configuration. Bump Version
// whenever timestamp trust, clock-skew limits or certified device adapters change.
// Timestamp allowlist keys use source@adapterVersion; certified adapter keys
// use capability@source@version.
type EvidencePolicy struct {
	Version                        string
	AsOf                           time.Time
	MaximumClockSkew               time.Duration
	TrustedSourceTimestampAdapters map[string]bool
	CertifiedDeviceAdapters        map[string]bool
}

type EvidenceDerivation struct {
	Status         string
	ReasonCode     string
	EvidenceDigest string
	UsageInterval  *UsageInterval
}

// DeriveUsageInterval validates the independent Provider, Kueue, Pod and device
// authority paths, then derives the intersection of Provider cost liability and
// Workload attribution. It has no persistence or external side effects.
func DeriveUsageInterval(evidence AllocationUsageEvidence, policy EvidencePolicy) (EvidenceDerivation, error) {
	if err := validateEvidencePolicy(policy); err != nil {
		return EvidenceDerivation{}, err
	}
	if err := validateEvidenceHeader(evidence); err != nil {
		return EvidenceDerivation{}, err
	}
	incomplete := func(reason string) (EvidenceDerivation, error) {
		return EvidenceDerivation{Status: EvidenceStatusIncomplete, ReasonCode: reason}, nil
	}

	expectedGPUCount, err := strconv.ParseInt(evidence.GPUCount, 10, 64)
	if err != nil || expectedGPUCount <= 0 {
		return EvidenceDerivation{}, fmt.Errorf("%w: GPUCount must be a positive whole integer", ErrInvalidEvidence)
	}

	provider := evidence.Provider
	operation := provider.Operation
	if !nonBlank(operation.ID, operation.TenantID, operation.ProjectID, operation.WorkloadID,
		operation.AllocationID, operation.ProviderID, operation.RequestDigest,
		provider.ResourceID, provider.ResourceUID, provider.Binding.ObservationID,
		provider.CostLiabilityStart.ObservationID) ||
		!isSHA256Digest(operation.RequestDigest) || operation.Status != "SUCCEEDED" {
		return incomplete(ReasonProviderBindingMissing)
	}
	if operation.TenantID != evidence.TenantID || operation.ProjectID != evidence.ProjectID ||
		operation.WorkloadID != evidence.WorkloadID || operation.AllocationID != evidence.AllocationID ||
		operation.DesiredGeneration != evidence.AllocationGeneration ||
		operation.ProviderID != evidence.ProviderID {
		return incomplete(ReasonProviderIdentityMismatch)
	}
	if provider.Binding.ComputeOperationID != operation.ID {
		return incomplete(ReasonProviderIdentityMismatch)
	}
	if provider.GPUCount != expectedGPUCount {
		return incomplete(ReasonDeviceCountMismatch)
	}
	if provider.PoolID != evidence.PoolID || provider.ProfileID != evidence.ProfileID ||
		provider.GPUModel != evidence.GPUModel ||
		provider.Binding.NativeUID != provider.ResourceUID ||
		provider.CostLiabilityStart.NativeUID != provider.ResourceUID {
		return incomplete(ReasonProviderIdentityMismatch)
	}
	if provider.CostLiabilityEnd != nil && provider.CostLiabilityEnd.NativeUID != provider.ResourceUID {
		return incomplete(ReasonProviderIdentityMismatch)
	}
	if provider.Binding.Source != "provider" || provider.CostLiabilityStart.Source != "provider" ||
		(provider.CostLiabilityEnd != nil && provider.CostLiabilityEnd.Source != "provider") {
		return incomplete(ReasonProviderIdentityMismatch)
	}

	kueue := evidence.Kueue
	if !kueue.Admitted {
		return incomplete(ReasonKueueNotAdmitted)
	}
	if !nonBlank(kueue.WorkloadUID, kueue.JobUID, kueue.LocalQueue, kueue.ClusterQueue, kueue.ResourceFlavor, kueue.GPUResourceName) ||
		kueue.ProfileID != evidence.ProfileID || kueue.WorkloadUID != kueue.Admission.NativeUID ||
		kueue.Admission.Source != "kueue" || (kueue.Release != nil && kueue.Release.Source != "kueue") {
		return incomplete(ReasonKueueIdentityMismatch)
	}
	if kueue.AdmittedGPUCount != expectedGPUCount {
		return incomplete(ReasonDeviceCountMismatch)
	}

	pod := evidence.Pod
	if !pod.Scheduled {
		return incomplete(ReasonPodNotScheduled)
	}
	if !nonBlank(pod.PodUID, pod.JobUID, pod.NodeName) ||
		pod.PodUID != pod.ScheduledRecord.NativeUID || pod.JobUID != kueue.JobUID ||
		pod.ScheduledRecord.Source != "kubernetes" {
		return incomplete(ReasonKueueIdentityMismatch)
	}

	device := evidence.Device
	if !nonBlank(device.ResourceUID, device.PodUID, device.Driver) ||
		device.CapabilityProfileID != evidence.CapabilityProfileID ||
		device.PodUID != pod.PodUID || device.PoolID != evidence.PoolID ||
		device.ProfileID != evidence.ProfileID || device.ResourceUID != device.Assignment.NativeUID {
		return incomplete(ReasonDeviceAssignmentMissing)
	}
	switch device.Mode {
	case DeviceEvidenceModeDRA:
		if strings.TrimSpace(device.ClaimUID) == "" || device.ClaimUID != device.ResourceUID ||
			device.Assignment.Source != "dra" || (device.Release != nil && device.Release.Source != "dra") {
			return incomplete(ReasonDeviceAssignmentMissing)
		}
	case DeviceEvidenceModeCertifiedAdapter:
		key := device.CapabilityProfileID + "@" + device.Assignment.Source + "@" + device.Assignment.AdapterVersion
		if !policy.CertifiedDeviceAdapters[key] ||
			(device.Release != nil && (device.Release.Source != device.Assignment.Source ||
				device.Release.AdapterVersion != device.Assignment.AdapterVersion)) {
			return incomplete(ReasonDeviceAssignmentMissing)
		}
	default:
		return incomplete(ReasonDeviceAssignmentMissing)
	}
	if device.GPUCount != expectedGPUCount || len(device.DeviceIDs) != int(expectedGPUCount) {
		return incomplete(ReasonDeviceCountMismatch)
	}
	deviceIDs := append([]string(nil), device.DeviceIDs...)
	sort.Strings(deviceIDs)
	for i, id := range deviceIDs {
		if strings.TrimSpace(id) == "" || (i > 0 && deviceIDs[i-1] == id) {
			return incomplete(ReasonSourceConflict)
		}
	}
	if !nonBlank(device.Assignment.ObservationID) {
		return incomplete(ReasonDeviceAssignmentMissing)
	}

	// Finality requires all three release authorities: the exact Provider
	// liability lease, Kueue quota reservation, and device assignment.
	// Absence of any source must not silently close the costable interval.
	if provider.CostLiabilityEnd == nil || kueue.Release == nil || device.Release == nil {
		return incomplete(ReasonReleaseUnconfirmed)
	}
	if kueue.Release.NativeUID != kueue.WorkloadUID || device.Release.NativeUID != device.ResourceUID {
		return incomplete(ReasonReleaseUnconfirmed)
	}

	records := []EvidenceRecord{
		provider.Binding, provider.CostLiabilityStart, kueue.Admission,
		pod.ScheduledRecord, device.Assignment, *kueue.Release, *device.Release,
	}
	if provider.CostLiabilityEnd != nil {
		records = append(records, *provider.CostLiabilityEnd)
	}
	for _, record := range records {
		if reason := evidenceScopeMismatch(record, evidence); reason != "" {
			return incomplete(reason)
		}
	}
	if provider.Binding.NativeUID != provider.ResourceUID ||
		provider.CostLiabilityStart.NativeUID != provider.ResourceUID ||
		kueue.Admission.NativeUID != kueue.WorkloadUID ||
		pod.ScheduledRecord.NativeUID != pod.PodUID ||
		device.Assignment.NativeUID != device.ResourceUID {
		return incomplete(ReasonSourceConflict)
	}

	effective := make(map[string]time.Time, len(records))
	seenObservationIDs := make(map[string]struct{}, len(records))
	seenSourceRevisions := make(map[string]struct{}, len(records))
	for _, record := range records {
		if err := validateEvidenceRecord(record); err != nil {
			return EvidenceDerivation{}, err
		}
		if _, exists := seenObservationIDs[record.ObservationID]; exists {
			return incomplete(ReasonSourceConflict)
		}
		seenObservationIDs[record.ObservationID] = struct{}{}
		revisionKey := strings.Join([]string{
			record.Source, record.AdapterVersion, record.NativeUID, record.NativeResourceVersion,
		}, "\x00")
		if _, exists := seenSourceRevisions[revisionKey]; exists {
			return incomplete(ReasonSourceConflict)
		}
		seenSourceRevisions[revisionKey] = struct{}{}
		boundary, reason := effectiveEvidenceTime(record, policy)
		if reason != "" {
			return incomplete(reason)
		}
		effective[record.ObservationID] = boundary
	}

	start := latestTime(
		effective[provider.Binding.ObservationID],
		effective[provider.CostLiabilityStart.ObservationID],
		effective[kueue.Admission.ObservationID],
		effective[pod.ScheduledRecord.ObservationID],
		effective[device.Assignment.ObservationID],
	)
	end := earliestTime(
		effective[kueue.Release.ObservationID],
		effective[device.Release.ObservationID],
	)
	if provider.CostLiabilityEnd != nil {
		end = earliestTime(end, effective[provider.CostLiabilityEnd.ObservationID])
	}
	if !end.After(start) {
		return incomplete(ReasonSourceConflict)
	}

	digest, err := allocationUsageEvidenceDigest(evidence, policy, records, effective, start, end, deviceIDs)
	if err != nil {
		return EvidenceDerivation{}, fmt.Errorf("digest allocation usage evidence: %w", err)
	}
	sourceEventIDs := make([]string, 0, len(records))
	for _, record := range records {
		sourceEventIDs = append(sourceEventIDs, record.ObservationID)
	}
	sort.Strings(sourceEventIDs)

	usage := &UsageInterval{
		SchemaVersion:   usageSchemaVersion,
		UsageIntervalID: "usage_" + strings.TrimPrefix(digest, "sha256:"),
		TenantID:        evidence.TenantID, ProjectID: evidence.ProjectID,
		WorkloadID: evidence.WorkloadID, AllocationID: evidence.AllocationID,
		AllocationGeneration: evidence.AllocationGeneration,
		ProviderID:           evidence.ProviderID, PoolID: evidence.PoolID, ProfileID: evidence.ProfileID,
		ResourceClass: "GPU", GPUModel: evidence.GPUModel,
		Start: start, End: end, GPUCount: evidence.GPUCount, Unit: usageUnit,
		MeteringPolicyVersion: evidence.MeteringPolicyVersion,
		SourceEventIDs:        sourceEventIDs, Quality: "DERIVED",
		AttributionMethod: "DEVICE_BINDING", SourceCoverage: 1,
		EvidenceSetID: evidence.EvidenceSetID, EvidenceDigest: digest,
		CapabilityProfileID: evidence.CapabilityProfileID,
	}
	return EvidenceDerivation{Status: EvidenceStatusFinal, EvidenceDigest: digest, UsageInterval: usage}, nil
}

func validateEvidencePolicy(policy EvidencePolicy) error {
	if strings.TrimSpace(policy.Version) == "" || policy.AsOf.IsZero() || policy.MaximumClockSkew < 0 {
		return ErrInvalidEvidencePolicy
	}
	return nil
}

func validateEvidenceHeader(evidence AllocationUsageEvidence) error {
	if evidence.SchemaVersion != AllocationUsageEvidenceSchemaVersion ||
		!nonBlank(
			evidence.EvidenceSetID, evidence.TenantID, evidence.ProjectID, evidence.WorkloadID,
			evidence.AllocationID, evidence.ProviderID, evidence.PoolID, evidence.ProfileID,
			evidence.CapabilityProfileID, evidence.GPUModel, evidence.GPUCount, evidence.MeteringPolicyVersion,
		) || evidence.AllocationGeneration < 1 || !validWholePositiveInteger(evidence.GPUCount) {
		return ErrInvalidEvidence
	}
	return nil
}

func validateEvidenceRecord(record EvidenceRecord) error {
	if !nonBlank(
		record.ObservationID, record.TenantID, record.ProjectID, record.WorkloadID,
		record.AllocationID, record.ProviderID, record.Source, record.AdapterVersion,
		record.NativeUID, record.NativeResourceVersion, record.ResponseDigest, record.EvidenceRef,
	) || record.AllocationGeneration < 1 || record.ObservedAt.IsZero() ||
		record.RecordedAt.IsZero() || record.RecordedAt.Before(record.ObservedAt) ||
		!isSHA256Digest(record.ResponseDigest) ||
		(record.SourceTimestamp != nil && record.SourceTimestamp.IsZero()) {
		return fmt.Errorf("%w: incomplete observation provenance", ErrInvalidEvidence)
	}
	return nil
}

func evidenceScopeMismatch(record EvidenceRecord, evidence AllocationUsageEvidence) string {
	if record.AllocationGeneration != evidence.AllocationGeneration {
		return ReasonGenerationMismatch
	}
	if record.TenantID != evidence.TenantID || record.ProjectID != evidence.ProjectID ||
		record.WorkloadID != evidence.WorkloadID || record.AllocationID != evidence.AllocationID ||
		record.ProviderID != evidence.ProviderID {
		switch record.Source {
		case "provider":
			return ReasonProviderIdentityMismatch
		case "kueue", "kubernetes":
			return ReasonKueueIdentityMismatch
		default:
			return ReasonDeviceAssignmentMissing
		}
	}
	return ""
}

func effectiveEvidenceTime(record EvidenceRecord, policy EvidencePolicy) (time.Time, string) {
	if record.ObservedAt.After(policy.AsOf) || record.RecordedAt.After(policy.AsOf) {
		return time.Time{}, ReasonSourceStale
	}
	key := record.Source + "@" + record.AdapterVersion
	if policy.TrustedSourceTimestampAdapters[key] && record.SourceTimestamp != nil {
		if record.SourceTimestamp.After(record.ObservedAt.Add(policy.MaximumClockSkew)) ||
			record.SourceTimestamp.After(policy.AsOf.Add(policy.MaximumClockSkew)) {
			return time.Time{}, ReasonTimestampUntrusted
		}
		return record.SourceTimestamp.UTC(), ""
	}
	return record.ObservedAt.UTC(), ""
}

type canonicalEvidenceRecord struct {
	ObservationID         string
	ComputeOperationID    string
	Source                string
	AdapterVersion        string
	NativeUID             string
	NativeResourceVersion string
	ObservedAt            string
	RecordedAt            string
	SourceTimestamp       string
	EffectiveAt           string
	ResponseDigest        string
	EvidenceRef           string
}

func allocationUsageEvidenceDigest(
	evidence AllocationUsageEvidence,
	policy EvidencePolicy,
	records []EvidenceRecord,
	effective map[string]time.Time,
	start, end time.Time,
	deviceIDs []string,
) (string, error) {
	canonicalRecords := make([]canonicalEvidenceRecord, 0, len(records))
	for _, record := range records {
		sourceTimestamp := ""
		if record.SourceTimestamp != nil {
			sourceTimestamp = record.SourceTimestamp.UTC().Format(time.RFC3339Nano)
		}
		canonicalRecords = append(canonicalRecords, canonicalEvidenceRecord{
			ObservationID: record.ObservationID, ComputeOperationID: record.ComputeOperationID,
			Source: record.Source, AdapterVersion: record.AdapterVersion, NativeUID: record.NativeUID,
			NativeResourceVersion: record.NativeResourceVersion,
			ObservedAt:            record.ObservedAt.UTC().Format(time.RFC3339Nano),
			RecordedAt:            record.RecordedAt.UTC().Format(time.RFC3339Nano),
			SourceTimestamp:       sourceTimestamp,
			EffectiveAt:           effective[record.ObservationID].UTC().Format(time.RFC3339Nano),
			ResponseDigest:        record.ResponseDigest, EvidenceRef: record.EvidenceRef,
		})
	}
	sort.Slice(canonicalRecords, func(i, j int) bool {
		return canonicalRecords[i].ObservationID < canonicalRecords[j].ObservationID
	})
	canonicalDevices := append([]string(nil), deviceIDs...)
	sort.Strings(canonicalDevices)

	input := struct {
		SchemaVersion                  string
		EvidenceSetID                  string
		EvidencePolicyVersion          string
		TenantID                       string
		ProjectID                      string
		WorkloadID                     string
		AllocationID                   string
		AllocationGeneration           int64
		ProviderID                     string
		ProviderResourceID             string
		ProviderResourceUID            string
		PoolID                         string
		ProfileID                      string
		CapabilityProfileID            string
		GPUModel                       string
		GPUCount                       string
		MeteringPolicyVersion          string
		KueueWorkloadUID               string
		JobUID                         string
		PodUID                         string
		DeviceMode                     string
		DeviceResourceUID              string
		DeviceIDs                      []string
		ProviderPoolID                 string
		ProviderProfileID              string
		ProviderGPUModel               string
		ProviderGPUCount               int64
		KueueProfileID                 string
		KueueLocalQueue                string
		KueueClusterQueue              string
		KueueResourceFlavor            string
		KueueAdmitted                  bool
		KueueGPUCount                  int64
		PodNodeName                    string
		PodScheduled                   bool
		DeviceClaimUID                 string
		DeviceDriver                   string
		DevicePoolID                   string
		DeviceProfileID                string
		DeviceGPUCount                 int64
		ProviderComputeOperationID     string
		ProviderOperationTenantID      string
		ProviderOperationProjectID     string
		ProviderOperationWorkloadID    string
		ProviderOperationAllocationID  string
		ProviderOperationGeneration    int64
		ProviderOperationProviderID    string
		ProviderOperationStatus        string
		ProviderOperationRequestDigest string
		ProviderBindingOperationID     string
		KueueGPUResourceName           string
		Records                        []canonicalEvidenceRecord
		CostableStart                  string
		CostableEnd                    string
	}{
		SchemaVersion: evidence.SchemaVersion, EvidenceSetID: evidence.EvidenceSetID,
		EvidencePolicyVersion: policy.Version,
		TenantID:              evidence.TenantID, ProjectID: evidence.ProjectID,
		WorkloadID: evidence.WorkloadID, AllocationID: evidence.AllocationID,
		AllocationGeneration: evidence.AllocationGeneration,
		ProviderID:           evidence.ProviderID, ProviderResourceID: evidence.Provider.ResourceID,
		ProviderResourceUID: evidence.Provider.ResourceUID,
		PoolID:              evidence.PoolID, ProfileID: evidence.ProfileID,
		CapabilityProfileID: evidence.CapabilityProfileID,
		GPUModel:            evidence.GPUModel, GPUCount: evidence.GPUCount,
		MeteringPolicyVersion: evidence.MeteringPolicyVersion,
		KueueWorkloadUID:      evidence.Kueue.WorkloadUID, JobUID: evidence.Kueue.JobUID,
		PodUID: evidence.Pod.PodUID, DeviceMode: evidence.Device.Mode,
		DeviceResourceUID: evidence.Device.ResourceUID, DeviceIDs: canonicalDevices,
		ProviderComputeOperationID:     evidence.Provider.Operation.ID,
		ProviderOperationTenantID:      evidence.Provider.Operation.TenantID,
		ProviderOperationProjectID:     evidence.Provider.Operation.ProjectID,
		ProviderOperationWorkloadID:    evidence.Provider.Operation.WorkloadID,
		ProviderOperationAllocationID:  evidence.Provider.Operation.AllocationID,
		ProviderOperationGeneration:    evidence.Provider.Operation.DesiredGeneration,
		ProviderOperationProviderID:    evidence.Provider.Operation.ProviderID,
		ProviderOperationStatus:        evidence.Provider.Operation.Status,
		ProviderOperationRequestDigest: evidence.Provider.Operation.RequestDigest,
		ProviderBindingOperationID:     evidence.Provider.Binding.ComputeOperationID,
		KueueGPUResourceName:           evidence.Kueue.GPUResourceName,
		ProviderPoolID:                 evidence.Provider.PoolID, ProviderProfileID: evidence.Provider.ProfileID,
		ProviderGPUModel: evidence.Provider.GPUModel, ProviderGPUCount: evidence.Provider.GPUCount,
		KueueProfileID: evidence.Kueue.ProfileID, KueueLocalQueue: evidence.Kueue.LocalQueue,
		KueueClusterQueue: evidence.Kueue.ClusterQueue, KueueResourceFlavor: evidence.Kueue.ResourceFlavor,
		KueueAdmitted: evidence.Kueue.Admitted, KueueGPUCount: evidence.Kueue.AdmittedGPUCount,
		PodNodeName: evidence.Pod.NodeName, PodScheduled: evidence.Pod.Scheduled,
		DeviceClaimUID: evidence.Device.ClaimUID, DeviceDriver: evidence.Device.Driver,
		DevicePoolID: evidence.Device.PoolID, DeviceProfileID: evidence.Device.ProfileID,
		DeviceGPUCount: evidence.Device.GPUCount,
		Records:        canonicalRecords,
		CostableStart:  start.UTC().Format(time.RFC3339Nano),
		CostableEnd:    end.UTC().Format(time.RFC3339Nano),
	}
	encoded, err := json.Marshal(input)
	if err != nil {
		return "", err
	}
	sum := sha256.Sum256(encoded)
	return "sha256:" + hex.EncodeToString(sum[:]), nil
}

func isSHA256Digest(value string) bool {
	if !strings.HasPrefix(value, "sha256:") || len(value) != len("sha256:")+64 {
		return false
	}
	digest := strings.TrimPrefix(value, "sha256:")
	if strings.ToLower(digest) != digest {
		return false
	}
	_, err := hex.DecodeString(digest)
	return err == nil
}

func latestTime(values ...time.Time) time.Time {
	latest := values[0].UTC()
	for _, value := range values[1:] {
		if value.After(latest) {
			latest = value.UTC()
		}
	}
	return latest
}

func earliestTime(values ...time.Time) time.Time {
	earliest := values[0].UTC()
	for _, value := range values[1:] {
		if value.Before(earliest) {
			earliest = value.UTC()
		}
	}
	return earliest
}
