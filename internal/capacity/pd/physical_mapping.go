package pd

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"sort"
	"time"
)

// ScheduledDevice is a separately observed, UID-pinned Kubernetes/DRA
// scheduler-side claim. The caller MUST obtain it from the trusted
// Pod/Claim/ResourceSlice/Node collectors; this struct is not self-authenticating.
type ScheduledDevice struct {
	Driver               string
	Pool                 string
	Device               string
	ClaimUID             string
	PodUID               string
	NodeUID              string
	SliceUID             string
	SliceResourceVersion string
}

// PhysicalDeviceBinding correlates scheduler-side DRA identity to a signed
// inventory mapping. It does not certify Driver Prepare, GPU health/use,
// Allocation READY/RELEASED, Pod execution or financial finality.
type PhysicalDeviceBinding struct {
	PhysicalUID string
	IslandID    string
	ScheduledDevice
}

type PhysicalDeviceMapping struct {
	Bindings       []PhysicalDeviceBinding
	EvidenceSHA256 string
	PhysicalReady  bool
}

type deviceLocation struct {
	driver string
	pool   string
	device string
}

func addressOf(g PhysicalGPU) deviceLocation {
	return deviceLocation{g.Driver, g.Pool, g.Device}
}

func scheduledAddress(d ScheduledDevice) deviceLocation {
	return deviceLocation{d.Driver, d.Pool, d.Device}
}

// BindScheduledDevices verifies the independently signed physical inventory
// and measured-fabric provenance, then rejects every unmatched, duplicate,
// cross-Node or obsolete ResourceSlice mapping. Claimed devices must be ACTIVE
// or RESERVED in the signed source, not FREE. A Claim UID may cover multiple
// whole GPUs only for the SAME Pod UID.
//
// IMPORTANT: Unlike VerifyTopology, this function joins an independent
// scheduler observation supplied by the caller. The caller is responsible for
// authenticating its Kubernetes cluster and collection time. This is an
// identity candidate, NOT driver/physical preparation proof.
func BindScheduledDevices(now time.Time, evidence TopologyEvidence, policy TopologyPolicy, claims []ScheduledDevice) (PhysicalDeviceMapping, error) {
	fail := func() (PhysicalDeviceMapping, error) {
		return PhysicalDeviceMapping{}, ErrUnproven
	}
	verified, err := VerifyTopology(now, evidence, policy)
	if err != nil || len(claims) == 0 || len(claims) > 128 {
		return fail()
	}
	byAddress := make(map[deviceLocation]PhysicalGPU, len(evidence.Inventory.Devices))
	for _, device := range evidence.Inventory.Devices {
		byAddress[addressOf(device)] = device
	}
	mapped := make([]PhysicalDeviceBinding, 0, len(claims))
	seenPhysical := make(map[string]bool, len(claims))
	claimPod := make(map[string]string, len(claims))
	for _, claim := range claims {
		for _, id := range []string{
			claim.Driver, claim.Pool, claim.Device, claim.ClaimUID, claim.PodUID,
			claim.NodeUID, claim.SliceUID, claim.SliceResourceVersion,
		} {
			if !validID(id) {
				return fail()
			}
		}
		gpu, found := byAddress[scheduledAddress(claim)]
		if !found || seenPhysical[gpu.PhysicalUID] ||
			gpu.NodeUID != claim.NodeUID ||
			gpu.SliceUID != claim.SliceUID ||
			gpu.SliceResourceVersion != claim.SliceResourceVersion ||
			!gpu.WholeDevice || !gpu.Healthy ||
			(gpu.State != "RESERVED" && gpu.State != "ACTIVE") {
			return fail()
		}
		if existing, ok := claimPod[claim.ClaimUID]; ok && existing != claim.PodUID {
			return fail()
		}
		seenPhysical[gpu.PhysicalUID] = true
		claimPod[claim.ClaimUID] = claim.PodUID
		mapped = append(mapped, PhysicalDeviceBinding{
			PhysicalUID: gpu.PhysicalUID,
			IslandID:    gpu.IslandID,
			ScheduledDevice: claim,
		})
	}
	sort.Slice(mapped, func(i, j int) bool {
		return mapped[i].PhysicalUID < mapped[j].PhysicalUID
	})
	payload, err := json.Marshal(struct {
		SourceDigest string
		Bindings     []PhysicalDeviceBinding
	}{SourceDigest: verified.digest, Bindings: mapped})
	if err != nil {
		return fail()
	}
	sum := sha256.Sum256(append([]byte("ccp/pd-physical-claim-join/v1\x00"), payload...))
	return PhysicalDeviceMapping{
		Bindings:       mapped,
		EvidenceSHA256: hex.EncodeToString(sum[:]),
		PhysicalReady:  false,
	}, nil
}
