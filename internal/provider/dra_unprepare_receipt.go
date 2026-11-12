package provider

import (
	"crypto/ed25519"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"time"
)

// ErrUncorrelatedDRAUnprepare means the signed claim, source, clock or
// historical Allocation identities do not match. It does not establish
// physical device presence, release, or cost liability.
var ErrUncorrelatedDRAUnprepare = errors.New("driver NodeUnprepare receipt cannot be authenticated or correlated")

// DRAUnprepareEvent is a custom driver-adapter assertion about an observed
// successful per-Claim NodeUnprepareResources result. It is NOT a Kubernetes
// API object, gRPC response, or signed event emitted by Kubernetes. Real
// adapter instrumentation must tie it to the exact driver RPC result.
type DRAUnprepareEvent struct {
	Version        string    `json:"version"`
	ClusterID      string    `json:"cluster_id"`
	SourceID       string    `json:"source_id"`
	KeyID          string    `json:"key_id"`
	EventID        string    `json:"event_id"`
	RPC            string    `json:"rpc"`
	Result         string    `json:"result"`
	Driver         string    `json:"driver"`
	NodeName       string    `json:"node_name"`
	ClaimNamespace string    `json:"claim_namespace"`
	ClaimName      string    `json:"claim_name"`
	ClaimUID       string    `json:"claim_uid"`
	CompletedAt    time.Time `json:"completed_at"`
}

// SignedDRAUnprepare is a custom Ed25519 envelope (not a Kubernetes protobuf).
// Its signature covers all event fields, including key and event IDs.
type SignedDRAUnprepare struct {
	Event     DRAUnprepareEvent
	Signature []byte
}

// DRAUnprepareAuthority must be provisioned from an independent, trusted
// Provider/cluster source-key registry. It must NEVER be tenant/caller/receipt
// supplied. Such a production registry and source attestation are not wired.
type DRAUnprepareAuthority struct {
	ClusterID string
	SourceID  string
	KeyID     string
	Driver    string
	PublicKey ed25519.PublicKey
}

// DRAUnprepareVerifier verifies signature, scope and freshness only.
// It does not implement registry trust, replay prevention or authorization.
type DRAUnprepareVerifier struct {
	authority  DRAUnprepareAuthority
	maximumAge time.Duration
	futureSkew time.Duration
}

// DRAUnprepareCorrelation retains the original signature, provisioned signer
// key fingerprint, exact source event and signed-payload digest for later
// independently authorized verification. It has no RELEASED, physical-device,
// READY, or cost-finality status.
type DRAUnprepareCorrelation struct {
	Release       ReleaseEvidenceCorrelation
	Event         DRAUnprepareEvent
	PayloadSHA256 string
	KeySHA256     string
	Signature     [ed25519.SignatureSize]byte
}

// Fixed-field struct JSON with a domain separator. No maps or arbitrary input
// are signed. A real adapter must implement precisely this canonical contract;
// its transport, source key custody and operation-result authenticity remain
// separate production gates.
func draUnprepareSigningBytes(event DRAUnprepareEvent) []byte {
	payload, _ := json.Marshal(event) // fixed scalar fields + time.Time; cannot fail
	return append([]byte("CCP:DRA:NodeUnprepareResources:v1\n"), payload...)
}

func NewDRAUnprepareVerifier(
	a DRAUnprepareAuthority, maximumAge, futureSkew time.Duration,
) (*DRAUnprepareVerifier, error) {
	if !validDRAIdentity(a.ClusterID) || !validDRAIdentity(a.SourceID) ||
		!validDRAIdentity(a.KeyID) || !validDRAIdentity(a.Driver) ||
		len(a.PublicKey) != ed25519.PublicKeySize ||
		maximumAge <= 0 || futureSkew < 0 || futureSkew > maximumAge {
		return nil, ErrUncorrelatedDRAUnprepare
	}
	// Never retain caller-owned mutable public-key storage.
	pinned := make(ed25519.PublicKey, ed25519.PublicKeySize)
	copy(pinned, a.PublicKey)
	a.PublicKey = pinned
	return &DRAUnprepareVerifier{authority: a, maximumAge: maximumAge, futureSkew: futureSkew}, nil
}

// Correlate cross-checks a signed driver event against the historical durable
// CREATE/DELETE, Provider absence, Pod absence, Kueue and ResourceClaim
// observations. Signature verification is meaningful ONLY with an independently
// authenticated, provisioned driver key and clock. This method does not connect
// to a driver, verify RPC execution, prevent durable replay, or prove physical
// GPU deallocation. It authorizes NO Allocation/cost state transition.
func (v *DRAUnprepareVerifier) Correlate(
	candidate ReleaseEvidenceCandidate, signed SignedDRAUnprepare, asOf time.Time,
) (DRAUnprepareCorrelation, error) {
	reject := func() (DRAUnprepareCorrelation, error) {
		return DRAUnprepareCorrelation{}, ErrUncorrelatedDRAUnprepare
	}
	if v == nil || asOf.IsZero() || len(v.authority.PublicKey) != ed25519.PublicKeySize ||
		len(signed.Signature) != ed25519.SignatureSize {
		return reject()
	}
	release, err := CorrelateReleaseEvidence(candidate)
	if err != nil {
		return reject()
	}
	event := signed.Event
	prior := candidate.PriorPodClaim
	if event.Version != "ccp.dra.node-unprepare.v1" ||
		event.RPC != "NodeUnprepareResources" || event.Result != "SUCCESS" ||
		event.ClusterID != v.authority.ClusterID ||
		event.SourceID != v.authority.SourceID ||
		event.KeyID != v.authority.KeyID ||
		event.Driver != v.authority.Driver ||
		!validDRAIdentity(event.EventID) ||
		!validKubeName(event.ClaimNamespace) || !validKubeName(event.ClaimName) ||
		!validDRAIdentity(event.ClaimUID) ||
		event.ClaimNamespace != prior.Claim.Claim.Namespace ||
		event.ClaimName != prior.Claim.Claim.Name ||
		event.ClaimUID != prior.Claim.Claim.UID ||
		event.Driver != prior.Claim.Device.Driver ||
		event.NodeName != prior.NodeName ||
		!validDRAIdentity(event.NodeName) ||
		event.CompletedAt.IsZero() ||
		event.CompletedAt.Before(asOf.Add(-v.maximumAge)) ||
		event.CompletedAt.After(asOf.Add(v.futureSkew)) {
		return reject()
	}
	payload := draUnprepareSigningBytes(event)
	if !ed25519.Verify(v.authority.PublicKey, payload, signed.Signature) {
		return reject()
	}
	digest := sha256.Sum256(payload)
	keyDigest := sha256.Sum256(v.authority.PublicKey)
	var signature [ed25519.SignatureSize]byte
	copy(signature[:], signed.Signature)
	return DRAUnprepareCorrelation{
		Release:       release,
		Event:         event,
		PayloadSHA256: "sha256:" + hex.EncodeToString(digest[:]),
		KeySHA256:     "sha256:" + hex.EncodeToString(keyDigest[:]),
		Signature:     signature,
	}, nil
}
