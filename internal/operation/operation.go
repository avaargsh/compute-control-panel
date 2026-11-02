// Package operation records provider-side uncertainty without claiming
// ownership of Kubernetes scheduling or actual resource lifecycle.
package operation

import "errors"

type Kind string
const (
    Create Kind = "CREATE"
    Delete Kind = "DELETE"
)
type Status string
const (
    Pending Status = "PENDING"
    InFlight Status = "IN_FLIGHT"
    Unknown Status = "UNKNOWN"
    Verified Status = "VERIFIED"
)
type Existence string
const (
    Exists Existence = "EXISTS"
    Absent Existence = "ABSENT"
    Indeterminate Existence = "INDETERMINATE"
)
var (
    ErrInvalid = errors.New("invalid operation")
    ErrStale = errors.New("stale operation version")
    ErrTransition = errors.New("forbidden operation transition")
    ErrObservation = errors.New("inconclusive or mismatched provider observation")
)
type Operation struct {
    ID string
    TenantID string
    WorkloadID string
    ProviderID string
    Generation uint64
    Version uint64
    Kind Kind
    Status Status
}
// Observation must come from the provider adapter's authenticated inspect
// response, never from untrusted API caller JSON or a mutation ACK.
type Observation struct {
    OperationID string
    Generation uint64
    ProviderResourceID string
    Existence Existence
}
func (o Operation) Validate() error {
    if o.ID == "" || o.TenantID == "" || o.WorkloadID == "" ||
        o.ProviderID == "" || o.Generation == 0 || o.Version == 0 {
        return ErrInvalid
    }
    if o.Kind != Create && o.Kind != Delete { return ErrInvalid }
    switch o.Status {
    case Pending, InFlight, Unknown, Verified: return nil
    }
    return ErrInvalid
}
func (o Operation) Begin(expectedVersion uint64) (Operation, error) {
    if err := o.Validate(); err != nil { return o, err }
    if o.Version != expectedVersion { return o, ErrStale }
    if o.Status != Pending { return o, ErrTransition }
    o.Status = InFlight
    o.Version++
    return o, nil
}
func (o Operation) LostAck(expectedVersion uint64) (Operation, error) {
    if err := o.Validate(); err != nil { return o, err }
    if o.Version != expectedVersion { return o, ErrStale }
    if o.Status != InFlight { return o, ErrTransition }
    o.Status = Unknown
    o.Version++
    return o, nil
}
// Verify never interprets a successful delete ACK as release finality.
// DELETE requires provider-observed absence for this exact generation;
// CREATE requires provider-observed existence and a resource identity.
func (o Operation) Verify(expectedVersion uint64, obs Observation) (Operation, error) {
    if err := o.Validate(); err != nil { return o, err }
    if o.Version != expectedVersion { return o, ErrStale }
    if o.Status != InFlight && o.Status != Unknown { return o, ErrTransition }
    if obs.OperationID != o.ID || obs.Generation != o.Generation {
        return o, ErrObservation
    }
    if o.Kind == Create && (obs.Existence != Exists || obs.ProviderResourceID == "") {
        return o, ErrObservation
    }
    if o.Kind == Delete && obs.Existence != Absent {
        return o, ErrObservation
    }
    o.Status = Verified
    o.Version++
    return o, nil
}
