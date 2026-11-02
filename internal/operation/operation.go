// Package operation owns the durable intent to mutate a compute provider.
// Provider observations are separate evidence; an API acknowledgement alone
// never proves allocation, execution, or release.
package operation

import (
	"encoding/hex"
	"errors"
	"strings"
	"time"
)

type Kind string

const (
	Create Kind = "CREATE"
	Update Kind = "UPDATE"
	Delete Kind = "DELETE"
)

type Status string

const (
	Pending      Status = "PENDING"
	Leased       Status = "LEASED"
	InFlight     Status = "IN_FLIGHT"
	Unknown      Status = "UNKNOWN"
	Reconciling  Status = "RECONCILING"
	Succeeded    Status = "SUCCEEDED"
	Failed       Status = "FAILED"
	ManualReview Status = "MANUAL_REVIEW"
	Superseded   Status = "SUPERSEDED"
)

type ObservationResult string

const (
	ResourcePresent          ObservationResult = "RESOURCE_PRESENT"
	TargetAbsent             ObservationResult = "TARGET_ABSENT"
	OperationNotAppliedFinal ObservationResult = "OPERATION_NOT_APPLIED_FINAL"
	OperationInProgress      ObservationResult = "OPERATION_IN_PROGRESS"
	OperationFailedFinal     ObservationResult = "OPERATION_FAILED_FINAL"
	ObservationConflict      ObservationResult = "CONFLICT"
)

var (
	ErrInvalid       = errors.New("invalid compute operation")
	ErrStale         = errors.New("stale compute operation version")
	ErrTransition    = errors.New("forbidden compute operation transition")
	ErrStaleLease    = errors.New("stale compute operation lease or fencing token")
	ErrLeaseExpired  = errors.New("compute operation lease expired")
	ErrLeaseActive   = errors.New("compute operation lease is still active")
	ErrObservation   = errors.New("provider observation does not match operation")
	ErrInvalidResult = errors.New("invalid provider observation result")
)

type Operation struct {
	ID             string
	TenantID       string
	WorkloadID     string
	ProviderID     string
	IdempotencyKey string
	RequestDigest  string

	Generation uint64
	Version    uint64
	FenceToken uint64
	Kind       Kind
	Status     Status

	LeaseOwner     string
	LeaseExpiresAt time.Time

	TargetResourceID  string
	TargetResourceUID string

	ProviderResourceID  string
	ProviderResourceUID string
	ErrorCode           string
	ManualReviewCode    string
	CleanupRequired     bool
	ReconcileRequired   bool
}

// Observation is created by a trusted provider adapter after inspect/lookup.
// It must not be populated from an API caller's request body.
type Observation struct {
	OperationID       string
	TenantID          string
	WorkloadID        string
	ProviderID        string
	DesiredGeneration uint64

	Result ObservationResult

	ResourceID          string
	ResourceUID         string
	ResourceOperationID string
	ResourceGeneration  uint64

	TargetResourceID  string
	TargetResourceUID string

	ResourceVersion string
	ObservedAt      time.Time
	ErrorCode       string
	ReasonCode      string
}

func validDigest(value string) bool {
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

func (o Operation) Validate() error {
	if strings.TrimSpace(o.ID) == "" ||
		strings.TrimSpace(o.TenantID) == "" ||
		strings.TrimSpace(o.WorkloadID) == "" ||
		strings.TrimSpace(o.ProviderID) == "" ||
		strings.TrimSpace(o.IdempotencyKey) == "" ||
		!validDigest(o.RequestDigest) ||
		o.Generation == 0 || o.Version == 0 {
		return ErrInvalid
	}
	if o.Kind != Create && o.Kind != Update && o.Kind != Delete {
		return ErrInvalid
	}
	if (o.Kind == Update || o.Kind == Delete) &&
		(o.TargetResourceID == "" || o.TargetResourceUID == "") {
		return ErrInvalid
	}
	if o.Kind == Create && (o.TargetResourceID != "" || o.TargetResourceUID != "") {
		return ErrInvalid
	}

	leased := o.Status == Leased || o.Status == InFlight || o.Status == Reconciling
	if leased {
		if strings.TrimSpace(o.LeaseOwner) == "" || o.LeaseExpiresAt.IsZero() || o.FenceToken == 0 {
			return ErrInvalid
		}
	} else if o.LeaseOwner != "" || !o.LeaseExpiresAt.IsZero() {
		return ErrInvalid
	}

	switch o.Status {
	case Pending, Leased, InFlight, Unknown, Reconciling:
		if o.ProviderResourceID != "" || o.ProviderResourceUID != "" ||
			o.ErrorCode != "" || o.ManualReviewCode != "" ||
			o.CleanupRequired || o.ReconcileRequired {
			return ErrInvalid
		}
	case Succeeded:
		if o.ErrorCode != "" || o.ManualReviewCode != "" ||
			o.CleanupRequired || o.ReconcileRequired {
			return ErrInvalid
		}
		if o.Kind == Delete {
			if o.TargetResourceID == "" || o.TargetResourceUID == "" ||
				o.ProviderResourceID != "" || o.ProviderResourceUID != "" {
				return ErrInvalid
			}
		} else if o.ProviderResourceID == "" || o.ProviderResourceUID == "" {
			return ErrInvalid
		} else if o.Kind == Update &&
			(o.ProviderResourceID != o.TargetResourceID || o.ProviderResourceUID != o.TargetResourceUID) {
			return ErrInvalid
		}
	case Failed:
		if o.ErrorCode == "" || o.ManualReviewCode != "" ||
			o.ProviderResourceID != "" || o.ProviderResourceUID != "" ||
			o.CleanupRequired || o.ReconcileRequired {
			return ErrInvalid
		}
	case ManualReview:
		if o.ManualReviewCode == "" || o.ErrorCode != "" ||
			o.ProviderResourceID != "" || o.ProviderResourceUID != "" ||
			o.CleanupRequired || o.ReconcileRequired {
			return ErrInvalid
		}
	case Superseded:
		if o.ErrorCode != "" || o.ManualReviewCode != "" || !o.ReconcileRequired {
			return ErrInvalid
		}
		if o.CleanupRequired && (o.ProviderResourceID == "" || o.ProviderResourceUID == "") {
			return ErrInvalid
		}
		if !o.CleanupRequired &&
			((o.ProviderResourceID == "") != (o.ProviderResourceUID == "")) {
			return ErrInvalid
		}
	default:
		return ErrInvalid
	}
	return nil
}

func (o Operation) checkVersion(expected uint64) error {
	if o.Version != expected {
		return ErrStale
	}
	return nil
}

func (o Operation) checkLease(owner string, token uint64, now time.Time) error {
	if owner == "" || o.LeaseOwner != owner || o.FenceToken != token {
		return ErrStaleLease
	}
	if now.IsZero() || !now.Before(o.LeaseExpiresAt) {
		return ErrLeaseExpired
	}
	return nil
}

func (o Operation) clearLease() Operation {
	o.LeaseOwner = ""
	o.LeaseExpiresAt = time.Time{}
	return o
}

func (o Operation) toManualReview(code string) Operation {
	o = o.clearLease()
	o.Status = ManualReview
	o.ManualReviewCode = code
	o.Version++
	return o
}

func (o Operation) Claim(expectedVersion uint64, owner string, now time.Time, ttl time.Duration) (Operation, error) {
	if err := o.Validate(); err != nil {
		return o, err
	}
	if err := o.checkVersion(expectedVersion); err != nil {
		return o, err
	}
	if o.Status != Pending {
		return o, ErrTransition
	}
	if strings.TrimSpace(owner) == "" || now.IsZero() || ttl <= 0 {
		return o, ErrInvalid
	}
	o.Status = Leased
	o.LeaseOwner = owner
	o.LeaseExpiresAt = now.Add(ttl)
	o.FenceToken++
	o.Version++
	return o, nil
}

func (o Operation) Start(expectedVersion uint64, owner string, token uint64, now time.Time) (Operation, error) {
	if err := o.Validate(); err != nil {
		return o, err
	}
	if err := o.checkVersion(expectedVersion); err != nil {
		return o, err
	}
	if o.Status != Leased {
		return o, ErrTransition
	}
	if err := o.checkLease(owner, token, now); err != nil {
		return o, err
	}
	o.Status = InFlight
	o.Version++
	return o, nil
}

func (o Operation) MarkUnknown(expectedVersion uint64, owner string, token uint64, now time.Time) (Operation, error) {
	if err := o.Validate(); err != nil {
		return o, err
	}
	if err := o.checkVersion(expectedVersion); err != nil {
		return o, err
	}
	if o.Status != InFlight {
		return o, ErrTransition
	}
	if err := o.checkLease(owner, token, now); err != nil {
		return o, err
	}
	o = o.clearLease()
	o.Status = Unknown
	o.Version++
	return o, nil
}

func (o Operation) ExpireLease(expectedVersion uint64, now time.Time) (Operation, error) {
	if err := o.Validate(); err != nil {
		return o, err
	}
	if err := o.checkVersion(expectedVersion); err != nil {
		return o, err
	}
	if o.Status != Leased && o.Status != InFlight && o.Status != Reconciling {
		return o, ErrTransition
	}
	if now.IsZero() || now.Before(o.LeaseExpiresAt) {
		return o, ErrLeaseActive
	}
	o = o.clearLease()
	o.Status = Unknown
	o.FenceToken++
	o.Version++
	return o, nil
}

// BeginReconciliation accepts UNKNOWN after a lost ACK or IN_FLIGHT after a
// provider ACK. For IN_FLIGHT the current lease owner/token must prove
// ownership; reconciliation then increments the fencing token.
func (o Operation) BeginReconciliation(expectedVersion uint64, currentOwner string, currentToken uint64, newOwner string, now time.Time, ttl time.Duration) (Operation, error) {
	if err := o.Validate(); err != nil {
		return o, err
	}
	if err := o.checkVersion(expectedVersion); err != nil {
		return o, err
	}
	if strings.TrimSpace(newOwner) == "" || now.IsZero() || ttl <= 0 {
		return o, ErrInvalid
	}

	switch o.Status {
	case Unknown:
		if currentOwner != "" || currentToken != 0 {
			return o, ErrStaleLease
		}
	case InFlight:
		if err := o.checkLease(currentOwner, currentToken, now); err != nil {
			return o, err
		}
	default:
		return o, ErrTransition
	}

	o.Status = Reconciling
	o.LeaseOwner = newOwner
	o.LeaseExpiresAt = now.Add(ttl)
	o.FenceToken++
	o.Version++
	return o, nil
}

func (o Operation) ApplyObservation(expectedVersion uint64, owner string, token uint64, now time.Time, currentDesiredGeneration uint64, obs Observation) (Operation, error) {
	if err := o.Validate(); err != nil {
		return o, err
	}
	if err := o.checkVersion(expectedVersion); err != nil {
		return o, err
	}
	if o.Status != Reconciling {
		return o, ErrTransition
	}
	if err := o.checkLease(owner, token, now); err != nil {
		return o, err
	}
	if currentDesiredGeneration == 0 || obs.OperationID != o.ID ||
		obs.TenantID != o.TenantID || obs.WorkloadID != o.WorkloadID ||
		obs.ProviderID != o.ProviderID || obs.DesiredGeneration != o.Generation ||
		obs.ResourceVersion == "" || obs.ObservedAt.IsZero() || obs.ObservedAt.After(now) {
		return o, ErrObservation
	}

	switch obs.Result {
	case ResourcePresent:
		if o.Kind == Delete || obs.ResourceID == "" || obs.ResourceUID == "" ||
			obs.ResourceOperationID != o.ID || obs.ResourceGeneration != o.Generation ||
			obs.TargetResourceID != "" || obs.TargetResourceUID != "" ||
			obs.ErrorCode != "" || obs.ReasonCode != "" {
			return o, ErrObservation
		}
		if o.Kind == Update &&
			(obs.ResourceID != o.TargetResourceID || obs.ResourceUID != o.TargetResourceUID) {
			return o, ErrObservation
		}
	case TargetAbsent:
		if o.Kind != Delete || obs.TargetResourceID != o.TargetResourceID ||
			obs.TargetResourceUID != o.TargetResourceUID ||
			obs.ResourceID != "" || obs.ResourceUID != "" ||
			obs.ResourceOperationID != "" || obs.ResourceGeneration != 0 ||
			obs.ErrorCode != "" || obs.ReasonCode != "" {
			return o, ErrObservation
		}
	case OperationNotAppliedFinal, OperationInProgress:
		if obs.ResourceID != "" || obs.ResourceUID != "" ||
			obs.ResourceOperationID != "" || obs.ResourceGeneration != 0 ||
			obs.TargetResourceID != "" || obs.TargetResourceUID != "" ||
			obs.ErrorCode != "" || obs.ReasonCode != "" {
			return o, ErrObservation
		}
	case OperationFailedFinal:
		if obs.ErrorCode == "" || obs.ResourceID != "" || obs.ResourceUID != "" ||
			obs.ResourceOperationID != "" || obs.ResourceGeneration != 0 ||
			obs.TargetResourceID != "" || obs.TargetResourceUID != "" ||
			obs.ReasonCode != "" {
			return o, ErrObservation
		}
	case ObservationConflict:
		if obs.ReasonCode == "" {
			return o, ErrObservation
		}
	default:
		return o, ErrInvalidResult
	}

	if currentDesiredGeneration != o.Generation {
		switch obs.Result {
		case ResourcePresent:
			o.ProviderResourceID = obs.ResourceID
			o.ProviderResourceUID = obs.ResourceUID
			o.CleanupRequired = o.Kind == Create
			o.ReconcileRequired = true
			o.Status = Superseded
		case TargetAbsent, OperationNotAppliedFinal, OperationFailedFinal:
			o.ReconcileRequired = true
			o.Status = Superseded
		default:
			return o.toManualReview("STALE_GENERATION_OUTCOME_AMBIGUOUS"), nil
		}
		o = o.clearLease()
		o.Version++
		return o, nil
	}

	switch obs.Result {
	case ResourcePresent:
		o.ProviderResourceID = obs.ResourceID
		o.ProviderResourceUID = obs.ResourceUID
		o.Status = Succeeded
	case TargetAbsent, OperationNotAppliedFinal:
		if obs.Result == OperationNotAppliedFinal {
			// Retry is safe only because the provider's operation lookup
			// authoritatively proved no effect. Keep operation ID and digest.
			o.Status = Pending
		} else {
			o.Status = Succeeded
		}
	case OperationInProgress:
		o = o.clearLease()
		o.Status = Unknown
		o.Version++
		return o, nil
	case OperationFailedFinal:
		o.ErrorCode = obs.ErrorCode
		o.Status = Failed
	case ObservationConflict:
		return o.toManualReview(obs.ReasonCode), nil
	}
	o = o.clearLease()
	o.Version++
	return o, nil
}
