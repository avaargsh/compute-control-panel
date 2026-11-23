// Package observation projects read-only infrastructure facts for the CCP
// console. It is not a provider, device-allocation or billing authority.
package observation

import (
	"errors"
	"strings"
	"time"
)

type Mode string

const (
	ModeLive Mode = "live"
	ModeDemo Mode = "demo"
)

func ValidateMode(mode Mode) error {
	if mode != ModeLive && mode != ModeDemo {
		return errors.New("unknown observation data mode")
	}
	return nil
}

type Condition string

const (
	ConditionAvailable   Condition = "AVAILABLE"
	ConditionDegraded    Condition = "DEGRADED"
	ConditionUnavailable Condition = "UNAVAILABLE"
	ConditionUnknown     Condition = "UNKNOWN"
)

type Status string

const (
	StatusAvailable   Status = "AVAILABLE"
	StatusDegraded    Status = "DEGRADED"
	StatusUnavailable Status = "UNAVAILABLE"
	StatusUnknown     Status = "UNKNOWN"
	StatusStale       Status = "STALE"
)

type ResourceRef struct {
	ClusterID       string `json:"cluster_id"`
	Namespace       string `json:"namespace,omitempty"`
	Kind            string `json:"kind"`
	Name            string `json:"name"`
	UID             string `json:"uid,omitempty"`
	ResourceVersion string `json:"resource_version,omitempty"`
}

// Quantity is a Kubernetes Node-reported resource quantity. It is not a
// measured, physically free, or independently authenticated GPU device count.
type Quantity struct {
	ResourceName string `json:"resource_name"`
	Capacity     string `json:"capacity"`
	Allocatable  string `json:"allocatable"`
}

type Record struct {
	// Scope is resolved and enforced server-side, never from browser data.
	TenantID       string      `json:"-"`
	ProjectID      string      `json:"-"`
	Resource       ResourceRef `json:"resource"`
	Quantity       *Quantity   `json:"reported_quantity,omitempty"`
	Condition      Condition   `json:"condition"`
	Source         string      `json:"source"`
	SourceRevision string      `json:"source_revision,omitempty"`
	ObservedAt     time.Time   `json:"observed_at"`
	ExpiresAt      time.Time   `json:"expires_at"`
	ReasonCode     string      `json:"reason_code,omitempty"`
	Message        string      `json:"message,omitempty"`
	EvidenceRef    string      `json:"evidence_ref,omitempty"`
}

type View struct {
	ProjectID      string      `json:"project_id"`
	Resource       ResourceRef `json:"resource"`
	Quantity       *Quantity   `json:"reported_quantity,omitempty"`
	Status         Status      `json:"status"`
	ReasonCode     string      `json:"reason_code,omitempty"`
	Message        string      `json:"message,omitempty"`
	Source         string      `json:"source"`
	SourceRevision string      `json:"source_revision,omitempty"`
	ObservedAt     time.Time   `json:"observed_at"`
	ExpiresAt      time.Time   `json:"expires_at"`
	EvidenceRef    string      `json:"evidence_ref,omitempty"`
}

// Project is deterministic and does not change the stored external fact.
// An AVAILABLE observation means only that the *source* reported availability;
// it is never proof of physical GPU assignment or finance FINAL.
func Project(record Record, now time.Time) (View, error) {
	if strings.TrimSpace(record.TenantID) == "" || strings.TrimSpace(record.ProjectID) == "" ||
		strings.TrimSpace(record.Resource.ClusterID) == "" || strings.TrimSpace(record.Resource.Kind) == "" ||
		strings.TrimSpace(record.Resource.Name) == "" || now.IsZero() {
		return View{}, errors.New("missing observation scope, identity or clock")
	}
	view := View{
		ProjectID: record.ProjectID, Resource: record.Resource, Quantity: record.Quantity, Status: StatusUnknown,
		ReasonCode: record.ReasonCode, Message: record.Message,
		Source: record.Source, SourceRevision: record.SourceRevision,
		ObservedAt: record.ObservedAt, ExpiresAt: record.ExpiresAt,
		EvidenceRef: record.EvidenceRef,
	}
	switch {
	case strings.TrimSpace(record.Source) == "" || record.ObservedAt.IsZero() || record.ExpiresAt.IsZero() || !record.ExpiresAt.After(record.ObservedAt):
		view.ReasonCode = "OBSERVATION_INVALID"
	case record.ObservedAt.After(now):
		view.ReasonCode = "OBSERVATION_IN_FUTURE"
	case !now.Before(record.ExpiresAt):
		view.Status = StatusStale
		view.ReasonCode = "OBSERVATION_EXPIRED"
	default:
		switch record.Condition {
		case ConditionAvailable:
			view.Status = StatusAvailable
		case ConditionDegraded:
			view.Status = StatusDegraded
		case ConditionUnavailable:
			view.Status = StatusUnavailable
		case ConditionUnknown:
			if view.ReasonCode == "" {
				view.ReasonCode = "SOURCE_REPORTED_UNKNOWN"
			}
		default:
			view.ReasonCode = "CONDITION_INVALID"
		}
	}
	return view, nil
}
