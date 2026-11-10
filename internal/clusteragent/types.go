// Package clusteragent implements the outbound-only remote Cluster Agent protocol.
package clusteragent

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/avaargsh/compute-control-panel/internal/operation"
)

const ProtocolVersion = "compute.cluster-agent/v1"

type CommandAction string

const (
	ActionApply   CommandAction = "APPLY"
	ActionInspect CommandAction = "INSPECT"
)

type CommandState string

const (
	CommandPending   CommandState = "PENDING"
	CommandLeased    CommandState = "LEASED"
	CommandSucceeded CommandState = "SUCCEEDED"
	CommandFailed    CommandState = "FAILED"
	CommandUnknown   CommandState = "UNKNOWN"
)

type Outcome string

const (
	OutcomeSucceeded Outcome = "SUCCEEDED"
	OutcomeFailed    Outcome = "FAILED"
	OutcomeUnknown   Outcome = "UNKNOWN"
)

var (
	ErrInvalid                 = errors.New("invalid Cluster Agent protocol message")
	ErrUnauthorized            = errors.New("Cluster Agent identity is not registered")
	ErrAgentUnavailable        = errors.New("Cluster Agent is not connected or eligible")
	ErrStaleSession            = errors.New("Cluster Agent session is stale")
	ErrCommandNotFound         = errors.New("Cluster Agent command not found")
	ErrCommandConflict         = errors.New("Cluster Agent command identity or digest conflict")
	ErrCommandFence            = errors.New("stale Cluster Agent command fence")
	ErrCommandUnknown          = errors.New("Cluster Agent command outcome is unknown")
	ErrCommandFailed           = errors.New("Cluster Agent command failed")
	ErrProviderBindingMissing  = errors.New("Provider has no Cluster Agent binding")
	ErrInvalidConfiguration    = errors.New("invalid Cluster Agent configuration")
)

type ProviderRequest struct {
	OperationID       string         `json:"operation_id"`
	TenantID          string         `json:"tenant_id"`
	WorkloadID        string         `json:"workload_id"`
	ProviderID        string         `json:"provider_id"`
	IdempotencyKey    string         `json:"idempotency_key"`
	RequestDigest     string         `json:"request_digest"`
	DesiredGeneration uint64         `json:"desired_generation"`
	OperationKind     operation.Kind `json:"operation_kind"`
	TargetResourceID  string         `json:"target_resource_id,omitempty"`
	TargetResourceUID string         `json:"target_resource_uid,omitempty"`
}

func (r ProviderRequest) Validate() error {
	op := operation.Operation{
		ID: r.OperationID, TenantID: r.TenantID, WorkloadID: r.WorkloadID,
		ProviderID: r.ProviderID, IdempotencyKey: r.IdempotencyKey,
		RequestDigest: r.RequestDigest, Generation: r.DesiredGeneration,
		Version: 1, Kind: r.OperationKind, Status: operation.Pending,
		TargetResourceID: r.TargetResourceID, TargetResourceUID: r.TargetResourceUID,
	}
	if err := op.Validate(); err != nil {
		return fmt.Errorf("%w: invalid immutable Provider request", ErrInvalid)
	}
	return nil
}

type Command struct {
	CommandID       string          `json:"command_id"`
	SchemaVersion   string          `json:"schema_version"`
	Action          CommandAction   `json:"action"`
	AgentID         string          `json:"agent_id"`
	ClusterScope    string          `json:"cluster_scope"`
	ProviderID      string          `json:"provider_id"`
	OperationID     string          `json:"operation_id"`
	TenantID        string          `json:"tenant_id"`
	WorkloadID      string          `json:"workload_id"`
	DesiredGeneration uint64        `json:"desired_generation"`
	RequestDigest   string          `json:"request_digest"`
	Request         ProviderRequest `json:"request"`
	InstanceID      string          `json:"instance_id"`
	LeaseEpoch      uint64          `json:"lease_epoch"`
	CommandFence    uint64          `json:"command_fence"`
	LeaseExpiresAt  time.Time       `json:"lease_expires_at"`
}

func (c Command) Validate() error {
	for _, value := range []string{c.CommandID, c.AgentID, c.ClusterScope, c.ProviderID,
		c.OperationID, c.TenantID, c.WorkloadID, c.InstanceID} {
		if value == "" || value != strings.TrimSpace(value) || len(value) > 256 {
			return ErrInvalid
		}
	}
	if c.SchemaVersion != ProtocolVersion ||
		(c.Action != ActionApply && c.Action != ActionInspect) ||
		c.DesiredGeneration == 0 || c.LeaseEpoch == 0 || c.CommandFence == 0 ||
		c.LeaseExpiresAt.IsZero() || c.RequestDigest == "" ||
		c.RequestDigest != c.Request.RequestDigest ||
		c.ProviderID != c.Request.ProviderID ||
		c.OperationID != c.Request.OperationID ||
		c.TenantID != c.Request.TenantID ||
		c.WorkloadID != c.Request.WorkloadID ||
		c.DesiredGeneration != c.Request.DesiredGeneration {
		return ErrInvalid
	}
	return c.Request.Validate()
}

type ExecutionResult struct {
	Outcome   Outcome         `json:"outcome"`
	Payload   json.RawMessage `json:"payload,omitempty"`
	ErrorCode string          `json:"error_code,omitempty"`
}

func (r ExecutionResult) Validate() error {
	switch r.Outcome {
	case OutcomeSucceeded:
		if len(r.Payload) == 0 || !json.Valid(r.Payload) || r.ErrorCode != "" {
			return ErrInvalid
		}
	case OutcomeFailed, OutcomeUnknown:
		if r.ErrorCode == "" || len(r.ErrorCode) > 128 || strings.TrimSpace(r.ErrorCode) != r.ErrorCode ||
			len(r.Payload) != 0 {
			return ErrInvalid
		}
	default:
		return ErrInvalid
	}
	return nil
}

type HeartbeatRequest struct {
	InstanceID       string          `json:"instance_id"`
	AgentVersion     string          `json:"agent_version"`
	Capabilities     json.RawMessage `json:"capabilities"`
}

func (r HeartbeatRequest) Validate() error {
	if r.InstanceID == "" || r.InstanceID != strings.TrimSpace(r.InstanceID) || len(r.InstanceID) > 128 ||
		r.AgentVersion == "" || r.AgentVersion != strings.TrimSpace(r.AgentVersion) || len(r.AgentVersion) > 128 ||
		len(r.Capabilities) == 0 || !json.Valid(r.Capabilities) {
		return ErrInvalid
	}
	var object map[string]json.RawMessage
	if err := json.Unmarshal(r.Capabilities, &object); err != nil || object == nil {
		return ErrInvalid
	}
	return nil
}

type Session struct {
	AgentID              string    `json:"agent_id"`
	ClusterScope         string    `json:"cluster_scope"`
	InstanceID           string    `json:"instance_id"`
	LeaseEpoch           uint64    `json:"lease_epoch"`
	LeaseExpiresAt       time.Time `json:"lease_expires_at"`
	HeartbeatIntervalSec int       `json:"heartbeat_interval_seconds"`
	DispatchAllowed      bool      `json:"dispatch_allowed"`
}

type ClaimRequest struct {
	InstanceID string `json:"instance_id"`
	LeaseEpoch uint64 `json:"lease_epoch"`
}

func (r ClaimRequest) Validate() error {
	if r.InstanceID == "" || r.InstanceID != strings.TrimSpace(r.InstanceID) ||
		len(r.InstanceID) > 128 || r.LeaseEpoch == 0 {
		return ErrInvalid
	}
	return nil
}

type Completion struct {
	CommandID   string          `json:"command_id"`
	InstanceID  string          `json:"instance_id"`
	LeaseEpoch  uint64          `json:"lease_epoch"`
	CommandFence uint64         `json:"command_fence"`
	Result      ExecutionResult `json:"result"`
}

func (c Completion) Validate() error {
	if c.CommandID == "" || c.CommandID != strings.TrimSpace(c.CommandID) ||
		c.InstanceID == "" || c.InstanceID != strings.TrimSpace(c.InstanceID) ||
		c.LeaseEpoch == 0 || c.CommandFence == 0 {
		return ErrInvalid
	}
	return c.Result.Validate()
}

type AgentService interface {
	Heartbeat(context.Context, string, HeartbeatRequest) (Session, error)
	Claim(context.Context, string, ClaimRequest, time.Duration) (Command, bool, error)
	Complete(context.Context, string, Completion) error
}

type CommandGateway interface {
	Execute(context.Context, string, CommandAction, ProviderRequest) (ExecutionResult, error)
}

type APIClient interface {
	Heartbeat(context.Context, HeartbeatRequest) (Session, error)
	Claim(context.Context, ClaimRequest) (Command, bool, error)
	Complete(context.Context, Completion) error
}

type Executor interface {
	Execute(context.Context, Command) ExecutionResult
}

type LocalJournal interface {
	Execute(context.Context, Command, Executor) ExecutionResult
}
