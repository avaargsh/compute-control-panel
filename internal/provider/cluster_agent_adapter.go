package provider

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"

	"github.com/avaargsh/compute-control-panel/internal/clusteragent"
	"github.com/avaargsh/compute-control-panel/internal/operation"
)

// RemoteClusterAgentAdapter keeps the existing Provider SPI unchanged while
// routing its operations through the durable outbound Cluster Agent channel.
type RemoteClusterAgentAdapter struct {
	gateway clusteragent.CommandGateway
}

func NewRemoteClusterAgentAdapter(gateway clusteragent.CommandGateway) (*RemoteClusterAgentAdapter, error) {
	if gateway == nil {
		return nil, clusteragent.ErrInvalidConfiguration
	}
	return &RemoteClusterAgentAdapter{gateway: gateway}, nil
}

func clusterAgentRequest(request Request) clusteragent.ProviderRequest {
	return clusteragent.ProviderRequest{
		OperationID: request.OperationID, TenantID: request.TenantID,
		WorkloadID: request.WorkloadID, ProviderID: request.ProviderID,
		IdempotencyKey: request.IdempotencyKey, RequestDigest: request.RequestDigest,
		DesiredGeneration: request.DesiredGeneration, OperationKind: request.Kind,
		TargetResourceID: request.TargetResourceID, TargetResourceUID: request.TargetResourceUID,
	}
}

func (a *RemoteClusterAgentAdapter) Apply(ctx context.Context, request Request) (Acknowledgement, error) {
	if a == nil || a.gateway == nil || request.Validate() != nil {
		return Acknowledgement{}, ErrInvalidRequest
	}
	result, err := a.gateway.Execute(ctx, request.ProviderID, clusteragent.ActionApply, clusterAgentRequest(request))
	if errors.Is(err, clusteragent.ErrCommandUnknown) || result.Outcome == clusteragent.OutcomeUnknown {
		return Acknowledgement{}, ErrAcknowledgementLost
	}
	if err != nil {
		return Acknowledgement{}, fmt.Errorf("remote Cluster Agent APPLY failed: %w", err)
	}
	if result.Validate() != nil || result.Outcome != clusteragent.OutcomeSucceeded {
		return Acknowledgement{}, ErrAcknowledgementLost
	}
	var acknowledgement Acknowledgement
	if err := json.Unmarshal(result.Payload, &acknowledgement); err != nil ||
		acknowledgement.OperationID != request.OperationID ||
		acknowledgement.RequestDigest != request.RequestDigest {
		return Acknowledgement{}, ErrAcknowledgementLost
	}
	return acknowledgement, nil
}

func (a *RemoteClusterAgentAdapter) Inspect(ctx context.Context, request Request) (operation.Observation, error) {
	if a == nil || a.gateway == nil || request.Validate() != nil {
		return operation.Observation{}, ErrInvalidRequest
	}
	result, err := a.gateway.Execute(ctx, request.ProviderID, clusteragent.ActionInspect, clusterAgentRequest(request))
	if err != nil {
		return operation.Observation{}, fmt.Errorf("remote Cluster Agent INSPECT failed: %w", err)
	}
	if result.Validate() != nil || result.Outcome != clusteragent.OutcomeSucceeded {
		return operation.Observation{}, clusteragent.ErrCommandUnknown
	}
	var observation operation.Observation
	if err := json.Unmarshal(result.Payload, &observation); err != nil {
		return operation.Observation{}, fmt.Errorf("decode remote Provider observation: %w", err)
	}
	if observation.OperationID != request.OperationID ||
		observation.TenantID != request.TenantID ||
		observation.WorkloadID != request.WorkloadID ||
		observation.ProviderID != request.ProviderID ||
		observation.DesiredGeneration != request.DesiredGeneration {
		return operation.Observation{}, operation.ErrObservation
	}
	return observation, nil
}

// ClusterAgentExecutor converts the typed protocol back to the existing local
// Provider SPI. It is constructed inside the remote cluster and receives only
// the adapter selected by trusted Agent bootstrap configuration.
type ClusterAgentExecutor struct {
	adapter Adapter
}

func NewClusterAgentExecutor(adapter Adapter) (*ClusterAgentExecutor, error) {
	if adapter == nil {
		return nil, clusteragent.ErrInvalidConfiguration
	}
	return &ClusterAgentExecutor{adapter: adapter}, nil
}

func (e *ClusterAgentExecutor) Execute(ctx context.Context, command clusteragent.Command) clusteragent.ExecutionResult {
	if e == nil || e.adapter == nil || command.Validate() != nil {
		return clusteragent.ExecutionResult{Outcome: clusteragent.OutcomeFailed, ErrorCode: "INVALID_PROVIDER_COMMAND"}
	}
	request := Request{
		OperationID: command.Request.OperationID, TenantID: command.Request.TenantID,
		WorkloadID: command.Request.WorkloadID, ProviderID: command.Request.ProviderID,
		IdempotencyKey: command.Request.IdempotencyKey, RequestDigest: command.Request.RequestDigest,
		DesiredGeneration: command.Request.DesiredGeneration, Kind: command.Request.OperationKind,
		TargetResourceID: command.Request.TargetResourceID, TargetResourceUID: command.Request.TargetResourceUID,
	}
	if request.Validate() != nil {
		return clusteragent.ExecutionResult{Outcome: clusteragent.OutcomeFailed, ErrorCode: "INVALID_PROVIDER_REQUEST"}
	}
	switch command.Action {
	case clusteragent.ActionApply:
		ack, err := e.adapter.Apply(ctx, request)
		if errors.Is(err, ErrInvalidRequest) || errors.Is(err, ErrOperationConflict) {
			return clusteragent.ExecutionResult{Outcome: clusteragent.OutcomeFailed, ErrorCode: "PROVIDER_REQUEST_REJECTED"}
		}
		if err != nil {
			return clusteragent.ExecutionResult{Outcome: clusteragent.OutcomeUnknown, ErrorCode: "PROVIDER_APPLY_OUTCOME_UNKNOWN"}
		}
		if ack.OperationID != request.OperationID || ack.RequestDigest != request.RequestDigest {
			return clusteragent.ExecutionResult{Outcome: clusteragent.OutcomeUnknown, ErrorCode: "PROVIDER_ACK_MISMATCH"}
		}
		payload, err := json.Marshal(ack)
		if err != nil {
			return clusteragent.ExecutionResult{Outcome: clusteragent.OutcomeUnknown, ErrorCode: "PROVIDER_ACK_ENCODE_FAILED"}
		}
		return clusteragent.ExecutionResult{Outcome: clusteragent.OutcomeSucceeded, Payload: payload}

	case clusteragent.ActionInspect:
		observation, err := e.adapter.Inspect(ctx, request)
		if err != nil {
			return clusteragent.ExecutionResult{Outcome: clusteragent.OutcomeUnknown, ErrorCode: "PROVIDER_INSPECT_UNAVAILABLE"}
		}
		if observation.OperationID != request.OperationID ||
			observation.TenantID != request.TenantID ||
			observation.WorkloadID != request.WorkloadID ||
			observation.ProviderID != request.ProviderID ||
			observation.DesiredGeneration != request.DesiredGeneration {
			return clusteragent.ExecutionResult{Outcome: clusteragent.OutcomeFailed, ErrorCode: "PROVIDER_OBSERVATION_IDENTITY_MISMATCH"}
		}
		payload, err := json.Marshal(observation)
		if err != nil {
			return clusteragent.ExecutionResult{Outcome: clusteragent.OutcomeUnknown, ErrorCode: "PROVIDER_OBSERVATION_ENCODE_FAILED"}
		}
		return clusteragent.ExecutionResult{Outcome: clusteragent.OutcomeSucceeded, Payload: payload}
	default:
		return clusteragent.ExecutionResult{Outcome: clusteragent.OutcomeFailed, ErrorCode: "UNSUPPORTED_PROVIDER_COMMAND"}
	}
}

var _ Adapter = (*RemoteClusterAgentAdapter)(nil)
var _ clusteragent.Executor = (*ClusterAgentExecutor)(nil)
