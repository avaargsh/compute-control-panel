package postgres

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"strings"
	"time"

	"github.com/avaargsh/compute-control-panel/internal/clusteragent"
)

const (
	clusterAgentHeartbeatTTL = 45 * time.Second
	clusterAgentWaitInterval = 150 * time.Millisecond
)

func clusterAgentCapabilityDigest(raw json.RawMessage) (json.RawMessage, string, error) {
	var capabilities map[string]json.RawMessage
	if err := json.Unmarshal(raw, &capabilities); err != nil || capabilities == nil {
		return nil, "", clusteragent.ErrInvalid
	}
	canonical, err := json.Marshal(capabilities)
	if err != nil {
		return nil, "", err
	}
	digest := sha256.Sum256(canonical)
	return canonical, "sha256:" + hex.EncodeToString(digest[:]), nil
}

func (r *Repository) Heartbeat(ctx context.Context, spiffeID string, request clusteragent.HeartbeatRequest) (clusteragent.Session, error) {
	if r == nil || r.db == nil || !validClusterAgentSPIFFE(spiffeID) || request.Validate() != nil {
		return clusteragent.Session{}, clusteragent.ErrInvalid
	}
	capabilities, capabilityDigest, err := clusterAgentCapabilityDigest(request.Capabilities)
	if err != nil {
		return clusteragent.Session{}, err
	}
	tx, err := r.db.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelReadCommitted})
	if err != nil {
		return clusteragent.Session{}, fmt.Errorf("begin Cluster Agent heartbeat: %w", err)
	}
	defer func() { _ = tx.Rollback() }()

	var agentID, clusterScope, state string
	var currentInstance sql.NullString
	var version, epoch int64
	err = tx.QueryRowContext(ctx, `SELECT agent_id, cluster_scope, state,
		version, lease_epoch, instance_id
		FROM public.compute_cluster_agents WHERE spiffe_id=$1 FOR UPDATE`, spiffeID,
	).Scan(&agentID, &clusterScope, &state, &version, &epoch, &currentInstance)
	if errors.Is(err, sql.ErrNoRows) {
		return clusteragent.Session{}, clusteragent.ErrUnauthorized
	}
	if err != nil {
		return clusteragent.Session{}, fmt.Errorf("resolve Cluster Agent identity: %w", err)
	}
	if state == "RETIRED" {
		return clusteragent.Session{}, clusteragent.ErrUnauthorized
	}

	instanceChanged := !currentInstance.Valid || currentInstance.String != request.InstanceID
	nextState, nextVersion, nextEpoch := state, version, epoch
	reason := "HEARTBEAT"
	if state == "REGISTERED" || state == "SUSPECT" {
		nextState = "CONNECTED"
	}
	if instanceChanged {
		nextEpoch++
		nextVersion++
		reason = "SESSION_CONNECTED"
	}
	if nextState != state && !instanceChanged {
		nextVersion++
		reason = "HEARTBEAT_RECOVERED"
	}
	if nextEpoch <= 0 || nextVersion <= 0 {
		return clusteragent.Session{}, errors.New("invalid Cluster Agent session counter")
	}

	var session clusteragent.Session
	var dispatchAllowed bool
	err = tx.QueryRowContext(ctx, `UPDATE public.compute_cluster_agents
		SET state=$2, version=$3, lease_epoch=$4, instance_id=$5,
		    agent_version=$6, observed_capabilities=$7::jsonb,
		    capability_digest=$8, last_seen_at=pg_catalog.clock_timestamp(),
		    lease_expires_at=pg_catalog.clock_timestamp()+$9::interval,
		    last_reason_code=$10, updated_at=pg_catalog.clock_timestamp()
		WHERE agent_id=$1 AND state<>'RETIRED'
		RETURNING agent_id, cluster_scope, instance_id, lease_epoch,
		          lease_expires_at, (state='CONNECTED')`,
		agentID, nextState, nextVersion, nextEpoch, request.InstanceID,
		request.AgentVersion, []byte(capabilities), capabilityDigest,
		clusterAgentHeartbeatTTL.String(), reason,
	).Scan(&session.AgentID, &session.ClusterScope, &session.InstanceID,
		&session.LeaseEpoch, &session.LeaseExpiresAt, &dispatchAllowed)
	if err != nil {
		return clusteragent.Session{}, fmt.Errorf("persist Cluster Agent heartbeat: %w", err)
	}
	session.HeartbeatIntervalSec = 15
	session.DispatchAllowed = dispatchAllowed
	if err := tx.Commit(); err != nil {
		return clusteragent.Session{}, fmt.Errorf("commit Cluster Agent heartbeat: %w", err)
	}
	return session, nil
}

func validClusterAgentSPIFFE(value string) bool {
	return strings.HasPrefix(value, "spiffe://") && value == strings.TrimSpace(value) &&
		len(value) <= 1024 && !strings.ContainsAny(value, " \r\n\t")
}

type clusterAgentCommandScanner interface {
	Scan(...any) error
}

const clusterAgentCommandColumns = `command_id, schema_version, command_action,
	agent_id, cluster_scope, provider_id, operation_id, tenant_id, workload_id,
	desired_generation, command_sequence, request_digest, request, lease_owner_instance_id,
	lease_epoch, command_fence, lease_expires_at`

func scanClusterAgentCommand(row clusterAgentCommandScanner) (clusteragent.Command, error) {
	var command clusteragent.Command
	var schemaVersion, action string
	var generation, sequence, epoch, fence int64
	var requestBody []byte
	if err := row.Scan(
		&command.CommandID, &schemaVersion, &action,
		&command.AgentID, &command.ClusterScope, &command.ProviderID,
		&command.OperationID, &command.TenantID, &command.WorkloadID,
		&generation, &sequence, &command.RequestDigest, &requestBody,
		&command.InstanceID, &epoch, &fence, &command.LeaseExpiresAt,
	); err != nil {
		return clusteragent.Command{}, err
	}
	if generation <= 0 || sequence <= 0 || epoch <= 0 || fence <= 0 {
		return clusteragent.Command{}, clusteragent.ErrInvalid
	}
	command.SchemaVersion = schemaVersion
	command.Action = clusteragent.CommandAction(action)
	command.DesiredGeneration = uint64(generation)
	command.CommandSequence = uint64(sequence)
	command.LeaseEpoch = uint64(epoch)
	command.CommandFence = uint64(fence)
	if err := json.Unmarshal(requestBody, &command.Request); err != nil {
		return clusteragent.Command{}, fmt.Errorf("decode persisted Cluster Agent request: %w", err)
	}
	if err := command.Validate(); err != nil {
		return clusteragent.Command{}, err
	}
	return command, nil
}

func (r *Repository) Claim(ctx context.Context, spiffeID string, request clusteragent.ClaimRequest, ttl time.Duration) (clusteragent.Command, bool, error) {
	if r == nil || r.db == nil || !validClusterAgentSPIFFE(spiffeID) ||
		request.Validate() != nil || ttl < 5*time.Second || ttl > 5*time.Minute {
		return clusteragent.Command{}, false, clusteragent.ErrInvalid
	}
	tx, err := r.db.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelReadCommitted})
	if err != nil {
		return clusteragent.Command{}, false, fmt.Errorf("begin Cluster Agent claim: %w", err)
	}
	defer func() { _ = tx.Rollback() }()

	var agentID, state, instanceID string
	var epoch int64
	var leaseValid bool
	err = tx.QueryRowContext(ctx, `SELECT agent_id, state, instance_id,
		lease_epoch, lease_expires_at > pg_catalog.clock_timestamp()
		FROM public.compute_cluster_agents
		WHERE spiffe_id=$1 FOR UPDATE`, spiffeID,
	).Scan(&agentID, &state, &instanceID, &epoch, &leaseValid)
	if errors.Is(err, sql.ErrNoRows) {
		return clusteragent.Command{}, false, clusteragent.ErrUnauthorized
	}
	if err != nil {
		return clusteragent.Command{}, false, fmt.Errorf("read Cluster Agent session: %w", err)
	}
	if state != "CONNECTED" || !leaseValid || instanceID != request.InstanceID ||
		uint64(epoch) != request.LeaseEpoch {
		return clusteragent.Command{}, false, clusteragent.ErrStaleSession
	}

	if _, err := tx.ExecContext(ctx, `UPDATE public.compute_cluster_agent_commands
		SET state='UNKNOWN', outcome='UNKNOWN', error_code='AGENT_LEASE_EXPIRED',
		    lease_owner_instance_id=NULL, lease_epoch=NULL, lease_expires_at=NULL,
		    updated_at=pg_catalog.clock_timestamp()
		WHERE agent_id=$1 AND state='LEASED' AND command_action='APPLY'
		  AND lease_expires_at <= pg_catalog.clock_timestamp()`, agentID); err != nil {
		return clusteragent.Command{}, false, fmt.Errorf("expire ambiguous Agent APPLY: %w", err)
	}
	if _, err := tx.ExecContext(ctx, `UPDATE public.compute_cluster_agent_commands
		SET state='PENDING', lease_owner_instance_id=NULL, lease_epoch=NULL,
		    lease_expires_at=NULL, updated_at=pg_catalog.clock_timestamp()
		WHERE agent_id=$1 AND state='LEASED' AND command_action='INSPECT'
		  AND lease_expires_at <= pg_catalog.clock_timestamp()`, agentID); err != nil {
		return clusteragent.Command{}, false, fmt.Errorf("requeue expired Agent INSPECT: %w", err)
	}

	var commandID string
	err = tx.QueryRowContext(ctx, `SELECT command_id
		FROM public.compute_cluster_agent_commands
		WHERE agent_id=$1 AND state='PENDING'
		ORDER BY created_at, command_id
		FOR UPDATE SKIP LOCKED LIMIT 1`, agentID,
	).Scan(&commandID)
	if errors.Is(err, sql.ErrNoRows) {
		if err := tx.Commit(); err != nil {
			return clusteragent.Command{}, false, err
		}
		return clusteragent.Command{}, false, nil
	}
	if err != nil {
		return clusteragent.Command{}, false, fmt.Errorf("select pending Agent command: %w", err)
	}

	_, err = tx.ExecContext(ctx, `UPDATE public.compute_cluster_agent_commands
		SET state='LEASED', attempts=attempts+1, command_fence=command_fence+1,
		    lease_owner_instance_id=$2, lease_epoch=$3,
		    lease_expires_at=pg_catalog.clock_timestamp()+$4::interval,
		    updated_at=pg_catalog.clock_timestamp()
		WHERE command_id=$1 AND state='PENDING'`,
		commandID, request.InstanceID, request.LeaseEpoch, ttl.String(),
	)
	if err != nil {
		return clusteragent.Command{}, false, fmt.Errorf("lease pending Agent command: %w", err)
	}
	command, err := scanClusterAgentCommand(tx.QueryRowContext(ctx, `SELECT `+
		clusterAgentCommandColumns+` FROM public.compute_cluster_agent_commands WHERE command_id=$1`, commandID))
	if err != nil {
		return clusteragent.Command{}, false, fmt.Errorf("read leased Agent command: %w", err)
	}
	if command.InstanceID != request.InstanceID || command.LeaseEpoch != request.LeaseEpoch {
		return clusteragent.Command{}, false, clusteragent.ErrInvalid
	}
	if err := tx.Commit(); err != nil {
		return clusteragent.Command{}, false, fmt.Errorf("commit Agent command lease: %w", err)
	}
	return command, true, nil
}

func (r *Repository) Complete(ctx context.Context, spiffeID string, completion clusteragent.Completion) error {
	if r == nil || r.db == nil || !validClusterAgentSPIFFE(spiffeID) || completion.Validate() != nil {
		return clusteragent.ErrInvalid
	}
	tx, err := r.db.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelReadCommitted})
	if err != nil {
		return fmt.Errorf("begin Agent command completion: %w", err)
	}
	defer func() { _ = tx.Rollback() }()

	var agentID, currentInstance, agentState string
	var currentEpoch int64
	err = tx.QueryRowContext(ctx, `SELECT agent_id, state, instance_id, lease_epoch
		FROM public.compute_cluster_agents WHERE spiffe_id=$1 FOR SHARE`, spiffeID,
	).Scan(&agentID, &agentState, &currentInstance, &currentEpoch)
	if errors.Is(err, sql.ErrNoRows) {
		return clusteragent.ErrUnauthorized
	}
	if err != nil {
		return fmt.Errorf("resolve Agent completion identity: %w", err)
	}
	if currentInstance != completion.InstanceID || currentEpoch != int64(completion.LeaseEpoch) ||
		agentState == "RETIRED" {
		return clusteragent.ErrStaleSession
	}

	var state, leaseOwner string
	var epoch, fence int64
	var leaseValid bool
	var existingOutcome, existingError sql.NullString
	var existingResult []byte
	err = tx.QueryRowContext(ctx, `SELECT state, lease_owner_instance_id,
		lease_epoch, command_fence, lease_expires_at > pg_catalog.clock_timestamp(),
		outcome, result, error_code
		FROM public.compute_cluster_agent_commands
		WHERE command_id=$1 AND agent_id=$2 FOR UPDATE`,
		completion.CommandID, agentID,
	).Scan(&state, &leaseOwner, &epoch, &fence, &leaseValid,
		&existingOutcome, &existingResult, &existingError)
	if errors.Is(err, sql.ErrNoRows) {
		return clusteragent.ErrCommandNotFound
	}
	if err != nil {
		return fmt.Errorf("read Agent command for completion: %w", err)
	}
	if state != "LEASED" {
		var sameResult any
		var submittedResult any
		_ = json.Unmarshal(existingResult, &sameResult)
		_ = json.Unmarshal(completion.Result.Payload, &submittedResult)
		if state == string(clusteragent.CommandSucceeded) || state == string(clusteragent.CommandFailed) ||
			state == string(clusteragent.CommandUnknown) {
			if existingOutcome.String == string(completion.Result.Outcome) &&
				existingError.String == completion.Result.ErrorCode &&
				reflect.DeepEqual(sameResult, submittedResult) {
				return tx.Commit()
			}
		}
		return clusteragent.ErrCommandFence
	}
	if leaseOwner != completion.InstanceID || epoch != int64(completion.LeaseEpoch) ||
		fence != int64(completion.CommandFence) || !leaseValid {
		return clusteragent.ErrCommandFence
	}

	nextState := ""
	var result any
	var errorCode any
	switch completion.Result.Outcome {
	case clusteragent.OutcomeSucceeded:
		nextState, result = "SUCCEEDED", []byte(completion.Result.Payload)
	case clusteragent.OutcomeFailed:
		nextState, errorCode = "FAILED", completion.Result.ErrorCode
	case clusteragent.OutcomeUnknown:
		nextState, errorCode = "UNKNOWN", completion.Result.ErrorCode
	default:
		return clusteragent.ErrInvalid
	}
	_, err = tx.ExecContext(ctx, `UPDATE public.compute_cluster_agent_commands
		SET state=$2, outcome=$3, result=$4::jsonb, error_code=$5,
		    lease_owner_instance_id=NULL, lease_epoch=NULL, lease_expires_at=NULL,
		    updated_at=pg_catalog.clock_timestamp()
		WHERE command_id=$1 AND state='LEASED' AND lease_owner_instance_id=$6
		  AND lease_epoch=$7 AND command_fence=$8
		  AND lease_expires_at > pg_catalog.clock_timestamp()`,
		completion.CommandID, nextState, completion.Result.Outcome,
		result, errorCode, completion.InstanceID, completion.LeaseEpoch, completion.CommandFence,
	)
	if err != nil {
		return fmt.Errorf("persist Agent command result: %w", err)
	}
	return tx.Commit()
}

func randomClusterAgentCommandID() (string, error) {
	var entropy [16]byte
	if _, err := rand.Read(entropy[:]); err != nil {
		return "", err
	}
	return "cluster-command-" + hex.EncodeToString(entropy[:]), nil
}

func (r *Repository) Execute(ctx context.Context, providerID string, action clusteragent.CommandAction, request clusteragent.ProviderRequest) (clusteragent.ExecutionResult, error) {
	if r == nil || r.db == nil || strings.TrimSpace(providerID) == "" ||
		(action != clusteragent.ActionApply && action != clusteragent.ActionInspect) ||
		request.Validate() != nil || request.ProviderID != providerID {
		return clusteragent.ExecutionResult{}, clusteragent.ErrInvalid
	}
	var agentID, clusterScope, agentState string
	var leaseValid bool
	err := r.db.QueryRowContext(ctx, `SELECT a.agent_id, a.cluster_scope, a.state,
		a.lease_expires_at > pg_catalog.clock_timestamp()
		FROM public.compute_cluster_agent_provider_bindings b
		JOIN public.compute_cluster_agents a
		  ON a.agent_id=b.agent_id AND a.cluster_scope=b.cluster_scope
		JOIN public.compute_providers p
		  ON p.provider_id=b.provider_id AND p.cluster_scope=b.cluster_scope
		WHERE b.provider_id=$1 AND p.state='REGISTERED'`, providerID,
	).Scan(&agentID, &clusterScope, &agentState, &leaseValid)
	if errors.Is(err, sql.ErrNoRows) {
		return clusteragent.ExecutionResult{}, clusteragent.ErrProviderBindingMissing
	}
	if err != nil {
		return clusteragent.ExecutionResult{}, fmt.Errorf("resolve Provider Cluster Agent: %w", err)
	}
	if agentState != "CONNECTED" || !leaseValid {
		return clusteragent.ExecutionResult{}, clusteragent.ErrAgentUnavailable
	}
	requestJSON, err := json.Marshal(request)
	if err != nil {
		return clusteragent.ExecutionResult{}, err
	}
	commandID, err := randomClusterAgentCommandID()
	if err != nil {
		return clusteragent.ExecutionResult{}, err
	}
	_, err = r.db.ExecContext(ctx, `INSERT INTO public.compute_cluster_agent_commands (
		command_id, agent_id, cluster_scope, provider_id, operation_id, tenant_id,
		workload_id, desired_generation, command_action, request_digest, request
	) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11::jsonb)
	ON CONFLICT (agent_id, operation_id, command_action) DO NOTHING`,
		commandID, agentID, clusterScope, providerID, request.OperationID,
		request.TenantID, request.WorkloadID, request.DesiredGeneration,
		string(action), request.RequestDigest, requestJSON,
	)
	if err != nil {
		return clusteragent.ExecutionResult{}, fmt.Errorf("enqueue remote Provider command: %w", err)
	}

	var storedDigest string
	var storedBody []byte
	var storedState string
	err = r.db.QueryRowContext(ctx, `SELECT request_digest, request, state
		FROM public.compute_cluster_agent_commands
		WHERE agent_id=$1 AND operation_id=$2 AND command_action=$3`,
		agentID, request.OperationID, string(action),
	).Scan(&storedDigest, &storedBody, &storedState)
	if err != nil {
		return clusteragent.ExecutionResult{}, fmt.Errorf("read remote Provider command: %w", err)
	}
	var storedRequest clusteragent.ProviderRequest
	if err := json.Unmarshal(storedBody, &storedRequest); err != nil {
		return clusteragent.ExecutionResult{}, clusteragent.ErrCommandConflict
	}
	if storedDigest != request.RequestDigest || storedRequest != request {
		return clusteragent.ExecutionResult{}, clusteragent.ErrCommandConflict
	}
	if storedState == "UNKNOWN" {
		return clusteragent.ExecutionResult{Outcome: clusteragent.OutcomeUnknown, ErrorCode: "REMOTE_COMMAND_UNKNOWN"}, clusteragent.ErrCommandUnknown
	}

	ticker := time.NewTicker(clusterAgentWaitInterval)
	defer ticker.Stop()
	for {
		var state string
		var outcome, errorCode sql.NullString
		var result []byte
		err := r.db.QueryRowContext(ctx, `SELECT state, outcome, result, error_code
			FROM public.compute_cluster_agent_commands
			WHERE agent_id=$1 AND operation_id=$2 AND command_action=$3`,
			agentID, request.OperationID, string(action),
		).Scan(&state, &outcome, &result, &errorCode)
		if err != nil {
			return clusteragent.ExecutionResult{}, fmt.Errorf("wait for remote Provider command: %w", err)
		}
		switch state {
		case "SUCCEEDED":
			return clusteragent.ExecutionResult{Outcome: clusteragent.OutcomeSucceeded, Payload: json.RawMessage(result)}, nil
		case "FAILED":
			return clusteragent.ExecutionResult{Outcome: clusteragent.OutcomeFailed, ErrorCode: errorCode.String}, clusteragent.ErrCommandFailed
		case "UNKNOWN":
			return clusteragent.ExecutionResult{Outcome: clusteragent.OutcomeUnknown, ErrorCode: errorCode.String}, clusteragent.ErrCommandUnknown
		}
		select {
		case <-ctx.Done():
			return clusteragent.ExecutionResult{Outcome: clusteragent.OutcomeUnknown, ErrorCode: "REMOTE_COMMAND_WAIT_EXPIRED"}, clusteragent.ErrCommandUnknown
		case <-ticker.C:
		}
	}
}

var _ clusteragent.AgentService = (*Repository)(nil)
var _ clusteragent.CommandGateway = (*Repository)(nil)
