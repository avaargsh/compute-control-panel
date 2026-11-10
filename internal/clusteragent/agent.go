package clusteragent

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"time"
)

type AgentConfig struct {
	AgentVersion   string
	Capabilities   []byte
	PollInterval   time.Duration
	RequestTimeout time.Duration
}

type Agent struct {
	client   APIClient
	executor Executor
	journal  LocalJournal
	config   AgentConfig
	heartbeat HeartbeatRequest
}

func NewAgent(client APIClient, executor Executor, journal LocalJournal, config AgentConfig) (*Agent, error) {
	if client == nil || executor == nil || journal == nil ||
		config.AgentVersion == "" || config.PollInterval < 100*time.Millisecond ||
		config.PollInterval > time.Minute || config.RequestTimeout < time.Second ||
		config.RequestTimeout > 2*time.Minute || len(config.Capabilities) == 0 {
		return nil, ErrInvalidConfiguration
	}
	var entropy [16]byte
	if _, err := rand.Read(entropy[:]); err != nil {
		return nil, fmt.Errorf("generate Cluster Agent instance ID: %w", err)
	}
	heartbeat := HeartbeatRequest{
		InstanceID: "instance-" + hex.EncodeToString(entropy[:]),
		AgentVersion: config.AgentVersion,
		Capabilities: append([]byte(nil), config.Capabilities...),
	}
	if err := heartbeat.Validate(); err != nil {
		return nil, ErrInvalidConfiguration
	}
	return &Agent{client: client, executor: executor, journal: journal, config: config, heartbeat: heartbeat}, nil
}

func (a *Agent) callContext(ctx context.Context) (context.Context, context.CancelFunc) {
	return context.WithTimeout(ctx, a.config.RequestTimeout)
}

func waitContext(ctx context.Context, delay time.Duration) error {
	timer := time.NewTimer(delay)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}

// Run is a single-writer pull loop. All network traffic is initiated by the
// Agent. A transient failure backs off and reconnects; the server-issued
// lease_epoch is refreshed before the Agent may claim more commands.
func (a *Agent) Run(ctx context.Context) error {
	if a == nil || a.client == nil || a.executor == nil || a.journal == nil {
		return ErrInvalidConfiguration
	}
	var session Session
	nextHeartbeat := time.Time{}
	for {
		if err := ctx.Err(); err != nil {
			return nil
		}
		if session.LeaseEpoch == 0 || !time.Now().Before(nextHeartbeat) {
			requestCtx, cancel := a.callContext(ctx)
			updated, err := a.client.Heartbeat(requestCtx, a.heartbeat)
			cancel()
			if err != nil {
				if errors.Is(err, ErrUnauthorized) || errors.Is(err, ErrInvalid) {
					return err
				}
				if waitContext(ctx, a.config.PollInterval) != nil {
					return nil
				}
				continue
			}
			session = updated
			nextHeartbeat = time.Now().Add(time.Duration(session.HeartbeatIntervalSec) * time.Second)
		}
		if !session.DispatchAllowed {
			if waitContext(ctx, a.config.PollInterval) != nil {
				return nil
			}
			continue
		}
		requestCtx, cancel := a.callContext(ctx)
		command, found, err := a.client.Claim(requestCtx, ClaimRequest{
			InstanceID: session.InstanceID, LeaseEpoch: session.LeaseEpoch,
		})
		cancel()
		if err != nil {
			if errors.Is(err, ErrStaleSession) {
				session = Session{}
				continue
			}
			if waitContext(ctx, a.config.PollInterval) != nil {
				return nil
			}
			continue
		}
		if !found {
			if waitContext(ctx, a.config.PollInterval) != nil {
				return nil
			}
			continue
		}
		result := a.journal.Execute(ctx, command, a.executor)
		completion := Completion{
			CommandID: command.CommandID, InstanceID: session.InstanceID,
			LeaseEpoch: session.LeaseEpoch, CommandFence: command.CommandFence,
			Result: result,
		}
		completeCtx, completeCancel := a.callContext(ctx)
		err = a.client.Complete(completeCtx, completion)
		completeCancel()
		if errors.Is(err, ErrStaleSession) || errors.Is(err, ErrCommandFence) {
			session = Session{}
			continue
		}
		if err != nil {
			// Re-submit the same completion on the next loop only when it is
			// still within the command lease. The server treats exact duplicate
			// completions as idempotent and rejects stale fences.
			if waitContext(ctx, a.config.PollInterval) != nil {
				return nil
			}
			requestCtx, requestCancel := a.callContext(ctx)
			err = a.client.Complete(requestCtx, completion)
			requestCancel()
			if errors.Is(err, ErrStaleSession) || errors.Is(err, ErrCommandFence) {
				session = Session{}
				continue
			}
		}
	}
}

var _ = fmt.Sprintf
