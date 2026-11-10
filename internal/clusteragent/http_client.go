package clusteragent

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"time"
)

const maxAgentResponseBody = 2 << 20

type HTTPClient struct {
	baseURL string
	client  *http.Client
}

func NewHTTPClient(baseURL string, client *http.Client) (*HTTPClient, error) {
	parsed, err := url.Parse(baseURL)
	if err != nil || parsed.Scheme != "https" || parsed.Host == "" ||
		parsed.User != nil || parsed.RawQuery != "" || parsed.Fragment != "" ||
		strings.TrimSpace(baseURL) != baseURL || client == nil {
		return nil, ErrInvalidConfiguration
	}
	copyClient := *client
	copyClient.CheckRedirect = func(*http.Request, []*http.Request) error {
		return http.ErrUseLastResponse
	}
	return &HTTPClient{
		baseURL: strings.TrimRight(parsed.String(), "/"),
		client:  &copyClient,
	}, nil
}

func (c *HTTPClient) post(ctx context.Context, path string, request any, response any, allowNoContent bool) error {
	if c == nil || c.client == nil || !strings.HasPrefix(path, "/api/v1/cluster-agent/") {
		return ErrInvalidConfiguration
	}
	body, err := json.Marshal(request)
	if err != nil {
		return fmt.Errorf("%w: encode request", ErrInvalid)
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, c.baseURL+path, bytes.NewReader(body))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Accept", "application/json")
	req.Header.Set("Cache-Control", "no-store")
	resp, err := c.client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if allowNoContent && resp.StatusCode == http.StatusNoContent {
		_, _ = io.Copy(io.Discard, io.LimitReader(resp.Body, maxAgentResponseBody))
		return nil
	}
	if resp.StatusCode != http.StatusOK {
		_, _ = io.Copy(io.Discard, io.LimitReader(resp.Body, 4096))
		return fmt.Errorf("Cluster Agent API returned HTTP %d", resp.StatusCode)
	}
	if response == nil {
		_, err := io.Copy(io.Discard, io.LimitReader(resp.Body, maxAgentResponseBody))
		return err
	}
	decoder := json.NewDecoder(io.LimitReader(resp.Body, maxAgentResponseBody+1))
	if err := decoder.Decode(response); err != nil {
		return fmt.Errorf("decode Cluster Agent API response: %w", err)
	}
	var trailing any
	if err := decoder.Decode(&trailing); !errors.Is(err, io.EOF) {
		return ErrInvalid
	}
	return nil
}

func (c *HTTPClient) Heartbeat(ctx context.Context, request HeartbeatRequest) (Session, error) {
	if err := request.Validate(); err != nil {
		return Session{}, err
	}
	var session Session
	err := c.post(ctx, "/api/v1/cluster-agent/heartbeat", request, &session, false)
	if err != nil {
		return Session{}, err
	}
	if session.AgentID == "" || session.ClusterScope == "" ||
		session.InstanceID != request.InstanceID || session.LeaseEpoch == 0 ||
		session.LeaseExpiresAt.IsZero() || session.HeartbeatIntervalSec < 1 {
		return Session{}, ErrInvalid
	}
	return session, nil
}

func (c *HTTPClient) Claim(ctx context.Context, request ClaimRequest) (Command, bool, error) {
	if err := request.Validate(); err != nil {
		return Command{}, false, err
	}
	var command Command
	err := c.post(ctx, "/api/v1/cluster-agent/commands/claim", request, &command, true)
	if err != nil {
		// The server uses 204 to indicate an empty queue. Distinguish it from
		// successful JSON responses without accepting a synthetic empty command.
		if strings.Contains(err.Error(), "HTTP 204") {
			return Command{}, false, nil
		}
		return Command{}, false, err
	}
	if command.CommandID == "" {
		return Command{}, false, nil
	}
	if command.InstanceID != request.InstanceID || command.LeaseEpoch != request.LeaseEpoch {
		return Command{}, false, ErrStaleSession
	}
	if err := command.Validate(); err != nil {
		return Command{}, false, err
	}
	return command, true, nil
}

func (c *HTTPClient) Complete(ctx context.Context, completion Completion) error {
	if err := completion.Validate(); err != nil {
		return err
	}
	path := "/api/v1/cluster-agent/commands/" + url.PathEscape(completion.CommandID) + "/complete"
	return c.post(ctx, path, completion, nil, true)
}

var _ APIClient = (*HTTPClient)(nil)

func boundedContext(parent context.Context, timeout time.Duration) (context.Context, context.CancelFunc) {
	if timeout <= 0 {
		timeout = 10 * time.Second
	}
	return context.WithTimeout(parent, timeout)
}
