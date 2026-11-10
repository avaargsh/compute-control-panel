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

	"crypto/x509"
)

const maxAgentRequestBody = 64 << 10

type Handler struct {
	trustDomain string
	service     AgentService
	leaseTTL    time.Duration
}

func NewHandler(trustDomain string, service AgentService, leaseTTL time.Duration) (http.Handler, error) {
	domain := strings.TrimSpace(strings.ToLower(trustDomain))
	if domain == "" || strings.ContainsAny(domain, "/:@?#") || strings.Contains(domain, "..") ||
		service == nil || leaseTTL < 5*time.Second || leaseTTL > 5*time.Minute {
		return nil, ErrInvalidConfiguration
	}
	return &Handler{trustDomain: domain, service: service, leaseTTL: leaseTTL}, nil
}

func (h *Handler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Cache-Control", "no-store")
	w.Header().Set("Pragma", "no-cache")
	if r.Method != http.MethodPost {
		w.Header().Set("Allow", http.MethodPost)
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	spiffeID, err := h.verifiedSPIFFEID(r)
	if err != nil {
		writeAgentError(w, http.StatusUnauthorized, "AGENT_UNAUTHENTICATED")
		return
	}

	switch {
	case r.URL.Path == "/api/v1/cluster-agent/heartbeat":
		var request HeartbeatRequest
		if !readAgentJSON(w, r, &request) || request.Validate() != nil {
			writeAgentError(w, http.StatusBadRequest, "INVALID_AGENT_HEARTBEAT")
			return
		}
		session, err := h.service.Heartbeat(r.Context(), spiffeID, request)
		if err != nil {
			writeAgentServiceError(w, err)
			return
		}
		writeAgentJSON(w, http.StatusOK, session)

	case r.URL.Path == "/api/v1/cluster-agent/commands/claim":
		var request ClaimRequest
		if !readAgentJSON(w, r, &request) || request.Validate() != nil {
			writeAgentError(w, http.StatusBadRequest, "INVALID_AGENT_CLAIM")
			return
		}
		command, found, err := h.service.Claim(r.Context(), spiffeID, request, h.leaseTTL)
		if err != nil {
			writeAgentServiceError(w, err)
			return
		}
		if !found {
			w.WriteHeader(http.StatusNoContent)
			return
		}
		if err := command.Validate(); err != nil {
			writeAgentError(w, http.StatusInternalServerError, "INVALID_PERSISTED_AGENT_COMMAND")
			return
		}
		writeAgentJSON(w, http.StatusOK, command)

	case strings.HasPrefix(r.URL.Path, "/api/v1/cluster-agent/commands/") &&
		strings.HasSuffix(r.URL.Path, "/complete"):
		commandID := strings.TrimSuffix(strings.TrimPrefix(r.URL.Path, "/api/v1/cluster-agent/commands/"), "/complete")
		if commandID == "" || strings.Contains(commandID, "/") || commandID != strings.TrimSpace(commandID) {
			writeAgentError(w, http.StatusNotFound, "AGENT_COMMAND_NOT_FOUND")
			return
		}
		var completion Completion
		if !readAgentJSON(w, r, &completion) || completion.Validate() != nil ||
			completion.CommandID != commandID {
			writeAgentError(w, http.StatusBadRequest, "INVALID_AGENT_COMPLETION")
			return
		}
		if err := h.service.Complete(r.Context(), spiffeID, completion); err != nil {
			writeAgentServiceError(w, err)
			return
		}
		w.WriteHeader(http.StatusNoContent)

	default:
		http.NotFound(w, r)
	}
}

func (h *Handler) verifiedSPIFFEID(r *http.Request) (string, error) {
	if r == nil || r.TLS == nil || len(r.TLS.PeerCertificates) == 0 ||
		len(r.TLS.VerifiedChains) == 0 || len(r.TLS.VerifiedChains[0]) == 0 {
		return "", ErrUnauthorized
	}
	leaf := r.TLS.PeerCertificates[0]
	verifiedLeaf := r.TLS.VerifiedChains[0][0]
	if leaf == nil || verifiedLeaf == nil || !sameAgentCertificate(leaf, verifiedLeaf) {
		return "", ErrUnauthorized
	}
	var found string
	for _, uri := range leaf.URIs {
		if uri == nil || uri.Scheme != "spiffe" {
			continue
		}
		if uri.Host != h.trustDomain || uri.User != nil || uri.RawQuery != "" ||
			uri.Fragment != "" || uri.Opaque != "" || uri.Path == "" ||
			!strings.HasPrefix(uri.Path, "/") {
			return "", ErrUnauthorized
		}
		for _, part := range strings.Split(strings.TrimPrefix(uri.Path, "/"), "/") {
			if part == "" || part == "." || part == ".." {
				return "", ErrUnauthorized
			}
		}
		if found != "" {
			return "", ErrUnauthorized
		}
		found = uri.String()
	}
	if found == "" {
		return "", ErrUnauthorized
	}
	return found, nil
}

func sameAgentCertificate(a, b *x509.Certificate) bool {
	return a != nil && b != nil && len(a.Raw) > 0 && bytes.Equal(a.Raw, b.Raw)
}

func readAgentJSON(w http.ResponseWriter, r *http.Request, destination any) bool {
	body, err := io.ReadAll(http.MaxBytesReader(w, r.Body, maxAgentRequestBody))
	if err != nil || len(body) == 0 || validateAgentJSON(body) != nil {
		return false
	}
	decoder := json.NewDecoder(bytes.NewReader(body))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(destination); err != nil {
		return false
	}
	var trailing any
	return errors.Is(decoder.Decode(&trailing), io.EOF)
}

// validateAgentJSON rejects duplicate object keys and explicit nulls before
// encoding/json can collapse them into Go zero values.
func validateAgentJSON(body []byte) error {
	decoder := json.NewDecoder(bytes.NewReader(body))
	if err := consumeAgentJSONValue(decoder); err != nil {
		return err
	}
	if _, err := decoder.Token(); !errors.Is(err, io.EOF) {
		return errors.New("trailing JSON value")
	}
	return nil
}

func consumeAgentJSONValue(decoder *json.Decoder) error {
	token, err := decoder.Token()
	if err != nil {
		return err
	}
	if token == nil {
		return errors.New("explicit null is not valid")
	}
	delim, ok := token.(json.Delim)
	if !ok {
		return nil
	}
	switch delim {
	case '{':
		seen := make(map[string]struct{})
		for decoder.More() {
			keyToken, err := decoder.Token()
			if err != nil {
				return err
			}
			key, ok := keyToken.(string)
			if !ok {
				return errors.New("invalid JSON object key")
			}
			if _, exists := seen[key]; exists {
				return errors.New("duplicate JSON object key")
			}
			seen[key] = struct{}{}
			if err := consumeAgentJSONValue(decoder); err != nil {
				return err
			}
		}
		end, err := decoder.Token()
		if err != nil || end != json.Delim('}') {
			return errors.New("unterminated JSON object")
		}
	case '[':
		for decoder.More() {
			if err := consumeAgentJSONValue(decoder); err != nil {
				return err
			}
		}
		end, err := decoder.Token()
		if err != nil || end != json.Delim(']') {
			return errors.New("unterminated JSON array")
		}
	default:
		return errors.New("unexpected JSON delimiter")
	}
	return nil
}

func writeAgentJSON(w http.ResponseWriter, status int, value any) {
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("X-Content-Type-Options", "nosniff")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(value)
}

func writeAgentError(w http.ResponseWriter, status int, code string) {
	writeAgentJSON(w, status, map[string]string{"code": code})
}

func writeAgentServiceError(w http.ResponseWriter, err error) {
	switch {
	case errors.Is(err, ErrUnauthorized):
		writeAgentError(w, http.StatusUnauthorized, "AGENT_UNAUTHENTICATED")
	case errors.Is(err, ErrAgentUnavailable):
		writeAgentError(w, http.StatusConflict, "AGENT_UNAVAILABLE")
	case errors.Is(err, ErrStaleSession), errors.Is(err, ErrCommandFence):
		writeAgentError(w, http.StatusConflict, "STALE_AGENT_FENCE")
	case errors.Is(err, ErrCommandNotFound):
		writeAgentError(w, http.StatusNotFound, "AGENT_COMMAND_NOT_FOUND")
	case errors.Is(err, ErrCommandConflict):
		writeAgentError(w, http.StatusConflict, "AGENT_COMMAND_CONFLICT")
	case errors.Is(err, ErrInvalid):
		writeAgentError(w, http.StatusBadRequest, "INVALID_AGENT_MESSAGE")
	default:
		writeAgentError(w, http.StatusInternalServerError, "AGENT_OPERATION_FAILED")
	}
}

var _ = fmt.Sprintf
