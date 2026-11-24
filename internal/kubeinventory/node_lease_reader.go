package kubeinventory

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"os"
	"reflect"
	"strings"
	"time"

	"github.com/avaargsh/compute-control-panel/internal/observation"
)

// validNodeLeaseName accepts only canonical Kubernetes DNS-subdomain names.
// A user-supplied Node name can never select an arbitrary endpoint.
func validNodeLeaseName(name string) bool {
	if len(name) == 0 || len(name) > 253 {
		return false
	}
	for _, label := range strings.Split(name, ".") {
		if len(label) == 0 || len(label) > 63 {
			return false
		}
		for i := 0; i < len(label); i++ {
			c := label[i]
			letter := c >= 'a' && c <= 'z'
			digit := c >= '0' && c <= '9'
			if (i == 0 || i == len(label)-1) && !letter && !digit {
				return false
			}
			if !letter && !digit && c != '-' {
				return false
			}
		}
	}
	return true
}

// readBoundKubernetesObject reuses the Reader's trusted TLS transport and
// reloads its ServiceAccount token per request. Never expose raw API errors,
// token bytes or server body, follow redirects, or make a Kubernetes write.
func (r *Reader) readBoundKubernetesObject(ctx context.Context, path string, target any) error {
	tokenBytes, err := os.ReadFile(r.tokenFile)
	if err != nil || len(tokenBytes) > maxTokenSize {
		return ErrCredentials
	}
	token := strings.TrimSpace(string(tokenBytes))
	if token == "" || strings.ContainsAny(token, "\r\n\t ") {
		return ErrCredentials
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, r.endpoint+path, nil)
	if err != nil {
		return ErrInvalidResponse
	}
	req.Header.Set("Authorization", "Bearer "+token)
	req.Header.Set("Accept", "application/json")
	resp, err := r.client.Do(req)
	if err != nil {
		return ErrUnavailable
	}
	defer resp.Body.Close()
	switch resp.StatusCode {
	case http.StatusOK:
	case http.StatusUnauthorized:
		return ErrUnauthorized
	case http.StatusForbidden:
		return ErrForbidden
	default:
		return ErrUnavailable
	}
	body, err := io.ReadAll(io.LimitReader(resp.Body, maxResponseSize+1))
	if err != nil || len(body) > maxResponseSize || json.Unmarshal(body, target) != nil {
		return ErrInvalidResponse
	}
	return nil
}

// ObserveNodeLease performs read-only, bounded Node / Lease / Node / Lease GET
// observations, rejecting changed source identities before returning a FRESH
// heartbeat assessment. These reads are not atomic or hardware attestation.
// A positive result never authorizes GPU placement, release or cost posting.
func (r *Reader) ObserveNodeLease(
	ctx context.Context, tenantID string, projects []string,
	nodeName, expectedNodeUID string, maxAge time.Duration,
) (nodeLeaseAssessment, error) {
	unknown := func(reason string, err error) (nodeLeaseAssessment, error) {
		return nodeLeaseAssessment{State: nodeLeaseUnknown, ReasonCode: reason}, err
	}
	if r == nil || tenantID != r.tenantID {
		return unknown("LEASE_SCOPE_DENIED", ErrScope)
	}
	authorized := false
	for _, project := range projects {
		if project == r.projectID {
			authorized = true
			break
		}
	}
	if !authorized {
		return unknown("LEASE_SCOPE_DENIED", ErrScope)
	}
	if !validNodeLeaseName(nodeName) ||
		expectedNodeUID == "" || expectedNodeUID != strings.TrimSpace(expectedNodeUID) ||
		len(expectedNodeUID) > 256 || strings.ContainsAny(expectedNodeUID, "/\r\n\t ") ||
		maxAge <= 0 || maxAge > 10*time.Minute {
		return unknown("LEASE_QUERY_INVALID", ErrInvalidResponse)
	}
	started := r.now().UTC()
	if started.IsZero() {
		return unknown("LEASE_CLOCK_INVALID", ErrInvalidResponse)
	}
	nodePath := "/api/v1/nodes/" + nodeName
	leasePath := "/apis/coordination.k8s.io/v1/namespaces/kube-node-lease/leases/" + nodeName
	var firstNode, finalNode node
	var firstLease, finalLease nodeLease
	if err := r.readBoundKubernetesObject(ctx, nodePath, &firstNode); err != nil {
		return unknown("LEASE_NODE_READ_FAILED", err)
	}
	if err := r.readBoundKubernetesObject(ctx, leasePath, &firstLease); err != nil {
		return unknown("LEASE_SOURCE_READ_FAILED", err)
	}
	if err := r.readBoundKubernetesObject(ctx, nodePath, &finalNode); err != nil {
		return unknown("LEASE_NODE_READ_FAILED", err)
	}
	if err := r.readBoundKubernetesObject(ctx, leasePath, &finalLease); err != nil {
		return unknown("LEASE_SOURCE_READ_FAILED", err)
	}
	completed := r.now().UTC()
	if completed.IsZero() || completed.Before(started) {
		return unknown("LEASE_CLOCK_INVALID", ErrInvalidResponse)
	}
	if firstNode.APIVersion != "v1" || firstNode.Kind != "Node" ||
		firstNode.Metadata.Name != nodeName || firstNode.Metadata.Namespace != "" ||
		firstNode.Metadata.UID != expectedNodeUID || firstNode.Metadata.ResourceVersion == "" ||
		!reflect.DeepEqual(firstNode, finalNode) ||
		!reflect.DeepEqual(firstLease, finalLease) {
		return unknown("LEASE_SOURCE_CHANGED_OR_INVALID", nil)
	}
	if condition, _ := nodeCondition(firstNode); condition != observation.ConditionAvailable {
		return unknown("LEASE_NODE_NOT_AVAILABLE", nil)
	}
	return assessNodeLease(nodeName, expectedNodeUID, firstLease, completed, maxAge), nil
}
