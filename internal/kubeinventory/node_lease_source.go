package kubeinventory

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"os"
	"reflect"
	"regexp"
	"strings"
	"time"

	"github.com/avaargsh/compute-control-panel/internal/observation"
)

var (
	ErrLeaseForbidden    = errors.New("Kubernetes Lease GET permission denied")
	ErrLeaseUnauthorized = errors.New("Kubernetes Lease GET authentication failed")
	ErrLeaseUnavailable  = errors.New("Kubernetes Lease API unavailable")
	ErrLeaseInvalid      = errors.New("Kubernetes Lease API returned invalid response")
	errLeaseMissing      = errors.New("Kubernetes Node Lease missing")
)

const maxLeaseResponseSize = 64 << 10

var nodeDNSLabel = regexp.MustCompile(`^[a-z0-9]([-a-z0-9]*[a-z0-9])?$`)

type nodeLeaseSourceObservation struct {
	NodeUID             string
	NodeResourceVersion string
	LeaseUID            string
	LeaseResourceVersion string
	ObservedAt          time.Time
	Assessment          nodeLeaseAssessment
}

// validNodeDNSName enforces the core Node DNS subdomain contract before a
// supplied name is used in a URL path. In particular, neither a slash nor a
// path traversal can target another Kubernetes API resource.
func validNodeDNSName(name string) bool {
	if name == "" || len(name) > 253 {
		return false
	}
	for _, label := range strings.Split(name, ".") {
		if len(label) == 0 || len(label) > 63 || !nodeDNSLabel.MatchString(label) {
			return false
		}
	}
	return true
}

// observeNodeLease reads only authenticated Kubernetes API facts. It bounds
// and brackets an exact Lease GET with two complete core/v1 Node lists, then
// requires the Node identity and status to remain identical and available.
// The result remains an unmounted observation, not a host-attestation,
// scheduler, GPU-allocation, or resource-reclamation authority.
func (r *Reader) observeNodeLease(
	ctx context.Context, tenantID string, projects []string,
	nodeName, expectedNodeUID string, maxAge time.Duration,
) (nodeLeaseSourceObservation, error) {
	unknown := func(reason string) nodeLeaseSourceObservation {
		return nodeLeaseSourceObservation{
			Assessment: nodeLeaseAssessment{State: nodeLeaseUnknown, ReasonCode: reason},
		}
	}
	if r == nil || tenantID != r.tenantID {
		return unknown("LEASE_SCOPE_DENIED"), ErrScope
	}
	allowed := false
	for _, p := range projects {
		if p == r.projectID {
			allowed = true
			break
		}
	}
	if !allowed {
		return unknown("LEASE_SCOPE_DENIED"), ErrScope
	}
	if !validNodeDNSName(nodeName) || strings.TrimSpace(expectedNodeUID) == "" || maxAge <= 0 {
		return unknown("LEASE_QUERY_INVALID"), ErrInvalidResponse
	}
	beforeList, err := r.listNodes(ctx)
	if err != nil {
		return unknown("NODE_SOURCE_UNAVAILABLE"), err
	}
	before, found := findLiveNode(beforeList, nodeName, expectedNodeUID)
	if !found {
		return unknown("NODE_NOT_READY"), nil
	}
	lease, err := r.getNodeLease(ctx, nodeName)
	if err != nil {
		if errors.Is(err, errLeaseMissing) {
			return unknown("LEASE_MISSING"), nil
		}
		return unknown("LEASE_SOURCE_UNAVAILABLE"), err
	}
	afterList, err := r.listNodes(ctx)
	if err != nil {
		return unknown("NODE_SOURCE_UNAVAILABLE"), err
	}
	after, found := findLiveNode(afterList, nodeName, expectedNodeUID)
	if !found || !reflect.DeepEqual(before, after) {
		return unknown("NODE_CHANGED_DURING_LEASE_READ"), nil
	}
	now := r.now().UTC()
	assessment := assessNodeLease(nodeName, expectedNodeUID, lease, now, maxAge)
	return nodeLeaseSourceObservation{
		NodeUID: expectedNodeUID, NodeResourceVersion: before.Metadata.ResourceVersion,
		LeaseUID: lease.Metadata.UID, LeaseResourceVersion: lease.Metadata.ResourceVersion,
		ObservedAt: now, Assessment: assessment,
	}, nil
}

func findLiveNode(nodes []node, name, uid string) (node, bool) {
	for _, candidate := range nodes {
		if candidate.Metadata.Name != name || candidate.Metadata.UID != uid {
			continue
		}
		condition, _ := nodeCondition(candidate)
		if condition != observation.ConditionAvailable {
			return node{}, false
		}
		return candidate, true
	}
	return node{}, false
}

func (r *Reader) getNodeLease(ctx context.Context, nodeName string) (nodeLease, error) {
	var result nodeLease
	// nodeName has already passed validNodeDNSName; it cannot contain '/', '?',
	// '#', escaped separators or traversal tokens.
	path := "/apis/coordination.k8s.io/v1/namespaces/kube-node-lease/leases/" + nodeName
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, r.endpoint+path, nil)
	if err != nil {
		return result, ErrLeaseInvalid
	}
	tokenBytes, err := os.ReadFile(r.tokenFile)
	if err != nil || len(tokenBytes) > maxTokenSize {
		return result, ErrCredentials
	}
	token := strings.TrimSpace(string(tokenBytes))
	if token == "" || strings.ContainsAny(token, "\r\n\t ") {
		return result, ErrCredentials
	}
	req.Header.Set("Authorization", "Bearer "+token)
	req.Header.Set("Accept", "application/json")
	resp, err := r.client.Do(req)
	if err != nil {
		return result, ErrLeaseUnavailable
	}
	defer resp.Body.Close()
	switch resp.StatusCode {
	case http.StatusOK:
	case http.StatusNotFound:
		return result, errLeaseMissing
	case http.StatusUnauthorized:
		return result, ErrLeaseUnauthorized
	case http.StatusForbidden:
		return result, ErrLeaseForbidden
	default:
		return result, ErrLeaseUnavailable
	}
	body, err := io.ReadAll(io.LimitReader(resp.Body, maxLeaseResponseSize+1))
	if err != nil || len(body) > maxLeaseResponseSize || json.Unmarshal(body, &result) != nil {
		return nodeLease{}, ErrLeaseInvalid
	}
	return result, nil
}
