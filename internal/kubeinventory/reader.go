// Package kubeinventory reads Kubernetes core/v1 Node status with a
// TLS-verified, read-only service-account identity. It does not allocate GPUs.
package kubeinventory

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/url"
	"os"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/avaargsh/compute-control-panel/internal/observation"
)

var (
	ErrScope           = errors.New("cluster inventory scope is not authorized")
	ErrCredentials     = errors.New("Kubernetes client credentials are unavailable")
	ErrUnauthorized    = errors.New("Kubernetes API authentication failed")
	ErrForbidden       = errors.New("Kubernetes API node list permission denied")
	ErrUnavailable     = errors.New("Kubernetes API inventory unavailable")
	ErrInvalidResponse = errors.New("Kubernetes API returned invalid node inventory")
)

const (
	listPageSize    = 100
	maxPages        = 10
	maxNodes        = 400
	maxResponseSize = 2 << 20
	maxTokenSize    = 8192
)

var gpuResourceName = regexp.MustCompile(`^[A-Za-z0-9.-]+/[A-Za-z0-9._-]+$`)

// Config comes from the trusted cluster administrator, never from browser data.
// Only explicitly selected Node.status resource names are reported.
type Config struct {
	APIServer      string
	CAFile         string
	TokenFile      string
	ClusterID      string
	TenantID       string
	ProjectID      string
	ResourceNames  []string
	Timeout        time.Duration
	ObservationTTL time.Duration
	Now            func() time.Time
}

type Reader struct {
	endpoint  string
	tokenFile string
	clusterID string
	tenantID  string
	projectID string
	names     []string
	client    *http.Client
	ttl       time.Duration
	now       func() time.Time
}

func NewReader(cfg Config) (*Reader, error) {
	u, err := url.Parse(cfg.APIServer)
	if err != nil || u == nil || u.Scheme != "https" || u.Host == "" ||
		u.User != nil || u.RawQuery != "" || u.Fragment != "" || u.Opaque != "" ||
		(u.Path != "" && u.Path != "/") {
		return nil, errors.New("Kubernetes API server must be an exact HTTPS origin")
	}
	if strings.TrimSpace(cfg.ClusterID) == "" || strings.TrimSpace(cfg.TenantID) == "" ||
		strings.TrimSpace(cfg.ProjectID) == "" || cfg.CAFile == "" || cfg.TokenFile == "" ||
		cfg.ObservationTTL <= 0 || cfg.ObservationTTL > 10*time.Minute ||
		len(cfg.ResourceNames) == 0 || len(cfg.ResourceNames) > 16 {
		return nil, errors.New("Kubernetes reader requires cluster scope, credentials, TTL and GPU resource names")
	}
	names := make([]string, len(cfg.ResourceNames))
	used := make(map[string]bool, len(cfg.ResourceNames))
	for i, name := range cfg.ResourceNames {
		prefix, suffix, hasSlash := strings.Cut(name, "/")
		if !hasSlash || !gpuResourceName.MatchString(name) ||
			!strings.Contains(prefix, ".") || strings.HasPrefix(prefix, ".") ||
			strings.HasSuffix(prefix, ".") || strings.Contains(prefix, "..") ||
			strings.HasPrefix(suffix, ".") || used[name] {
			return nil, errors.New("invalid or duplicate GPU extended resource name")
		}
		used[name] = true
		names[i] = name
	}
	caPEM, err := os.ReadFile(cfg.CAFile)
	if err != nil {
		return nil, ErrCredentials
	}
	roots := x509.NewCertPool()
	if !roots.AppendCertsFromPEM(caPEM) {
		return nil, ErrCredentials
	}
	stat, err := os.Stat(cfg.TokenFile)
	if err != nil || stat.IsDir() {
		return nil, ErrCredentials
	}
	if cfg.Timeout <= 0 {
		cfg.Timeout = 5 * time.Second
	}
	if cfg.Timeout > 30*time.Second {
		return nil, errors.New("Kubernetes API timeout must be at most 30s")
	}
	if cfg.Now == nil {
		cfg.Now = time.Now
	}
	transport := &http.Transport{TLSClientConfig: &tls.Config{RootCAs: roots, MinVersion: tls.VersionTLS12}}
	return &Reader{
		endpoint:  strings.TrimSuffix(cfg.APIServer, "/"),
		tokenFile: cfg.TokenFile, clusterID: cfg.ClusterID,
		tenantID: cfg.TenantID, projectID: cfg.ProjectID,
		names: names, ttl: cfg.ObservationTTL, now: cfg.Now,
		client: &http.Client{
			Transport: transport, Timeout: cfg.Timeout,
			CheckRedirect: func(_ *http.Request, _ []*http.Request) error { return http.ErrUseLastResponse },
		},
	}, nil
}

// ListObservations enforces an administrator-bound tenant/project scope before
// calling Kubernetes. It uses no list/watch permission beyond core/v1 nodes.
func (r *Reader) ListObservations(ctx context.Context, tenantID string, projects []string) (observation.Snapshot, error) {
	if r == nil || tenantID != r.tenantID {
		return observation.Snapshot{}, ErrScope
	}
	allowed := false
	for _, project := range projects {
		if project == r.projectID {
			allowed = true
			break
		}
	}
	snapshot := observation.Snapshot{DataMode: observation.ModeLive, Items: []observation.Record{}}
	if !allowed {
		return snapshot, nil
	}
	// Timestamp the complete paginated collection from its first read.
	// Finishing later must not renew the lifetime of earlier Node facts.
	startedAt := r.now().UTC()
	if startedAt.IsZero() {
		return observation.Snapshot{}, ErrInvalidResponse
	}
	nodes, err := r.listNodes(ctx)
	if err != nil {
		return observation.Snapshot{}, err
	}
	expiresAt := startedAt.Add(r.ttl)
	completedAt := r.now().UTC()
	if completedAt.Before(startedAt) || !completedAt.Before(expiresAt) {
		return observation.Snapshot{}, ErrInvalidResponse
	}
	for _, n := range nodes {
		condition, reason := nodeCondition(n)
		base := observation.Record{
			TenantID: r.tenantID, ProjectID: r.projectID,
			Source:         "kubernetes/core/v1/node.status",
			SourceRevision: n.Metadata.ResourceVersion,
			ObservedAt:     startedAt, ExpiresAt: expiresAt,
		}
		nr := base
		nr.Resource = observation.ResourceRef{
			ClusterID: r.clusterID, Kind: "Node", Name: n.Metadata.Name,
			UID: n.Metadata.UID, ResourceVersion: n.Metadata.ResourceVersion,
		}
		nr.Condition, nr.ReasonCode = condition, reason
		snapshot.Items = append(snapshot.Items, nr)
		for _, name := range r.names {
			capacity, capOK := n.Status.Capacity[name]
			allocatable, allocOK := n.Status.Allocatable[name]
			if !capOK && !allocOK {
				continue
			}
			if !capOK || !allocOK || !nonnegativeInteger(capacity) || !nonnegativeInteger(allocatable) ||
				exceedsCapacity(capacity, allocatable) {
				return observation.Snapshot{}, ErrInvalidResponse
			}
			gpu := base
			gpu.Resource = observation.ResourceRef{
				ClusterID: r.clusterID, Kind: "NodeGPUResource",
				Name:            n.Metadata.Name + "/" + name,
				ResourceVersion: n.Metadata.ResourceVersion,
				// Node UID is deliberately not mislabeled as physical GPU UID.
			}
			gpu.Quantity = &observation.Quantity{ResourceName: name, Capacity: capacity, Allocatable: allocatable}
			gpu.Condition = observation.ConditionUnknown
			gpu.ReasonCode = "NODE_ALLOCATABLE_NOT_FREE_CAPACITY"
			gpu.Message = "Node reports total resource quantities; free GPUs and physical GPU identity are unverified"
			snapshot.Items = append(snapshot.Items, gpu)
		}
	}
	return snapshot, nil
}

func nonnegativeInteger(s string) bool {
	if s == "" || strings.TrimSpace(s) != s || strings.HasPrefix(s, "-") {
		return false
	}
	_, err := strconv.ParseUint(s, 10, 64)
	return err == nil
}

func exceedsCapacity(capacity, allocatable string) bool {
	total, _ := strconv.ParseUint(capacity, 10, 64)
	availableToScheduler, _ := strconv.ParseUint(allocatable, 10, 64)
	return availableToScheduler > total
}

type nodeList struct {
	Metadata struct {
		Continue        string `json:"continue"`
		ResourceVersion string `json:"resourceVersion"`
	} `json:"metadata"`
	Items []node `json:"items"`
}

type node struct {
	Metadata struct {
		Name              string  `json:"name"`
		UID               string  `json:"uid"`
		ResourceVersion   string  `json:"resourceVersion"`
		DeletionTimestamp *string `json:"deletionTimestamp"`
	} `json:"metadata"`
	Spec struct {
		Unschedulable bool `json:"unschedulable"`
	} `json:"spec"`
	Status struct {
		Capacity    map[string]string `json:"capacity"`
		Allocatable map[string]string `json:"allocatable"`
		Conditions  []struct {
			Type   string `json:"type"`
			Status string `json:"status"`
		} `json:"conditions"`
	} `json:"status"`
}

func nodeCondition(n node) (observation.Condition, string) {
	// A terminating Node cannot be available, even while its last reported
	// Ready condition remains True. Raw GPU quantities remain non-authoritative.
	if n.Metadata.DeletionTimestamp != nil {
		return observation.ConditionUnavailable, "NODE_TERMINATING"
	}
	ready := ""
	for _, item := range n.Status.Conditions {
		if item.Type != "Ready" {
			continue
		}
		if ready != "" && ready != item.Status {
			return observation.ConditionUnknown, "READY_CONDITION_CONFLICT"
		}
		ready = item.Status
	}
	if n.Spec.Unschedulable {
		return observation.ConditionDegraded, "NODE_UNSCHEDULABLE"
	}
	switch ready {
	case "True":
		return observation.ConditionAvailable, ""
	case "False":
		return observation.ConditionUnavailable, "NODE_NOT_READY"
	case "Unknown":
		return observation.ConditionUnknown, "NODE_READY_UNKNOWN"
	default:
		return observation.ConditionUnknown, "NODE_READY_MISSING"
	}
}

func (r *Reader) listNodes(ctx context.Context) ([]node, error) {
	var nodes []node
	seen := make(map[string]bool)
	seenUIDs := make(map[string]bool)
	seenTokens := make(map[string]bool)
	continuation := ""
	resourceVersion := ""
	for page := 0; page < maxPages; page++ {
		result, err := r.getNodePage(ctx, continuation)
		if err != nil {
			return nil, err
		}
		if result.Metadata.ResourceVersion == "" {
			return nil, ErrInvalidResponse
		}
		if resourceVersion == "" {
			resourceVersion = result.Metadata.ResourceVersion
		} else if resourceVersion != result.Metadata.ResourceVersion {
			return nil, ErrInvalidResponse
		}
		for _, n := range result.Items {
			if n.Metadata.Name == "" || n.Metadata.UID == "" || n.Metadata.ResourceVersion == "" ||
				seen[n.Metadata.Name] || seenUIDs[n.Metadata.UID] || len(nodes) == maxNodes {
				return nil, ErrInvalidResponse
			}
			seen[n.Metadata.Name] = true
			seenUIDs[n.Metadata.UID] = true
			nodes = append(nodes, n)
		}
		continuation = result.Metadata.Continue
		if continuation == "" {
			return nodes, nil
		}
		if seenTokens[continuation] {
			return nil, ErrInvalidResponse
		}
		seenTokens[continuation] = true
	}
	return nil, ErrInvalidResponse
}

func (r *Reader) getNodePage(ctx context.Context, continuation string) (nodeList, error) {
	var result nodeList
	params := url.Values{}
	params.Set("limit", strconv.Itoa(listPageSize))
	if continuation != "" {
		params.Set("continue", continuation)
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, r.endpoint+"/api/v1/nodes?"+params.Encode(), nil)
	if err != nil {
		return result, ErrInvalidResponse
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
		return result, ErrUnavailable
	}
	defer resp.Body.Close()
	switch resp.StatusCode {
	case http.StatusOK:
	case http.StatusUnauthorized:
		return result, ErrUnauthorized
	case http.StatusForbidden:
		return result, ErrForbidden
	default:
		return result, ErrUnavailable
	}
	body, err := io.ReadAll(io.LimitReader(resp.Body, maxResponseSize+1))
	if err != nil || len(body) > maxResponseSize || json.Unmarshal(body, &result) != nil {
		return nodeList{}, ErrInvalidResponse
	}
	return result, nil
}
