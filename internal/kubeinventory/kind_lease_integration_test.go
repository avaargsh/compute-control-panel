package kubeinventory

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"
)

// TestKindLeaseBeforeGrantIsForbidden uses the actual short-lived
// ServiceAccount Token, not a kubectl impersonated SubjectAccessReview.
// It is run before creating the disposable Lease RoleBinding.
func TestKindLeaseBeforeGrantIsForbidden(t *testing.T) {
	if os.Getenv("CCP_KIND_INVENTORY_SMOKE") != "1" || os.Getenv("CCP_KIND_LEASE_PREGRANT") != "1" {
		t.Skip("requires the disposable kind pre-grant RBAC phase")
	}
	required := func(name string) string {
		t.Helper()
		value := os.Getenv(name)
		if value == "" {
			t.Fatalf("missing kind pre-grant environment %s", name)
		}
		return value
	}
	reader, err := NewReader(Config{
		APIServer: required("CCP_KIND_API_SERVER"),
		CAFile: required("CCP_KIND_CA_FILE"),
		TokenFile: required("CCP_KIND_ALLOWED_TOKEN_FILE"),
		ClusterID: "kind-inventory", TenantID: "tenant-kind", ProjectID: "project-kind",
		ResourceNames: []string{"nvidia.com/gpu"},
		ObservationTTL: time.Minute, Timeout: 10 * time.Second,
	})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	if _, err := reader.getNodeLease(ctx, required("CCP_KIND_LEASE_NODE_NAME")); !errors.Is(err, ErrLeaseForbidden) {
		t.Fatalf("authorized inventory SA unexpectedly has pre-grant Lease GET (expected 403), err=%v", err)
	}
}

// TestKindLeaseGetLeastPrivilege is an opt-in, real Kubernetes API and RBAC
// acceptance test. The disposable kind script provides short-lived credentials
// and the exact Node Lease name. It does not prove physical GPU health.
func TestKindLeaseGetLeastPrivilege(t *testing.T) {
	if os.Getenv("CCP_KIND_INVENTORY_SMOKE") != "1" {
		t.Skip("requires a disposable kind cluster and real Lease RBAC")
	}
	required := func(key string) string {
		t.Helper()
		value := os.Getenv(key)
		if value == "" {
			t.Fatalf("missing kind Lease test environment %s", key)
		}
		return value
	}
	nodeName := required("CCP_KIND_LEASE_NODE_NAME")
	allowToken, err := os.ReadFile(required("CCP_KIND_ALLOWED_TOKEN_FILE"))
	if err != nil || len(allowToken) == 0 {
		t.Fatal("authorized token unavailable")
	}
	denyToken, err := os.ReadFile(required("CCP_KIND_DENIED_TOKEN_FILE"))
	if err != nil || len(denyToken) == 0 {
		t.Fatal("denied token unavailable")
	}
	rotatingPath := filepath.Join(t.TempDir(), "lease-token")
	writeToken := func(data []byte) {
		t.Helper()
		if err := os.WriteFile(rotatingPath, data, 0600); err != nil {
			t.Fatal(err)
		}
	}
	writeToken(allowToken)
	reader, err := NewReader(Config{
		APIServer: required("CCP_KIND_API_SERVER"),
		CAFile: required("CCP_KIND_CA_FILE"), TokenFile: rotatingPath,
		ClusterID: "kind-inventory", TenantID: "tenant-kind", ProjectID: "project-kind",
		ResourceNames: []string{"nvidia.com/gpu"}, ObservationTTL: time.Minute,
		Timeout: 10 * time.Second,
	})
	if err != nil {
		t.Fatal(err)
	}
	checkRealLease := func() {
		t.Helper()
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		lease, err := reader.getNodeLease(ctx, nodeName)
		if err != nil {
			t.Fatalf("authorized exact Lease GET failed: %v", err)
		}
		if lease.APIVersion != "coordination.k8s.io/v1" || lease.Kind != "Lease" ||
			lease.Metadata.Name != nodeName || lease.Metadata.Namespace != "kube-node-lease" ||
			lease.Metadata.UID == "" || lease.Metadata.ResourceVersion == "" ||
			lease.Metadata.DeletionTimestamp != nil || lease.Spec.RenewTime == nil {
			t.Fatalf("real Lease missing source identity or renewal time: kind=%s name=%s uid-present=%t rv-present=%t",
				lease.Kind, lease.Metadata.Name, lease.Metadata.UID != "", lease.Metadata.ResourceVersion != "")
		}
		nodes, err := reader.listNodes(ctx)
		if err != nil {
			t.Fatalf("authorized Node list failed: %v", err)
		}
		var nodeUID string
		for _, node := range nodes {
			if node.Metadata.Name == nodeName {
				nodeUID = node.Metadata.UID
			}
		}
		if nodeUID == "" {
			t.Fatal("real Lease has no corresponding Node UID")
		}
		// A Lease is not itself proof of a physical host identity; if the
		// owner reference is absent or inconsistent, assessor must not
		// promote it to FRESH. Never fake an OwnerReference in kind.
		assessment := assessNodeLease(nodeName, nodeUID, lease, time.Now().UTC(), time.Minute)
		if assessment.State == nodeLeaseFresh && (len(lease.Metadata.OwnerReferences) != 1 ||
			lease.Metadata.OwnerReferences[0].UID != nodeUID) {
			t.Fatal("unbound Lease promoted to FRESH")
		}
	}
	// The exact named Lease is the only allowed coordination resource.
	checkRealLease()
	if _, err := reader.getNodeLease(context.Background(), nodeName+"-not-granted"); !errors.Is(err, ErrLeaseForbidden) {
		t.Fatalf("GET on a different Lease name must be denied: %v", err)
	}
	writeToken(denyToken)
	if _, err := reader.getNodeLease(context.Background(), nodeName); !errors.Is(err, ErrLeaseForbidden) {
		t.Fatalf("denied ServiceAccount must receive sanitized 403: %v", err)
	}
	writeToken(allowToken)
	checkRealLease()
	writeToken(nil)
	if _, err := reader.getNodeLease(context.Background(), nodeName); !errors.Is(err, ErrCredentials) {
		t.Fatalf("empty rotated token must deny Lease GET: %v", err)
	}
}
