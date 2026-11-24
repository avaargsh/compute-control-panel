package kubeinventory

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/avaargsh/compute-control-panel/internal/observation"
)

// TestKindInventoryLeastPrivilege is an opt-in real Kubernetes API acceptance
// test. Its environment is provided by scripts/kubeinventory_kind_smoke.sh.
// Ordinary unit testing skips it; fake TLS tests are in reader_test.go.
func TestKindInventoryLeastPrivilege(t *testing.T) {
	if os.Getenv("CCP_KIND_INVENTORY_SMOKE") != "1" {
		t.Skip("requires a disposable kind cluster and short-lived service-account tokens")
	}
	required := func(name string) string {
		t.Helper()
		value := os.Getenv(name)
		if value == "" {
			t.Fatalf("missing required kind integration environment %s", name)
		}
		return value
	}
	endpoint := required("CCP_KIND_API_SERVER")
	caPath := required("CCP_KIND_CA_FILE")
	allowedPath := required("CCP_KIND_ALLOWED_TOKEN_FILE")
	deniedPath := required("CCP_KIND_DENIED_TOKEN_FILE")

	allowedToken, err := os.ReadFile(allowedPath)
	if err != nil || len(allowedToken) == 0 {
		t.Fatal("authorized ServiceAccount token file unavailable")
	}
	deniedToken, err := os.ReadFile(deniedPath)
	if err != nil || len(deniedToken) == 0 {
		t.Fatal("denied ServiceAccount token file unavailable")
	}

	// Use an ephemeral duplicate token path to test rotation without modifying
	// the credentials created and maintained by the Kubernetes test script.
	tokenPath := filepath.Join(t.TempDir(), "rotating-token")
	writeToken := func(token []byte) {
		t.Helper()
		if err := os.WriteFile(tokenPath, token, 0600); err != nil {
			t.Fatal("could not write ephemeral token fixture")
		}
	}
	writeToken(allowedToken)

	reader, err := NewReader(Config{
		APIServer: endpoint, CAFile: caPath, TokenFile: tokenPath,
		ClusterID: "kind-inventory", TenantID: "tenant-kind", ProjectID: "project-kind",
		ResourceNames: []string{"nvidia.com/gpu"}, ObservationTTL: time.Minute,
		Timeout: 10 * time.Second,
	})
	if err != nil {
		t.Fatal(err)
	}

	checkLiveRead := func() {
		t.Helper()
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		snapshot, err := reader.ListObservations(ctx, "tenant-kind", []string{"project-kind"})
		if err != nil {
			t.Fatalf("real Kubernetes API node list failed: %v", err)
		}
		if snapshot.DataMode != observation.ModeLive || len(snapshot.Items) == 0 {
			t.Fatalf("no real node facts: mode=%q count=%d", snapshot.DataMode, len(snapshot.Items))
		}
		nodes := 0
		for _, record := range snapshot.Items {
			if record.Resource.Kind != "Node" {
				// A disposable kind node has no GPU Device Plugin. Never create
				// synthetic GPU device records to make an L2 test look like L3.
				t.Fatalf("unverified kind GPU resource record: %+v", record.Resource)
			}
			nodes++
			if record.Resource.UID == "" || record.Resource.ResourceVersion == "" ||
				record.SourceRevision == "" || record.Source != "kubernetes/core/v1/node.status" ||
				record.TenantID != "tenant-kind" || record.ProjectID != "project-kind" ||
				record.ObservedAt.IsZero() || !record.ExpiresAt.After(record.ObservedAt) {
				t.Fatalf("missing native source identity or scope: %+v", record)
			}
			projected, err := observation.Project(record, time.Now().UTC())
			if err != nil || projected.Status == observation.StatusStale {
				t.Fatalf("real node observation already invalid/stale: view=%+v err=%v", projected, err)
			}
		}
		if nodes == 0 {
			t.Fatal("no Kubernetes node record returned")
		}
	}

	// Real positive admission using the exact 'list nodes' grant.
	checkLiveRead()

	// A distinct SA without list permission must get a real 403; token bytes
	// are never printed to a test failure or GitHub Actions log.
	writeToken(deniedToken)
	_, err = reader.ListObservations(context.Background(), "tenant-kind", []string{"project-kind"})
	if !errors.Is(err, ErrForbidden) {
		t.Fatalf("denied ServiceAccount not rejected with sanitized 403: %v", err)
	}

	// Verify a projected SA token is reloaded per request, not cached forever.
	writeToken(allowedToken)
	checkLiveRead()

	// Project scope is evaluated before touching the Kubernetes API.
	out, err := reader.ListObservations(context.Background(), "tenant-kind", []string{"outside-project"})
	if err != nil || len(out.Items) != 0 || out.DataMode != observation.ModeLive {
		t.Fatalf("project-scoped read failed closed incorrectly: mode=%s items=%d err=%v", out.DataMode, len(out.Items), err)
	}
	_, err = reader.ListObservations(context.Background(), "outside-tenant", []string{"project-kind"})
	if !errors.Is(err, ErrScope) {
		t.Fatalf("outside tenant must fail before API access, got %v", err)
	}

	writeToken(nil)
	_, err = reader.ListObservations(context.Background(), "tenant-kind", []string{"project-kind"})
	if !errors.Is(err, ErrCredentials) {
		t.Fatalf("missing rotated credential was not rejected: %v", err)
	}
}

// TestKindNodeLeaseUIDPinnedRead exercises the real Kubernetes coordination
// Lease GET endpoint with a distinct read-only service account. This is an
// authenticated L2 source-identity test, NOT physical GPU readiness evidence.
func TestKindNodeLeaseUIDPinnedRead(t *testing.T) {
	if os.Getenv("CCP_KIND_INVENTORY_SMOKE") != "1" {
		t.Skip("requires opt-in disposable kind Node list and pinned Lease GET RBAC")
	}
	required := func(name string) string {
		t.Helper()
		value := os.Getenv(name)
		if value == "" {
			t.Fatalf("missing required Lease kind environment %s", name)
		}
		return value
	}
	endpoint := required("CCP_KIND_API_SERVER")
	caPath := required("CCP_KIND_CA_FILE")
	allowed := required("CCP_KIND_LEASE_TOKEN_FILE")
	denied := required("CCP_KIND_DENIED_TOKEN_FILE")
	nodeName := required("CCP_KIND_LEASE_NODE")
	nodeUID := required("CCP_KIND_LEASE_NODE_UID")

	makeReader := func(token string) *Reader {
		t.Helper()
		reader, err := NewReader(Config{
			APIServer: endpoint, CAFile: caPath, TokenFile: token,
			ClusterID: "kind-inventory", TenantID: "tenant-kind", ProjectID: "project-kind",
			ResourceNames: []string{"nvidia.com/gpu"}, ObservationTTL: time.Minute,
			Timeout: 10 * time.Second,
		})
		if err != nil {
			t.Fatal(err)
		}
		return reader
	}
	reader := makeReader(allowed)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	observed, err := reader.observeNodeLease(ctx, "tenant-kind", []string{"project-kind"}, nodeName, nodeUID, time.Minute)
	if err != nil || observed.Assessment.State != nodeLeaseFresh || !observed.Assessment.ExpiresAt.After(time.Now().UTC()) {
		t.Fatalf("real Node/Lease API identity not current: state=%s reason=%s err=%v", observed.Assessment.State, observed.Assessment.ReasonCode, err)
	}
	stale, err := reader.observeNodeLease(ctx, "tenant-kind", []string{"project-kind"}, nodeName, "different-node-uid", time.Minute)
	if err != nil || stale.Assessment.State != nodeLeaseUnknown {
		t.Fatalf("same-name successor Node UID was adopted: %+v err=%v", stale, err)
	}
	scoped, err := reader.observeNodeLease(ctx, "tenant-kind", []string{"outside-project"}, nodeName, nodeUID, time.Minute)
	if !errors.Is(err, ErrScope) || scoped.Assessment.State == nodeLeaseFresh {
		t.Fatalf("ungranted project was not denied: %+v err=%v", scoped, err)
	}
	blocked := makeReader(denied)
	rejected, err := blocked.observeNodeLease(ctx, "tenant-kind", []string{"project-kind"}, nodeName, nodeUID, time.Minute)
	if !errors.Is(err, ErrForbidden) || rejected.Assessment.State == nodeLeaseFresh {
		t.Fatalf("unprivileged account read Node/Lease: %+v err=%v", rejected, err)
	}
}
