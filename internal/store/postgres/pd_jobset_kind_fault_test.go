package postgres

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"syscall"
	"testing"
	"time"

	"github.com/avaargsh/compute-control-panel/internal/operation"
	"github.com/avaargsh/compute-control-panel/internal/provider"
)

// TestPDPostgresKubeJobSetFaults is a separate opt-in matrix. Each case
// has a new canonical intent and actual restricted staging identity.
func TestPDPostgresKubeJobSetFaults(t *testing.T) {
	if os.Getenv("CCP_PD_KIND_API_URL") == "" {
		t.Skip("requires disposable Kind + PostgreSQL integration harness")
	}
	if os.Getenv("CCP_DISPOSABLE_KIND_SMOKE") != "1" ||
		os.Getenv("CCP_PD_KIND_NAMESPACE") != "ccp-pd-paired-smoke" ||
		os.Getenv("CCP_PD_KIND_STAGE_BINARY") == "" ||
		os.Getenv("CCP_PD_KIND_FAULT_RECEIPT_PATH") == "" {
		t.Fatal("requires explicit disposable fault fixture")
	}
	results := make(map[string]string)
	for _, mode := range []string{"pre-post", "post-before-pin", "replacement", "binding-disconnect"} {
		t.Run(mode, func(t *testing.T) {
			results[mode] = runPDKindFault(t, mode)
		})
	}
	if t.Failed() || len(results) != 4 {
		t.Fatal("incomplete fault matrix")
	}
	receipt, err := json.Marshal(map[string]any{
		"outcomes": results, "physical_gpu_verified": false,
		"allocation_ready": false, "released": false, "cost_final": false,
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(os.Getenv("CCP_PD_KIND_FAULT_RECEIPT_PATH"), receipt, 0600); err != nil {
		t.Fatal(err)
	}
}

func runPDKindFault(t *testing.T, mode string) string {
	t.Helper()
	db := openIntegrationDB(t)
	ctx, cancel := context.WithTimeout(context.Background(), 55*time.Second)
	defer cancel()
	repo, owner, intent := pdPromotionFixture(t, ctx, db, 1)
	// Isolated fixture transition, NOT physical GPU release certification.
	fixturePDRelease(t, ctx, db, intent.Candidate.Workload)
	promotion, created, err := repo.PromotePDIntent(ctx, owner, intent.Candidate.IntentID, "pd-kind-golden")
	if err != nil || !created {
		t.Fatalf("promote same signed intent: %+v created=%t err=%v", promotion, created, err)
	}
	claimed, err := repo.ClaimPDStagingOperation(ctx, owner, intent.Candidate.IntentID, 1, "pd-golden-worker", 20*time.Second)
	if err != nil {
		t.Fatalf("claim source-fenced lease: %v", err)
	}
	started, err := repo.StartPDStagingOperation(
		ctx, owner, intent.Candidate.IntentID, claimed.Operation.Version,
		"pd-golden-worker", claimed.Operation.FenceToken,
	)
	if err != nil {
		t.Fatalf("start lease with current independent fence: %v", err)
	}
	snap, op := started.Snapshot, started.Operation
	plan := provider.PDJobSetPlan{
		IntentID: snap.Proposal.IntentID, TenantID: snap.TenantID,
		WorkloadID: snap.WorkloadID, ProviderID: snap.ProviderID,
		Generation: op.Generation, PrefillReplicas: snap.PrefillReplicas,
		DecodeReplicas:        snap.DecodeReplicas,
		PrefillGPUsPerReplica: 1, DecodeGPUsPerReplica: 1,
		ResourceClaimTemplateName: "pd-gpu",
	}
	req := provider.Request{
		OperationID: op.ID, TenantID: op.TenantID, WorkloadID: op.WorkloadID,
		ProviderID: op.ProviderID, IdempotencyKey: op.IdempotencyKey,
		RequestDigest: op.RequestDigest, DesiredGeneration: op.Generation,
		Kind: operation.Create,
	}
	openWriter := func() (*provider.PDJobSetAdapter, error) {
		return provider.NewServiceAccountPDJobSetAdapter(
			os.Getenv("CCP_PD_KIND_API_URL"),
			os.Getenv("CCP_PD_KIND_CA_PATH"),
			os.Getenv("CCP_PD_KIND_TOKEN_PATH"),
			os.Getenv("CCP_PD_KIND_NAMESPACE"),
			os.Getenv("CCP_PD_KIND_NAMESPACE"),
			os.Getenv("CCP_PD_KIND_BINDING_DSN"), plan,
		)
	}
	marker := filepath.Join(t.TempDir(), "fault")
	input, err := json.Marshal(map[string]any{
		"Owner": owner, "IntentID": intent.Candidate.IntentID,
		"Version": op.Version, "Worker": op.LeaseOwner, "Token": op.FenceToken,
		"ProviderID": snap.ProviderID, "PoolID": snap.PoolID, "Marker": marker,
		"Fault": mode,
	})
	if err != nil {
		t.Fatal(err)
	}
	testName := "^TestDisposablePDFaultProcess$"
	expectedMarker := mode + "\n"
	if mode == "replacement" {
		testName = "^TestDisposablePDStageProcess$"
		expectedMarker = "staged\n"
		// The canonical success child disallows unknown JSON fields.
		input, err = json.Marshal(map[string]any{
			"Owner": owner, "IntentID": intent.Candidate.IntentID,
			"Version": op.Version, "Worker": op.LeaseOwner, "Token": op.FenceToken,
			"ProviderID": snap.ProviderID, "PoolID": snap.PoolID, "Marker": marker,
		})
		if err != nil {
			t.Fatal(err)
		}
	}
	cmd := exec.CommandContext(ctx, os.Getenv("CCP_PD_KIND_STAGE_BINARY"), "-test.run="+testName, "-test.v")
	cmd.Env = append(os.Environ(), "CCP_PD_KIND_STAGE_CHILD=1")
	cmd.Stdin = bytes.NewReader(input)
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	finished := make(chan error, 1)
	go func() { finished <- cmd.Wait() }()
	waited := false
	defer func() {
		if !waited {
			_ = cmd.Process.Kill()
			<-finished
		}
	}()
	for {
		if data, err := os.ReadFile(marker); err == nil && string(data) == expectedMarker {
			break
		}
		select {
		case err := <-finished:
			waited = true
			// Disconnect is a controlled error and returns normally, so
			// allow an already-exited process only with its exact marker.
			if mode == "binding-disconnect" && err == nil {
				if data, readErr := os.ReadFile(marker); readErr == nil && string(data) == expectedMarker {
					break
				}
			}
			t.Fatalf("fault child exited before marker: %v", err)
		case <-ctx.Done():
			t.Fatal("fault marker timeout")
		case <-time.After(50 * time.Millisecond):
			continue
		}
		break
	}
	if mode == "binding-disconnect" {
		if !waited {
			err = <-finished
			waited = true
			if err != nil {
				t.Fatalf("disconnect child failed: %v", err)
			}
		}
	} else {
		if err := cmd.Process.Kill(); err != nil {
			t.Fatal(err)
		}
		err = <-finished
		waited = true
		var exited *exec.ExitError
		if !errors.As(err, &exited) {
			t.Fatalf("expected SIGKILL: %v", err)
		}
		status, ok := exited.Sys().(syscall.WaitStatus)
		if !ok || !status.Signaled() || status.Signal() != syscall.SIGKILL {
			t.Fatal("fault child was not killed")
		}
	}

	readPins := func() (int, string) {
		t.Helper()
		var count int
		var uid string
		if err := db.QueryRowContext(ctx, `SELECT count(*),COALESCE(min(job_uid),'')
			FROM public.kueue_job_create_pins
			WHERE namespace=$1 AND tenant_id=$2 AND operation_id=$3`,
			os.Getenv("CCP_PD_KIND_NAMESPACE"), owner.TenantID, req.OperationID).Scan(&count, &uid); err != nil {
			t.Fatal(err)
		}
		return count, uid
	}
	pinCount, pinnedUID := readPins()
	live := readPDFaultJobSets(t, ctx, intent.Candidate.IntentID)
	switch mode {
	case "pre-post":
		if len(live) != 0 || pinCount != 0 {
			t.Fatal("pre-POST crash created a JobSet or pin")
		}
	case "post-before-pin", "binding-disconnect":
		if len(live) != 1 || pinCount != 0 {
			t.Fatal("lost pin must leave exactly one unbound JobSet")
		}
	case "replacement":
		if len(live) != 1 || pinCount != 1 || pdFaultUID(t, live[0]) != pinnedUID {
			t.Fatal("replacement requires original pinned JobSet")
		}
		replacePDFaultJobSet(t, ctx, live[0], pinnedUID)
		live = readPDFaultJobSets(t, ctx, intent.Candidate.IntentID)
		if len(live) != 1 || pdFaultUID(t, live[0]) == pinnedUID {
			t.Fatal("same name replacement did not acquire a different UID")
		}
	}
	var unknownOp operation.Operation
	if mode == "binding-disconnect" {
		unknownOp, err = repo.GetOperation(ctx, owner.TenantID, op.ID)
	} else {
		for {
			var expired bool
			if err := db.QueryRowContext(ctx, "SELECT clock_timestamp() >= $1::timestamptz", op.LeaseExpiresAt).Scan(&expired); err != nil {
				t.Fatal(err)
			}
			if expired {
				break
			}
			select {
			case <-ctx.Done():
				t.Fatal("lease expiry timeout")
			case <-time.After(100 * time.Millisecond):
			}
		}
		unknownOp, err = repo.ExpireOperationLease(ctx, owner.TenantID, op.ID, op.Version)
	}
	if err != nil || unknownOp.Status != operation.Unknown ||
		unknownOp.Version <= op.Version || unknownOp.ProviderResourceUID != "" {
		t.Fatalf("ambiguous operation not UNKNOWN: %+v err=%v", unknownOp, err)
	}
	// Neither stale nor current UNKNOWN may re-enter the staging callback.
	for _, version := range []uint64{op.Version, unknownOp.Version} {
		called := false
		_, err = repo.WithCurrentPDStagingFence(ctx, owner, intent.Candidate.IntentID,
			version, op.LeaseOwner, op.FenceToken, func(context.Context, PDStageLease) error {
				called = true
				return nil
			})
		if err == nil || called {
			t.Fatal("ambiguous operation allowed another POST")
		}
	}
	writer, err := openWriter()
	if err != nil {
		t.Fatalf("fresh binding login: %v", err)
	}
	lease, err := repo.BeginOperationReconciliation(ctx, owner.TenantID, op.ID,
		unknownOp.Version, "", 0, "pd-fault-inspector", 10*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	observed, err := writer.Inspect(ctx, req)
	if err != nil || observed.ResourceUID != "" {
		t.Fatalf("uncertain resource adopted: %+v err=%v", observed, err)
	}
	wantResult, wantStatus := operation.OperationInProgress, operation.Unknown
	if mode == "pre-post" || mode == "replacement" {
		wantResult, wantStatus = operation.ObservationConflict, operation.ManualReview
	}
	if observed.Result != wantResult {
		t.Fatalf("inspection result=%s want=%s", observed.Result, wantResult)
	}
	if mode == "replacement" && observed.ReasonCode != "PD_JOBSET_UID_REPLACED" {
		t.Fatalf("replacement reason=%s", observed.ReasonCode)
	}
	recovered, err := repo.ReconcileOperation(ctx, owner.TenantID, op.ID, lease.Version,
		"pd-fault-inspector", lease.FenceToken, InspectionEvidence{
			ObservationID:  "pd-fault-inspect-" + randomSuffix(t),
			EventID:        "pd-fault-event-" + randomSuffix(t),
			CorrelationID:  "pd-fault-correlation-" + randomSuffix(t),
			Source:         "jobset-controller-kind",
			AdapterVersion: "pd-jobset-v1alpha2",
			Observation:    observed,
		})
	if err != nil || recovered.Status != wantStatus || recovered.ProviderResourceUID != "" {
		t.Fatalf("unexpected reconciliation %+v err=%v", recovered, err)
	}
	var phase string
	if err := db.QueryRowContext(ctx, `SELECT phase FROM public.compute_allocations
		WHERE allocation_id=$1 AND tenant_id=$2`, promotion.AllocationID, owner.TenantID).Scan(&phase); err != nil || phase != "PENDING" {
		t.Fatalf("unsafe allocation phase=%s err=%v", phase, err)
	}
	afterCount, afterUID := readPins()
	after := readPDFaultJobSets(t, ctx, intent.Candidate.IntentID)
	if afterCount != pinCount || afterUID != pinnedUID || len(after) != len(live) {
		t.Fatal("inspection changed resource or pin cardinality")
	}
	if len(after) == 1 && pdFaultUID(t, after[0]) != pdFaultUID(t, live[0]) {
		t.Fatal("inspection changed JobSet UID")
	}
	t.Logf("pd_fault=%s result=%s operation=%s allocation=%s pins=%d objects=%d", mode, observed.Result, recovered.Status, phase, afterCount, len(after))
	return string(recovered.Status)
}

func pdFaultKubectl(t *testing.T, ctx context.Context, input []byte, args ...string) []byte {
	t.Helper()
	cmd := exec.CommandContext(ctx, "kubectl", args...)
	cmd.Stdin = bytes.NewReader(input)
	out, err := cmd.Output()
	if err != nil {
		t.Fatalf("disposable kubectl %s: %v", args[0], err)
	}
	return out
}

func readPDFaultJobSets(t *testing.T, ctx context.Context, intentID string) []map[string]any {
	t.Helper()
	ns := "ccp-pd-paired-smoke"
	if string(bytes.TrimSpace(pdFaultKubectl(t, ctx, nil, "config", "current-context"))) != "kind-ccp-kueue-smoke" {
		t.Fatal("not disposable kind context")
	}
	raw := pdFaultKubectl(t, ctx, nil, "-n", ns, "get", "jobsets.jobset.x-k8s.io", "-o", "json")
	var list struct {
		Items []map[string]any
	}
	if err := json.Unmarshal(raw, &list); err != nil {
		t.Fatal(err)
	}
	var matches []map[string]any
	for _, obj := range list.Items {
		meta, _ := obj["metadata"].(map[string]any)
		ann, _ := meta["annotations"].(map[string]any)
		if ann["compute-control-panel.io/pd-intent-id"] != intentID {
			continue
		}
		spec, _ := obj["spec"].(map[string]any)
		if meta["namespace"] != ns || spec["suspend"] != true ||
			ann["kueue.x-k8s.io/admission-gated-by"] != "compute-control-panel.io/pd-activation" {
			t.Fatal("fault fixture is not inert")
		}
		matches = append(matches, obj)
	}
	return matches
}

func pdFaultUID(t *testing.T, obj map[string]any) string {
	t.Helper()
	meta, _ := obj["metadata"].(map[string]any)
	uid, _ := meta["uid"].(string)
	if uid == "" {
		t.Fatal("missing live JobSet UID")
	}
	return uid
}

func replacePDFaultJobSet(t *testing.T, ctx context.Context, obj map[string]any, uid string) {
	t.Helper()
	meta, _ := obj["metadata"].(map[string]any)
	name, _ := meta["name"].(string)
	ns := "ccp-pd-paired-smoke"
	if name == "" || pdFaultUID(t, obj) != uid || meta["namespace"] != ns {
		t.Fatal("replacement identity mismatch")
	}
	// This fixture-only admin action is scoped to the exact original UID.
	// The staging ServiceAccount remains GET/CREATE-only.
	options, err := json.Marshal(map[string]any{
		"apiVersion": "v1", "kind": "DeleteOptions",
		"preconditions": map[string]string{"uid": uid},
	})
	if err != nil {
		t.Fatal(err)
	}
	pdFaultKubectl(t, ctx, options, "delete", "--raw",
		"/apis/jobset.x-k8s.io/v1alpha2/namespaces/"+ns+"/jobsets/"+name, "-f", "-")
	for {
		out := pdFaultKubectl(t, ctx, nil, "-n", ns, "get", "jobset", name, "--ignore-not-found", "-o", "name")
		if len(bytes.TrimSpace(out)) == 0 {
			break
		}
		select {
		case <-ctx.Done():
			t.Fatal("original fixture did not disappear")
		case <-time.After(100 * time.Millisecond):
		}
	}
	replacement, err := json.Marshal(map[string]any{
		"apiVersion": obj["apiVersion"], "kind": obj["kind"], "spec": obj["spec"],
		"metadata": map[string]any{
			"name": name, "namespace": ns, "labels": meta["labels"], "annotations": meta["annotations"],
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	pdFaultKubectl(t, ctx, replacement, "-n", ns, "create", "-f", "-")
}
