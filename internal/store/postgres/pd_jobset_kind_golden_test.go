package postgres

import (
	"context"
	"errors"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/avaargsh/compute-control-panel/internal/operation"
	"github.com/avaargsh/compute-control-panel/internal/provider"
)

// TestPDPostgresKubeJobSetGolden is opt-in: it requires BOTH a migrated,
// disposable PostgreSQL and an actual Kind kube-apiserver with JobSet/Kueue
// and a GET/CREATE-only staging ServiceAccount. It never attests a physical
// GPU or authenticates a real old-Allocation release (fixture only).
//
// One exact immutable PD Intent travels through Promotion -> leased
// IN_FLIGHT -> source-held callback -> real HTTPS JobSet CREATE -> restricted
// PostgreSQL UID pin -> deliberately lost control-plane ACK -> durable UNKNOWN
// -> NEW adapter and repository -> Inspect-first -> Operation-only receipt.
// No JobSet activation, Pod execution, GPU READY/RELEASED or cost FINAL.
func TestPDPostgresKubeJobSetGolden(t *testing.T) {
	if os.Getenv("CCP_PD_KIND_API_URL") == "" {
		t.Skip("requires disposable Kind + PostgreSQL integration harness")
	}
	if os.Getenv("CCP_DISPOSABLE_KIND_SMOKE") != "1" ||
		os.Getenv("CCP_PD_KIND_NAMESPACE") != "ccp-pd-paired-smoke" {
		t.Fatal("real PD cross-service smoke must run only in explicit disposable Kind")
	}
	for _, k := range []string{
		"DATABASE_URL", "CCP_PD_KIND_BINDING_DSN", "CCP_PD_KIND_CA_PATH",
		"CCP_PD_KIND_TOKEN_PATH", "CCP_PD_KIND_RECEIPT_PATH",
	} {
		if os.Getenv(k) == "" {
			t.Fatalf("disposable cross-service harness omitted required %s", k)
		}
	}
	db := openIntegrationDB(t)
	ctx, cancel := context.WithTimeout(context.Background(), 35*time.Second)
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
		DecodeReplicas: snap.DecodeReplicas,
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
	writer, err := openWriter()
	if err != nil {
		t.Fatalf("restricted database and staging ServiceAccount bootstrap: %v", err)
	}
	var applyCalls int
	unknown, err := repo.WithCurrentPDStagingFence(
		ctx, owner, intent.Candidate.IntentID, op.Version, "pd-golden-worker", op.FenceToken,
		func(callCtx context.Context, locked PDStageLease) error {
			applyCalls++
			if locked != started {
				t.Fatal("locked source changed from the exact claimed PD generation")
			}
			ack, err := writer.Apply(callCtx, req)
			if err != nil {
				return err
			}
			if ack.OperationID != req.OperationID || ack.RequestDigest != req.RequestDigest {
				return provider.ErrAcknowledgementLost
			}
			// This is a controlled fault injection AFTER actual HTTPS POST
			// and durable pin but BEFORE the control-plane ACK is recorded.
			return provider.ErrAcknowledgementLost
		},
	)
	if !errors.Is(err, provider.ErrAcknowledgementLost) || applyCalls != 1 ||
		unknown.Operation.Status != operation.Unknown ||
		unknown.Operation.Version != op.Version+1 {
		t.Fatalf("real API POST lost-ACK did not leave durable UNKNOWN: %+v calls=%d err=%v", unknown, applyCalls, err)
	}
	// A fresh repository and independent restricted database login must see
	// the same Kubernetes UID. A same-name JobSet without a pin is NOT proof.
	otherRepo, err := NewRepository(db)
	if err != nil {
		t.Fatal(err)
	}
	otherWriter, err := openWriter()
	if err != nil {
		t.Fatalf("restarted restricted writer: %v", err)
	}
	stored, err := otherRepo.GetOperation(ctx, owner.TenantID, op.ID)
	if err != nil || stored.Status != operation.Unknown ||
		stored.Version != unknown.Operation.Version ||
		stored.ProviderResourceUID != "" {
		t.Fatalf("restarted Operation incorrectly adopted an object: %+v %v", stored, err)
	}
	_, err = otherRepo.WithCurrentPDStagingFence(
		ctx, owner, intent.Candidate.IntentID, op.Version,
		"pd-golden-worker", op.FenceToken,
		func(context.Context, PDStageLease) error {
			applyCalls++
			return nil
		},
	)
	if err == nil || applyCalls != 1 {
		t.Fatalf("restarted worker re-POSTed UNKNOWN: %v calls=%d", err, applyCalls)
	}
	// A new reconciliation lease precedes the independent provider Inspect.
	reconcile, err := otherRepo.BeginOperationReconciliation(
		ctx, owner.TenantID, op.ID, stored.Version, "", 0, "pd-inspector", 10*time.Second,
	)
	if err != nil {
		t.Fatalf("claim UNKNOWN inspection: %v", err)
	}
	observed, err := otherWriter.Inspect(ctx, req)
	if err != nil || observed.Result != operation.ResourcePresent ||
		observed.ResourceUID == "" || observed.ResourceVersion == "" {
		t.Fatalf("restarted authoritative UID-pinned Inspect: %+v %v", observed, err)
	}
	evidence := InspectionEvidence{
		ObservationID: "pd-kind-inspect-" + randomSuffix(t),
		EventID: "pd-kind-event-" + randomSuffix(t),
		CorrelationID: "pd-kind-correlation-" + randomSuffix(t),
		Source: "jobset-controller-kind",
		AdapterVersion: "pd-jobset-v1alpha2",
		Observation: observed,
	}
	recovered, err := otherRepo.ReconcileOperation(
		ctx, owner.TenantID, op.ID, reconcile.Version,
		"pd-inspector", reconcile.FenceToken, evidence,
	)
	if err != nil || recovered.Status != operation.Succeeded ||
		recovered.ProviderResourceUID != observed.ResourceUID {
		t.Fatalf("provider CREATE outcome not safely reconciled: %+v %v", recovered, err)
	}
	var allocationPhase string
	if err := db.QueryRowContext(ctx,
		`SELECT phase FROM public.compute_allocations
		   WHERE allocation_id=$1 AND tenant_id=$2`,
		promotion.AllocationID, owner.TenantID).Scan(&allocationPhase); err != nil {
		t.Fatal(err)
	}
	if allocationPhase != "PENDING" {
		t.Fatalf("mere JobSet creation improperly changed GPU allocation to %s", allocationPhase)
	}
	var pins int
	if err := db.QueryRowContext(ctx,
		`SELECT count(*) FROM public.kueue_job_create_pins
		  WHERE namespace=$1 AND tenant_id=$2 AND compute_operation_id=$3
		    AND resource_uid=$4`,
		os.Getenv("CCP_PD_KIND_NAMESPACE"), owner.TenantID,
		req.OperationID, observed.ResourceUID).Scan(&pins); err != nil || pins != 1 {
		t.Fatalf("restricted durable UID pin not unique: pins=%d err=%v", pins, err)
	}
	// Receipt contains no secrets or physical readiness assertion. Runner
	// independently checks this intent's actual suspended JobSet and no Pods.
	receipt := fmt.Sprintf(
		"{\"intent_id\":%q,\"uid\":%q,\"operation_status\":\"SUCCEEDED\",\"allocation_phase\":\"PENDING\",\"lost_ack_unknown\":true,\"inspect_first\":true,\"physical_gpu_verified\":false,\"driver_prepared\":false,\"cost_final\":false}\n",
		intent.Candidate.IntentID, observed.ResourceUID,
	)
	if err := os.WriteFile(os.Getenv("CCP_PD_KIND_RECEIPT_PATH"), []byte(receipt), 0600); err != nil {
		t.Fatalf("write scoped non-secret fixture receipt: %v", err)
	}
	t.Logf("PASS: actual PostgreSQL lease + HTTPS JobSet POST + scoped UID pin + UNKNOWN + restarted Inspect/Reconcile; uid=%s allocation=%s (not GPU READY)", observed.ResourceUID, allocationPhase)
}
