package pdstaging

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/avaargsh/compute-control-panel/internal/capacity/pd"
	"github.com/avaargsh/compute-control-panel/internal/operation"
	"github.com/avaargsh/compute-control-panel/internal/provider"
	"github.com/avaargsh/compute-control-panel/internal/store/postgres"
)

type stubFence struct {
	stage  postgres.PDStageLease
	calls  int
	denied error
}

func (f *stubFence) WithCurrentPDStagingFence(
	ctx context.Context, owner postgres.PDIntentOwner, intentID string,
	expectedVersion uint64, worker string, token uint64,
	apply func(context.Context, postgres.PDStageLease) error,
) (postgres.PDStageLease, error) {
	f.calls++
	if f.denied != nil {
		return postgres.PDStageLease{}, f.denied
	}
	if err := apply(ctx, f.stage); err != nil {
		// Production PostgreSQL fence persists UNKNOWN on any uncertain
		// callback outcome; fake is deliberately only a unit wiring stub.
		return postgres.PDStageLease{}, err
	}
	return f.stage, nil
}

type stubWriter struct {
	requests []provider.Request
	ack      provider.Acknowledgement
	err      error
}

func (w *stubWriter) Apply(_ context.Context, req provider.Request) (provider.Acknowledgement, error) {
	w.requests = append(w.requests, req)
	if w.err != nil {
		return provider.Acknowledgement{}, w.err
	}
	if w.ack.OperationID != "" {
		return w.ack, nil
	}
	return provider.Acknowledgement{OperationID: req.OperationID, RequestDigest: req.RequestDigest}, nil
}

func stagingFixture() (postgres.PDStageLease, postgres.PDIntentOwner, Config) {
	intent := strings.Repeat("b", 64)
	owner := postgres.PDIntentOwner{
		TenantID: "tenant-a", ProjectID: "project-a",
		Service: "owner-svc", ResourceType: "deployment", ResourceID: "deployment-1",
	}
	stage := postgres.PDStageLease{
		Snapshot: postgres.PDStagingSnapshot{
			Promotion: postgres.PDPromotion{
				IntentID: intent, OperationID: "pd-operation-" + intent,
				AllocationID: "alloc-1", ProviderID: "provider-a", PoolID: "pool-a", Generation: 2,
			},
			Proposal: pd.Proposal{
				IntentID: intent, Workload: "workload-a", Generation: 2,
				Scope: pd.Scope{Tenant: owner.TenantID, Project: owner.ProjectID, Pool: "pool-a"},
				PrefillReplicas: 2, DecodeReplicas: 3, GPUsNeeded: 5,
				PoolRevision: 7, TopologyRevision: 9, LoadRevision: 3,
			},
			TenantID: owner.TenantID, WorkloadID: "workload-a",
			ProviderID: "provider-a", PoolID: "pool-a",
			PrefillReplicas: 2, DecodeReplicas: 3, GPUsPerReplica: 1,
			AuthorityExpiresAt: time.Now().Add(30*time.Second),
		},
		Operation: operation.Operation{
			ID: "pd-operation-" + intent, TenantID: owner.TenantID,
			WorkloadID: "workload-a", AllocationID: "alloc-1",
			ProviderID: "provider-a", IdempotencyKey: "pd-create-" + intent,
			RequestDigest: "sha256:" + intent, Generation: 2,
			Version: 3, FenceToken: 1, Kind: operation.Create, Status: operation.InFlight,
			LeaseOwner: "worker-1", LeaseExpiresAt: time.Now().Add(10*time.Second),
		},
	}
	cfg := Config{
		APIURL: "https://kubernetes.default.svc:443",
		CAPath: "/var/run/secrets/kubernetes.io/serviceaccount/ca.crt",
		TokenPath: "/var/run/secrets/kubernetes.io/serviceaccount/token",
		Namespace: "tenant-a", LocalQueue: "gpu-queue",
		BindingDSN: "postgres://ccp_kueue_job_binding@example.invalid/compute?sslmode=verify-full",
		TenantID: owner.TenantID, ProjectID: owner.ProjectID,
		ProviderID: "provider-a", PoolID: "pool-a",
		ResourceClaimTemplateName: "pd-one-gpu",
	}
	return stage, owner, cfg
}

func TestStageUsesOnlyCurrentImmutablePGStateNotCallerSuppliedOperation(t *testing.T) {
	stage, owner, cfg := stagingFixture()
	gate := &stubFence{stage: stage}
	writer := &stubWriter{}
	var plan provider.PDJobSetPlan
	got, err := stageWith(ctxBackground(), gate, owner, stage.Snapshot.Proposal.IntentID,
		3, "worker-1", 1, cfg, func(p provider.PDJobSetPlan) (stageWriter, error) {
			plan = p
			return writer, nil
		})
	if err != nil || gate.calls != 1 || len(writer.requests) != 1 || got != stage {
		t.Fatalf("trusted stage: %+v calls=%d requests=%d err=%v", got, gate.calls, len(writer.requests), err)
	}
	if plan.IntentID != stage.Snapshot.Proposal.IntentID ||
		plan.TenantID != stage.Snapshot.TenantID ||
		plan.WorkloadID != stage.Snapshot.WorkloadID ||
		plan.ProviderID != stage.Snapshot.ProviderID || plan.Generation != stage.Operation.Generation ||
		plan.PrefillReplicas != 2 || plan.DecodeReplicas != 3 ||
		plan.PrefillGPUsPerReplica != 1 || plan.DecodeGPUsPerReplica != 1 ||
		plan.ResourceClaimTemplateName != cfg.ResourceClaimTemplateName {
		t.Fatalf("plan escaped authorized fixed projection: %+v", plan)
	}
	req := writer.requests[0]
	if req.Validate() != nil || req.OperationID != stage.Operation.ID ||
		req.IdempotencyKey != stage.Operation.IdempotencyKey ||
		req.RequestDigest != stage.Operation.RequestDigest ||
		req.TenantID != stage.Operation.TenantID || req.WorkloadID != stage.Operation.WorkloadID ||
		req.ProviderID != stage.Operation.ProviderID || req.DesiredGeneration != stage.Operation.Generation ||
		req.Kind != operation.Create || req.TargetResourceID != "" || req.TargetResourceUID != "" {
		t.Fatalf("writer got caller-controlled/incorrect CREATE: %+v", req)
	}
}

func TestStageRejectsDeploymentScopeAndUntrustedProposalWithoutOpeningWriter(t *testing.T) {
	stage, owner, base := stagingFixture()
	for _, tc := range []struct {
		name string
		edit func(*postgres.PDStageLease, *Config)
	}{
		{"wrong-tenant", func(_ *postgres.PDStageLease, c *Config) { c.TenantID = "tenant-b" }},
		{"wrong-project", func(_ *postgres.PDStageLease, c *Config) { c.ProjectID = "project-b" }},
		{"wrong-provider", func(_ *postgres.PDStageLease, c *Config) { c.ProviderID = "provider-b" }},
		{"wrong-pool", func(_ *postgres.PDStageLease, c *Config) { c.PoolID = "pool-b" }},
		{"wrong-proposal-intent", func(s *postgres.PDStageLease, _ *Config) { s.Snapshot.Proposal.IntentID = strings.Repeat("c", 64) }},
		{"wrong-operation-generation", func(s *postgres.PDStageLease, _ *Config) { s.Operation.Generation++ }},
		{"wrong-workload", func(s *postgres.PDStageLease, _ *Config) { s.Snapshot.WorkloadID = "other-workload" }},
		{"untrusted-multi-GPU", func(s *postgres.PDStageLease, _ *Config) { s.Snapshot.GPUsPerReplica = 2 }},
		{"wrong-phase-cardinality", func(s *postgres.PDStageLease, _ *Config) { s.Snapshot.PrefillReplicas++ }},
		{"unknown-operation-kind", func(s *postgres.PDStageLease, _ *Config) { s.Operation.Kind = operation.Delete }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			copy, cfg := stage, base
			tc.edit(&copy, &cfg)
			gate := &stubFence{stage: copy}
			opened := 0
			_, err := stageWith(ctxBackground(), gate, owner, stage.Snapshot.Proposal.IntentID,
				3, "worker-1", 1, cfg, func(p provider.PDJobSetPlan) (stageWriter, error) {
					opened++
					return &stubWriter{}, nil
				})
			if err == nil || gate.calls != 1 || opened != 0 {
				t.Fatalf("%s escaped scope guard: err=%v calls=%d opened=%d", tc.name, err, gate.calls, opened)
			}
		})
	}
}

func TestStageRejectsUntrustedConfigurationBeforeEnteringFence(t *testing.T) {
	stage, owner, cfg := stagingFixture()
	for _, mode := range []string{"empty-api", "empty-binding", "empty-claim", "empty-namespace", "empty-queue", "missing-provider"} {
		t.Run(mode, func(t *testing.T) {
			c := cfg
			switch mode {
			case "empty-api":
				c.APIURL = ""
			case "empty-binding":
				c.BindingDSN = ""
			case "empty-claim":
				c.ResourceClaimTemplateName = ""
			case "empty-namespace":
				c.Namespace = ""
			case "empty-queue":
				c.LocalQueue = ""
			case "missing-provider":
				c.ProviderID = ""
			}
			gate := &stubFence{stage: stage}
			_, err := stageWith(ctxBackground(), gate, owner, stage.Snapshot.Proposal.IntentID,
				3, "worker-1", 1, c, func(provider.PDJobSetPlan) (stageWriter, error) {
					t.Fatal("untrusted config opened Kubernetes writer")
					return nil, nil
				})
			if err == nil || gate.calls != 0 {
				t.Fatalf("%s entered fence with invalid admin config: %v", mode, err)
			}
		})
	}
}

func TestStagePropagatesUnknownAckAndRejectsWrongAcknowledgement(t *testing.T) {
	stage, owner, cfg := stagingFixture()
	for _, mode := range []string{"lost-ack", "wrong-ack-digest", "writer-constructor"} {
		t.Run(mode, func(t *testing.T) {
			gate := &stubFence{stage: stage}
			writer := &stubWriter{}
			lost := provider.ErrAcknowledgementLost
			_, err := stageWith(ctxBackground(), gate, owner, stage.Snapshot.Proposal.IntentID,
				3, "worker-1", 1, cfg, func(provider.PDJobSetPlan) (stageWriter, error) {
					switch mode {
					case "lost-ack":
						writer.err = lost
					case "wrong-ack-digest":
						writer.ack = provider.Acknowledgement{OperationID: stage.Operation.ID, RequestDigest: "sha256:" + strings.Repeat("0", 64)}
					case "writer-constructor":
						return nil, errors.New("trusted store unavailable")
					}
					return writer, nil
				})
			if err == nil || gate.calls != 1 {
				t.Fatalf("%s reported successful create: %v", mode, err)
			}
			if mode == "lost-ack" && !errors.Is(err, lost) {
				t.Fatalf("provider ambiguity was lost: %v", err)
			}
			if mode == "writer-constructor" && len(writer.requests) != 0 {
				t.Fatal("failed authenticated adapter open performed a CREATE")
			}
		})
	}
}

func TestStageDoesNotEnterAdapterWhenDatabaseAuthorityAlreadyRevoked(t *testing.T) {
	stage, owner, cfg := stagingFixture()
	gate := &stubFence{stage: stage, denied: postgres.ErrPDPromotionBlocked}
	var invoked bool
	_, err := stageWith(ctxBackground(), gate, owner, stage.Snapshot.Proposal.IntentID,
		3, "worker-1", 1, cfg, func(provider.PDJobSetPlan) (stageWriter, error) {
			invoked = true
			return &stubWriter{}, nil
		})
	if !errors.Is(err, postgres.ErrPDPromotionBlocked) || invoked || gate.calls != 1 {
		t.Fatalf("revoked DB source opened provider adapter: err=%v invoked=%v calls=%d", err, invoked, gate.calls)
	}
}

func ctxBackground() context.Context { return context.Background() }
