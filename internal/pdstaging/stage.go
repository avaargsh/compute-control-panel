// Package pdstaging wires the canonical promoted PD Operation to a
// restricted, inert Kubernetes JobSet writer. It does NOT activate a JobSet,
// authorize execution, attest a physical device, or finalize GPU accounting.
package pdstaging

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"github.com/avaargsh/compute-control-panel/internal/operation"
	"github.com/avaargsh/compute-control-panel/internal/provider"
	"github.com/avaargsh/compute-control-panel/internal/store/postgres"
)

var (
	ErrInvalidStagingConfig = errors.New("invalid trusted PD staging deployment scope")
	ErrPDStagingScopeDrift  = errors.New("PD staging snapshot and deployment authority mismatch")
	ErrPDStagingAckMismatch = errors.New("inert JobSet CREATE acknowledgement mismatched canonical Operation")
)

// Config is provisioned by the operator, never supplied by the workload
// tenant or an Agent. Namespace, LocalQueue and ResourceClaimTemplateName
// identify one scoped Kubernetes staging destination; the independent DRA
// authority must still verify the ClaimTemplate's physical device count.
// The DSN is the restricted ccp_kueue_job_binding login, NOT an admin login.
type Config struct {
	APIURL, CAPath, TokenPath string
	Namespace, LocalQueue     string
	BindingDSN                string
	TenantID, ProjectID       string
	ProviderID, PoolID        string
	ResourceClaimTemplateName string
}

func (c Config) valid() bool {
	for _, value := range []string{
		c.APIURL, c.CAPath, c.TokenPath, c.Namespace, c.LocalQueue,
		c.BindingDSN, c.TenantID, c.ProjectID, c.ProviderID,
		c.PoolID, c.ResourceClaimTemplateName,
	} {
		if value == "" || value != strings.TrimSpace(value) {
			return false
		}
	}
	return true
}

// stagingFence is fulfilled by the real PostgreSQL Repository. The callback
// is performed under the SAME lock that verifies current independent policy,
// Workload generation, registered Provider and a live IN_FLIGHT Operation.
// This private seam exists solely for test-driven contract verification.
type stagingFence interface {
	WithCurrentPDStagingFence(
		context.Context, postgres.PDIntentOwner, string,
		uint64, string, uint64, func(context.Context, postgres.PDStageLease) error,
	) (postgres.PDStageLease, error)
}

type stageWriter interface {
	Apply(context.Context, provider.Request) (provider.Acknowledgement, error)
}

type stageWriterFactory func(provider.PDJobSetPlan) (stageWriter, error)

func currentPlanAndRequest(
	stage postgres.PDStageLease, owner postgres.PDIntentOwner,
	intentID, worker string, expectedVersion, token uint64, cfg Config,
) (provider.PDJobSetPlan, provider.Request, error) {
	snapshot, op := stage.Snapshot, stage.Operation
	proposal, promotion := snapshot.Proposal, snapshot.Promotion
	if cfg.TenantID != owner.TenantID || cfg.ProjectID != owner.ProjectID ||
		snapshot.TenantID != cfg.TenantID || proposal.Scope.Tenant != cfg.TenantID ||
		proposal.Scope.Project != cfg.ProjectID ||
		snapshot.PoolID != cfg.PoolID || proposal.Scope.Pool != cfg.PoolID ||
		snapshot.ProviderID != cfg.ProviderID || promotion.ProviderID != cfg.ProviderID ||
		promotion.PoolID != cfg.PoolID ||
		proposal.IntentID != intentID || promotion.IntentID != intentID ||
		snapshot.WorkloadID == "" || snapshot.WorkloadID != proposal.Workload ||
		snapshot.WorkloadID != op.WorkloadID ||
		op.TenantID != cfg.TenantID || op.ProviderID != cfg.ProviderID ||
		op.AllocationID == "" || op.AllocationID != promotion.AllocationID ||
		op.ID != promotion.OperationID || op.ID != "pd-operation-"+intentID ||
		op.IdempotencyKey != "pd-create-"+intentID ||
		op.RequestDigest != "sha256:"+intentID ||
		op.Generation <= 1 || op.Generation != proposal.Generation ||
		op.Generation != promotion.Generation ||
		op.Kind != operation.Create || op.Status != operation.InFlight ||
		op.Version != expectedVersion || op.FenceToken != token ||
		op.LeaseOwner != worker || op.TargetResourceID != "" || op.TargetResourceUID != "" ||
		snapshot.GPUsPerReplica != 1 ||
		snapshot.PrefillReplicas != proposal.PrefillReplicas ||
		snapshot.DecodeReplicas != proposal.DecodeReplicas ||
		snapshot.PrefillReplicas < 1 || snapshot.DecodeReplicas < 1 ||
		snapshot.PrefillReplicas > 32 || snapshot.DecodeReplicas > 32 ||
		proposal.GPUsNeeded != snapshot.PrefillReplicas+snapshot.DecodeReplicas ||
		proposal.PoolRevision < 1 || proposal.TopologyRevision < 1 || proposal.LoadRevision < 1 {
		return provider.PDJobSetPlan{}, provider.Request{}, ErrPDStagingScopeDrift
	}
	plan := provider.PDJobSetPlan{
		IntentID: intentID, TenantID: snapshot.TenantID,
		WorkloadID: snapshot.WorkloadID, ProviderID: snapshot.ProviderID,
		Generation:      op.Generation,
		PrefillReplicas: snapshot.PrefillReplicas, DecodeReplicas: snapshot.DecodeReplicas,
		PrefillGPUsPerReplica: 1, DecodeGPUsPerReplica: 1,
		ResourceClaimTemplateName: cfg.ResourceClaimTemplateName,
	}
	req := provider.Request{
		OperationID: op.ID, TenantID: op.TenantID, WorkloadID: op.WorkloadID,
		ProviderID: op.ProviderID, IdempotencyKey: op.IdempotencyKey,
		RequestDigest: op.RequestDigest, DesiredGeneration: op.Generation,
		Kind: operation.Create,
	}
	if err := req.Validate(); err != nil {
		return provider.PDJobSetPlan{}, provider.Request{}, fmt.Errorf("%w: %v", ErrPDStagingScopeDrift, err)
	}
	return plan, req, nil
}

func stageWith(
	ctx context.Context, gate stagingFence, owner postgres.PDIntentOwner,
	intentID string, expectedVersion uint64, worker string, token uint64,
	cfg Config, open stageWriterFactory,
) (postgres.PDStageLease, error) {
	if gate == nil || open == nil || !cfg.valid() {
		return postgres.PDStageLease{}, ErrInvalidStagingConfig
	}
	return gate.WithCurrentPDStagingFence(ctx, owner, intentID,
		expectedVersion, worker, token,
		func(callCtx context.Context, stage postgres.PDStageLease) error {
			plan, req, err := currentPlanAndRequest(
				stage, owner, intentID, worker, expectedVersion, token, cfg,
			)
			if err != nil {
				return err
			}
			writer, err := open(plan)
			if err != nil {
				return err
			}
			if writer == nil {
				return ErrPDStagingScopeDrift
			}
			ack, err := writer.Apply(callCtx, req)
			if err != nil {
				return err
			}
			if ack.OperationID != req.OperationID || ack.RequestDigest != req.RequestDigest {
				return ErrPDStagingAckMismatch
			}
			return nil
		})
}

// StageInertPDJobSet is the production-facing current-intent staging entry.
// Its Request and JobSet shape are derived exclusively from the current
// database Promotion/Operation under the row-locked revocation fence; it
// never accepts tenant-supplied image, command, Pod spec or Operation IDs.
// The adapter is constructed with a CA-pinned rotating ServiceAccount token
// and a strictly attested durable UID-binding PostgreSQL login; in-memory
// binding fallback is not available.
//
// Call only from an operator-authenticated worker whose Kubernetes Role can
// GET/CREATE JobSets but cannot PATCH/UPDATE/DELETE or remove AdmissionGatedBy.
// The independent activation authority, DRA device cardinality attestation,
// actual GPU Prepare/Unprepare and cost FINAL remain separate gates.
func StageInertPDJobSet(
	ctx context.Context, repo *postgres.Repository, owner postgres.PDIntentOwner,
	intentID string, expectedVersion uint64, worker string, token uint64,
	cfg Config,
) (postgres.PDStageLease, error) {
	if repo == nil {
		return postgres.PDStageLease{}, ErrInvalidStagingConfig
	}
	return stageWith(ctx, repo, owner, intentID,
		expectedVersion, worker, token, cfg,
		func(plan provider.PDJobSetPlan) (stageWriter, error) {
			return provider.NewServiceAccountPDJobSetAdapter(
				cfg.APIURL, cfg.CAPath, cfg.TokenPath,
				cfg.Namespace, cfg.LocalQueue, cfg.BindingDSN, plan)
		})
}
