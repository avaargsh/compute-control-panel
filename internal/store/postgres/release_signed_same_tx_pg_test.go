package postgres

import (
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"database/sql"
	"encoding/json"
	"errors"
	"net/url"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/avaargsh/compute-control-panel/internal/operation"
	"github.com/avaargsh/compute-control-panel/internal/provider"
	"github.com/jackc/pgx/v5/pgconn"
)

func signedSameTxFixture(t *testing.T, p ReleaseCommitFence, asOf time.Time) (
	provider.ReleaseEvidenceCandidate, provider.SignedDRAUnprepare,
	*provider.DRAUnprepareVerifier, ed25519.PrivateKey,
) {
	t.Helper()
	ns, jobName, ok := strings.Cut(p.TargetResourceID, "/")
	if !ok {
		t.Fatal("missing namespace/name in test target")
	}
	job := provider.KubernetesResourceRef{
		Namespace: ns, Name: jobName, UID: p.TargetResourceUID, ResourceVersion: "11",
	}
	pod := provider.KubernetesResourceRef{
		Namespace: ns, Name: "gpu-pod-a", UID: "pod-uid-a", ResourceVersion: "221",
	}
	claim := provider.KubernetesResourceRef{
		Namespace: ns, Name: "gpu-claim-a", UID: "claim-uid-a", ResourceVersion: "150",
	}
	device := provider.DRAAllocatedDevice{
		Request: "gpu", Driver: "gpu.example.com", Pool: "pool-a",
		Device: "device-a",
	}
	q := provider.KueueQueueBindingEvidence{
		Job: provider.KueueJobEvidence{
			TenantID: p.TenantID, WorkloadID: p.WorkloadID,
			DesiredGeneration: uint64(p.AllocationGeneration),
			JobTenantID:       p.TenantID, JobWorkloadID: p.WorkloadID,
			JobDesiredGeneration: uint64(p.AllocationGeneration),
			Job:                  job, Pod: pod, PodOwnerJobUID: job.UID,
			PodScheduled: true, PodNodeName: "gpu-node-a",
			KueueWorkload: provider.KubernetesResourceRef{
				Namespace: ns, Name: "kueue-a", UID: "kueue-uid-a", ResourceVersion: "22",
			},
			KueueOwnerJobUID: job.UID, KueueClusterQueue: "cq-a",
			KueueQuotaReserved:      true,
			KueueQuotaCondition:     provider.KueueConditionEvidence{Status: "True"},
			KueueAdmitted:           true,
			KueueAdmissionCondition: provider.KueueConditionEvidence{Status: "True"},
		},
		Stage: provider.KueuePodPlaced,
		LocalQueue: provider.KubernetesResourceRef{
			Namespace: ns, Name: "lq-a", UID: "local-uid-a", ResourceVersion: "51",
		},
		ClusterQueue: provider.KubernetesResourceRef{
			Name: "cq-a", UID: "cluster-uid-a", ResourceVersion: "61",
		},
	}
	previous := provider.DRAClaimObservation{
		Claim: claim, PodUID: pod.UID, Device: device,
	}
	candidate := provider.ReleaseEvidenceCandidate{
		ExpectedAllocationID: p.AllocationID,
		Create: operation.Operation{
			ID: p.CreateOperationID, TenantID: p.TenantID, WorkloadID: p.WorkloadID,
			AllocationID: p.AllocationID, ProviderID: p.ProviderID,
			IdempotencyKey: "test-create", RequestDigest: "sha256:" + strings.Repeat("b", 64),
			Generation: uint64(p.AllocationGeneration), Version: uint64(p.CreateOperationVersion),
			Kind: operation.Create, Status: operation.Succeeded,
			ProviderResourceID: p.TargetResourceID, ProviderResourceUID: p.TargetResourceUID,
		},
		Delete: operation.Operation{
			ID: p.DeleteOperationID, TenantID: p.TenantID, WorkloadID: p.WorkloadID,
			ProviderID: p.ProviderID, IdempotencyKey: "test-delete",
			RequestDigest: "sha256:" + strings.Repeat("c", 64),
			Generation:    uint64(p.DeleteGeneration), Version: uint64(p.DeleteOperationVersion),
			Kind: operation.Delete, Status: operation.Succeeded,
			TargetResourceID: p.TargetResourceID, TargetResourceUID: p.TargetResourceUID,
		},
		PriorQueue: q,
		PodAbsent: provider.KubernetesPodAbsentObservation{
			PreviousPod: pod, PodListResourceVersion: "300",
		},
		QuotaCleared: provider.KueueQuotaClearedObservation{
			PreviousWorkload:            q.Job.KueueWorkload,
			WorkloadListResourceVersion: "70", LocalQueue: q.LocalQueue,
			ClusterQueue: provider.KubernetesResourceRef{
				Name: "cq-a", UID: "cluster-uid-a", ResourceVersion: "65",
			},
		},
		PriorPodClaim: provider.DRAPodClaimObservation{
			Pod: pod, NodeName: "gpu-node-a", PodClaimAlias: "gpu",
			ConsumerContainer: "worker", Claim: previous,
		},
		ClaimUnallocated: provider.DRAClaimUnallocatedObservation{
			PreviousClaim: claim, PreviousPodUID: pod.UID,
			PreviousDevice: device,
			CurrentClaim: provider.KubernetesResourceRef{
				Namespace: ns, Name: claim.Name, UID: claim.UID, ResourceVersion: "151",
			},
		},
	}
	snapshot := provider.KueuePreDeleteDRASnapshot{
		Namespace: ns, TenantID: p.TenantID, WorkloadID: p.WorkloadID, ProviderID: p.ProviderID,
		CreateOperationID: p.CreateOperationID, DeleteOperationID: p.DeleteOperationID,
		CreateGeneration: uint64(p.AllocationGeneration), DeleteGeneration: uint64(p.DeleteGeneration),
		Job: job, KueueWorkload: q.Job.KueueWorkload, Pod: pod, NodeName: "gpu-node-a",
		Claim: claim, ClaimTemplateName: "gpu-template", PodClaimAlias: "gpu",
		ConsumerContainer: "worker", Devices: []provider.DRAAllocatedDevice{device},
		Source: "postgres-test", AdapterVersion: "test-v1", ObservedAt: asOf.Add(-2 * time.Minute),
	}
	snapshotRaw, err := json.Marshal(snapshot)
	if err != nil {
		t.Fatal(err)
	}
	snapshotDigest := sha256.Sum256(snapshotRaw)
	snapshot.Digest = "sha256:" + hex.EncodeToString(snapshotDigest[:])
	candidate.PreDeleteSnapshot = &snapshot
	candidate.ProviderAbsence = operation.Observation{
		OperationID: p.DeleteOperationID, TenantID: p.TenantID, WorkloadID: p.WorkloadID,
		ProviderID: p.ProviderID, DesiredGeneration: uint64(p.DeleteGeneration),
		Result: operation.TargetAbsent, TargetResourceID: p.TargetResourceID,
		TargetResourceUID: p.TargetResourceUID,
		ResourceVersion:   "delete-rv-5", ObservedAt: asOf.Add(-time.Minute),
	}
	publicKey, privateKey, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	authority := provider.DRAUnprepareAuthority{
		ClusterID: "cluster-" + p.WorkloadID, SourceID: "node-plugin-a",
		KeyID: "test-driver-key-a", Driver: device.Driver, PublicKey: publicKey,
	}
	verifier, err := provider.NewDRAUnprepareVerifier(authority, 10*time.Minute, 30*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	event := provider.DRAUnprepareEvent{
		Version: "ccp.dra.node-unprepare.v1", ClusterID: authority.ClusterID,
		SourceID: authority.SourceID, KeyID: authority.KeyID, EventID: "event-" + p.WorkloadID,
		RPC: "NodeUnprepareResources", Result: "SUCCESS",
		Driver: device.Driver, NodeName: "gpu-node-a",
		ClaimNamespace: ns, ClaimName: claim.Name, ClaimUID: claim.UID,
		PodUID: pod.UID, AllocatedClaimResourceVersion: claim.ResourceVersion,
		AllocatedDevice: device, CompletedAt: asOf.Add(-25 * time.Second),
	}
	payload, err := json.Marshal(event)
	if err != nil {
		t.Fatal(err)
	}
	signed := provider.SignedDRAUnprepare{Event: event,
		Signature: ed25519.Sign(privateKey, append([]byte("CCP:DRA:NodeUnprepareResources:v1\n"), payload...))}
	return candidate, signed, verifier, privateKey
}

func TestSignedRecordedAndCurrentGenerationOneTransaction(t *testing.T) {
	db := openIntegrationDB(t)
	var isAdmin bool
	if err := db.QueryRow("SELECT rolsuper FROM pg_roles WHERE rolname=current_user").Scan(&isAdmin); err != nil {
		t.Fatal(err)
	}
	if !isAdmin {
		t.Skip("requires disposable PostgreSQL admin to provision source")
	}
	repo, err := NewRepository(db)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
	defer cancel()
	suffix := randomSuffix(t)
	params := acceptanceFixture(t,
		"tenant-signed-"+suffix, "workload-accept-"+suffix,
		"create-request-"+suffix, "create-event-"+suffix)
	accepted, created, err := repo.AcceptWorkload(ctx, params)
	if err != nil || !created {
		t.Fatalf("accept: created=%t err=%v", created, err)
	}
	p := ReleaseCommitFence{
		TenantID: params.Workload.TenantID, WorkloadID: accepted.Workload.WorkloadID,
		AllocationID:      accepted.Allocation.AllocationID,
		ProviderID:        accepted.Allocation.ProviderID,
		CreateOperationID: accepted.Operation.ID, DeleteOperationID: "delete-" + suffix,
		TargetResourceID:     params.Workload.TenantID + "/job-a",
		TargetResourceUID:    "job-uid-" + suffix,
		AllocationGeneration: 1, DeleteGeneration: 2,
		WorkloadVersion: 2, AllocationVersion: 2,
		CreateOperationVersion: 2, DeleteOperationVersion: 1,
	}
	mustExecOne := func(query string, args ...any) {
		t.Helper()
		result, e := db.ExecContext(ctx, query, args...)
		if e != nil {
			t.Fatal(e)
		}
		n, e := result.RowsAffected()
		if e != nil || n != 1 {
			t.Fatalf("changed %d rows: %v", n, e)
		}
	}
	mustExecOne("UPDATE public.compute_operations SET status='SUCCEEDED', provider_resource_id=$3, provider_resource_uid=$4, version=version+1 WHERE tenant_id=$1 AND compute_operation_id=$2",
		p.TenantID, p.CreateOperationID, p.TargetResourceID, p.TargetResourceUID)
	mustExecOne("UPDATE public.compute_workloads SET desired_generation=2, desired_state='DELETE_REQUESTED', observed_phase='RELEASING', version=version+1 WHERE tenant_id=$1 AND workload_id=$2", p.TenantID, p.WorkloadID)
	mustExecOne("UPDATE public.compute_allocations SET phase='RELEASING',version=version+1 WHERE tenant_id=$1 AND allocation_id=$2", p.TenantID, p.AllocationID)
	mustExecOne("INSERT INTO public.compute_operations(compute_operation_id,tenant_id,workload_id,allocation_id,provider_id,operation_kind,desired_generation,provider_request_digest,idempotency_key,status,target_resource_id,target_resource_uid) VALUES ($1,$2,$3,NULL,$4,'DELETE',2,$5,$6,'SUCCEEDED',$7,$8)",
		p.DeleteOperationID, p.TenantID, p.WorkloadID, p.ProviderID,
		"sha256:"+strings.Repeat("c", 64), "delete-req-"+suffix, p.TargetResourceID, p.TargetResourceUID)
	asOf := time.Now().UTC()
	candidate, signed, verifier, privateKey := signedSameTxFixture(t, p, asOf)
	probe := func(c provider.ReleaseEvidenceCandidate, s provider.SignedDRAUnprepare) error {
		t.Helper()
		tx, e := db.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelReadCommitted})
		if e != nil {
			t.Fatal(e)
		}
		defer tx.Rollback()
		return ValidateRecordedReleaseSameTx(ctx, tx, p.TenantID, p.WorkloadID, p.AllocationID, p.CreateOperationID, p.DeleteOperationID, verifier, c, s, asOf)
	}
	if e := probe(candidate, signed); !errors.Is(e, provider.ErrDRAReceiptSourceUntrusted) {
		t.Fatalf("unregistered signer accepted before journal lookup: %v", e)
	}
	// A separate disposable-DBA test principal registers the exact signer.
	// This does not represent a real driver's authenticated RPC transport.
	_, err = db.ExecContext(ctx, "INSERT INTO public.dra_unprepare_source_keys(cluster_id,source_id,key_id,tenant_id,provider_id,driver,public_key,valid_from,valid_until,maximum_age_seconds,future_skew_seconds) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,600,30)",
		signed.Event.ClusterID, signed.Event.SourceID, signed.Event.KeyID, p.TenantID, p.ProviderID, signed.Event.Driver,
		privateKey.Public().(ed25519.PublicKey), asOf.Add(-time.Hour), asOf.Add(time.Hour))
	if err != nil {
		t.Fatal(err)
	}
	if e := probe(candidate, signed); !errors.Is(e, provider.ErrDRAReceiptNotRecorded) {
		t.Fatalf("registered but not recorded event accepted: %v", e)
	}
	dsn, err := url.Parse(os.Getenv("DATABASE_URL"))
	if err != nil {
		t.Fatal(err)
	}
	dsn.User = url.UserPassword("ccp_dra_receipt_ingest", "dra-ingest-ci-only")
	journal, writerDB, err := provider.OpenRestrictedDRAUnprepareReceiptJournal(ctx, dsn.String())
	if err != nil {
		t.Fatal(err)
	}
	defer writerDB.Close()
	if fresh, e := journal.VerifyAndRecord(ctx, verifier, candidate, signed, asOf); e != nil || !fresh {
		t.Fatalf("restricted signed journal append: fresh=%t err=%v", fresh, e)
	}
	if e := probe(candidate, signed); e != nil {
		t.Fatalf("matching current generation and recorded source refused: %v", e)
	}
	t.Run("historical node binding without native Kueue admission rejected", func(t *testing.T) {
		for _, nativeStatus := range []string{"False", "Unknown", ""} {
			bad := candidate
			bad.PriorQueue.Job.KueueAdmitted = false
			bad.PriorQueue.Job.KueueAdmissionCondition = provider.KueueConditionEvidence{Status: nativeStatus}
			if e := probe(bad, signed); !errors.Is(e, provider.ErrUncorrelatedDRAUnprepare) {
				t.Fatalf("native admission %q promoted release evidence: %v", nativeStatus, e)
			}
		}
	})
	t.Run("successor Job UID rejected", func(t *testing.T) {
		bad := candidate
		bad.Create.ProviderResourceUID = "successor-uid"
		if e := probe(bad, signed); !errors.Is(e, provider.ErrUncorrelatedDRAUnprepare) {
			t.Fatalf("mismatched CREATE resource UID accepted: %v", e)
		}
	})
	t.Run("signed but unrecorded new event rejected", func(t *testing.T) {
		other := signed
		other.Event.EventID = "other-" + suffix
		payload, e := json.Marshal(other.Event)
		if e != nil {
			t.Fatal(e)
		}
		other.Signature = ed25519.Sign(privateKey, append([]byte("CCP:DRA:NodeUnprepareResources:v1\n"), payload...))
		if e := probe(candidate, other); !errors.Is(e, provider.ErrDRAReceiptNotRecorded) {
			t.Fatalf("unrecorded event accepted: %v", e)
		}
	})
	t.Run("tampered receipt rejected", func(t *testing.T) {
		tampered := signed
		tampered.Signature = append([]byte(nil), signed.Signature...)
		tampered.Signature[0] ^= 0xff
		if e := probe(candidate, tampered); !errors.Is(e, provider.ErrUncorrelatedDRAUnprepare) {
			t.Fatalf("bad signature accepted: %v", e)
		}
	})
	t.Run("same transaction holds state and source-key locks", func(t *testing.T) {
		holder, e := db.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelReadCommitted})
		if e != nil {
			t.Fatal(e)
		}
		defer holder.Rollback()
		if e := ValidateRecordedReleaseSameTx(ctx, holder, p.TenantID, p.WorkloadID, p.AllocationID, p.CreateOperationID, p.DeleteOperationID, verifier, candidate, signed, asOf); e != nil {
			t.Fatalf("same-transaction preflight: %v", e)
		}
		for _, tc := range []struct {
			name, query string
			args        []any
		}{
			{"workload", "SELECT version FROM public.compute_workloads WHERE tenant_id=$1 AND workload_id=$2 FOR UPDATE NOWAIT", []any{p.TenantID, p.WorkloadID}},
			{"allocation", "SELECT version FROM public.compute_allocations WHERE tenant_id=$1 AND allocation_id=$2 FOR UPDATE NOWAIT", []any{p.TenantID, p.AllocationID}},
			{"CREATE operation", "SELECT version FROM public.compute_operations WHERE tenant_id=$1 AND compute_operation_id=$2 FOR UPDATE NOWAIT", []any{p.TenantID, p.CreateOperationID}},
			{"DELETE operation", "SELECT version FROM public.compute_operations WHERE tenant_id=$1 AND compute_operation_id=$2 FOR UPDATE NOWAIT", []any{p.TenantID, p.DeleteOperationID}},
			{"source key", "SELECT lock_token FROM public.dra_unprepare_source_keys WHERE cluster_id=$1 AND source_id=$2 AND key_id=$3 FOR KEY SHARE NOWAIT", []any{signed.Event.ClusterID, signed.Event.SourceID, signed.Event.KeyID}},
		} {
			contender, e := db.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelReadCommitted})
			if e != nil {
				t.Fatal(e)
			}
			var value int64
			e = contender.QueryRowContext(ctx, tc.query, tc.args...).Scan(&value)
			_ = contender.Rollback()
			var pgErr *pgconn.PgError
			if !errors.As(e, &pgErr) || pgErr.Code != "55P03" {
				t.Fatalf("%s not locked through caller transaction: %v", tc.name, e)
			}
		}
		if e := holder.Rollback(); e != nil {
			t.Fatal(e)
		}
	})
	t.Run("revoked registered source rejects persisted receipt", func(t *testing.T) {
		_, e := db.ExecContext(ctx, "INSERT INTO public.dra_unprepare_source_revocations(cluster_id,source_id,key_id,reason) VALUES ($1,$2,$3,'signed-same-tx-test-revocation')",
			signed.Event.ClusterID, signed.Event.SourceID, signed.Event.KeyID)
		if e != nil {
			t.Fatal(e)
		}
		if e := probe(candidate, signed); !errors.Is(e, provider.ErrDRAReceiptSourceUntrusted) {
			t.Fatalf("revoked signer accepted: %v", e)
		}
	})
	t.Run("superseding generation remains fail closed", func(t *testing.T) {
		mustExecOne("UPDATE public.compute_workloads SET desired_generation=3, version=version+1 WHERE tenant_id=$1 AND workload_id=$2", p.TenantID, p.WorkloadID)
		if e := probe(candidate, signed); !errors.Is(e, ErrReleaseCommitFence) {
			t.Fatalf("stale generation passed: %v", e)
		}
	})
	var allocPhase, workloadPhase string
	if e := db.QueryRowContext(ctx, "SELECT a.phase,w.observed_phase FROM public.compute_allocations a JOIN public.compute_workloads w ON w.tenant_id=a.tenant_id AND w.workload_id=a.workload_id WHERE a.tenant_id=$1 AND a.allocation_id=$2",
		p.TenantID, p.AllocationID).Scan(&allocPhase, &workloadPhase); e != nil {
		t.Fatal(e)
	}
	if allocPhase != "RELEASING" || workloadPhase != "RELEASING" {
		t.Fatalf("read-only composite changed protected state: Allocation=%s Workload=%s", allocPhase, workloadPhase)
	}
}
