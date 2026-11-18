package postgres

import (
	"context"
	"crypto/ed25519"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"errors"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/avaargsh/compute-control-panel/internal/capacity/pd"
	"github.com/avaargsh/compute-control-panel/internal/capacity/pd/rawphase"
)

func pdIntentFixture(t *testing.T, now time.Time, scope pd.Scope, workload, providerID string) pd.VerifiedProposal {
	t.Helper()
	key := func() (ed25519.PublicKey, ed25519.PrivateKey) {
		pub, priv, err := ed25519.GenerateKey(nil)
		if err != nil {
			t.Fatal(err)
		}
		return pub, priv
	}
	sign := func(raw []byte, err error, priv ed25519.PrivateKey) []byte {
		if err != nil {
			t.Fatal(err)
		}
		return ed25519.Sign(priv, raw)
	}
	hash := func(raw []byte) string {
		sum := sha256.Sum256(raw)
		return hex.EncodeToString(sum[:])
	}
	bPub, bPriv := key()
	iPub, iPriv := key()
	fPub, fPriv := key()
	end := now.UnixMilli()
	start := end - 1000
	m := rawphase.Manifest{Version: "pd-raw/v1", RunID: "run", Tenant: scope.Tenant, Project: scope.Project, Pool: scope.Pool, Model: scope.Model, GPU: scope.GPU, Shape: scope.Shape, Fabric: scope.Fabric, EvaluatorCommit: strings.Repeat("a", 40), DeploymentSHA256: strings.Repeat("b", 64), PrefillWorker: "pf", DecodeWorker: "dc", PrefillGPUs: 2, DecodeGPUs: 1, StartMS: start, EndMS: end, Attempts: 1, KVBytesLimit: 100}
	a := rawphase.Attempt{AttemptID: "a", TransferID: "kv", PrefillWorker: "pf", DecodeWorker: "dc", Success: true, PrefillTokens: 100, DecodeTokens: 3, KVBytes: 100, RequestStartMS: start, PrefillStartMS: start, PrefillEndMS: start + 100, TransferStartMS: start + 100, TransferEndMS: start + 200, DecodeStartMS: start + 200, FirstTokenMS: start + 400, LastTokenMS: start + 800}
	raw, err := json.Marshal(m)
	if err != nil {
		t.Fatal(err)
	}
	row, err := json.Marshal(a)
	if err != nil {
		t.Fatal(err)
	}
	raw = append(append(raw, '\n'), row...)
	claim := pd.Profile{Scope: scope, KeyID: "benchmark", RunID: m.RunID, ArtifactSHA256: hash(raw), ObservedAtMillis: end, PrefillGPUs: 2, DecodeGPUs: 1, PrefillTokensPerSecond: 100, DecodeTokensPerSecond: 3, MaxKVBytesPerRequest: 100, P95TTFTms: 400, P95TPOTms: 200, P95TransferMs: 100}
	demand := pd.Demand{Scope: scope, Workload: workload, Generation: 2, LoadRevision: 3, ObservedAtMillis: end, PrefillTokensPerSecond: 100, DecodeTokensPerSecond: 3, RequestsPerSecond: 1, MaxKVBytesPerRequest: 100, MaxTTFTms: 400, MaxTPOTms: 200, MaxTransferP95ms: 100}
	bPolicy := pd.Policy{Scope: scope, KeyID: claim.KeyID, PublicKey: bPub, ValidFrom: now.Add(-time.Hour), ValidUntil: now.Add(time.Hour), MaxAge: 5 * time.Minute}
	meta := pd.SourceMeta{Scope: scope, ClusterUID: "cluster-uid", ProviderID: providerID, KeyID: "inventory", AdapterID: "inventory/v1", PoolRevision: 7, TopologyRevision: 9, ObservedAtMillis: end - 3000}
	inv := pd.InventoryClaim{Source: meta, Complete: true}
	proof := strings.Repeat("a", 64)
	for _, id := range []string{"a", "b", "c"} {
		inv.Devices = append(inv.Devices, pd.PhysicalGPU{PhysicalUID: "gpu-" + id, Driver: "gpu.example.com", Pool: "pool", Device: id, SliceUID: "slice", SliceResourceVersion: "123", NodeUID: "node", IslandID: "island", State: "FREE", WholeDevice: true, Healthy: true, OccupancySHA256: proof, HealthSHA256: proof, ConnectivitySHA256: proof})
	}
	ib, err := pd.InventorySigningBytes(inv)
	isig := sign(ib, err, iPriv)
	fm := meta
	fm.KeyID, fm.AdapterID, fm.ObservedAtMillis = "fabric", "fabric/v1", end
	fabric := pd.FabricClaim{Source: fm, InventorySHA256: hash(ib), BottleneckID: "link", CoveredIslands: []string{"island"}, Windows: []pd.LinkWindow{{StartMS: end - 1000, EndMS: end, MeasuredBytes: 1000, HeadroomBytesPerSecond: 100}}}
	fb, err := pd.FabricSigningBytes(fabric)
	fsig := sign(fb, err, fPriv)
	sourcePolicy := func(s pd.SourceMeta, pub ed25519.PublicKey) pd.SourcePolicy {
		return pd.SourcePolicy{Scope: scope, ClusterUID: s.ClusterUID, ProviderID: s.ProviderID, KeyID: s.KeyID, AdapterID: s.AdapterID, PublicKey: pub, ValidFrom: now.Add(-time.Hour), ValidUntil: now.Add(time.Hour), MaxAge: 5 * time.Minute}
	}
	tp := pd.TopologyPolicy{Inventory: sourcePolicy(meta, iPub), Fabric: sourcePolicy(fm, fPub), MinimumWindows: 1, MinimumWindowMS: 1000, MinimumHeadroomBytesPerSecond: 100}
	bb, err := pd.SigningBytes(claim)
	bsig := sign(bb, err, bPriv)
	v, err := pd.ProposeFromSources(now, demand, claim, bsig, bPolicy, time.Minute, raw, pd.RawEvidence{Manifest: m, ArtifactSHA256: claim.ArtifactSHA256, MinimumAttempts: 1, MinimumWindowMS: 1000}, pd.TopologyEvidence{Inventory: inv, InventorySignature: isig, Fabric: fabric, FabricSignature: fsig}, tp)
	if err != nil {
		t.Fatal(err)
	}
	return v
}

func preparePDIntent(t *testing.T, ctx context.Context, db *sql.DB) (*Repository, PDIntentOwner, pd.VerifiedProposal) {
	t.Helper()
	repo, err := NewRepository(db)
	if err != nil {
		t.Fatal(err)
	}
	provider := providerRegistrationFixture(t)
	if _, _, err := repo.RegisterProvider(ctx, provider); err != nil {
		t.Fatal(err)
	}
	params := acceptanceFixture(t, "tenant-pd-"+randomSuffix(t), "workload-"+randomSuffix(t), "op-"+randomSuffix(t), "event-"+randomSuffix(t))
	params.ProviderID, params.Operation.ProviderID = provider.ProviderID, provider.ProviderID
	w, created, err := repo.AcceptWorkload(ctx, params)
	if err != nil || !created {
		t.Fatalf("accept workload: %t %v", created, err)
	}
	owner := PDIntentOwner{TenantID: params.Workload.TenantID, ProjectID: params.Workload.ProjectID, Service: params.Workload.OwnerService, ResourceType: params.Workload.OwnerResourceType, ResourceID: params.Workload.OwnerResourceID}
	var now time.Time
	if err := db.QueryRowContext(ctx, "SELECT clock_timestamp()").Scan(&now); err != nil {
		t.Fatal(err)
	}
	scope := pd.Scope{Tenant: owner.TenantID, Project: owner.ProjectID, Pool: params.PoolID, Model: "model", GPU: "gpu", Shape: "shape", Fabric: "fabric"}
	return repo, owner, pdIntentFixture(t, now, scope, w.Workload.WorkloadID, provider.ProviderID)
}

func seedPDIntentFence(t *testing.T, ctx context.Context, db *sql.DB, v pd.VerifiedProposal) {
	t.Helper()
	c := v.Candidate()
	b, err := json.Marshal(c)
	if err != nil {
		t.Fatal(err)
	}
	_, err = db.ExecContext(ctx, `INSERT INTO public.compute_pd_intent_fences
		(workload_id,tenant_id,project_id,provider_id,target_generation,candidate,fence_version,valid_until)
		VALUES($1,$2,$3,$4,$5,$6::jsonb,1,$7)`, c.Workload, c.Scope.Tenant, c.Scope.Project, v.ProviderID(), c.Generation, b, v.ValidUntil())
	if err != nil {
		t.Fatal(err)
	}
}

func TestPDIntentDurableCASReplayAndNonActuatingOutbox(t *testing.T) {
	db := openIntegrationDB(t)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	repo, owner, v := preparePDIntent(t, ctx, db)
	c := v.Candidate()
	if _, _, err := repo.RecordPDIntent(ctx, owner, v, "missing-fence"); !errors.Is(err, ErrPDIntentFence) {
		t.Fatalf("missing authoritative fence accepted: %v", err)
	}
	seedPDIntentFence(t, ctx, db, v)
	wrongOwner := owner
	wrongOwner.ProjectID += "-wrong"
	if _, _, err := repo.RecordPDIntent(ctx, wrongOwner, v, "wrong-owner"); err == nil {
		t.Fatal("cross-project request accepted")
	}
	var wg sync.WaitGroup
	type result struct {
		record  PDIntentRecord
		created bool
		err     error
	}
	results := make(chan result, 8)
	for range 8 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			r, created, err := repo.RecordPDIntent(ctx, owner, v, "same-intent")
			results <- result{r, created, err}
		}()
	}
	wg.Wait()
	close(results)
	count := 0
	var first PDIntentRecord
	for x := range results {
		if x.err != nil || x.record.Candidate != c {
			t.Fatalf("record: %+v %v", x.record, x.err)
		}
		if x.created {
			first = x.record
			count++
		}
	}
	if count != 1 {
		t.Fatalf("created %d records", count)
	}
	reopened, _ := NewRepository(db)
	got, err := reopened.GetRecordedPDIntent(ctx, owner, c.Workload, c.Generation, c.IntentID)
	if err != nil || got.Candidate != first.Candidate || got.EventID != first.EventID {
		t.Fatalf("durable lookup: %+v %v", got, err)
	}
	other := pdIntentFixture(t, time.Now(), c.Scope, c.Workload, v.ProviderID())
	if _, _, err := repo.RecordPDIntent(ctx, owner, other, "conflict"); !errors.Is(err, ErrIdempotencyConflict) {
		t.Fatalf("same generation different digest: %v", err)
	}
	var generation, intents, events, allocations, operations int
	err = db.QueryRowContext(ctx, `SELECT desired_generation,
		(SELECT count(*) FROM public.compute_pd_intents WHERE workload_id=$1),
		(SELECT count(*) FROM public.compute_outbox_events WHERE event_id=$2 AND event_type='compute.pd.intent.recorded.v1'),
		(SELECT count(*) FROM public.compute_allocations WHERE workload_id=$1),
		(SELECT count(*) FROM public.compute_operations WHERE workload_id=$1)
		FROM public.compute_workloads WHERE workload_id=$1`, c.Workload, first.EventID).Scan(&generation, &intents, &events, &allocations, &operations)
	if err != nil || generation != 1 || intents != 1 || events != 1 || allocations != 1 || operations != 1 {
		t.Fatalf("generation/intents/events/allocations/operations=%d/%d/%d/%d/%d err=%v", generation, intents, events, allocations, operations, err)
	}
}

func TestPDIntentRejectsRevokedFenceAndAdvancedGeneration(t *testing.T) {
	db := openIntegrationDB(t)
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	for _, mode := range []string{"revoked", "advanced", "expired", "outbox_conflict"} {
		t.Run(mode, func(t *testing.T) {
			repo, owner, v := preparePDIntent(t, ctx, db)
			c := v.Candidate()
			seedPDIntentFence(t, ctx, db, v)
			var err error
			switch mode {
			case "revoked":
				_, err = db.ExecContext(ctx, "UPDATE public.compute_pd_intent_fences SET revoked=true,fence_version=2 WHERE workload_id=$1", c.Workload)
			case "advanced":
				_, err = db.ExecContext(ctx, "UPDATE public.compute_workloads SET desired_generation=2,version=version+1 WHERE workload_id=$1", c.Workload)
			case "expired":
				_, err = db.ExecContext(ctx, "UPDATE public.compute_pd_intent_fences SET valid_until=clock_timestamp()-interval '1 second',fence_version=2 WHERE workload_id=$1", c.Workload)
			case "outbox_conflict":
				_, err = db.ExecContext(ctx, `INSERT INTO public.compute_outbox_events(event_id,producer,event_type,schema_version,tenant_id,aggregate_type,aggregate_id,aggregate_version,occurred_at,correlation_id,payload)
					VALUES($1,'fixture','fixture',1,$2,'fixture',$1,1,clock_timestamp(),'fixture','{}'::jsonb)`, "pd-intent-"+c.IntentID, owner.TenantID)
			}
			if err != nil {
				t.Fatal(err)
			}
			if _, _, err := repo.RecordPDIntent(ctx, owner, v, mode); err == nil {
				t.Fatal("stale or incomplete intent accepted")
			}
			var count int
			if err := db.QueryRowContext(ctx, "SELECT count(*) FROM public.compute_pd_intents WHERE workload_id=$1", c.Workload).Scan(&count); err != nil || count != 0 {
				t.Fatalf("partial acceptance: %d %v", count, err)
			}
		})
	}
}
