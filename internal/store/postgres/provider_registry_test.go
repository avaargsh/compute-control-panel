package postgres

import (
	"context"
	"errors"
	"strings"
	"sync"
	"testing"
	"time"
)

func providerRegistrationFixture(t *testing.T) ProviderRegistration {
	t.Helper()
	return ProviderRegistration{
		ProviderID:       "provider-reg-" + randomSuffix(t),
		ClusterScope:     "cluster-staging-1",
		AdapterSource:    "kubernetes",
		AdapterVersion:   "kube-adapter/v1",
		CapabilityDigest: "sha256:" + strings.Repeat("a", 64),
	}
}

func TestProviderRegistryRegisterReplayConflictAndDrain(t *testing.T) {
	db := openIntegrationDB(t)
	repo, err := NewRepository(db)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	p := providerRegistrationFixture(t)
	first, created, err := repo.RegisterProvider(ctx, p)
	if err != nil || !created || first.State != ProviderRegistered || first.Version != 1 {
		t.Fatalf("registration: %+v, created=%v, err=%v", first, created, err)
	}
	second, created, err := repo.RegisterProvider(ctx, p)
	if err != nil || created || second.ProviderID != first.ProviderID || second.Version != 1 {
		t.Fatalf("exact registration replay: %+v, created=%v, err=%v", second, created, err)
	}
	conflict := p
	conflict.CapabilityDigest = "sha256:" + strings.Repeat("b", 64)
	if _, _, err := repo.RegisterProvider(ctx, conflict); !errors.Is(err, ErrProviderConflict) {
		t.Fatalf("changed capabilities unexpectedly overwrote registry: %v", err)
	}
	conflict = p
	conflict.ClusterScope = "other-cluster"
	if _, _, err := repo.RegisterProvider(ctx, conflict); !errors.Is(err, ErrProviderConflict) {
		t.Fatalf("changed cluster unexpectedly reused registration: %v", err)
	}

	drained, err := repo.BeginProviderDrain(ctx, p.ProviderID, 1, "PLANNED_MAINTENANCE")
	if err != nil || drained.State != ProviderDraining || drained.Version != 2 {
		t.Fatalf("BeginProviderDrain: %+v, %v", drained, err)
	}
	otherRepo, err := NewRepository(db)
	if err != nil {
		t.Fatal(err)
	}
	persisted, err := otherRepo.GetProvider(ctx, p.ProviderID)
	if err != nil || persisted.State != ProviderDraining || persisted.Version != drained.Version {
		t.Fatalf("durable drain not visible after restart: %+v, %v", persisted, err)
	}
	if _, err := repo.BeginProviderDrain(ctx, p.ProviderID, 1, "RETRY"); !errors.Is(err, ErrProviderStale) {
		t.Fatalf("stale owner unexpectedly changed version: %v", err)
	}
	if _, err := repo.BeginProviderDrain(ctx, p.ProviderID, 2, "RETRY"); !errors.Is(err, ErrProviderTransition) {
		t.Fatalf("already-draining Provider was changed again: %v", err)
	}
	replay, created, err := repo.RegisterProvider(ctx, p)
	if err != nil || created || replay.State != ProviderDraining || replay.Version != 2 {
		t.Fatalf("replay resurrected draining Provider: %+v, created=%v, err=%v", replay, created, err)
	}
	var auditCount int
	if err := db.QueryRowContext(ctx,
		"SELECT count(*) FROM compute_provider_lifecycle_audit WHERE provider_id=$1",
		p.ProviderID).Scan(&auditCount); err != nil {
		t.Fatal(err)
	}
	if auditCount != 2 {
		t.Fatalf("audit count = %d, want exact REGISTERED and DRAINING events", auditCount)
	}
}

func TestProviderRegistryConcurrentDrainHasOneWinner(t *testing.T) {
	db := openIntegrationDB(t)
	repo, _ := NewRepository(db)
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	p := providerRegistrationFixture(t)
	if _, created, err := repo.RegisterProvider(ctx, p); err != nil || !created {
		t.Fatalf("register: created=%v err=%v", created, err)
	}

	const parallel = 8
	errs := make([]error, parallel)
	var wg sync.WaitGroup
	for i := range errs {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			_, errs[i] = repo.BeginProviderDrain(ctx, p.ProviderID, 1, "DRAIN_REQUEST")
		}(i)
	}
	wg.Wait()
	winners := 0
	for _, err := range errs {
		switch {
		case err == nil:
			winners++
		case errors.Is(err, ErrProviderStale):
		default:
			t.Fatalf("concurrent CAS returned unexpected error: %v", err)
		}
	}
	if winners != 1 {
		t.Fatalf("concurrent drain winners = %d, want 1", winners)
	}
}

func TestProviderRegistryDatabaseRejectsUnverifiedReadinessAndDeletion(t *testing.T) {
	db := openIntegrationDB(t)
	repo, _ := NewRepository(db)
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	p := providerRegistrationFixture(t)
	if _, _, err := repo.RegisterProvider(ctx, p); err != nil {
		t.Fatal(err)
	}

	// This slice must not expose a way to mark READY or RETIRED based only on
	// caller-authored labels. A future authenticated adapter needs independent
	// health and release evidence before those transitions are authorized.
	if _, err := db.ExecContext(ctx,
		"UPDATE compute_providers SET state='READY', version=2 WHERE provider_id=$1",
		p.ProviderID); err == nil {
		t.Fatal("direct SQL promoted unverified Provider to READY")
	}
	if _, err := db.ExecContext(ctx,
		"DELETE FROM compute_providers WHERE provider_id=$1", p.ProviderID); err == nil {
		t.Fatal("Provider deletion bypassed lifecycle retention")
	}
	current, err := repo.GetProvider(ctx, p.ProviderID)
	if err != nil || current.State != ProviderRegistered || current.Version != 1 {
		t.Fatalf("unauthorized mutation changed registry: %+v %v", current, err)
	}
	var count int
	if err := db.QueryRowContext(ctx, "SELECT count(*) FROM compute_provider_lifecycle_audit WHERE provider_id=$1", p.ProviderID).Scan(&count); err != nil {
		t.Fatal(err)
	}
	if count != 1 {
		t.Fatalf("rejected mutations polluted audit: %d", count)
	}
}

func TestProviderRegistryRejectsInvalidInputsBeforePersistence(t *testing.T) {
	db := openIntegrationDB(t)
	repo, _ := NewRepository(db)
	ctx := context.Background()
	p := providerRegistrationFixture(t)
	for _, mutate := range []func(*ProviderRegistration){
		func(p *ProviderRegistration) { p.ProviderID = " " },
		func(p *ProviderRegistration) { p.ClusterScope = "" },
		func(p *ProviderRegistration) { p.AdapterSource = " kubernetes" },
		func(p *ProviderRegistration) { p.AdapterVersion = "" },
		func(p *ProviderRegistration) { p.CapabilityDigest = "sha256:not-a-digest" },
	} {
		invalid := p
		mutate(&invalid)
		if _, _, err := repo.RegisterProvider(ctx, invalid); !errors.Is(err, ErrProviderInvalid) {
			t.Fatalf("invalid registration was accepted: %+v, err=%v", invalid, err)
		}
	}
	if _, err := repo.BeginProviderDrain(ctx, p.ProviderID, 0, "VALID"); !errors.Is(err, ErrProviderInvalid) {
		t.Fatalf("zero version accepted: %v", err)
	}
	if _, err := repo.BeginProviderDrain(ctx, p.ProviderID, 1, " "); !errors.Is(err, ErrProviderInvalid) {
		t.Fatalf("blank audit reason accepted: %v", err)
	}
	if _, err := repo.GetProvider(ctx, " "); !errors.Is(err, ErrProviderInvalid) {
		t.Fatalf("blank provider read accepted: %v", err)
	}
}
