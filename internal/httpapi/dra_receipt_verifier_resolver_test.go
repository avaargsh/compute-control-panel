package httpapi

import (
	"bytes"
	"context"
	"crypto/ed25519"
	"errors"
	"testing"
	"time"

	"github.com/avaargsh/compute-control-panel/internal/provider"
)

type registeredDRAReceiptVerifierSourceStub struct {
	calls    int
	tenantID string
	event    provider.DRAUnprepareEvent
	verifier *provider.DRAUnprepareVerifier
	database time.Time
	err      error
}

func (s *registeredDRAReceiptVerifierSourceStub) ResolveRegisteredDRAUnprepareVerifier(
	_ context.Context, tenantID string, event provider.DRAUnprepareEvent,
) (*provider.DRAUnprepareVerifier, time.Time, error) {
	s.calls++
	s.tenantID = tenantID
	s.event = event
	return s.verifier, s.database, s.err
}

func registeredDRAReceiptTestVerifier(t *testing.T) *provider.DRAUnprepareVerifier {
	t.Helper()
	private := ed25519.NewKeyFromSeed(bytes.Repeat([]byte{7}, ed25519.SeedSize))
	verifier, err := provider.NewDRAUnprepareVerifier(provider.DRAUnprepareAuthority{
		ClusterID: "cluster-a", SourceID: "source-a", KeyID: "key-a",
		Driver: "gpu.example.com", PublicKey: private.Public().(ed25519.PublicKey),
	}, time.Minute, 10*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	return verifier
}

func registeredDRAReceiptTestPrincipal() WorkloadPrincipal {
	return WorkloadPrincipal{
		Subject:  "spiffe://example.org/ns/dra/sa/driver",
		TenantID: "tenant-a",
		Service:  "dra-node-driver",
	}
}

func TestRegisteredDRAReceiptVerifierResolverUsesAuthenticatedTenantAndDatabaseClock(t *testing.T) {
	databaseNow := time.Date(2026, 11, 24, 9, 0, 0, 0, time.UTC)
	source := &registeredDRAReceiptVerifierSourceStub{
		verifier: registeredDRAReceiptTestVerifier(t),
		database: databaseNow,
	}
	resolver := &RegisteredDRAReceiptVerifierResolver{source: source}
	event := provider.DRAUnprepareEvent{ClusterID: "cluster-a", SourceID: "source-a", KeyID: "key-a"}
	got, err := resolver.ResolveDRAReceiptVerifier(context.Background(), registeredDRAReceiptTestPrincipal(), event)
	if err != nil {
		t.Fatal(err)
	}
	if source.calls != 1 || source.tenantID != "tenant-a" || source.event != event {
		t.Fatalf("registry lookup did not use authenticated scope: calls=%d tenant=%q event=%+v", source.calls, source.tenantID, source.event)
	}
	if got.Verifier != source.verifier || !got.DatabaseNow.Equal(databaseNow) {
		t.Fatalf("resolver changed verifier/DB clock: got=%+v", got)
	}
}

func TestRegisteredDRAReceiptVerifierResolverRejectsNonDriverPrincipalBeforeLookup(t *testing.T) {
	source := &registeredDRAReceiptVerifierSourceStub{verifier: registeredDRAReceiptTestVerifier(t), database: time.Now().UTC()}
	resolver := &RegisteredDRAReceiptVerifierResolver{source: source}
	principal := registeredDRAReceiptTestPrincipal()
	principal.Service = "agent-runtime"
	if _, err := resolver.ResolveDRAReceiptVerifier(context.Background(), principal, provider.DRAUnprepareEvent{}); !errors.Is(err, provider.ErrDRAReceiptSourceUntrusted) {
		t.Fatalf("non-driver principal was accepted: %v", err)
	}
	if source.calls != 0 {
		t.Fatalf("non-driver principal reached source registry: calls=%d", source.calls)
	}
}

func TestRegisteredDRAReceiptVerifierResolverRejectsMissingDatabaseClock(t *testing.T) {
	source := &registeredDRAReceiptVerifierSourceStub{verifier: registeredDRAReceiptTestVerifier(t)}
	resolver := &RegisteredDRAReceiptVerifierResolver{source: source}
	if _, err := resolver.ResolveDRAReceiptVerifier(context.Background(), registeredDRAReceiptTestPrincipal(), provider.DRAUnprepareEvent{}); !errors.Is(err, provider.ErrDRAReceiptJournalUnavailable) {
		t.Fatalf("missing registry clock did not fail closed: %v", err)
	}
}

func TestRegisteredDRAReceiptVerifierResolverPropagatesRegistryTrustFailure(t *testing.T) {
	source := &registeredDRAReceiptVerifierSourceStub{err: provider.ErrDRAReceiptSourceUntrusted}
	resolver := &RegisteredDRAReceiptVerifierResolver{source: source}
	if _, err := resolver.ResolveDRAReceiptVerifier(context.Background(), registeredDRAReceiptTestPrincipal(), provider.DRAUnprepareEvent{}); !errors.Is(err, provider.ErrDRAReceiptSourceUntrusted) {
		t.Fatalf("registry trust failure was not preserved: %v", err)
	}
}

func TestNewRegisteredDRAReceiptVerifierResolverRejectsNilSource(t *testing.T) {
	if resolver, err := NewRegisteredDRAReceiptVerifierResolver(nil); err == nil || resolver != nil {
		t.Fatalf("nil journal created resolver: resolver=%v err=%v", resolver, err)
	}
}
