// Package provider contains provider-neutral operation contracts and strictly
// bounded observation helpers.
package provider

import (
	"context"
	"errors"
	"fmt"
	"time"
)

// ErrCollectionFailed is returned when evidence cannot be collected in a
// fail-closed manner. Callers must not treat a partial result as valid.
var ErrCollectionFailed = errors.New("kueue evidence collection failed")

// CollectionProvenance records the source and freshness of a collected
// observation. It does not attest authenticity; that remains a separate gate.
type CollectionProvenance struct {
	Source           string    // e.g. "fake", "envtest", "cluster"
	ObservedAt       time.Time
	ResourceVersions map[string]string // optional kind -> resourceVersion
}

// KueueEvidenceResult is the output of a successful collection.
// The Evidence is only valid if AssessKueueJobObservation accepts it.
type KueueEvidenceResult struct {
	Evidence   KueueJobEvidence
	Provenance CollectionProvenance
}

// KueueCollector is a read-only evidence collector for the built-in Kueue
// Job integration. Implementations must not mutate Kubernetes or any
// control-plane state. Identity contradictions must fail closed.
type KueueCollector interface {
	// Collect fills a KueueJobEvidence for the given identifiers.
	// It returns ErrCollectionFailed (or a wrapped error) on any identity
	// mismatch, missing required object, or untrusted source.
	Collect(ctx context.Context, tenantID, workloadID string, desiredGeneration uint64, namespace, jobName string) (*KueueEvidenceResult, error)
}

// FakeCollector is an in-memory, test-only implementation.
// It never performs real Kubernetes I/O.
type FakeCollector struct {
	// EvidenceByKey maps a composite key to a pre-canned result.
	// Key format: tenantID|workloadID|generation|namespace|jobName
	EvidenceByKey map[string]*KueueEvidenceResult
	// FailKeys force a collection error for matching keys.
	FailKeys map[string]error
}

// NewFakeCollector returns a ready-to-use fake with empty maps.
func NewFakeCollector() *FakeCollector {
	return &FakeCollector{
		EvidenceByKey: make(map[string]*KueueEvidenceResult),
		FailKeys:      make(map[string]error),
	}
}

func (f *FakeCollector) key(tenantID, workloadID string, gen uint64, ns, name string) string {
	return fmt.Sprintf("%s|%s|%d|%s|%s", tenantID, workloadID, gen, ns, name)
}

// Collect implements KueueCollector.
func (f *FakeCollector) Collect(ctx context.Context, tenantID, workloadID string, desiredGeneration uint64, namespace, jobName string) (*KueueEvidenceResult, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	k := f.key(tenantID, workloadID, desiredGeneration, namespace, jobName)
	if err, ok := f.FailKeys[k]; ok {
		return nil, fmt.Errorf("%w: %v", ErrCollectionFailed, err)
	}
	res, ok := f.EvidenceByKey[k]
	if !ok {
		return nil, fmt.Errorf("%w: no canned evidence for key %s", ErrCollectionFailed, k)
	}
	// Defensive copy of provenance time.
	out := *res
	out.Provenance.ObservedAt = res.Provenance.ObservedAt
	return &out, nil
}
