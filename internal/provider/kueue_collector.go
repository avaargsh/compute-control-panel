// Package provider contains provider-neutral operation contracts and strictly
// bounded observation helpers. This file does not perform Kubernetes I/O.
package provider

import (
	"context"
	"strings"
	"sync"
	"time"
)

// Collector reads one built-in Kueue Job observation. Implementations must
// not create, update, or delete Kubernetes objects, mark an Allocation READY,
// or claim device assignment. A successful collection is structural evidence
// plus reader provenance, not a capacity or readiness decision.
type Collector interface {
	Collect(context.Context, KueueJobQuery) (KueueJobCollection, error)
}

// KueueJobQuery is the caller's expected Job identity. Namespace and JobName
// address the object. ExpectedJobUID pins the immutable Job UID so a later
// object that reuses the name cannot satisfy an older observation. The query
// does not authorize a Kubernetes write or an Allocation transition.
type KueueJobQuery struct {
	TenantID          string
	WorkloadID        string
	DesiredGeneration uint64
	Namespace         string
	JobName           string
	ExpectedJobUID    string
}

// KueueEvidenceProvenance identifies the reader that produced a collection.
// Source and AdapterVersion are collection labels. They do not attest cluster
// authentication, RBAC, quota release, or device assignment.
type KueueEvidenceProvenance struct {
	Source         string
	AdapterVersion string
	ObservedAt     time.Time
}

// KueueJobCollection is a read-only Job/Kueue/Pod snapshot plus the provenance
// of that read. Stage is the structural result of AssessKueueJobObservation.
// KueuePodPlaced does not mean Allocation READY or device assignment.
type KueueJobCollection struct {
	Evidence   KueueJobEvidence
	Provenance KueueEvidenceProvenance
	Stage      KueueObservationStage
}

func validObservationLabel(value string) bool {
	return value != "" && value == strings.TrimSpace(value) &&
		!strings.ContainsRune(value, 0) && len(value) <= 256
}

func (q KueueJobQuery) validate() error {
	if !validObservationLabel(q.TenantID) || !validObservationLabel(q.WorkloadID) ||
		!validObservationLabel(q.Namespace) || !validObservationLabel(q.JobName) ||
		!validObservationLabel(q.ExpectedJobUID) || q.DesiredGeneration == 0 {
		return ErrInvalidKueueObservation
	}
	return nil
}

func (p KueueEvidenceProvenance) validate() error {
	if !validObservationLabel(p.Source) || !validObservationLabel(p.AdapterVersion) ||
		p.ObservedAt.IsZero() {
		return ErrInvalidKueueObservation
	}
	return nil
}

// bindKueueJobCollection returns the observed snapshot only when its canonical
// identity, Job UID, and classifier result all agree with the query. A
// contradiction returns UNPROVEN, no evidence, no provenance, and
// ErrInvalidKueueObservation. It performs no Kubernetes I/O and does not
// rewrite the snapshot to force a match.
func bindKueueJobCollection(
	q KueueJobQuery, observed KueueJobEvidence, provenance KueueEvidenceProvenance,
) (KueueJobCollection, error) {
	reject := func() (KueueJobCollection, error) {
		return KueueJobCollection{Stage: KueueUnproven}, ErrInvalidKueueObservation
	}
	if q.validate() != nil || provenance.validate() != nil {
		return reject()
	}
	if observed.TenantID != q.TenantID || observed.WorkloadID != q.WorkloadID ||
		observed.DesiredGeneration != q.DesiredGeneration ||
		observed.Job.Namespace != q.Namespace || observed.Job.Name != q.JobName ||
		observed.Job.UID != q.ExpectedJobUID ||
		observed.JobTenantID != q.TenantID || observed.JobWorkloadID != q.WorkloadID ||
		observed.JobDesiredGeneration != q.DesiredGeneration {
		return reject()
	}
	stage, err := AssessKueueJobObservation(observed)
	if err != nil {
		return reject()
	}
	return KueueJobCollection{
		Evidence: observed, Provenance: provenance, Stage: stage,
	}, nil
}

type kueueJobKey struct {
	namespace string
	name      string
}

// FakeCollector is an in-memory Collector for tests. It must never be used as
// production authoritative Kubernetes, Kueue, or device evidence.
type FakeCollector struct {
	mu             sync.Mutex
	source         string
	adapterVersion string
	now            func() time.Time
	jobs           map[kueueJobKey]KueueJobEvidence
}

// NewFakeCollector records the provenance labels stamped on later collections.
// Blank labels are rejected so a collection cannot look sourced when it is not.
func NewFakeCollector(source, adapterVersion string) (*FakeCollector, error) {
	if !validObservationLabel(source) || !validObservationLabel(adapterVersion) {
		return nil, ErrInvalidKueueObservation
	}
	return &FakeCollector{
		source: source, adapterVersion: adapterVersion,
		now:  func() time.Time { return time.Now().UTC() },
		jobs: make(map[kueueJobKey]KueueJobEvidence),
	}, nil
}

// SetClock overrides the observation clock. A nil clock is ignored.
func (f *FakeCollector) SetClock(now func() time.Time) {
	if f == nil || now == nil {
		return
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	f.now = now
}

// Seed stores one observed Job snapshot addressed by its namespace and name.
// Seeding does not classify the snapshot or promote status. A later Collect
// fails closed when that snapshot does not match the query. Seeding the same
// namespace and name again replaces the previous snapshot, which is how a
// successor Job UID is represented.
func (f *FakeCollector) Seed(observed KueueJobEvidence) error {
	if f == nil || observed.Job.Namespace == "" || observed.Job.Name == "" ||
		observed.Job.Namespace != strings.TrimSpace(observed.Job.Namespace) ||
		observed.Job.Name != strings.TrimSpace(observed.Job.Name) ||
		strings.ContainsRune(observed.Job.Namespace, 0) ||
		strings.ContainsRune(observed.Job.Name, 0) {
		return ErrInvalidKueueObservation
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	f.jobs[kueueJobKey{namespace: observed.Job.Namespace, name: observed.Job.Name}] = observed
	return nil
}

// Collect returns a populated KueueJobEvidence and provenance when the seeded
// snapshot matches the query and passes AssessKueueJobObservation. Identity
// mismatches, missing snapshots, and incomplete provenance return UNPROVEN
// with ErrInvalidKueueObservation and no evidence. The returned stage is not
// an Allocation phase and does not claim device assignment.
func (f *FakeCollector) Collect(ctx context.Context, q KueueJobQuery) (KueueJobCollection, error) {
	if err := ctx.Err(); err != nil {
		return KueueJobCollection{}, err
	}
	if f == nil {
		return KueueJobCollection{Stage: KueueUnproven}, ErrInvalidKueueObservation
	}
	f.mu.Lock()
	observed, ok := f.jobs[kueueJobKey{namespace: q.Namespace, name: q.JobName}]
	now := f.now
	source, version := f.source, f.adapterVersion
	f.mu.Unlock()
	if !ok {
		return KueueJobCollection{Stage: KueueUnproven}, ErrInvalidKueueObservation
	}
	return bindKueueJobCollection(q, observed, KueueEvidenceProvenance{
		Source: source, AdapterVersion: version, ObservedAt: now().UTC(),
	})
}

var _ Collector = (*FakeCollector)(nil)
