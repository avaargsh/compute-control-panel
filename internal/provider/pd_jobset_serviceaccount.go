package provider

// NewServiceAccountPDJobSetAdapter is the in-cluster bootstrap for the
// deliberately inert JobSet CREATE adapter. It reuses the existing pinned
// Kubernetes CA/rotating ServiceAccount token client and the independently
// attested least-privilege PostgreSQL Kueue UID binding login. A missing,
// unreachable or overprivileged database connection fails before any
// Kubernetes side effect, with NO memory-store fallback.
//
// The caller must load PDJobSetPlan from the immutable, authorized PD Intent
// and bind the Provider identity and current Workload generation independently.
// This constructor does NOT authenticate the plan's source, issue an
// activation permit, or prove Kubernetes RBAC itself. The staging ServiceAccount
// must have only CREATE/GET on namespaced jobsets.jobset.x-k8s.io and no PATCH,
// UPDATE or DELETE. Its inability to remove AdmissionGatedBy is tested with
// an actual JobSet/Kueue controller in the disposable Kind acceptance.
//
// The JobSet stays suspended/admission-gated until a separately authorized
// activator removes the gate after current, revocable policy verification.
// Neither construction nor Apply is GPU READY, Driver Prepare, usage, release,
// or cost FINAL evidence.
func NewServiceAccountPDJobSetAdapter(
	apiURL, caPath, tokenPath, namespace, localQueue, bindingDSN string,
	plan PDJobSetPlan,
) (*PDJobSetAdapter, error) {
	if !validKubeName(namespace) || !validKubeName(localQueue) ||
		!validPDJobSetPlan(plan) {
		return nil, ErrInvalidRequest
	}
	wire, err := NewServiceAccountKueueJobAdapter(
		apiURL, caPath, tokenPath, namespace, bindingDSN,
	)
	if err != nil {
		return nil, err
	}
	return &PDJobSetAdapter{
		wire:       wire,
		localQueue: localQueue,
		plan:       plan,
	}, nil
}
