// Package identity defines trusted caller identities resolved from verified
// workload credentials and an authoritative service identity directory.
package identity

// WorkloadPrincipal is an authenticated service identity. Tenant and Project
// grants must come from the identity directory; never populate them from an
// HTTP request body or an unverified token/certificate claim.
type WorkloadPrincipal struct {
	TenantID   string
	Subject    string
	Service    string
	ProjectIDs []string
}
