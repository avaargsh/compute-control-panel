package httpapi

import (
	"bytes"
	"context"
	"crypto/ed25519"
	"crypto/tls"
	"encoding/json"
	"errors"
	"io"
	"mime"
	"net/http"
	"net/url"
	"reflect"
	"strings"
	"time"

	"github.com/avaargsh/compute-control-panel/internal/provider"
)

const (
	draReceiptIngestPath = "/internal/v1/dra/node-unprepare-receipts"
	maxDRAReceiptBody    = 64 << 10
)

// DRAReceiptIngestGrant independently binds a verified SPIFFE driver identity
// to exactly one cluster, signer, driver and node. A tenant or Project grant
// alone must never authorize custom NodeUnprepare receipt ingestion.
type DRAReceiptIngestGrant struct {
	Subject   string
	TenantID  string
	Service   string
	ClusterID string
	SourceID  string
	Driver    string
	NodeName  string
}

var ErrDRAReceiptIngressDenied = errors.New("DRA receipt source identity is not authorized")

type DRAReceiptIngestAuthorizer interface {
	AuthorizeDRAReceipt(context.Context, WorkloadPrincipal, provider.DRAUnprepareEvent) error
}

type ExactDRAReceiptIngestAuthorizer struct {
	grants map[DRAReceiptIngestGrant]struct{}
}

func validDRAReceiptIngestGrant(g DRAReceiptIngestGrant) bool {
	if g.Service != "dra-node-driver" ||
		!validProviderManageLabel(g.TenantID) ||
		!validProviderManageID(g.ClusterID) || !validProviderManageID(g.SourceID) ||
		!validProviderManageID(g.Driver) || !validProviderManageID(g.NodeName) ||
		strings.ContainsAny(g.TenantID, "*?") {
		return false
	}
	uri, err := url.Parse(g.Subject)
	if err != nil || uri.Scheme != "spiffe" || uri.Host == "" || uri.Host != strings.ToLower(uri.Host) ||
		uri.String() != g.Subject {
		return false
	}
	id, err := uniqueSPIFFEID([]*url.URL{uri}, uri.Host)
	return err == nil && id == g.Subject
}

func NewExactDRAReceiptIngestAuthorizer(grants []DRAReceiptIngestGrant) (*ExactDRAReceiptIngestAuthorizer, error) {
	if len(grants) == 0 {
		return nil, ErrDRAReceiptIngressDenied
	}
	p := &ExactDRAReceiptIngestAuthorizer{grants: make(map[DRAReceiptIngestGrant]struct{}, len(grants))}
	for _, g := range grants {
		if !validDRAReceiptIngestGrant(g) {
			return nil, ErrDRAReceiptIngressDenied
		}
		if _, exists := p.grants[g]; exists {
			return nil, ErrDRAReceiptIngressDenied
		}
		p.grants[g] = struct{}{}
	}
	return p, nil
}

func (p *ExactDRAReceiptIngestAuthorizer) AuthorizeDRAReceipt(_ context.Context, principal WorkloadPrincipal, e provider.DRAUnprepareEvent) error {
	if p == nil || !validWorkloadPrincipal(principal) {
		return ErrDRAReceiptIngressDenied
	}
	scope := DRAReceiptIngestGrant{
		Subject: principal.Subject, TenantID: principal.TenantID,
		Service: principal.Service, ClusterID: e.ClusterID, SourceID: e.SourceID,
		Driver: e.Driver, NodeName: e.NodeName,
	}
	if _, found := p.grants[scope]; !found {
		return ErrDRAReceiptIngressDenied
	}
	return nil
}

// DRAReceiptVerifierResolver resolves ONLY the DBA-provisioned signer key
// and freshness policy for the authenticated principal/event scope. It must
// not query historical Kubernetes/Allocation evidence. The handler verifies
// the envelope before invoking DRAReceiptEvidenceResolver.
type DRAReceiptVerifierResolver interface {
	ResolveDRAReceiptVerifier(context.Context, WorkloadPrincipal, provider.DRAUnprepareEvent) (*provider.DRAUnprepareVerifier, error)
}

// DRAReceiptEvidenceResolver resolves historical CREATE/DELETE, Provider
// absence, Kueue/Pod and ResourceClaim evidence from authenticated,
// server-controlled stores/collectors. It is called only after the signature,
// source scope and freshness have been verified. Event fields are index hints,
// never authority or evidence supplied by the HTTP caller.
type DRAReceiptEvidenceResolver interface {
	ResolveDRAReceiptEvidence(context.Context, WorkloadPrincipal, provider.DRAUnprepareEvent) (provider.ReleaseEvidenceCandidate, error)
}

// DRAReceiptRestrictedJournal is only implemented by the production journal
// through its fail-closed VerifyAndRecordRestricted entrypoint. Plain
// NewDRAUnprepareReceiptJournal(db) must never satisfy this as an authority.
type DRAReceiptRestrictedJournal interface {
	VerifyAndRecordRestricted(context.Context, *provider.DRAUnprepareVerifier, provider.ReleaseEvidenceCandidate, provider.SignedDRAUnprepare, time.Time) (bool, error)
}

type DRAReceiptIngestDependencies struct {
	Authenticator   WorkloadAuthenticator
	Authorizer      DRAReceiptIngestAuthorizer
	VerifierResolver DRAReceiptVerifierResolver
	Resolver        DRAReceiptEvidenceResolver
	Journal         DRAReceiptRestrictedJournal
}

type draReceiptIngestHandler struct {
	deps DRAReceiptIngestDependencies
}

// NewDRAReceiptIngestHandler returns a standalone internal endpoint. It is
// intentionally NOT mounted on the normal CCP API mux. Deployment must use
// tls.RequireAndVerifyClientCert with narrow ClientCAs, independently
// provisioned SPIFFE identity-directory grants and restricted PG journal DSN.
// This provides an evidence-only transport boundary; it does not establish
// that a real DRA driver executed NodeUnprepareResources or release a GPU.
func NewDRAReceiptIngestHandler(deps DRAReceiptIngestDependencies) (http.Handler, error) {
	// An arbitrary Authenticator could claim a Subject unrelated to the
	// TLS peer. Only the built-in SPIFFE implementation checks the verified
	// URI SAN against the independently resolved directory Subject.
	identity, ok := deps.Authenticator.(*SPIFFEWorkloadAuthenticator)
	if !ok || identity == nil || identity.trustDomain == "" ||
		missingProviderManagementDependency(identity.resolver) {
		return nil, errors.New("DRA receipt ingress requires the verified SPIFFE workload authenticator")
	}
	if missingProviderManagementDependency(deps.Authenticator) ||
		missingProviderManagementDependency(deps.Authorizer) ||
		missingProviderManagementDependency(deps.VerifierResolver) ||
		missingProviderManagementDependency(deps.Resolver) ||
		missingProviderManagementDependency(deps.Journal) {
		return nil, errors.New("DRA receipt ingress requires verified authentication, source authorization, signer resolver, historical evidence resolver and restricted journal")
	}
	h := &draReceiptIngestHandler{deps: deps}
	mux := http.NewServeMux()
	mux.HandleFunc("POST "+draReceiptIngestPath, h.ingest)
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Cache-Control", "no-store")
		mux.ServeHTTP(w, r)
	}), nil
}

func verifiedDRAClientTLS(r *http.Request) bool {
	if r == nil || r.TLS == nil || r.TLS.Version < tls.VersionTLS13 || len(r.TLS.PeerCertificates) == 0 {
		return false
	}
	peer := r.TLS.PeerCertificates[0]
	for _, chain := range r.TLS.VerifiedChains {
		if len(chain) > 0 && sameCertificate(peer, chain[0]) {
			return true
		}
	}
	return false
}

// Require every field's exact canonical JSON spelling, including nested DRA
// device fields; encoding/json alone permits case-folded aliases. The
// existing duplicate-token validator rejects duplicate raw object keys.
func exactDRAJSONFields(raw []byte, typ reflect.Type) bool {
	var fields map[string]json.RawMessage
	if json.Unmarshal(raw, &fields) != nil || fields == nil || len(fields) != typ.NumField() {
		return false
	}
	for i := 0; i < typ.NumField(); i++ {
		field := typ.Field(i)
		key := strings.Split(field.Tag.Get("json"), ",")[0]
		if key == "" {
			key = field.Name
		}
		if key == "-" {
			return false
		}
		if _, exists := fields[key]; !exists {
			return false
		}
	}
	if typ == reflect.TypeOf(provider.DRAUnprepareEvent{}) {
		return exactDRAJSONFields(fields["allocated_device"], reflect.TypeOf(provider.DRAAllocatedDevice{}))
	}
	return true
}

func readDRASignedEnvelope(w http.ResponseWriter, r *http.Request) (provider.SignedDRAUnprepare, bool) {
	reject := func() (provider.SignedDRAUnprepare, bool) {
		writeWorkloadAPIError(w, http.StatusBadRequest, "INVALID_DRA_RECEIPT", "Invalid, ambiguous or oversized signed receipt")
		return provider.SignedDRAUnprepare{}, false
	}
	if r.Body == nil {
		return reject()
	}
	raw, err := io.ReadAll(http.MaxBytesReader(w, r.Body, maxDRAReceiptBody))
	if err != nil || len(raw) == 0 || validateWorkloadJSONTokens(raw) != nil {
		return reject()
	}
	var fields map[string]json.RawMessage
	if json.Unmarshal(raw, &fields) != nil || len(fields) != 2 || fields["event"] == nil ||
		fields["signature"] == nil || !exactDRAJSONFields(fields["event"], reflect.TypeOf(provider.DRAUnprepareEvent{})) {
		return reject()
	}
	var envelope struct {
		Event     provider.DRAUnprepareEvent
		Signature []byte
	}
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.DisallowUnknownFields()
	if decoder.Decode(&envelope) != nil || len(envelope.Signature) != ed25519.SignatureSize {
		return reject()
	}
	return provider.SignedDRAUnprepare{Event: envelope.Event, Signature: envelope.Signature}, true
}

func (h *draReceiptIngestHandler) ingest(w http.ResponseWriter, r *http.Request) {
	// TLS is mandatory even when an injected authenticator is misconfigured.
	// The authoritative peer certificate must be the chain-verified leaf.
	if !verifiedDRAClientTLS(r) {
		writeWorkloadAPIError(w, http.StatusUnauthorized, "UNAUTHENTICATED_DRA_SOURCE", "Verified client TLS required")
		return
	}
	principal, err := h.deps.Authenticator.Authenticate(r.Context(), r)
	if err != nil || !validWorkloadPrincipal(principal) {
		writeWorkloadAPIError(w, http.StatusUnauthorized, "UNAUTHENTICATED_DRA_SOURCE", "Verified driver identity required")
		return
	}
	contentTypes := r.Header.Values("Content-Type")
	if len(contentTypes) != 1 {
		writeWorkloadAPIError(w, http.StatusUnsupportedMediaType, "INVALID_CONTENT_TYPE", "Expected exactly one Content-Type header")
		return
	}
	mediaType, _, err := mime.ParseMediaType(contentTypes[0])
	if err != nil || mediaType != "application/json" {
		writeWorkloadAPIError(w, http.StatusUnsupportedMediaType, "INVALID_CONTENT_TYPE", "Expected application/json")
		return
	}
	signed, ok := readDRASignedEnvelope(w, r)
	if !ok {
		return
	}
	if err := h.deps.Authorizer.AuthorizeDRAReceipt(r.Context(), principal, signed.Event); err != nil {
		writeWorkloadAPIError(w, http.StatusForbidden, "DRA_SOURCE_DENIED", "Driver identity is not authorized for this source")
		return
	}
	asOf := time.Now().UTC()
	verifier, err := h.deps.VerifierResolver.ResolveDRAReceiptVerifier(r.Context(), principal, signed.Event)
	if err != nil || verifier == nil {
		if errors.Is(err, provider.ErrDRAReceiptSourceUntrusted) {
			writeWorkloadAPIError(w, http.StatusForbidden, "DRA_RECEIPT_UNTRUSTED", "Signed receipt source is not registered")
		} else {
			writeWorkloadAPIError(w, http.StatusServiceUnavailable, "DRA_VERIFIER_UNAVAILABLE", "Independently registered receipt signer is unavailable")
		}
		return
	}
	if err := verifier.VerifySignedEnvelope(signed, asOf); err != nil {
		writeWorkloadAPIError(w, http.StatusForbidden, "DRA_RECEIPT_UNTRUSTED", "Signed receipt is invalid, stale or outside the registered source scope")
		return
	}
	candidate, err := h.deps.Resolver.ResolveDRAReceiptEvidence(r.Context(), principal, signed.Event)
	if err != nil {
		if errors.Is(err, provider.ErrDRAReceiptSourceUntrusted) {
			writeWorkloadAPIError(w, http.StatusForbidden, "DRA_RECEIPT_UNTRUSTED", "Signed receipt source is not registered")
		} else {
			writeWorkloadAPIError(w, http.StatusServiceUnavailable, "DRA_EVIDENCE_UNAVAILABLE", "Independently resolved historical evidence is unavailable")
		}
		return
	}
	created, err := h.deps.Journal.VerifyAndRecordRestricted(r.Context(), verifier, candidate, signed, asOf)
	if err != nil {
		switch {
		case errors.Is(err, provider.ErrDRAReceiptReplayConflict):
			writeWorkloadAPIError(w, http.StatusConflict, "DRA_RECEIPT_CONFLICT", "Receipt conflicts with an existing claim-device epoch")
		case errors.Is(err, provider.ErrUncorrelatedDRAUnprepare),
			errors.Is(err, provider.ErrDRAReceiptSourceUntrusted):
			writeWorkloadAPIError(w, http.StatusForbidden, "DRA_RECEIPT_UNTRUSTED", "Signed receipt is not correlated to a trusted source")
		default:
			writeWorkloadAPIError(w, http.StatusServiceUnavailable, "DRA_RECEIPT_UNAVAILABLE", "Restricted evidence journal is unavailable")
		}
		return
	}
	status, result := http.StatusOK, "EXACT_REPLAY_EVIDENCE_ONLY"
	if created {
		status, result = http.StatusAccepted, "NEW_EVIDENCE_ONLY"
	}
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(map[string]string{"journal_result": result})
}

var _ DRAReceiptRestrictedJournal = (*provider.DRAUnprepareReceiptJournal)(nil)
