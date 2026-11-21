// Package pdactivation defines the independent, signed, fail-closed
// authorization contract for removing the Kueue PD JobSet admission gate.
// It is a policy verification primitive, NOT a Kubernetes PATCH caller or a
// deployed ValidatingAdmissionWebhook. An actual mutating authority must use
// a failurePolicy=Fail validating webhook with a fresh authoritative loader;
// RBAC alone does not prevent malicious spec mutations.
package pdactivation

import (
	"bytes"
	"context"
	"crypto/ed25519"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"reflect"
	"regexp"
	"strings"
	"time"
)

const (
	AdmissionGateAnnotation = "kueue.x-k8s.io/admission-gated-by"
	GateValue = "compute-control-panel.io/pd-activation"
	PermitAnnotation = "compute-control-panel.io/pd-activation-permit"
	maxPermitSize = 8 << 10
	maxPermitTTL = 5 * time.Minute
)

var (
	ErrActivationDenied = errors.New("PD JobSet activation refused: no current independently verified scoped permit")
	kubeNameRE = regexp.MustCompile(`^[a-z0-9]([-a-z0-9]*[a-z0-9])?$`)
	hex64RE = regexp.MustCompile(`^[a-f0-9]{64}$`)
)

// Claims is a fixed-signature protocol: every field participates in the
// exact ed25519.Sign(json.Marshal(Claims)) message. Only an independent
// approval authority can issue valid permits. A current DB authorization
// check and admission diff gate are additionally required.
type Claims struct {
	Version               string `json:"version"`
	Issuer                string `json:"issuer"`
	KeyID                 string `json:"key_id"`
	Audience              string `json:"audience"`
	PermitID              string `json:"permit_id"`
	TenantID              string `json:"tenant_id"`
	ProjectID             string `json:"project_id"`
	PoolID                string `json:"pool_id"`
	ProviderID            string `json:"provider_id"`
	WorkloadID            string `json:"workload_id"`
	IntentID              string `json:"intent_id"`
	Generation            uint64 `json:"generation"`
	FenceVersion          uint64 `json:"fence_version"`
	Namespace             string `json:"namespace"`
	JobSetName            string `json:"jobset_name"`
	JobSetUID             string `json:"jobset_uid"`
	JobSetResourceVersion string `json:"jobset_resource_version"`
	JobSetSpecSHA256      string `json:"jobset_spec_sha256"`
	IssuedAtMS            int64  `json:"issued_at_ms"`
	NotBeforeMS           int64  `json:"not_before_ms"`
	ExpiresAtMS           int64  `json:"expires_at_ms"`
}

type SignedPermit struct {
	Claims    Claims `json:"claims"`
	Signature []byte `json:"signature"`
}

// Trust must be configured by the independent authority administrator, not
// from an annotation, workload request or a tenant-controlled configuration.
type Trust struct {
	Issuer string
	Audience string
	Keys map[string]ed25519.PublicKey
}

// Authority must originate from a non-tenant-controlled, fresh, fail-closed
// current source of truth. Version and revocation must be independently
// administered. The interface has no default-allow or cached fallback.
type Authority struct {
	Approved bool
	Revoked bool
	Version uint64
	ValidUntilMS int64
	TenantID, ProjectID, PoolID, ProviderID, WorkloadID, IntentID string
	Generation uint64
	Namespace, JobSetName, JobSetUID string
}

type AuthorityLoader func(context.Context, Claims) (Authority, error)

type OldNew struct {
	Old json.RawMessage
	New json.RawMessage
}

type VerifiedRelease struct {
	Claims
	EncodedPermit string
}

func validDNSName(s string) bool {
	return len(s)>0 && len(s)<=63 && kubeNameRE.MatchString(s)
}
func validIdentity(s string) bool {
	return len(s)>0 && len(s)<=256 && strings.TrimSpace(s)==s &&
		!strings.ContainsAny(s,"\x00\n\r")
}
func validClaims(c Claims, trust Trust, now time.Time) bool {
	if c.Version!="ccp.pd.activation.v1" || c.Issuer=="" ||
		c.Issuer!=trust.Issuer || c.Audience=="" || c.Audience!=trust.Audience ||
		!validIdentity(c.KeyID) || !hex64RE.MatchString(c.PermitID) ||
		!hex64RE.MatchString(c.IntentID) ||
		!validIdentity(c.TenantID) || !validIdentity(c.ProjectID) ||
		!validIdentity(c.ProviderID) || !validIdentity(c.PoolID) ||
		!validIdentity(c.WorkloadID) || !validDNSName(c.Namespace) ||
		!validDNSName(c.JobSetName) || !validIdentity(c.JobSetUID) ||
		!validIdentity(c.JobSetResourceVersion) || c.Generation<2 ||
		c.FenceVersion==0 || !strings.HasPrefix(c.JobSetSpecSHA256,"sha256:") ||
		!hex64RE.MatchString(strings.TrimPrefix(c.JobSetSpecSHA256,"sha256:")) ||
		c.IssuedAtMS<=0 || c.IssuedAtMS>c.NotBeforeMS ||
		c.NotBeforeMS>=c.ExpiresAtMS ||
		c.ExpiresAtMS-c.IssuedAtMS>maxPermitTTL.Milliseconds() {
		return false
	}
	millis:=now.UnixMilli()
	return c.IssuedAtMS<=millis && c.NotBeforeMS<=millis && millis<c.ExpiresAtMS
}

func decodePermit(annotation string) (SignedPermit,error) {
	if len(annotation)==0 || len(annotation)>maxPermitSize {
		return SignedPermit{},ErrActivationDenied
	}
	reader:=json.NewDecoder(strings.NewReader(annotation))
	reader.DisallowUnknownFields()
	var permit SignedPermit
	if err:=reader.Decode(&permit);err!=nil {
		return SignedPermit{},ErrActivationDenied
	}
	var extra any
	if err:=reader.Decode(&extra);err!=io.EOF {
		return SignedPermit{},ErrActivationDenied
	}
	if len(permit.Signature)!=ed25519.SignatureSize {
		return SignedPermit{},ErrActivationDenied
	}
	return permit,nil
}

func mapObject(raw []byte) (map[string]any,error) {
	if len(raw)==0 || len(raw)>(2<<20) {
		return nil,ErrActivationDenied
	}
	var obj map[string]any
	if err:=json.Unmarshal(raw,&obj);err!=nil || obj==nil {
		return nil,ErrActivationDenied
	}
	return obj,nil
}
func fieldMap(obj map[string]any,key string) (map[string]any,error) {
	v,ok:=obj[key].(map[string]any)
	if !ok {return nil,ErrActivationDenied}
	return v,nil
}
func getString(obj map[string]any,key string) string {
	v,_:=obj[key].(string)
	return v
}

// VerifyGateRelease is the policy core for a future fail-closed validating
// admission webhook. It checks the signed permit from the NEW object, exact
// target UID/RV of the OLD object, permitted annotation delta, full jobset
// spec hash and *independent live* approval/revocation/epoch state. It has
// NO network access and never patches Kubernetes. The caller must supply an
// authenticated AuthorityLoader; absent/error results are always denied.
func VerifyGateRelease(ctx context.Context, pair OldNew, trust Trust,
	load AuthorityLoader, now time.Time) (VerifiedRelease,error) {
	deny:=func()(VerifiedRelease,error){return VerifiedRelease{},ErrActivationDenied}
	if ctx==nil || ctx.Err()!=nil || load==nil || trust.Issuer=="" ||
		trust.Audience=="" || len(trust.Keys)==0 || now.IsZero() {
		return deny()
	}
	oldObj,err:=mapObject(pair.Old)
	if err!=nil{return deny()}
	newObj,err:=mapObject(pair.New)
	if err!=nil{return deny()}
	if getString(oldObj,"apiVersion")!="jobset.x-k8s.io/v1alpha2" ||
		getString(oldObj,"kind")!="JobSet" ||
		getString(newObj,"apiVersion")!="jobset.x-k8s.io/v1alpha2" ||
		getString(newObj,"kind")!="JobSet" {
		return deny()
	}
	oldMeta,err:=fieldMap(oldObj,"metadata");if err!=nil{return deny()}
	newMeta,err:=fieldMap(newObj,"metadata");if err!=nil{return deny()}
	oldAnnotations,err:=fieldMap(oldMeta,"annotations");if err!=nil{return deny()}
	newAnnotations,err:=fieldMap(newMeta,"annotations");if err!=nil{return deny()}
	if getString(oldAnnotations,AdmissionGateAnnotation)!=GateValue ||
		getString(newAnnotations,AdmissionGateAnnotation)!="" ||
		getString(oldAnnotations,PermitAnnotation)!="" {
		return deny()
	}
	encoded:=getString(newAnnotations,PermitAnnotation)
	signed,err:=decodePermit(encoded);if err!=nil{return deny()}
	c:=signed.Claims
	if !validClaims(c,trust,now) {return deny()}
	key,ok:=trust.Keys[c.KeyID]
	if !ok || len(key)!=ed25519.PublicKeySize {return deny()}
	sigPayload,err:=json.Marshal(c)
	if err!=nil || !ed25519.Verify(key,sigPayload,signed.Signature) {return deny()}
	if getString(oldMeta,"name")!=c.JobSetName ||
		getString(oldMeta,"namespace")!=c.Namespace ||
		getString(oldMeta,"uid")!=c.JobSetUID ||
		getString(oldMeta,"resourceVersion")!=c.JobSetResourceVersion ||
		getString(oldAnnotations,"compute-control-panel.io/pd-intent-id")!=c.IntentID {
		return deny()
	}
	spec,err:=fieldMap(oldObj,"spec");if err!=nil{return deny()}
	if spec["suspend"]!=true {return deny()}
	phases,ok:=spec["replicatedJobs"].([]any)
	if !ok || len(phases)!=2 {return deny()}
	seen:=map[string]bool{}
	for _,phase:=range phases {
		p,ok:=phase.(map[string]any)
		if !ok || seen[getString(p,"name")] || (getString(p,"name")!="prefill"&&getString(p,"name")!="decode") {
			return deny()
		}
		seen[getString(p,"name")]=true
	}
	encodedSpec,err:=json.Marshal(spec)
	if err!=nil{return deny()}
	h:=sha256.Sum256(encodedSpec)
	if c.JobSetSpecSHA256!="sha256:"+hex.EncodeToString(h[:]) {return deny()}
	// Compare all server-side fields, allowing ONLY api-server-maintained
	// resourceVersion/managedFields and the exact two annotation changes.
	// A broad RBAC PATCH may not change images, commands, Pod templates,
	// replicas, suspend, labels, owner refs, finalizers, or any other field.
	delete(oldAnnotations,AdmissionGateAnnotation)
	delete(newAnnotations,PermitAnnotation)
	for _,meta:=range []map[string]any{oldMeta,newMeta} {
		delete(meta,"resourceVersion")
		delete(meta,"managedFields")
	}
	if !reflect.DeepEqual(oldObj,newObj) {return deny()}
	state,err:=load(ctx,c)
	if err!=nil || !state.Approved || state.Revoked ||
		state.Version!=c.FenceVersion || state.ValidUntilMS<=now.UnixMilli() ||
		state.ValidUntilMS<c.ExpiresAtMS ||
		state.TenantID!=c.TenantID || state.ProjectID!=c.ProjectID ||
		state.PoolID!=c.PoolID || state.ProviderID!=c.ProviderID ||
		state.WorkloadID!=c.WorkloadID || state.IntentID!=c.IntentID ||
		state.Generation!=c.Generation ||
		state.Namespace!=c.Namespace || state.JobSetName!=c.JobSetName ||
		state.JobSetUID!=c.JobSetUID {
		return deny()
	}
	return VerifiedRelease{Claims:c,EncodedPermit:encoded},nil
}

// GateRemovalPatch produces a five-op UID+resourceVersion+gate guarded patch,
// adding the verified signed permit in the same atomic API request. It does
// not send the patch; ONLY a separately authorized service with a deployed
// validating admission webhook and current authority may do so.
func GateRemovalPatch(v VerifiedRelease) ([]byte,error) {
	if v.EncodedPermit=="" || v.JobSetUID=="" || v.JobSetResourceVersion=="" {
		return nil,ErrActivationDenied
	}
	// Round-trip this encoded transport to ensure it was already verified
	// and is consistent with the returned target. No user-controlled paths.
	s,err:=decodePermit(v.EncodedPermit)
	if err!=nil || s.Claims!=v.Claims {
		return nil,ErrActivationDenied
	}
	return json.Marshal([]struct{
		Op string `json:"op"`
		Path string `json:"path"`
		Value any `json:"value,omitempty"`
	}{
		{"test","/metadata/uid",v.JobSetUID},
		{"test","/metadata/resourceVersion",v.JobSetResourceVersion},
		{"test","/metadata/annotations/kueue.x-k8s.io~1admission-gated-by",GateValue},
		{"add","/metadata/annotations/compute-control-panel.io~1pd-activation-permit",v.EncodedPermit},
		{"remove","/metadata/annotations/kueue.x-k8s.io~1admission-gated-by",nil},
	})
}

// DecodeSpecDigest is a helper for independent approval issuers: it hashes
// the parsed JSON value, eliminating formatting-only ambiguity. Its output
// is NOT an approval. Issuance is outside this package.
func DecodeSpecDigest(raw json.RawMessage) (string,error) {
	var value any
	if len(raw)==0 || json.Unmarshal(raw,&value)!=nil || value==nil {
		return "",ErrActivationDenied
	}
	normalized,err:=json.Marshal(value)
	if err!=nil{return "",ErrActivationDenied}
	sum:=sha256.Sum256(normalized)
	return "sha256:"+hex.EncodeToString(sum[:]),nil
}

var _ = bytes.Compare
