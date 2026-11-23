package pdactivation

import (
	"bytes"
	"context"
	"crypto/ed25519"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"strings"
	"testing"
	"time"
)

func permitFixture(t *testing.T) (OldNew, SignedPermit, Authority, map[string]ed25519.PublicKey, time.Time) {
	t.Helper()
	now := time.Date(2026, 11, 21, 9, 0, 0, 0, time.UTC)
	seed := sha256.Sum256([]byte("disposable pd activation test signer; no production key"))
	private := ed25519.NewKeyFromSeed(seed[:])
	old := map[string]any{
		"apiVersion": "jobset.x-k8s.io/v1alpha2",
		"kind":       "JobSet",
		"metadata": map[string]any{
			"namespace": "ccp-pd-paired-smoke", "name": "pd-gated", "uid": "uuid-gated-001",
			"resourceVersion": "2055", "generation": float64(1),
			"labels": map[string]any{"kueue.x-k8s.io/queue-name": "ccp-pd-paired-smoke"},
			"annotations": map[string]any{
				"kueue.x-k8s.io/admission-gated-by":     "compute-control-panel.io/pd-activation",
				"compute-control-panel.io/pd-intent-id": strings.Repeat("a", 64),
			},
		},
		"spec": map[string]any{
			"suspend": true, "replicatedJobs": []any{
				map[string]any{"name": "prefill", "replicas": float64(1),
					"template": map[string]any{"spec": map[string]any{
						"template": map[string]any{"spec": map[string]any{
							"containers": []any{map[string]any{"name": "ccp-pd-inert", "image": "registry.k8s.io/pause:3.10"}},
						}},
					}}},
				map[string]any{"name": "decode", "replicas": float64(1),
					"template": map[string]any{"spec": map[string]any{
						"template": map[string]any{"spec": map[string]any{
							"containers": []any{map[string]any{"name": "ccp-pd-inert", "image": "registry.k8s.io/pause:3.10"}},
						}},
					}}},
			},
		},
	}
	spec, err := json.Marshal(old["spec"])
	if err != nil {
		t.Fatal(err)
	}
	digest := sha256.Sum256(spec)
	claims := Claims{
		Version: "ccp.pd.activation.v1", Issuer: "independent-approval-service",
		KeyID: "approval-key-01", Audience: "ccp-pd-activation-webhook",
		PermitID: strings.Repeat("b", 64), TenantID: "tenant-a",
		ProjectID: "project-a", PoolID: "pool-a", ProviderID: "provider-a",
		WorkloadID: "workload-a", IntentID: strings.Repeat("a", 64),
		Generation: 2, FenceVersion: 5,
		Namespace: "ccp-pd-paired-smoke", JobSetName: "pd-gated",
		JobSetUID: "uuid-gated-001", JobSetResourceVersion: "2055",
		JobSetSpecSHA256: "sha256:" + hex.EncodeToString(digest[:]),
		IssuedAtMS:       now.Add(-time.Second).UnixMilli(),
		NotBeforeMS:      now.Add(-time.Second).UnixMilli(),
		ExpiresAtMS:      now.Add(time.Minute).UnixMilli(),
	}
	signedBytes, err := json.Marshal(claims)
	if err != nil {
		t.Fatal(err)
	}
	permit := SignedPermit{Claims: claims, Signature: ed25519.Sign(private, signedBytes)}
	newObject := cloneJSONMap(t, old)
	delete(newObject["metadata"].(map[string]any)["annotations"].(map[string]any), AdmissionGateAnnotation)
	payload, err := json.Marshal(permit)
	if err != nil {
		t.Fatal(err)
	}
	newObject["metadata"].(map[string]any)["annotations"].(map[string]any)[PermitAnnotation] = string(payload)
	// Allowed Kubernetes bookkeeping effects are resourceVersion and managedFields.
	newObject["metadata"].(map[string]any)["resourceVersion"] = "2056"
	newObject["metadata"].(map[string]any)["managedFields"] = []any{map[string]any{"manager": "kube-apiserver"}}
	authority := Authority{
		Approved: true, Revoked: false, Version: 5,
		ValidUntilMS: now.Add(2 * time.Minute).UnixMilli(),
		TenantID:     claims.TenantID, ProjectID: claims.ProjectID,
		ProviderID: claims.ProviderID, PoolID: claims.PoolID,
		WorkloadID: claims.WorkloadID, IntentID: claims.IntentID,
		Generation: claims.Generation, Namespace: claims.Namespace,
		JobSetName: claims.JobSetName, JobSetUID: claims.JobSetUID,
	}
	previous, _ := json.Marshal(old)
	next, _ := json.Marshal(newObject)
	return OldNew{Old: previous, New: next},
		permit, authority, map[string]ed25519.PublicKey{"approval-key-01": private.Public().(ed25519.PublicKey)}, now
}
func cloneJSONMap(t *testing.T, old map[string]any) map[string]any {
	t.Helper()
	b, err := json.Marshal(old)
	if err != nil {
		t.Fatal(err)
	}
	var out map[string]any
	if err := json.Unmarshal(b, &out); err != nil {
		t.Fatal(err)
	}
	return out
}
func copyObject(t *testing.T, raw []byte) map[string]any {
	t.Helper()
	var out map[string]any
	if err := json.Unmarshal(raw, &out); err != nil {
		t.Fatal(err)
	}
	return out
}
func asJSON(t *testing.T, obj map[string]any) []byte {
	t.Helper()
	b, err := json.Marshal(obj)
	if err != nil {
		t.Fatal(err)
	}
	return b
}
func signMutation(t *testing.T, signed SignedPermit, mutate func(*Claims)) SignedPermit {
	t.Helper()
	seed := sha256.Sum256([]byte("disposable pd activation test signer; no production key"))
	priv := ed25519.NewKeyFromSeed(seed[:])
	mutate(&signed.Claims)
	raw, err := json.Marshal(signed.Claims)
	if err != nil {
		t.Fatal(err)
	}
	signed.Signature = ed25519.Sign(priv, raw)
	return signed
}
func setPermit(t *testing.T, pair OldNew, signed SignedPermit) OldNew {
	t.Helper()
	newObj := copyObject(t, pair.New)
	encoded, err := json.Marshal(signed)
	if err != nil {
		t.Fatal(err)
	}
	newObj["metadata"].(map[string]any)["annotations"].(map[string]any)[PermitAnnotation] = string(encoded)
	pair.New = asJSON(t, newObj)
	return pair
}

func TestSignedScopedPermitAcceptsOnlyExactGateRelease(t *testing.T) {
	pair, permit, authority, keys, now := permitFixture(t)
	verified, err := VerifyGateRelease(context.Background(), pair, trustFixture(keys), func(context.Context, Claims) (Authority, error) {
		return authority, nil
	}, now)
	if err != nil || verified.PermitID != permit.Claims.PermitID ||
		verified.JobSetUID != permit.Claims.JobSetUID || verified.JobSetResourceVersion != "2055" {
		t.Fatalf("legitimate independent signed activation denied: %+v %v", verified, err)
	}
	patch, err := GateRemovalPatch(verified)
	if err != nil {
		t.Fatal(err)
	}
	var ops []struct {
		Op    string `json:"op"`
		Path  string `json:"path"`
		Value any    `json:"value"`
	}
	if err := json.Unmarshal(patch, &ops); err != nil {
		t.Fatal(err)
	}
	if len(ops) != 5 ||
		ops[0].Op != "test" || ops[0].Path != "/metadata/uid" ||
		ops[1].Op != "test" || ops[1].Path != "/metadata/resourceVersion" ||
		ops[2].Op != "test" || ops[2].Path != "/metadata/annotations/kueue.x-k8s.io~1admission-gated-by" ||
		ops[3].Op != "add" || ops[3].Path != "/metadata/annotations/compute-control-panel.io~1pd-activation-permit" ||
		ops[4].Op != "remove" || ops[4].Path != "/metadata/annotations/kueue.x-k8s.io~1admission-gated-by" {
		t.Fatalf("non-exact gate-only JSON Patch: %s", patch)
	}
}

func TestScopedPermitRejectsUntrustedMutationAndReplay(t *testing.T) {
	pair, _, auth, keys, now := permitFixture(t)
	for _, tc := range []struct {
		name   string
		mutate func(map[string]any)
	}{
		{"spec-unsuspend", func(x map[string]any) { x["spec"].(map[string]any)["suspend"] = false }},
		{"spec-image", func(x map[string]any) {
			x["spec"].(map[string]any)["replicatedJobs"].([]any)[0].(map[string]any)["template"].(map[string]any)["spec"].(map[string]any)["template"].(map[string]any)["spec"].(map[string]any)["containers"].([]any)[0].(map[string]any)["image"] = "busybox"
		}},
		{"labels", func(x map[string]any) {
			x["metadata"].(map[string]any)["labels"].(map[string]any)["extra"] = "surprise"
		}},
		{"annotation-extra", func(x map[string]any) {
			x["metadata"].(map[string]any)["annotations"].(map[string]any)["other"] = "value"
		}},
		{"re-add-gate", func(x map[string]any) {
			x["metadata"].(map[string]any)["annotations"].(map[string]any)[AdmissionGateAnnotation] = GateValue
		}},
		{"uid-replacement", func(x map[string]any) { x["metadata"].(map[string]any)["uid"] = "other" }},
		{"name-replacement", func(x map[string]any) { x["metadata"].(map[string]any)["name"] = "other" }},
		{"namespace-replacement", func(x map[string]any) { x["metadata"].(map[string]any)["namespace"] = "other" }},
		{"metadata-new-field", func(x map[string]any) { x["metadata"].(map[string]any)["finalizers"] = []any{"evil"} }},
		{"permit-missing", func(x map[string]any) {
			delete(x["metadata"].(map[string]any)["annotations"].(map[string]any), PermitAnnotation)
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			x := copyObject(t, pair.New)
			tc.mutate(x)
			altered := OldNew{Old: pair.Old, New: asJSON(t, x)}
			if _, err := VerifyGateRelease(context.Background(), altered, trustFixture(keys), func(context.Context, Claims) (Authority, error) {
				return auth, nil
			}, now); !errors.Is(err, ErrActivationDenied) {
				t.Fatalf("unexpected accepted mutation %s: %v", tc.name, err)
			}
		})
	}
	// An unchanged signed permit cannot be replayed after Kubernetes
	// advances the OldObject resourceVersion: the signature binds 2055.
	replayOld := copyObject(t, pair.Old)
	replayOld["metadata"].(map[string]any)["resourceVersion"] = "2056"
	if _, err := VerifyGateRelease(context.Background(),
		OldNew{Old: asJSON(t, replayOld), New: pair.New}, trustFixture(keys),
		func(context.Context, Claims) (Authority, error) { return auth, nil }, now,
	); !errors.Is(err, ErrActivationDenied) {
		t.Fatalf("replayed old version accepted: %v", err)
	}
}

func TestSignedPermitRejectsRevokedExpiredAuthorityAndBadSignatures(t *testing.T) {
	pair, permit, auth, keys, now := permitFixture(t)
	scenarios := []struct {
		name  string
		alter func(*SignedPermit, *Authority, *time.Time)
	}{
		{"unsigned-change", func(p *SignedPermit, _ *Authority, _ *time.Time) { p.Claims.JobSetUID = "attacker" }},
		{"wrong-audience", func(p *SignedPermit, _ *Authority, _ *time.Time) {
			*p = signMutation(t, *p, func(c *Claims) { c.Audience = "generic-api" })
		}},
		{"wrong-issuer", func(p *SignedPermit, _ *Authority, _ *time.Time) {
			*p = signMutation(t, *p, func(c *Claims) { c.Issuer = "tenant" })
		}},
		{"revoked", func(_ *SignedPermit, a *Authority, _ *time.Time) { a.Revoked = true }},
		{"not-approved", func(_ *SignedPermit, a *Authority, _ *time.Time) { a.Approved = false }},
		{"fence-version", func(_ *SignedPermit, a *Authority, _ *time.Time) { a.Version++ }},
		{"cross-tenant", func(_ *SignedPermit, a *Authority, _ *time.Time) { a.TenantID = "other" }},
		{"cross-provider", func(_ *SignedPermit, a *Authority, _ *time.Time) { a.ProviderID = "other" }},
		{"wrong-generation", func(_ *SignedPermit, a *Authority, _ *time.Time) { a.Generation++ }},
		{"wrong-jobset-uid", func(_ *SignedPermit, a *Authority, _ *time.Time) { a.JobSetUID = "other" }},
		{"expired-approval", func(_ *SignedPermit, a *Authority, _ *time.Time) { a.ValidUntilMS = now.Add(-time.Second).UnixMilli() }},
		{"expired-permit", func(_ *SignedPermit, _ *Authority, n *time.Time) { *n = now.Add(3 * time.Minute) }},
		{"not-yet-valid", func(p *SignedPermit, _ *Authority, _ *time.Time) {
			*p = signMutation(t, *p, func(c *Claims) { c.NotBeforeMS = now.Add(time.Minute).UnixMilli() })
		}},
		{"overlong-ttl", func(p *SignedPermit, _ *Authority, _ *time.Time) {
			*p = signMutation(t, *p, func(c *Claims) { c.ExpiresAtMS = now.Add(time.Hour).UnixMilli() })
		}},
		{"wrong-spec-digest", func(p *SignedPermit, _ *Authority, _ *time.Time) {
			*p = signMutation(t, *p, func(c *Claims) { c.JobSetSpecSHA256 = "sha256:" + strings.Repeat("c", 64) })
		}},
		{"unknown-key-id", func(p *SignedPermit, _ *Authority, _ *time.Time) {
			*p = signMutation(t, *p, func(c *Claims) { c.KeyID = "unknown" })
		}},
	}
	for _, tc := range scenarios {
		t.Run(tc.name, func(t *testing.T) {
			permitCopy, authority, clock := permit, auth, now
			tc.alter(&permitCopy, &authority, &clock)
			req := setPermit(t, pair, permitCopy)
			if _, err := VerifyGateRelease(context.Background(), req, trustFixture(keys),
				func(context.Context, Claims) (Authority, error) { return authority, nil }, clock,
			); !errors.Is(err, ErrActivationDenied) {
				t.Fatalf("invalid signed permit was trusted: %v", err)
			}
		})
	}
	for _, tc := range []struct {
		name   string
		loader AuthorityLoader
	}{
		{"missing", nil},
		{"not-found", func(context.Context, Claims) (Authority, error) { return Authority{}, errors.New("authority missing") }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if _, err := VerifyGateRelease(context.Background(), pair, trustFixture(keys), tc.loader, now); !errors.Is(err, ErrActivationDenied) {
				t.Fatalf("missing authoritative revocation check accepted: %v", err)
			}
		})
	}
}

func TestStrictPermitTransportDoesNotIgnoreExtraJSONFields(t *testing.T) {
	pair, _, auth, keys, now := permitFixture(t)
	newObject := copyObject(t, pair.New)
	annotation := newObject["metadata"].(map[string]any)["annotations"].(map[string]any)
	annotation[PermitAnnotation] = strings.TrimSuffix(annotation[PermitAnnotation].(string), "}") + `,"unrecognized":true}`
	if _, err := VerifyGateRelease(context.Background(),
		OldNew{Old: pair.Old, New: asJSON(t, newObject)}, trustFixture(keys),
		func(context.Context, Claims) (Authority, error) { return auth, nil }, now,
	); !errors.Is(err, ErrActivationDenied) {
		t.Fatalf("unknown transport field accepted: %v", err)
	}
	// A signed JSON object with extra root content cannot be treated as a
	// different trusted protocol even when its signature is otherwise valid.
	if bytes.Contains([]byte(annotation[PermitAnnotation].(string)), []byte("secret")) {
		t.Fatal("test signed transport leaked a secret")
	}
}
func trustFixture(keys map[string]ed25519.PublicKey) Trust {
	return Trust{Issuer: "independent-approval-service", Audience: "ccp-pd-activation-webhook", Keys: keys}
}

// A valid signature does not establish a live, scoped approval/revocation
// lookup. An arbitrary caller must not construct VerifiedRelease directly and
// obtain an activation patch without passing VerifyGateRelease first.
func TestGateRemovalPatchRejectsCallerFabricatedVerifiedRelease(t *testing.T) {
	pair, signed, _, _, _ := permitFixture(t)
	_ = pair
	raw, err := json.Marshal(signed)
	if err != nil {
		t.Fatal(err)
	}
	forged := VerifiedRelease{Claims: signed.Claims, EncodedPermit: string(raw)}
	if patch, err := GateRemovalPatch(forged); !errors.Is(err, ErrActivationDenied) || len(patch) != 0 {
		t.Fatalf("unverified caller-fabricated permit generated a JobSet patch: %s, %v", patch, err)
	}
}
