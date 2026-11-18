package provider

import (
	"strings"
	"testing"
)

func TestValidatePDReplicaClaims(t *testing.T) {
	q := PDJobSetQuery{TenantID: "tenant", WorkloadID: "w", IntentID: strings.Repeat("a", 64), Generation: 2, Namespace: "tenant", JobSetName: "pd", JobSetUID: "js", KueueName: "wl", KueueUID: "wluid", LocalQueue: "lq", ClusterQueue: "cq", GPUResource: "example.com/gpu", Prefill: PDPhaseQuota{1, 1}, Decode: PDPhaseQuota{1, 1}}
	replica := func(phase string) PDReplicaClaimQuery {
		return PDReplicaClaimQuery{Phase: phase, Index: 0, JobUID: phase + "-job", Pod: DRAPodClaimQuery{ExpectedJobUID: phase + "-job", ExpectedNodeName: "node", PodClaimAlias: "gpu", ClaimTemplateName: "gpu-template", Claim: DRAClaimQuery{Namespace: "tenant", ClaimName: phase + "-claim", ExpectedClaimUID: phase + "-claim-uid", PodName: phase + "-pod", ExpectedPodUID: phase + "-pod-uid", Request: "gpu", Driver: "driver", Pool: "pool", Device: phase + "-gpu"}}}
	}
	p, d := replica("prefill"), replica("decode")
	if !validPDReplicaClaims(q, []PDReplicaClaimQuery{p, d}) {
		t.Fatal("valid pair rejected")
	}
	for _, tc := range []struct {
		name string
		edit func(*PDJobSetQuery, *[]PDReplicaClaimQuery)
	}{
		{"half pair", func(_ *PDJobSetQuery, r *[]PDReplicaClaimQuery) { *r = (*r)[:1] }},
		{"duplicate device", func(_ *PDJobSetQuery, r *[]PDReplicaClaimQuery) { (*r)[1].Pod.Claim.Device = (*r)[0].Pod.Claim.Device }},
		{"duplicate pod", func(_ *PDJobSetQuery, r *[]PDReplicaClaimQuery) { (*r)[1].Pod.Claim.ExpectedPodUID = (*r)[0].Pod.Claim.ExpectedPodUID }},
		{"duplicate ordinal", func(_ *PDJobSetQuery, r *[]PDReplicaClaimQuery) { (*r)[1].Phase = "prefill" }},
		{"shared device", func(_ *PDJobSetQuery, r *[]PDReplicaClaimQuery) { (*r)[0].Pod.Claim.ShareID = "shared" }},
		{"cross tenant", func(_ *PDJobSetQuery, r *[]PDReplicaClaimQuery) { (*r)[0].Pod.Claim.Namespace = "other" }},
		{"multi-device", func(q *PDJobSetQuery, _ *[]PDReplicaClaimQuery) { q.Prefill.GPUsPerReplica = 2 }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			v, r := q, []PDReplicaClaimQuery{p, d}
			tc.edit(&v, &r)
			if validPDReplicaClaims(v, r) {
				t.Fatal("invalid replica evidence accepted")
			}
		})
	}
}
