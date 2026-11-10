package provider

import (
	"context"
	"fmt"
)

// DRAPodClaimQuery is a caller-pinned, single-claim Pod/Job identity.
// ExpectedJobUID and ExpectedNodeName must come from an independently
// authorized Kubernetes/Kueue observation, never from a tenant request.
type DRAPodClaimQuery struct {
	Claim            DRAClaimQuery
	ExpectedJobUID   string
	ExpectedNodeName string
	PodClaimAlias    string
}

// DRAPodClaimObservation is a structural association of an actual Pod
// consumer with a scheduler-side DRA Claim allocation. It is NOT evidence
// of a prepared GPU, driver readiness, physical use, release or accounting.
type DRAPodClaimObservation struct {
	Pod               KubernetesResourceRef
	NodeName          string
	PodClaimAlias     string
	ConsumerContainer string
	Claim             DRAClaimObservation
}

func (q DRAPodClaimQuery) validate() error {
	if q.Claim.validate() != nil ||
		!validDRAIdentity(q.ExpectedJobUID) ||
		!validDRAIdentity(q.ExpectedNodeName) ||
		!validKubeName(q.PodClaimAlias) {
		return ErrInvalidDRAObservation
	}
	return nil
}

type draPodObject struct {
	APIVersion string   `json:"apiVersion"`
	Kind       string   `json:"kind"`
	Metadata   kubeMeta `json:"metadata"`
	Spec       struct {
		NodeName       string `json:"nodeName"`
		ResourceClaims []struct {
			Name                      string `json:"name"`
			ResourceClaimName         string `json:"resourceClaimName"`
			ResourceClaimTemplateName string `json:"resourceClaimTemplateName"`
		} `json:"resourceClaims"`
		Containers []struct {
			Name      string `json:"name"`
			Resources struct {
				Claims []struct {
					Name string `json:"name"`
				} `json:"claims"`
			} `json:"resources"`
		} `json:"containers"`
	} `json:"spec"`
}

// exactDRAPodConsumer checks only a deliberately bounded single-claim,
// single-container-consumer case. Template-based and shared/multi-claim Pods
// need their own identity contract and are rejected rather than guessed.
func exactDRAPodConsumer(p draPodObject, q DRAPodClaimQuery) (string, bool) {
	if p.APIVersion != "v1" || p.Kind != "Pod" ||
		!validKubernetesRef(refFromKube(p.Metadata)) ||
		p.Metadata.Namespace != q.Claim.Namespace ||
		p.Metadata.Name != q.Claim.PodName ||
		p.Metadata.UID != q.Claim.ExpectedPodUID ||
		!isControlledBy(p.Metadata, q.ExpectedJobUID) ||
		p.Spec.NodeName != q.ExpectedNodeName ||
		len(p.Spec.ResourceClaims) != 1 {
		return "", false
	}
	claim := p.Spec.ResourceClaims[0]
	if claim.Name != q.PodClaimAlias ||
		claim.ResourceClaimName != q.Claim.ClaimName ||
		claim.ResourceClaimTemplateName != "" {
		return "", false
	}
	consumer := ""
	for _, container := range p.Spec.Containers {
		if container.Name == "" {
			return "", false
		}
		for _, ref := range container.Resources.Claims {
			if ref.Name != q.PodClaimAlias || consumer != "" {
				return "", false
			}
			consumer = container.Name
		}
	}
	return consumer, consumer != ""
}

// ObserveDRAPodClaim follows the exact Pod.spec.resourceClaims and
// container.resources.claims edges before reading a ResourceClaim. The same
// Pod is GETed again after the Claim to fail closed on Pod UID/RV drift.
//
// Reads are sequential, never an atomic Kubernetes snapshot. This method
// neither authorizes GPU placement nor proves node-plugin device preparation
// or three-authority release, and it must not be used for Allocation READY
// or RELEASED transitions.
func (r *KubernetesObservationReader) ObserveDRAPodClaim(
	ctx context.Context, q DRAPodClaimQuery,
) (DRAPodClaimObservation, error) {
	reject := func() (DRAPodClaimObservation, error) {
		return DRAPodClaimObservation{}, ErrInvalidDRAObservation
	}
	if q.validate() != nil || r == nil {
		return reject()
	}
	path := fmt.Sprintf("/api/v1/namespaces/%s/pods/%s", q.Claim.Namespace, q.Claim.PodName)
	var first draPodObject
	if r.read(ctx, path, &first) != nil {
		return reject()
	}
	consumer, ok := exactDRAPodConsumer(first, q)
	if !ok {
		return reject()
	}
	claim, err := r.ObserveDRAClaim(ctx, q.Claim)
	if err != nil || claim.PodUID != first.Metadata.UID ||
		claim.Claim.Namespace != first.Metadata.Namespace {
		return reject()
	}
	var second draPodObject
	if r.read(ctx, path, &second) != nil ||
		second.Metadata.UID != first.Metadata.UID ||
		second.Metadata.ResourceVersion != first.Metadata.ResourceVersion {
		return reject()
	}
	consumerAgain, ok := exactDRAPodConsumer(second, q)
	if !ok || consumerAgain != consumer {
		return reject()
	}
	return DRAPodClaimObservation{
		Pod:               refFromKube(first.Metadata),
		NodeName:          first.Spec.NodeName,
		PodClaimAlias:     q.PodClaimAlias,
		ConsumerContainer: consumer,
		Claim:             claim,
	}, nil
}
