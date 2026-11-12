package provider

import (
	"context"
	"fmt"
	"reflect"
	"strings"
)

// KubernetesPodAbsentObservation is a bounded, read-only Kubernetes API
// snapshot that does not list the previously pinned Pod UID or its old name.
// It is not an authenticated source receipt, kubelet termination, physical
// GPU release, Provider finality or Allocation RELEASED authorization.
type KubernetesPodAbsentObservation struct {
	PreviousPod            KubernetesResourceRef
	PodListResourceVersion string
}

type podAbsentListSnapshot struct {
	APIVersion string   `json:"apiVersion"`
	Kind       string   `json:"kind"`
	Metadata   kubeMeta `json:"metadata"`
	Items      []struct {
		Metadata kubeMeta `json:"metadata"`
	} `json:"items"`
}

func (s podAbsentListSnapshot) excludesPriorPod(prior KubernetesResourceRef) bool {
	if s.APIVersion != "v1" || s.Kind != "PodList" ||
		s.Items == nil || s.Metadata.Continue != "" ||
		s.Metadata.ResourceVersion == "" ||
		s.Metadata.ResourceVersion != strings.TrimSpace(s.Metadata.ResourceVersion) ||
		strings.ContainsRune(s.Metadata.ResourceVersion, 0) {
		return false
	}
	seenUID := make(map[string]bool, len(s.Items))
	seenName := make(map[string]bool, len(s.Items))
	for _, item := range s.Items {
		pod := refFromKube(item.Metadata)
		if !validKubernetesRef(pod) ||
			pod.Namespace != prior.Namespace ||
			pod.UID == prior.UID || pod.Name == prior.Name ||
			seenUID[pod.UID] || seenName[pod.Name] {
			return false
		}
		seenUID[pod.UID] = true
		seenName[pod.Name] = true
	}
	return true
}

// ObservePodUIDAbsent performs two unpaginated Pod LIST reads in the pinned
// namespace and requires the same resourceVersion and object snapshot.
// Old UID, same-name successor, malformed items, pagination, stale/moving
// snapshots and API errors all fail closed. It never issues writes.
//
// Two API reads are not a transactional finality guarantee. A caller must
// independently attest the earlier Pod identity and cluster authority, and
// must still obtain actual driver/kubelet deallocation evidence before any
// release or cost-liability transition.
func (r *KubernetesObservationReader) ObservePodUIDAbsent(
	ctx context.Context, prior KubernetesResourceRef,
) (KubernetesPodAbsentObservation, error) {
	reject := func() (KubernetesPodAbsentObservation, error) {
		return KubernetesPodAbsentObservation{}, ErrInvalidKueueObservation
	}
	if r == nil || !validKubeName(prior.Namespace) || !validKubeName(prior.Name) ||
		!validKubernetesRef(prior) || !validPinnedQueueUID(prior.UID) {
		return reject()
	}
	path := fmt.Sprintf("/api/v1/namespaces/%s/pods", prior.Namespace)
	var first, second podAbsentListSnapshot
	if r.read(ctx, path, &first) != nil || !first.excludesPriorPod(prior) ||
		r.read(ctx, path, &second) != nil || !second.excludesPriorPod(prior) ||
		first.Metadata.ResourceVersion != second.Metadata.ResourceVersion ||
		!reflect.DeepEqual(first.Items, second.Items) {
		return reject()
	}
	return KubernetesPodAbsentObservation{
		PreviousPod:            prior,
		PodListResourceVersion: first.Metadata.ResourceVersion,
	}, nil
}
