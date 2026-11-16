package provider

import (
	"encoding/json"
	"testing"
)

func TestQueuedJobExplicitSingleClaim(t *testing.T) {
	a := &KueueJobAdapter{namespace: "tenant-a", queueName: "queue-a"}
	if err := a.UseQueuedResourceClaim("claim-a"); err != nil {
		t.Fatal(err)
	}
	body, err := a.jobBody(kueueAdapterRequest(), "job-a")
	if err != nil {
		t.Fatal(err)
	}
	var raw map[string]any
	if err := json.Unmarshal(body, &raw); err != nil {
		t.Fatal(err)
	}
	spec := raw["spec"].(map[string]any)["template"].(map[string]any)["spec"].(map[string]any)
	claims := spec["resourceClaims"].([]any)
	if len(claims) != 1 || claims[0].(map[string]any)["resourceClaimName"] != "claim-a" {
		t.Fatalf("wrong claim: %s", body)
	}
	resources := spec["containers"].([]any)[0].(map[string]any)["resources"].(map[string]any)
	if resources["claims"].([]any)[0].(map[string]any)["name"] != claims[0].(map[string]any)["name"] {
		t.Fatalf("container does not consume claim: %s", body)
	}
	var job kubeObject
	if err := json.Unmarshal(body, &job); err != nil {
		t.Fatal(err)
	}
	if !a.exactJobSpec(job) {
		t.Fatal("submitted claim template rejected")
	}
	job.Spec.Template.Spec.Containers[0].Resources.Claims[0].Name = "other"
	if a.exactJobSpec(job) {
		t.Fatal("adopted missing container consumer")
	}
	job.Spec.Template.Spec.Containers[0].Resources.Claims[0].Name = "gpu"
	job.Spec.Template.Spec.ResourceClaims[0].ResourceClaimTemplateName = "template"
	if a.exactJobSpec(job) {
		t.Fatal("adopted unresolved template")
	}
	job.Spec.Template.Spec.ResourceClaims[0].ResourceClaimTemplateName = ""
	job.Spec.Template.Spec.ResourceClaims[0].ResourceClaimName = "replacement"
	if a.exactJobSpec(job) {
		t.Fatal("adopted different claim")
	}
	if (&KueueJobAdapter{queueName: "queue-a"}).exactJobSpec(job) {
		t.Fatal("default adapter adopted a claim consumer")
	}
}

func TestQueuedClaimConfigurationRejectsInvalidMode(t *testing.T) {
	for _, a := range []*KueueJobAdapter{nil, {}} {
		if a.UseQueuedResourceClaim("claim-a") == nil {
			t.Fatal("accepted unqueued claim")
		}
	}
	a := &KueueJobAdapter{queueName: "queue-a"}
	for _, name := range []string{"", "../claim", "claim/other"} {
		if a.UseQueuedResourceClaim(name) == nil {
			t.Fatalf("accepted %q", name)
		}
	}
}
