package provider

import (
	"encoding/json"
	"testing"
)

// The queued Job must reference the template; Kubernetes, not CCP, generates
// a new per-Pod ResourceClaim and binds its name in Pod status.
func TestQueuedJobExplicitSingleClaimTemplate(t *testing.T) {
	a := &KueueJobAdapter{namespace: "tenant-a", queueName: "queue-a"}
	if err := a.UseQueuedResourceClaimTemplate("template-a"); err != nil {
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
	if len(claims) != 1 || claims[0].(map[string]any)["resourceClaimTemplateName"] != "template-a" {
		t.Fatalf("wrong claim template: %s", body)
	}
	if _, exists := claims[0].(map[string]any)["resourceClaimName"]; exists {
		t.Fatalf("pre-created Claim mixed with template: %s", body)
	}
	resources := spec["containers"].([]any)[0].(map[string]any)["resources"].(map[string]any)
	if resources["claims"].([]any)[0].(map[string]any)["name"] != claims[0].(map[string]any)["name"] {
		t.Fatalf("container does not consume template: %s", body)
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
	job.Spec.Template.Spec.ResourceClaims[0].ResourceClaimTemplateName = "other-template"
	if a.exactJobSpec(job) {
		t.Fatal("adopted foreign template")
	}
	job.Spec.Template.Spec.ResourceClaims[0].ResourceClaimTemplateName = "template-a"
	job.Spec.Template.Spec.ResourceClaims[0].ResourceClaimName = "precreated-claim"
	if a.exactJobSpec(job) {
		t.Fatal("adopted direct-Claim injection")
	}
	job.Spec.Template.Spec.ResourceClaims[0].ResourceClaimName = ""
	job.Spec.Template.Spec.ResourceClaims[0].ResourceClaimTemplateName = ""
	if a.exactJobSpec(job) {
		t.Fatal("adopted unresolved template")
	}
	if (&KueueJobAdapter{queueName: "queue-a"}).exactJobSpec(job) {
		t.Fatal("default adapter adopted claim consumer")
	}
}

func TestQueuedClaimTemplateConfigurationRejectsInvalidMode(t *testing.T) {
	for _, a := range []*KueueJobAdapter{nil, {}} {
		if a.UseQueuedResourceClaimTemplate("template-a") == nil {
			t.Fatal("accepted unqueued claim template")
		}
	}
	a := &KueueJobAdapter{queueName: "queue-a"}
	for _, name := range []string{"", "../template", "template/other"} {
		if a.UseQueuedResourceClaimTemplate(name) == nil {
			t.Fatalf("accepted %q", name)
		}
	}
}
