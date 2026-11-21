package provider

import (
	"encoding/json"
	"reflect"
)

// JobSet v0.13.0's apiserver/admission/defaulting inserts a small, verified
// set of non-executable, fixed policy defaults into the exact inert CREATE.
// Strip ONLY these known values before strict spec decoding and comparison.
// Unknown/non-default options, executable Pod mutations, alternative JobSet
// startup policy and metadata injection must remain fatal. This is a pinned
// controller compatibility contract, not an accept-any-defaults escape hatch.
func removePDDefault(obj map[string]any, key string, expected any) error {
	got, ok := obj[key]
	if !ok {
		return nil
	}
	if !reflect.DeepEqual(got, expected) {
		return ErrInvalidKueueObservation
	}
	delete(obj, key)
	return nil
}

func pdMapField(obj map[string]any, key string) (map[string]any, error) {
	value, ok := obj[key].(map[string]any)
	if !ok {
		return nil, ErrInvalidKueueObservation
	}
	return value, nil
}

// normalizePDServerDefaults removes only the exact observed JobSet v0.13
// defaults from the admission webhook. The caller MUST then decode the result
// with DisallowUnknownFields and compare ALL owned fields, including image,
// resourceClaims, phase count, UID, annotations and the gated suspend state.
func normalizePDServerDefaults(raw json.RawMessage) ([]byte, error) {
	var spec map[string]any
	if len(raw) == 0 || json.Unmarshal(raw, &spec) != nil || spec == nil {
		return nil, ErrInvalidKueueObservation
	}
	for _, item := range []struct {
		key  string
		want any
	}{
		{"network", map[string]any{
			"enableDNSHostnames": true, "publishNotReadyAddresses": true,
		}},
		{"startupPolicy", map[string]any{"startupPolicyOrder": "AnyOrder"}},
		{"successPolicy", map[string]any{"operator": "All"}},
	} {
		if err := removePDDefault(spec, item.key, item.want); err != nil {
			return nil, err
		}
	}
	phases, ok := spec["replicatedJobs"].([]any)
	if !ok || len(phases) != 2 {
		return nil, ErrInvalidKueueObservation
	}
	for _, phase := range phases {
		job, ok := phase.(map[string]any)
		if !ok {
			return nil, ErrInvalidKueueObservation
		}
		if err := removePDDefault(job, "groupName", "default"); err != nil {
			return nil, err
		}
		tmpl, err := pdMapField(job, "template")
		if err != nil {
			return nil, err
		}
		if err := removePDDefault(tmpl, "metadata", map[string]any{}); err != nil {
			return nil, err
		}
		jobSpec, err := pdMapField(tmpl, "spec")
		if err != nil {
			return nil, err
		}
		if err := removePDDefault(jobSpec, "completionMode", "Indexed"); err != nil {
			return nil, err
		}
		podTemplate, err := pdMapField(jobSpec, "template")
		if err != nil {
			return nil, err
		}
		if err := removePDDefault(podTemplate, "metadata", map[string]any{}); err != nil {
			return nil, err
		}
	}
	clean, err := json.Marshal(spec)
	if err != nil {
		return nil, ErrInvalidKueueObservation
	}
	return clean, nil
}
