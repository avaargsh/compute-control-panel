package main

import (
	"strings"
	"testing"
)

func kubeEnv(key string) string {
	switch key {
	case "KUBERNETES_SERVICE_HOST":
		return "10.42.0.1"
	case "KUBERNETES_SERVICE_PORT_HTTPS":
		return "443"
	default:
		return ""
	}
}

func TestParseObservationOptionsRequiresUIDPinnedReadOnlyScope(t *testing.T) {
	opts, err := parseOptions([]string{
		"--tenant", "tenant-a", "--workload", "workload-a",
		"--generation", "7", "--namespace", "tenant-a",
		"--job", "job-a", "--uid", "job-uid-a",
	}, kubeEnv)
	if err != nil {
		t.Fatal(err)
	}
	if opts.apiURL != "https://10.42.0.1:443" ||
		opts.query.TenantID != "tenant-a" || opts.query.DesiredGeneration != 7 ||
		opts.query.ExpectedJobUID != "job-uid-a" ||
		!strings.HasSuffix(opts.tokenPath, "/serviceaccount/token") ||
		!strings.HasSuffix(opts.caPath, "/serviceaccount/ca.crt") {
		t.Fatalf("invalid observation target: %+v", opts)
	}
}

func TestParseObservationOptionsRejectsMissingIdentityAndInvalidClusterSettings(t *testing.T) {
	good := []string{
		"--tenant", "tenant-a", "--workload", "workload-a",
		"--generation", "7", "--namespace", "tenant-a",
		"--job", "job-a", "--uid", "job-uid-a",
	}
	cases := []struct {
		name string
		args []string
		env  func(string) string
	}{
		{"no target", nil, kubeEnv},
		{"UID omitted", good[:len(good)-2], kubeEnv},
		{"zero generation", append(append([]string{}, good...), "--generation", "0"), kubeEnv},
		{"invalid port", good, func(key string) string {
			if key == "KUBERNETES_SERVICE_PORT_HTTPS" {
				return "bad-port"
			}
			return kubeEnv(key)
		}},
		{"missing API host", good, func(key string) string {
			if key == "KUBERNETES_SERVICE_HOST" {
				return ""
			}
			return kubeEnv(key)
		}},
		{"unrecognized flag", append(append([]string{}, good...), "--delete-job"), kubeEnv},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if _, err := parseOptions(tc.args, tc.env); err == nil {
				t.Fatal("untrusted cluster or incomplete scope accepted")
			}
		})
	}
}
