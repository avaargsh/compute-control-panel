package main

import (
	"bytes"
	"context"
	"io"
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
	}, kubeEnv, io.Discard)
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
			if _, err := parseOptions(tc.args, tc.env, io.Discard); err == nil {
				t.Fatal("untrusted cluster or incomplete scope accepted")
			}
		})
	}
}

func TestObserverHelpIsOffline(t *testing.T) {
	for _, arg := range []string{"-h", "--help"} {
		t.Run(arg, func(t *testing.T) {
			var output bytes.Buffer
			err := run(context.Background(), []string{arg}, func(key string) string {
				t.Fatalf("help must not read Kubernetes environment variable %q", key)
				return ""
			}, &output)
			if err != nil {
				t.Fatal(err)
			}
			for _, word := range []string{"Usage: kueue-observe", "read-only", "-tenant", "-workload", "-generation", "-namespace", "-job", "-uid", "KUBERNETES_SERVICE_HOST", "KUBERNETES_SERVICE_PORT_HTTPS"} {
				if !strings.Contains(output.String(), word) {
					t.Errorf("help missing %q", word)
				}
			}
		})
	}
}
