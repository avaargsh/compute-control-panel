package main

import (
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
)

// A stale kind cluster is NOT permission to remove a similarly named Docker
// container that was started outside this disposable smoke invocation.
// Reproduce the early-exit cleanup path without Docker or Kubernetes.
func TestExistingKindClusterDoesNotRemoveUnownedPostgres(t *testing.T) {
	binDir := t.TempDir()
	logPath := filepath.Join(t.TempDir(), "docker-calls")
	fakes := map[string]string{
		"kind":    "#!/bin/sh\nif [ \"$1\" = get ] && [ \"$2\" = clusters ]; then echo ccp-kueue-smoke; exit 0; fi\nexit 2\n",
		"docker":  "#!/bin/sh\nprintf '%s\\n' \"$*\" >> \"$CCP_KIND_DOCKER_LOG\"\nexit 0\n",
		"kubectl": "#!/bin/sh\nexit 0\n",
		"jq":      "#!/bin/sh\nexit 0\n",
		"go":      "#!/bin/sh\nexit 0\n",
		"curl":    "#!/bin/sh\nexit 0\n",
	}
	for name, script := range fakes {
		if err := os.WriteFile(filepath.Join(binDir, name), []byte(script), 0700); err != nil {
			t.Fatal(err)
		}
	}
	cmd := exec.Command("bash", "../../scripts/kueue_kind_smoke.sh")
	cmd.Env = append(os.Environ(),
		"PATH="+binDir+":"+os.Getenv("PATH"),
		"CCP_DISPOSABLE_KIND_SMOKE=1",
		"CCP_KIND_DOCKER_LOG="+logPath,
	)
	out, err := cmd.CombinedOutput()
	if err == nil || !strings.Contains(string(out), "existing disposable cluster; refusing to reuse") {
		t.Fatalf("expected refusal before starting disposable resources, err=%v output=%s", err, out)
	}
	calls, err := os.ReadFile(logPath)
	if err != nil && !errors.Is(err, os.ErrNotExist) {
		t.Fatal(err)
	}
	if strings.Contains(string(calls), "rm -f") {
		t.Fatalf("early cleanup deleted an unowned Docker container: %s", calls)
	}
}
