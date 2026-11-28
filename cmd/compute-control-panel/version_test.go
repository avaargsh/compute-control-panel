package main

import (
	"bytes"
	"context"
	"errors"
	"os"
	"os/exec"
	"strings"
	"testing"
	"time"

	"github.com/avaargsh/compute-control-panel/internal/buildinfo"
)

func TestVersionHelper(t *testing.T) {
	if os.Getenv("CCP_VERSION_HELPER") != "1" {
		return
	}
	args, ok := argsAfterDoubleDash()
	if !ok {
		os.Exit(2)
	}
	os.Args = append([]string{"compute-control-panel"}, args...)
	main()
	os.Exit(0)
}

func TestVersionFlagExitsBeforeListen(t *testing.T) {
	for _, args := range [][]string{{"-version"}, {"--version"}} {
		t.Run(strings.Join(args, " "), func(t *testing.T) {
			stdout, stderr, code := runCLI(t, args)
			want := buildinfo.Line("compute-control-panel") + "\n"
			if code != 0 || stdout != want || stderr != "" {
				t.Fatalf("exit=%d stdout=%q stderr=%q", code, stdout, stderr)
			}
		})
	}
}

func TestControlPlaneWithoutVersionStillUsesListenAddr(t *testing.T) {
	stdout, stderr, code := runCLI(t, nil)
	if code == 0 || strings.Contains(stdout, "(commit ") || stderr == "" {
		t.Fatalf("exit=%d stdout=%q stderr=%q", code, stdout, stderr)
	}
}

func runCLI(t *testing.T, args []string) (stdout, stderr string, code int) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	cmd := exec.CommandContext(ctx, os.Args[0], append([]string{"-test.run=^TestVersionHelper$", "--"}, args...)...)
	cmd.Env = helperEnv([]string{
		"LISTEN_ADDR=bad-address",
		"MIGRATIONS_DIR=" + t.TempDir() + "/missing",
	})
	var out, errb bytes.Buffer
	cmd.Stdout = &out
	cmd.Stderr = &errb
	err := cmd.Run()
	if ctx.Err() != nil {
		t.Fatalf("timed out: stdout=%q stderr=%q err=%v", out.String(), errb.String(), err)
	}
	if err == nil {
		return out.String(), errb.String(), 0
	}
	var exit *exec.ExitError
	if !errors.As(err, &exit) {
		t.Fatalf("run: %v stdout=%q stderr=%q", err, out.String(), errb.String())
	}
	return out.String(), errb.String(), exit.ExitCode()
}

func helperEnv(extra []string) []string {
	drop := map[string]struct{}{
		"DATABASE_URL":                  {},
		"LISTEN_ADDR":                   {},
		"MIGRATIONS_DIR":                {},
		"KUBERNETES_SERVICE_HOST":       {},
		"KUBERNETES_SERVICE_PORT_HTTPS": {},
		"CCP_VERSION_HELPER":            {},
	}
	env := []string{"CCP_VERSION_HELPER=1"}
	for _, entry := range os.Environ() {
		key, _, ok := strings.Cut(entry, "=")
		if !ok {
			continue
		}
		if _, skip := drop[key]; skip {
			continue
		}
		env = append(env, entry)
	}
	return append(env, extra...)
}

func argsAfterDoubleDash() ([]string, bool) {
	for i, arg := range os.Args {
		if arg == "--" {
			return os.Args[i+1:], true
		}
	}
	return nil, false
}
