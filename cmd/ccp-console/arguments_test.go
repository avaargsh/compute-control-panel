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
)

func TestConsoleRejectsPositionalArgumentsBeforeConfig(t *testing.T) {
	for _, args := range [][]string{
		{"install"},
		{"-check-config", "unexpected"},
		{"unexpected", "-check-config"},
		{"--", "unexpected"},
	} {
		t.Run(strings.Join(args, " "), func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()
			cmd := exec.CommandContext(ctx, os.Args[0], append([]string{"-test.run=^TestConsoleArgumentHelper$", "--"}, args...)...)
			cmd.Env = append(os.Environ(), "CCP_CONSOLE_ARGUMENT_HELPER=1")
			var stdout, stderr bytes.Buffer
			cmd.Stdout, cmd.Stderr = &stdout, &stderr
			var exit *exec.ExitError
			err := cmd.Run()
			if !errors.As(err, &exit) || exit.ExitCode() != 2 || stdout.Len() != 0 ||
				!strings.Contains(stderr.String(), "console does not accept positional arguments") {
				t.Fatalf("err=%v stdout=%q stderr=%q", err, stdout.String(), stderr.String())
			}
		})
	}
}

func TestConsoleArgumentHelper(t *testing.T) {
	if os.Getenv("CCP_CONSOLE_ARGUMENT_HELPER") != "1" {
		return
	}
	// main parses package-level flags; replace the test harness arguments.
	os.Args = append([]string{"ccp-console"}, os.Args[3:]...)
	main()
	os.Exit(0)
}
