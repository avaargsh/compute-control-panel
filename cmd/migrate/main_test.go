package main

import (
	"errors"
	"os"
	"os/exec"
	"strings"
	"testing"
)

func TestMigrationCLI(t *testing.T) {
	for _, tc := range []struct {
		name    string
		args    []string
		dsn     string
		code    int
		message string
	}{
		{"offline-help", []string{"--help"}, "", 0, "Usage: migrate"},
		{"short-help", []string{"-h"}, "", 0, "Usage: migrate"},
		{"unsupported-flag", []string{"--dry-run"}, "not-used", 2, "does not accept arguments"},
		{"unsupported-operand", []string{"check"}, "not-used", 2, "does not accept arguments"},
		{"help-with-operand", []string{"--help", "check"}, "not-used", 2, "does not accept arguments"},
		{"missing-dsn", nil, "", 2, "DATABASE_URL is required"},
		{"normal-config-path", nil, "not-used", 1, "load migrations"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			cmd := exec.Command(os.Args[0], append([]string{"-test.run=^TestMigrationCLIHelper$", "--"}, tc.args...)...)
			cmd.Env = append(os.Environ(), "TEST_MIGRATION_CLI_HELPER=1", "DATABASE_URL="+tc.dsn, "MIGRATIONS_DIR="+t.TempDir()+"/missing")
			out, err := cmd.CombinedOutput()
			code := 0
			var exit *exec.ExitError
			if errors.As(err, &exit) {
				code = exit.ExitCode()
			} else if err != nil {
				t.Fatal(err)
			}
			if code != tc.code || !strings.Contains(string(out), tc.message) {
				t.Fatalf("exit=%d, output=%q; want exit=%d and %q", code, out, tc.code, tc.message)
			}
		})
	}
}

func TestMigrationCLIHelper(t *testing.T) {
	if os.Getenv("TEST_MIGRATION_CLI_HELPER") != "1" {
		return
	}
	os.Args = append([]string{"migrate"}, os.Args[3:]...)
	main()
	os.Exit(0)
}
