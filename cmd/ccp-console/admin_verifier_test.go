package main

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestReadAdminVerifierAppliesLocalFileContract(t *testing.T) {
	dir := t.TempDir()
	write := func(name, content string) string {
		t.Helper()
		path := filepath.Join(dir, name)
		if err := os.WriteFile(path, []byte(content), 0o600); err != nil {
			t.Fatal(err)
		}
		return path
	}
	valid := write("regular", " fixture-verifier\n")
	if got, err := readAdminVerifier(valid); err != nil || string(got) != "fixture-verifier" {
		t.Fatalf("regular file did not retain whitespace normalization: %v", err)
	}
	atLimit := write("limit", strings.Repeat("x", maxAdminVerifierSize))
	if got, err := readAdminVerifier(atLimit); err != nil || len(got) != maxAdminVerifierSize {
		t.Fatalf("size boundary not accepted: %v", err)
	}
	aboveLimit := write("oversized", strings.Repeat("x", maxAdminVerifierSize+1))
	for _, path := range []string{dir, filepath.Join(dir, "missing"), aboveLimit} {
		if got, err := readAdminVerifier(path); err == nil || got != nil {
			t.Fatal("invalid verifier input was accepted")
		} else if err.Error() != "console bcrypt verifier unavailable" {
			t.Fatalf("unexpected or path-bearing diagnostic: %v", err)
		}
	}
}
