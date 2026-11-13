package main

import "testing"

func TestParseRelistArgs(t *testing.T) {
	for _, phase := range []string{"record", "window", "converge", "recover-delete", "recover-successor"} {
		got, err := parseRelistArgs([]string{"relist", phase})
		if err != nil || got != phase {
			t.Fatalf("parse %s: %q %v", phase, got, err)
		}
	}
	for _, phase := range []string{"delete", "successor"} {
		got, err := parseRelistArgs([]string{"relist", phase, "--hold"})
		if err != nil || got != phase {
			t.Fatalf("parse hold %s: %q %v", phase, got, err)
		}
	}
	if _, err := parseRelistArgs([]string{"relist", "preemption"}); err == nil {
		t.Fatal("accepted a phase owned by another smoke")
	}
	if _, err := parseRelistArgs([]string{"admission", "create"}); err == nil {
		t.Fatal("accepted an admission command")
	}
}
