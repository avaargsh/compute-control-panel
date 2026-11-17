package main

import (
	"strings"
	"testing"
)

// A checkout SHA is useful reproducibility metadata, not trusted source
// authentication. Missing or malformed metadata must never be emitted as a
// plausible M1/M2 release evidence revision.
func TestCheckedSmokeSourceCommitFailsClosed(t *testing.T) {
	valid := strings.Repeat("ab", 20)
	check := func(name, source string, want bool) {
		t.Run(name, func(t *testing.T) {
			got, err := checkedSmokeSourceCommit(func(key string) string {
				if key != "CCP_SMOKE_SOURCE_COMMIT" {
					t.Fatalf("unexpected source of provenance: %s", key)
				}
				return source
			})
			if want {
				if err != nil || got != source {
					t.Fatalf("valid pinned revision rejected: got=%q err=%v", got, err)
				}
			} else if err == nil || got != "" {
				t.Fatalf("unsafe or missing revision promoted: got=%q err=%v", got, err)
			}
		})
	}
	check("exact lowercase 40-byte Git SHA", valid, true)
	check("missing", "", false)
	check("shortened", valid[:39], false)
	check("overlong", valid+"0", false)
	check("uppercase", strings.ToUpper(valid), false)
	check("leading whitespace", " "+valid, false)
	check("trailing newline", valid+"\n", false)
	check("embedded NUL", valid[:20]+"\x00"+valid[21:], false)
	check("non-hex", strings.Repeat("g", 40), false)
	if got, err := checkedSmokeSourceCommit(nil); err == nil || got != "" {
		t.Fatalf("nil environment reader accepted: got=%q err=%v", got, err)
	}
}
