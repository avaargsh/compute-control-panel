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
    tests := []struct {
        name string
        value string
        want bool
    }{
        {"exact lowercase 40-byte Git SHA", valid, true},
        {"missing", "", false},
        {"shortened", valid[:39], false},
        {"overlong", valid + "0", false},
        {"uppercase", strings.ToUpper(valid), false},
        {"leading whitespace", " " + valid, false},
        {"trailing newline", valid + "\n", false},
        {"embedded NUL", valid[:20] + "\x00" + valid[21:], false},
        {"non-hex", strings.Repeat("g", 40), false},
    }
    for _, tc := range tests {
        t.Run(tc.name, func(t *testing.T) {
            got, err := checkedSmokeSourceCommit(func(key string) string {
                if key != "CCP_SMOKE_SOURCE_COMMIT" {
                    t.Fatalf("unexpected source of provenance: %s", key)
                }
                return tc.value
            })
            if tc.want {
                if err != nil || got != tc.value {
                    t.Fatalf("valid pinned revision rejected: got=%q err=%v", got, err)
                }
            } else if err == nil || got != "" {
                t.Fatalf("unsafe or missing revision promoted: got=%q err=%v", got, err)
            }
        })
    }
    if got, err := checkedSmokeSourceCommit(nil); err == nil || got != "" {
        t.Fatalf("nil environment reader accepted: got=%q err=%v", got, err)
    }
}
