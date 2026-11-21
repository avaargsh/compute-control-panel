package rawbenchmark

import (
	"crypto/sha256"
	"fmt"
	"strings"
	"testing"
)

func TestVerifyRawLineBoundsBeforeTrimming(t *testing.T) {
	const record = `{"run_id":"r","request_id":"a","scenario":"s","model_revision":"m","success":true,"input_tokens":100,"output_tokens":3,"start_ms":0,"end_ms":1000,"first_content_token_ms":200}`
	for name, raw := range map[string]string{
		"leading padding":      strings.Repeat(" ", maxRowBytes) + record,
		"trailing padding":     record + strings.Repeat(" ", maxRowBytes),
		"blank oversized line": strings.Repeat(" ", maxRowBytes+1) + "\n" + record,
	} {
		t.Run(name, func(t *testing.T) {
			expected := Expectation{SHA256: fmt.Sprintf("%x", sha256.Sum256([]byte(raw))), RunID: "r", Scenario: "s", ModelRevision: "m"}
			if got, err := Verify([]byte(raw), expected); err == nil || got != (Summary{}) {
				t.Fatalf("oversized physical line accepted: %+v %v", got, err)
			}
		})
	}
	for _, raw := range []string{record, record + strings.Repeat(" ", maxRowBytes-len(record)), "\n" + record + "\n"} {
		expected := Expectation{SHA256: fmt.Sprintf("%x", sha256.Sum256([]byte(raw))), RunID: "r", Scenario: "s", ModelRevision: "m"}
		if got, err := Verify([]byte(raw), expected); err != nil || got.Successes != 1 {
			t.Fatalf("valid boundary rejected: %+v %v", got, err)
		}
	}
}
