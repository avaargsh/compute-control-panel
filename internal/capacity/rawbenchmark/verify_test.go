package rawbenchmark

import (
	"crypto/sha256"
	"fmt"
	"strings"
	"testing"
)

const first = `{"run_id":"r","request_id":"a","scenario":"s","model_revision":"m","success":true,"input_tokens":100,"output_tokens":3,"start_ms":0,"end_ms":1000,"first_content_token_ms":200}`
const second = `{"run_id":"r","request_id":"b","scenario":"s","model_revision":"m","success":true,"input_tokens":100,"output_tokens":3,"start_ms":200,"end_ms":2000,"first_content_token_ms":600}`

func pin(raw string) Expectation {
	return Expectation{SHA256: fmt.Sprintf("%x", sha256.Sum256([]byte(raw))), RunID: "r", Scenario: "s", ModelRevision: "m"}
}

func TestVerifyRecomputesWallMetrics(t *testing.T) {
	raw := first + "\n" + second + "\n"
	s, err := Verify([]byte(raw), pin(raw))
	if err != nil || s != (Summary{Attempts: 2, Successes: 2, TPOTSamples: 2, WallSeconds: 2, InputTokensPerSecond: 100, OutputTokensPerSecond: 3, P95TTFTms: 400, P95TPOTms: 700}) {
		t.Fatalf("%+v %v", s, err)
	}
}

func TestVerifyRejectsAmbiguousOrIncompleteRaw(t *testing.T) {
	for name, raw := range map[string]string{
		"duplicate request": first + "\n" + first,
		"duplicate field":   strings.Replace(first, `"success":true`, `"success":false,"success":true`, 1),
		"wrong scenario":    strings.Replace(first, `"scenario":"s"`, `"scenario":"other"`, 1),
		"missing success":   strings.Replace(first, `"success":true,`, "", 1),
		"null success":      strings.Replace(first, `"success":true`, `"success":null`, 1),
		"missing TPOT":      strings.Replace(first, `"output_tokens":3`, `"output_tokens":1`, 1),
		"first after end":   strings.Replace(first, `"first_content_token_ms":200`, `"first_content_token_ms":1100`, 1),
		"unknown field":     strings.Replace(first, `"success":true`, `"success":true,"extra":123`, 1),
		"trailing":          first + ` {}`,
		"empty":             "",
	} {
		t.Run(name, func(t *testing.T) {
			s, err := Verify([]byte(raw), pin(raw))
			if err == nil || s != (Summary{}) {
				t.Fatalf("accepted: %+v %v", s, err)
			}
		})
	}
	if _, err := Verify([]byte(first), pin(second)); err == nil {
		t.Fatal("digest mismatch accepted")
	}
}

func TestFailedAttemptsExtendWallButNotGoodput(t *testing.T) {
	raw := first + "\n" + `{"run_id":"r","request_id":"b","scenario":"s","model_revision":"m","success":false,"input_tokens":100,"output_tokens":1,"start_ms":1000,"end_ms":4000,"first_content_token_ms":null}`
	s, err := Verify([]byte(raw), pin(raw))
	if err != nil || s.Attempts != 2 || s.Successes != 1 || s.WallSeconds != 4 || s.InputTokensPerSecond != 25 || s.OutputTokensPerSecond != 0.75 {
		t.Fatalf("%+v %v", s, err)
	}
}
