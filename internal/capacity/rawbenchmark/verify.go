// Package rawbenchmark independently recomputes unified run metrics. It does
// not certify deployment identity, sign claims, or interpret PD phase rates.
package rawbenchmark

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"math"
	"sort"
	"strings"
	"unicode/utf8"
)

const maxArtifactBytes = 16 << 20
const maxRows = 100000
const maxRowBytes = 16 << 10
const maxExactTokens = 1 << 53

var ErrInvalid = errors.New("invalid pinned benchmark artifact")

// Expectation must come from an independently authenticated run manifest.
type Expectation struct{ SHA256, RunID, Scenario, ModelRevision string }
type Summary struct {
	Attempts, Successes, TPOTSamples                                               int64
	WallSeconds, InputTokensPerSecond, OutputTokensPerSecond, P95TTFTms, P95TPOTms float64
}

func id(s string) bool {
	return s != "" && len(s) <= 256 && strings.TrimSpace(s) == s && utf8.ValidString(s) && !strings.ContainsRune(s, utf8.RuneError)
}
func finite(f float64) bool { return !math.IsNaN(f) && !math.IsInf(f, 0) }

// parseRow rejects duplicate keys, null required fields and unknown fields.
// The v1 evaluator uses a flat object; nested data is not accepted.
func parseRow(line []byte) (map[string]json.RawMessage, error) {
	if len(line) > maxRowBytes || !utf8.Valid(line) {
		return nil, ErrInvalid
	}
	d := json.NewDecoder(bytes.NewReader(line))
	start, e := d.Token()
	if e != nil || start != json.Delim('{') {
		return nil, ErrInvalid
	}
	values := map[string]json.RawMessage{}
	for d.More() {
		token, e := d.Token()
		k, ok := token.(string)
		if e != nil || !ok {
			return nil, ErrInvalid
		}
		if _, ok := values[k]; ok {
			return nil, ErrInvalid
		}
		switch k {
		case "run_id", "request_id", "scenario", "model_revision", "success", "input_tokens", "output_tokens", "start_ms", "end_ms", "first_content_token_ms":
		default:
			return nil, ErrInvalid
		}
		var v json.RawMessage
		if d.Decode(&v) != nil {
			return nil, ErrInvalid
		}
		values[k] = v
	}
	end, e := d.Token()
	if e != nil || end != json.Delim('}') {
		return nil, ErrInvalid
	}
	if _, e = d.Token(); e != io.EOF {
		return nil, ErrInvalid
	}
	for _, k := range []string{"run_id", "request_id", "scenario", "model_revision", "success", "input_tokens", "output_tokens", "start_ms", "end_ms"} {
		if v := values[k]; len(v) == 0 || bytes.Equal(v, []byte("null")) {
			return nil, ErrInvalid
		}
	}
	return values, nil
}
func p95(v []float64) float64 { sort.Float64s(v); return v[(len(v)*95+99)/100-1] }

// Verify pins raw bytes and one run/scenario/model, then recomputes wall-clock
// goodput and nearest-rank p95 metrics from unique attempts. A successful
// attempt must have at least two output tokens and positive latency samples.
func Verify(raw []byte, expected Expectation) (Summary, error) {
	fail := func() (Summary, error) { return Summary{}, ErrInvalid }
	if len(raw) == 0 || len(raw) > maxArtifactBytes || !id(expected.RunID) || !id(expected.Scenario) || !id(expected.ModelRevision) {
		return fail()
	}
	sum := sha256.Sum256(raw)
	if hex.EncodeToString(sum[:]) != expected.SHA256 {
		return fail()
	}
	seen := map[string]bool{}
	var out Summary
	var ttft, tpot []float64
	var in, output uint64
	start, end := math.Inf(1), math.Inf(-1)
	// Bound physical lines before trimming and avoid allocating a slice per newline.
	for rest := raw; len(rest) > 0; {
		var line []byte
		line, rest, _ = bytes.Cut(rest, []byte{'\n'})
		if len(line) > maxRowBytes {
			return fail()
		}
		line = bytes.TrimSpace(line)
		if len(line) == 0 {
			continue
		}
		if out.Attempts >= maxRows {
			return fail()
		}
		row, e := parseRow(line)
		if e != nil {
			return fail()
		}
		var run, request, scenario, model string
		var success bool
		var inputN, outputN uint64
		var s, f float64
		if json.Unmarshal(row["run_id"], &run) != nil || json.Unmarshal(row["request_id"], &request) != nil || json.Unmarshal(row["scenario"], &scenario) != nil || json.Unmarshal(row["model_revision"], &model) != nil || json.Unmarshal(row["success"], &success) != nil || json.Unmarshal(row["input_tokens"], &inputN) != nil || json.Unmarshal(row["output_tokens"], &outputN) != nil || json.Unmarshal(row["start_ms"], &s) != nil || json.Unmarshal(row["end_ms"], &f) != nil {
			return fail()
		}
		if run != expected.RunID || scenario != expected.Scenario || model != expected.ModelRevision || !id(request) || seen[request] || inputN > maxExactTokens || outputN > maxExactTokens || !finite(s) || !finite(f) || f <= s {
			return fail()
		}
		seen[request] = true
		out.Attempts++
		start = math.Min(start, s)
		end = math.Max(end, f)
		var first *float64
		if v := row["first_content_token_ms"]; len(v) > 0 {
			if json.Unmarshal(v, &first) != nil {
				return fail()
			}
		}
		if first != nil && (!finite(*first) || *first < s || *first > f) {
			return fail()
		}
		if !success {
			continue
		}
		if first == nil || inputN == 0 || outputN < 2 || *first <= s || *first >= f || in > maxExactTokens-inputN || output > maxExactTokens-outputN {
			return fail()
		}
		a, b := *first-s, (f-*first)/float64(outputN-1)
		if !finite(a) || !finite(b) || a <= 0 || b <= 0 {
			return fail()
		}
		ttft = append(ttft, a)
		tpot = append(tpot, b)
		in += inputN
		output += outputN
		out.Successes++
		out.TPOTSamples++
	}
	if out.Successes == 0 {
		return fail()
	}
	out.WallSeconds = (end - start) / 1000
	out.InputTokensPerSecond = float64(in) / out.WallSeconds
	out.OutputTokensPerSecond = float64(output) / out.WallSeconds
	if !finite(out.WallSeconds) || out.WallSeconds <= 0 || !finite(out.InputTokensPerSecond) || out.InputTokensPerSecond <= 0 || !finite(out.OutputTokensPerSecond) || out.OutputTokensPerSecond <= 0 {
		return fail()
	}
	out.P95TTFTms = p95(ttft)
	out.P95TPOTms = p95(tpot)
	return out, nil
}
