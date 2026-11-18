// Package rawphase verifies offline, paired PD request traces. It cannot
// authenticate the collector or establish physical device placement.
package rawphase

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"reflect"
	"sort"
	"strings"
	"unicode/utf8"
)

const (
	maxArtifactBytes = 16 << 20
	maxLineBytes     = 16 << 10
	maxAttempts      = 100000
	maxTokens        = 1 << 53
	maxWindowMS      = 24 * 60 * 60 * 1000
)

var ErrInvalid = errors.New("invalid paired PD raw evidence")

// Manifest is pinned independently of the raw artifact. The first JSONL row
// must reproduce it exactly. DeploymentSHA256 binds the actual paired runtime
// configuration, including the clock domain, model revision and cache regime.
type Manifest struct {
	Version          string `json:"version"`
	RunID            string `json:"run_id"`
	Tenant           string `json:"tenant"`
	Project          string `json:"project"`
	Pool             string `json:"pool"`
	Model            string `json:"model"`
	GPU              string `json:"gpu"`
	Shape            string `json:"shape"`
	Fabric           string `json:"fabric"`
	EvaluatorCommit  string `json:"evaluator_commit"`
	DeploymentSHA256 string `json:"deployment_sha256"`
	PrefillWorker    string `json:"prefill_worker"`
	DecodeWorker     string `json:"decode_worker"`
	PrefillGPUs      int    `json:"prefill_gpus"`
	DecodeGPUs       int    `json:"decode_gpus"`
	StartMS          int64  `json:"start_ms"`
	EndMS            int64  `json:"end_ms"`
	Attempts         int    `json:"attempts"`
	KVBytesLimit     uint64 `json:"kv_bytes_limit"`
}

// Attempt records counters at the respective phase workers, not gateway token
// estimates. v1 requires complete successful attempts, a single confirmed KV
// transfer and first/last output tokens produced by Decode. KVBytes counts all
// wire bytes for that attempt, including protocol overhead.
type Attempt struct {
	AttemptID       string `json:"attempt_id"`
	TransferID      string `json:"transfer_id"`
	PrefillWorker   string `json:"prefill_worker"`
	DecodeWorker    string `json:"decode_worker"`
	Success         bool   `json:"success"`
	PrefillTokens   uint64 `json:"prefill_tokens"`
	DecodeTokens    uint64 `json:"decode_tokens"`
	KVBytes         uint64 `json:"kv_bytes"`
	RequestStartMS  int64  `json:"request_start_ms"`
	PrefillStartMS  int64  `json:"prefill_start_ms"`
	PrefillEndMS    int64  `json:"prefill_end_ms"`
	TransferStartMS int64  `json:"transfer_start_ms"`
	TransferEndMS   int64  `json:"transfer_end_ms"`
	DecodeStartMS   int64  `json:"decode_start_ms"`
	FirstTokenMS    int64  `json:"first_token_ms"`
	LastTokenMS     int64  `json:"last_token_ms"`
}

type Summary struct {
	Attempts                                      int
	PrefillTokensPerSecond, DecodeTokensPerSecond float64
	P95TTFTms, P95TPOTms, P95TransferMs           float64
	MaxObservedKVBytes                            uint64
}

func validID(s string) bool {
	return len(s) > 0 && len(s) <= 256 && strings.TrimSpace(s) == s && utf8.ValidString(s) && !strings.ContainsRune(s, utf8.RuneError)
}

func digest(s string, n int) bool {
	b, err := hex.DecodeString(s)
	return err == nil && len(b) == n && strings.ToLower(s) == s
}

// decodeFlat checks exact field names, duplicates, omissions, nulls and trailing
// data. Both wire structs are flat with required fields and explicit JSON tags.
func decodeFlat(line []byte, dst any) error {
	if len(line) == 0 || len(line) > maxLineBytes || !utf8.Valid(line) {
		return ErrInvalid
	}
	fields := reflect.TypeOf(dst).Elem()
	allowed := make(map[string]bool, fields.NumField())
	for i := 0; i < fields.NumField(); i++ {
		allowed[fields.Field(i).Tag.Get("json")] = true
	}
	d := json.NewDecoder(bytes.NewReader(line))
	start, err := d.Token()
	if err != nil || start != json.Delim('{') {
		return ErrInvalid
	}
	for d.More() {
		key, err := d.Token()
		k, ok := key.(string)
		if err != nil || !ok || !allowed[k] {
			return ErrInvalid
		}
		delete(allowed, k)
		var v json.RawMessage
		if d.Decode(&v) != nil || bytes.Equal(v, []byte("null")) {
			return ErrInvalid
		}
	}
	end, err := d.Token()
	if err != nil || end != json.Delim('}') || len(allowed) != 0 {
		return ErrInvalid
	}
	if _, err = d.Token(); err != io.EOF {
		return ErrInvalid
	}
	if json.Unmarshal(line, dst) != nil {
		return ErrInvalid
	}
	return nil
}

func validManifest(m Manifest, minimumAttempts int, minimumWindowMS int64) bool {
	for _, s := range []string{m.RunID, m.Tenant, m.Project, m.Pool, m.Model, m.GPU, m.Shape, m.Fabric, m.PrefillWorker, m.DecodeWorker} {
		if !validID(s) {
			return false
		}
	}
	return m.Version == "pd-raw/v1" && digest(m.EvaluatorCommit, 20) && digest(m.DeploymentSHA256, 32) && m.PrefillWorker != m.DecodeWorker && m.PrefillGPUs > 0 && m.DecodeGPUs > 0 && minimumAttempts > 0 && m.Attempts >= minimumAttempts && m.Attempts <= maxAttempts && minimumWindowMS > 0 && m.StartMS > 0 && m.EndMS > m.StartMS && m.EndMS-m.StartMS >= minimumWindowMS && m.EndMS-m.StartMS <= maxWindowMS && m.KVBytesLimit > 0
}

func validAttempt(a Attempt, m Manifest) bool {
	return validID(a.AttemptID) && validID(a.TransferID) && a.PrefillWorker == m.PrefillWorker && a.DecodeWorker == m.DecodeWorker && a.Success && a.PrefillTokens > 0 && a.PrefillTokens <= maxTokens && a.DecodeTokens >= 2 && a.DecodeTokens <= maxTokens && a.KVBytes > 0 && a.KVBytes <= m.KVBytesLimit && m.StartMS <= a.RequestStartMS && a.RequestStartMS <= a.PrefillStartMS && a.PrefillStartMS < a.PrefillEndMS && a.PrefillEndMS <= a.TransferStartMS && a.TransferStartMS < a.TransferEndMS && a.TransferEndMS <= a.DecodeStartMS && a.DecodeStartMS <= a.FirstTokenMS && a.FirstTokenMS < a.LastTokenMS && a.LastTokenMS <= m.EndMS
}

func p95(v []float64) float64 {
	sort.Float64s(v)
	return v[(len(v)*95+99)/100-1]
}

// Verify recomputes phase-specific counters over the same independently pinned
// wall-clock measurement window (including idle time), not summed durations or
// unified gateway throughput. It rejects failed/missing attempts rather than
// silently deriving an optimistic successful-subset profile. The caller still
// must authenticate the collector, manifest, clock and runtime configuration.
func Verify(raw []byte, artifactSHA256 string, expected Manifest, minimumAttempts int, minimumWindowMS int64) (Summary, error) {
	fail := func() (Summary, error) { return Summary{}, ErrInvalid }
	if len(raw) == 0 || len(raw) > maxArtifactBytes || !validManifest(expected, minimumAttempts, minimumWindowMS) {
		return fail()
	}
	sum := sha256.Sum256(raw)
	if hex.EncodeToString(sum[:]) != artifactSHA256 {
		return fail()
	}
	line, rest, _ := bytes.Cut(raw, []byte{'\n'})
	var m Manifest
	if decodeFlat(line, &m) != nil || m != expected {
		return fail()
	}
	var out Summary
	var prefill, decode uint64
	var ttft, tpot, transfer []float64
	attempts, transfers := map[string]bool{}, map[string]bool{}
	for len(rest) > 0 {
		line, rest, _ = bytes.Cut(rest, []byte{'\n'})
		var a Attempt
		if out.Attempts >= m.Attempts || decodeFlat(line, &a) != nil || !validAttempt(a, m) || attempts[a.AttemptID] || transfers[a.TransferID] || prefill > maxTokens-a.PrefillTokens || decode > maxTokens-a.DecodeTokens {
			return fail()
		}
		attempts[a.AttemptID], transfers[a.TransferID] = true, true
		out.Attempts++
		prefill += a.PrefillTokens
		decode += a.DecodeTokens
		out.MaxObservedKVBytes = max(out.MaxObservedKVBytes, a.KVBytes)
		ttft = append(ttft, float64(a.FirstTokenMS-a.RequestStartMS))
		tpot = append(tpot, float64(a.LastTokenMS-a.FirstTokenMS)/float64(a.DecodeTokens-1))
		transfer = append(transfer, float64(a.TransferEndMS-a.TransferStartMS))
	}
	if out.Attempts != m.Attempts {
		return fail()
	}
	seconds := float64(m.EndMS-m.StartMS) / 1000
	out.PrefillTokensPerSecond = float64(prefill) / seconds
	out.DecodeTokensPerSecond = float64(decode) / seconds
	out.P95TTFTms, out.P95TPOTms, out.P95TransferMs = p95(ttft), p95(tpot), p95(transfer)
	return out, nil
}
