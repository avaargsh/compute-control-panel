package rawphase

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"strings"
	"testing"
)

func scenario() (Manifest, []Attempt) {
	m := Manifest{Version: "pd-raw/v1", RunID: "run", Tenant: "t", Project: "p", Pool: "pool", Model: "m", GPU: "g", Shape: "s", Fabric: "f", EvaluatorCommit: strings.Repeat("a", 40), DeploymentSHA256: strings.Repeat("b", 64), PrefillWorker: "pf", DecodeWorker: "dc", PrefillGPUs: 2, DecodeGPUs: 1, StartMS: 1000, EndMS: 3000, Attempts: 2, KVBytesLimit: 1000}
	return m, []Attempt{
		{AttemptID: "a", TransferID: "x", PrefillWorker: "pf", DecodeWorker: "dc", Success: true, PrefillTokens: 100, DecodeTokens: 3, KVBytes: 800, RequestStartMS: 1000, PrefillStartMS: 1000, PrefillEndMS: 1100, TransferStartMS: 1100, TransferEndMS: 1150, DecodeStartMS: 1150, FirstTokenMS: 1200, LastTokenMS: 1400},
		{AttemptID: "b", TransferID: "y", PrefillWorker: "pf", DecodeWorker: "dc", Success: true, PrefillTokens: 100, DecodeTokens: 3, KVBytes: 900, RequestStartMS: 1500, PrefillStartMS: 1550, PrefillEndMS: 1650, TransferStartMS: 1650, TransferEndMS: 1750, DecodeStartMS: 1800, FirstTokenMS: 1900, LastTokenMS: 2300},
	}
}

func artifact(t *testing.T, m Manifest, rows []Attempt) []byte {
	t.Helper()
	b, err := json.Marshal(m)
	if err != nil {
		t.Fatal(err)
	}
	for _, row := range rows {
		r, err := json.Marshal(row)
		if err != nil {
			t.Fatal(err)
		}
		b = append(append(b, '\n'), r...)
	}
	return b
}

func hash(b []byte) string {
	sum := sha256.Sum256(b)
	return hex.EncodeToString(sum[:])
}

func TestVerifyRecomputesPhaseCountersOverPinnedWindow(t *testing.T) {
	m, rows := scenario()
	raw := artifact(t, m, rows)
	got, err := Verify(raw, hash(raw), m, 2, 2000)
	want := Summary{Attempts: 2, PrefillTokensPerSecond: 100, DecodeTokensPerSecond: 3, P95TTFTms: 400, P95TPOTms: 200, P95TransferMs: 100, MaxObservedKVBytes: 900}
	if err != nil || got != want {
		t.Fatalf("got %+v, want %+v, err %v", got, want, err)
	}
}

func TestVerifyRejectsIncompleteOrContradictoryEvidence(t *testing.T) {
	for name, change := range map[string]func(*Manifest, []Attempt){
		"failed attempt":      func(_ *Manifest, r []Attempt) { r[1].Success = false },
		"duplicate attempt":   func(_ *Manifest, r []Attempt) { r[1].AttemptID = r[0].AttemptID },
		"duplicate transfer":  func(_ *Manifest, r []Attempt) { r[1].TransferID = r[0].TransferID },
		"wrong worker":        func(_ *Manifest, r []Attempt) { r[1].DecodeWorker = "other" },
		"decode before KV":    func(_ *Manifest, r []Attempt) { r[1].DecodeStartMS = r[1].TransferEndMS - 1 },
		"KV exceeds envelope": func(m *Manifest, r []Attempt) { r[1].KVBytes = m.KVBytesLimit + 1 },
		"zero prefill":        func(_ *Manifest, r []Attempt) { r[1].PrefillTokens = 0 },
		"one decode token":    func(_ *Manifest, r []Attempt) { r[1].DecodeTokens = 1 },
		"outside window":      func(m *Manifest, r []Attempt) { r[1].LastTokenMS = m.EndMS + 1 },
		"missing attempt":     func(m *Manifest, _ []Attempt) { m.Attempts++ },
	} {
		t.Run(name, func(t *testing.T) {
			m, rows := scenario()
			change(&m, rows)
			raw := artifact(t, m, rows)
			if got, err := Verify(raw, hash(raw), m, 2, 2000); err == nil || got != (Summary{}) {
				t.Fatalf("accepted %+v %v", got, err)
			}
		})
	}
	m, rows := scenario()
	raw := artifact(t, m, rows)
	for _, bad := range []string{
		strings.Replace(string(raw), `"success":true`, `"success":true,"SUCCESS":false`, 1),
		strings.Replace(string(raw), `"success":true`, `"success":true,"success":true`, 1),
		strings.Replace(string(raw), `"success":true`, `"success":null`, 1),
		strings.Replace(string(raw), `"success":true`, `"extra":1,"success":true`, 1),
		strings.Replace(string(raw), `"success":true,`, "", 1),
		strings.Repeat(" ", maxLineBytes) + string(raw),
	} {
		if got, err := Verify([]byte(bad), hash([]byte(bad)), m, 2, 2000); err == nil || got != (Summary{}) {
			t.Fatalf("accepted malformed record: %+v %v", got, err)
		}
	}
	wrong := m
	wrong.DeploymentSHA256 = strings.Repeat("c", 64)
	if _, err := Verify(raw, hash(raw), wrong, 2, 2000); err == nil {
		t.Fatal("accepted wrong manifest")
	}
	if _, err := Verify(raw, strings.Repeat("0", 64), m, 2, 2000); err == nil {
		t.Fatal("accepted wrong digest")
	}
	if _, err := Verify(raw, hash(raw), m, 3, 2000); err == nil {
		t.Fatal("accepted short sample")
	}
	if _, err := Verify(raw, hash(raw), m, 2, 2001); err == nil {
		t.Fatal("accepted short window")
	}
}
