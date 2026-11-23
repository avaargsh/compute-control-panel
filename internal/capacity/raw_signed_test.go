package capacity

import (
	"crypto/sha256"
	"fmt"
	"testing"
	"time"
)

func TestAdviseRawSignedRecomputesBeforeReturningAdvice(t *testing.T) {
	now, d, c, policy, pool, priv := signedFixture(t)
	d.WorkloadShape, c.WorkloadShape, policy.WorkloadShape = "s", "s", "s"
	raw := []byte(`{"run_id":"run-1","request_id":"a","scenario":"s","model_revision":"rev-1","success":true,"input_tokens":100,"output_tokens":3,"start_ms":0,"end_ms":1000,"first_content_token_ms":200}`)
	c.RawSHA256 = fmt.Sprintf("%x", sha256.Sum256(raw))
	c.Attempts, c.Successes, c.TPOTSamples = 1, 1, 1
	c.ObservedWallSeconds, c.PrefillTokensPerSec, c.DecodeTokensPerSec = 1, 100, 3
	c.P95TTFTms, c.P95TPOTms = 200, 400
	d.PrefillTokensPerSec, d.DecodeTokensPerSec, d.MaxTPOTms = 100, 3, 500
	policy.MinAttempts, policy.MinWallSeconds = 1, 1
	a, err := AdviseRawSigned(now, d, raw, c, signClaim(t, c, priv), policy, pool, time.Minute)
	if err != nil || a.Outcome != EstimatedFit || a.GPUsNeeded != 2 || a.Admitted {
		t.Fatalf("verified raw advice: %+v %v", a, err)
	}
	c.PrefillTokensPerSec++
	a, err = AdviseRawSigned(now, d, raw, c, signClaim(t, c, priv), policy, pool, time.Minute)
	if err == nil || a != (Advice{Outcome: Unproven}) {
		t.Fatalf("signed incorrect summary accepted: %+v %v", a, err)
	}
	c.PrefillTokensPerSec--
	a, err = AdviseRawSigned(now, d, append(raw, '\n'), c, signClaim(t, c, priv), policy, pool, time.Minute)
	if err == nil || a != (Advice{Outcome: Unproven}) {
		t.Fatalf("raw digest mismatch accepted: %+v %v", a, err)
	}
}
