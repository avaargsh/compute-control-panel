package pd

import (
	"testing"
	"time"
)

func TestIntentGenerationIdentityAndRevalidation(t *testing.T) {
	now, d, p, policy, top, priv := fixture(t)
	sig := sign(t, p, priv)
	a, err := Propose(now, d, p, sig, policy, top, time.Minute)
	if err != nil || a.GenerationKey == "" || a.GenerationKey == a.IntentID {
		t.Fatalf("missing separate generation identity: %+v %v", a, err)
	}
	if err := RevalidateProposal(now, a, 2, d, p, sig, policy, top, time.Minute); err != nil {
		t.Fatal(err)
	}
	if err := RevalidateProposal(now, a, 3, d, p, sig, policy, top, time.Minute); err == nil {
		t.Fatal("stale generation accepted")
	}
	top.PoolRevision++
	b, err := Propose(now, d, p, sig, policy, top, time.Minute)
	if err != nil || a.GenerationKey != b.GenerationKey || a.IntentID == b.IntentID {
		t.Fatalf("changed revision must conflict under same generation key: %+v %v", b, err)
	}
	if err := RevalidateProposal(now, a, 2, d, p, sig, policy, top, time.Minute); err == nil {
		t.Fatal("old revision accepted")
	}
	b.GPUsNeeded--
	if err := RevalidateProposal(now, b, 2, d, p, sig, policy, top, time.Minute); err == nil {
		t.Fatal("changed proposal accepted")
	}
	d.Generation++
	c, err := Propose(now, d, p, sig, policy, top, time.Minute)
	if err != nil || c.GenerationKey == a.GenerationKey {
		t.Fatalf("new generation must have new key: %+v %v", c, err)
	}
}
