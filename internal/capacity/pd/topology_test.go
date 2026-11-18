package pd

import (
	"testing"
	"time"
)

func TestPlanRejectsFragmentedAndAmbiguousIslands(t *testing.T) {
	now, d, p, policy, top, priv := fixture(t)
	sig := sign(t, p, priv)
	top.Islands = []Island{{ID: "a", FreeGPUs: 1}, {ID: "b", FreeGPUs: 1}, {ID: "c", FreeGPUs: 1}, {ID: "d", FreeGPUs: 1}, {ID: "e", FreeGPUs: 1}, {ID: "f", FreeGPUs: 1}, {ID: "g", FreeGPUs: 1}}
	a, err := Plan(now, d, p, sig, policy, top, time.Minute)
	if err != nil || a.Outcome != InsufficientTopology {
		t.Fatalf("aggregate capacity is not placement evidence: %+v %v", a, err)
	}
	for name, islands := range map[string][]Island{
		"missing":      nil,
		"duplicate":    {{ID: "a", FreeGPUs: 4}, {ID: "a", FreeGPUs: 3}},
		"unordered":    {{ID: "b", FreeGPUs: 4}, {ID: "a", FreeGPUs: 3}},
		"sum mismatch": {{ID: "a", FreeGPUs: 8}},
		"negative":     {{ID: "a", FreeGPUs: -1}, {ID: "b", FreeGPUs: 8}},
	} {
		t.Run(name, func(t *testing.T) {
			top.Islands = islands
			a, err := Plan(now, d, p, sig, policy, top, time.Minute)
			if err == nil || a.Outcome != Unproven {
				t.Fatalf("ambiguous topology accepted: %+v %v", a, err)
			}
		})
	}
	top.Islands = []Island{{ID: "a", FreeGPUs: 4}, {ID: "b", FreeGPUs: 3}}
	a, err = Plan(now, d, p, sig, policy, top, time.Minute)
	if err != nil || a.Outcome != Fit {
		t.Fatalf("valid disjoint islands rejected: %+v %v", a, err)
	}
}
