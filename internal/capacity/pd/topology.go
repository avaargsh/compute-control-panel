package pd

// Island is a disjoint group of compatible free GPUs within one fabric. An
// authenticated inventory adapter must establish actual membership, disjointness
// and connectivity. An ID/count alone is not physical topology proof.
type Island struct {
	ID       string
	FreeGPUs int
}

// Require deterministic order, no duplicates, and reconciliation against the
// single shared free capacity. Limit complexity independently of GPU counts.
func validIslands(t Topology) bool {
	if len(t.Islands) == 0 || len(t.Islands) > 1024 {
		return false
	}
	total := 0
	last := ""
	for _, island := range t.Islands {
		if !validID(island.ID) || island.ID <= last || island.FreeGPUs < 0 || island.FreeGPUs > t.FreeGPUs-total {
			return false
		}
		total += island.FreeGPUs
		last = island.ID
	}
	return total == t.FreeGPUs
}

// packIslands is a bounded conservative witness search, not Kubernetes
// scheduling. Trying both phase priorities avoids rejecting simple valid
// placements missed by larger-first greedy. A failure still means no witness
// was found, not that every possible placement is impossible.
func packIslands(islands []Island, sizeA, countA, sizeB, countB int) bool {
	return packIslandsOrdered(islands, sizeA, countA, sizeB, countB) ||
		packIslandsOrdered(islands, sizeB, countB, sizeA, countA)
}

func packIslandsOrdered(islands []Island, sizeA, countA, sizeB, countB int) bool {
	for _, island := range islands {
		free := island.FreeGPUs
		a := min(countA, free/sizeA)
		countA -= a
		free -= a * sizeA
		b := min(countB, free/sizeB)
		countB -= b
		if countA == 0 && countB == 0 {
			return true
		}
	}
	return false
}
