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
// scheduling. Larger replicas go first; a failure means this algorithm did not
// find a packing, not that every possible placement is impossible.
func packIslands(islands []Island, sizeA, countA, sizeB, countB int) bool {
	if sizeA < sizeB {
		sizeA, sizeB = sizeB, sizeA
		countA, countB = countB, countA
	}
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
