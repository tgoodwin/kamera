package tracecheck

import "strconv"

// MergeStateNodes merges objects, pending reconciles, and kind sequences from the
// supplied state nodes into a single StateNode rooted at primary.
func MergeStateNodes(primary StateNode, others ...StateNode) StateNode {
	merged := primary

	pendingSeen := make(map[string]struct{})
	pending := make([]PendingReconcile, 0, len(merged.PendingReconciles))
	appendPending := func(items []PendingReconcile) {
		for _, pr := range items {
			key := pr.String()
			if pr.NotBeforeDepth > 0 {
				key = key + "@" + strconv.Itoa(pr.NotBeforeDepth)
			}
			if _, exists := pendingSeen[key]; exists {
				continue
			}
			pendingSeen[key] = struct{}{}
			pending = append(pending, pr)
		}
	}

	appendPending(merged.PendingReconciles)

	objects := merged.Objects()
	if objects == nil {
		objects = make(ObjectVersions)
	}

	allNodes := append([]StateNode{merged}, others...)

	for _, node := range others {
		for key, value := range node.Objects() {
			objects[key] = value
		}
		appendPending(node.PendingReconciles)
	}

	kindSeq := make(KindSequences)
	for key := range objects {
		canonicalKind := key.IdentityKey.CanonicalGroupKind()
		var (
			seq   int64
			found bool
		)
		for _, node := range allNodes {
			if node.Contents.KindSequences == nil {
				continue
			}
			if val, ok := node.Contents.KindSequences[canonicalKind]; ok {
				seq = val
				found = true
				break
			}
		}
		if !found {
			seq = 1
		}
		kindSeq[canonicalKind] = seq
	}

	merged.PendingReconciles = pending
	merged.Contents.KindSequences = kindSeq
	return merged
}
