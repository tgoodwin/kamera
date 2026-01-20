package tracecheck

import (
	"fmt"
	"slices"
	"strings"

	"github.com/samber/lo"
)

// GraphvizOpts controls DOT rendering.
type GraphvizOpts struct {
	// Label edges with controller (occurrence counts are not included).
	LabelEdges bool
	// Drop no-op reconcile edges (no changes, no error), regardless of whether they are self loops.
	DropNoOpEdges bool
	// Drop self-loop edges (from == to) even if they had effects; useful to trim cycles into the same state hash.
	DropSelfLoops bool
	// Include nodes that have no incoming/outgoing edges (isolated).
	IncludeIsolated bool
}

// RenderStateDAGDOT renders the DAG to Graphviz DOT format for external visualization.
// Nodes are keyed by ContentsHash; converged nodes are doublecircle and green, aborted nodes red.
func RenderStateDAGDOT(dag *StateDAG, opts GraphvizOpts) string {
	if dag == nil {
		return "digraph G {\n}\n"
	}

	var b strings.Builder
	b.WriteString("digraph StateDAG {\n")
	b.WriteString(`  rankdir=TB; node [shape=box, style="rounded,filled", fontname="monospace", fontsize=10];` + "\n")

	// Render nodes in deterministic order.
	hashes := lo.Keys(dag.Nodes)
	slices.Sort(hashes)
	for _, h := range hashes {
		node := dag.Nodes[h]
		if node == nil {
			continue
		}
		if !opts.IncludeIsolated && len(node.Outgoing) == 0 && len(node.Incoming) == 0 {
			continue
		}

		label := string(h)
		fill := "#e0e0e0" // gray: intermediate/unknown
		shape := "box"
		if len(node.ConvergedIDs) > 0 {
			// Green: converged states
			fill = "#c7f3c7"
			shape = "doublecircle"
		} else if len(node.AbortedIDs) > 0 {
			// Red: error-aborted states
			fill = "#f8d7da"
		} else if len(node.MaxDepthIDs) > 0 {
			// Yellow: max-depth aborted states
			fill = "#fff3cd"
		}
		b.WriteString(fmt.Sprintf(`  "%s" [label="%s", shape=%s, fillcolor="%s"];`+"\n",
			escapeDOT(string(h)), escapeDOT(label), shape, fill))
	}

	// Render edges deterministically and collapse duplicates per (from,to,controller).
	type edgeEntry struct {
		from       ContentsHash
		to         ContentsHash
		controller ReconcilerID
		effects    int
		errorStr   string
		count      int
	}
	edgeMap := make(map[string]*edgeEntry)
	for from, node := range dag.Nodes {
		if node == nil {
			continue
		}
		for to, list := range node.Outgoing {
			for _, e := range list {
				key := fmt.Sprintf("%s|%s|%s", from, to, e.Controller)
				entry, exists := edgeMap[key]
				if !exists {
					entry = &edgeEntry{
						from:       from,
						to:         to,
						controller: e.Controller,
						effects:    e.Effects,
						errorStr:   e.Error,
						count:      0,
					}
					edgeMap[key] = entry
				}
				entry.count += e.Occurrences
			}
		}
	}

	var edges []edgeEntry
	for _, e := range edgeMap {
		edges = append(edges, *e)
	}
	slices.SortFunc(edges, func(a, b edgeEntry) int {
		if a.from != b.from {
			if a.from < b.from {
				return -1
			}
			return 1
		}
		if a.to != b.to {
			if a.to < b.to {
				return -1
			}
			return 1
		}
		return strings.Compare(string(a.controller), string(b.controller))
	})

	for _, ent := range edges {
		if opts.DropNoOpEdges && ent.effects == 0 && ent.errorStr == "" {
			continue
		}
		if opts.DropSelfLoops && ent.from == ent.to {
			continue
		}
		label := ""
		if opts.LabelEdges {
			label = fmt.Sprintf("%s", ent.controller)
		}
		if label == "" {
			b.WriteString(fmt.Sprintf(`  "%s" -> "%s";`+"\n", escapeDOT(string(ent.from)), escapeDOT(string(ent.to))))
		} else {
			b.WriteString(fmt.Sprintf(`  "%s" -> "%s" [label="%s"];`+"\n",
				escapeDOT(string(ent.from)), escapeDOT(string(ent.to)), escapeDOT(label)))
		}
	}

	b.WriteString("}\n")
	return b.String()
}

func escapeDOT(s string) string {
	repl := strings.NewReplacer(`"`, `\"`, `\`, `\\`)
	return repl.Replace(s)
}

// RenderStateDAGNodeDetails renders a text summary with divergence analysis.
// Identifies true divergence points (LCA of converged nodes reaching different terminal states).
func RenderStateDAGNodeDetails(dag *StateDAG) string {
	if dag == nil {
		return ""
	}

	var b strings.Builder

	// Find all converged nodes
	var convergedHashes []ContentsHash
	for h, node := range dag.Nodes {
		if node != nil && len(node.ConvergedIDs) > 0 {
			convergedHashes = append(convergedHashes, h)
		}
	}
	slices.Sort(convergedHashes)

	b.WriteString("\n# Convergence Analysis\n")
	b.WriteString(fmt.Sprintf("# Terminal states: %d\n\n", len(convergedHashes)))

	if len(convergedHashes) <= 1 {
		b.WriteString("No divergence detected (single terminal state)\n")
		return b.String()
	}

	// List converged states
	b.WriteString("## Converged States\n")
	for _, h := range convergedHashes {
		b.WriteString(fmt.Sprintf("  - %s\n", h))
	}
	b.WriteString("\n")

	// Compute ancestors for each converged node (BFS backwards)
	ancestors := make(map[ContentsHash]map[ContentsHash]int) // node -> ancestor -> depth
	for _, target := range convergedHashes {
		ancestors[target] = computeAncestors(dag, target)
	}

	// Find divergence points: for each pair of converged nodes, find their LCA
	divergencePoints := make(map[ContentsHash][]ContentsHash) // divergence point -> which converged nodes diverge here
	for i := 0; i < len(convergedHashes); i++ {
		for j := i + 1; j < len(convergedHashes); j++ {
			a, b := convergedHashes[i], convergedHashes[j]
			lca := findLCA(ancestors[a], ancestors[b])
			if lca != "" {
				divergencePoints[lca] = append(divergencePoints[lca], a, b)
			}
		}
	}

	// Report divergence points
	b.WriteString("## Divergence Points\n")
	b.WriteString("# LCA = last common ancestor before paths split to different terminal states\n\n")

	dpHashes := lo.Keys(divergencePoints)
	slices.Sort(dpHashes)
	for _, dp := range dpHashes {
		targets := lo.Uniq(divergencePoints[dp])
		slices.Sort(targets)
		b.WriteString(fmt.Sprintf("[%s] diverges to: %v\n", dp, targets))
	}

	return b.String()
}

// computeAncestors returns all ancestors of a node with their depth (distance from target).
func computeAncestors(dag *StateDAG, target ContentsHash) map[ContentsHash]int {
	ancestors := make(map[ContentsHash]int)
	ancestors[target] = 0

	queue := []ContentsHash{target}
	for len(queue) > 0 {
		current := queue[0]
		queue = queue[1:]
		currentDepth := ancestors[current]

		node := dag.Nodes[current]
		if node == nil {
			continue
		}

		for parent := range node.Incoming {
			if _, seen := ancestors[parent]; !seen {
				ancestors[parent] = currentDepth + 1
				queue = append(queue, parent)
			}
		}
	}

	return ancestors
}

// RenderContentsHashMapping outputs a mapping from ContentsHash to path/step coordinates.
// This enables cross-referencing DAG nodes with dump file steps.
func RenderContentsHashMapping(states []ResultState) string {
	var b strings.Builder
	b.WriteString("\n# ContentsHash to Step Mapping\n")
	b.WriteString("# Format: [ContentsHash] state=N path=M step=S controller=CtrlName\n\n")

	for stateIdx, state := range states {
		for pathIdx, path := range state.Paths {
			for stepIdx, step := range path {
				if step == nil || len(step.StateAfter) == 0 {
					continue
				}
				stateNode := StateNode{
					Contents: NewStateSnapshot(step.StateAfter, step.KindSeqAfter, nil),
				}
				contentsHash := stateNode.ContentsHash()
				b.WriteString(fmt.Sprintf("[%s] state=%d path=%d step=%d controller=%s\n",
					contentsHash, stateIdx, pathIdx, stepIdx, step.ControllerID))
			}
		}
	}

	return b.String()
}

// findLCA finds the last common ancestor (closest to both terminals).
// Depth is measured from terminal, so smaller depth = closer to terminal.
// Returns empty string if no common ancestor exists.
func findLCA(ancestorsA, ancestorsB map[ContentsHash]int) ContentsHash {
	var lca ContentsHash
	minMaxDepth := -1

	for node, depthA := range ancestorsA {
		if depthB, ok := ancestorsB[node]; ok {
			// Use max depth as the "distance to furthest terminal"
			// We want the node closest to both terminals = smallest max depth
			maxDepth := depthA
			if depthB > maxDepth {
				maxDepth = depthB
			}
			if minMaxDepth == -1 || maxDepth < minMaxDepth {
				minMaxDepth = maxDepth
				lca = node
			}
		}
	}

	return lca
}
