package tracecheck

import (
	"fmt"
	"slices"
	"strings"

	"github.com/samber/lo"
	"github.com/tgoodwin/kamera/pkg/util"
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

		label := fmt.Sprintf("%s", util.ShortenHash(string(h)))
		fill := "#e0e0e0"
		shape := "box"
		if len(node.ConvergedIDs) > 0 {
			fill = "#c7f3c7"
			shape = "doublecircle"
		} else if len(node.AbortedIDs) > 0 {
			fill = "#f8d7da"
		}
		b.WriteString(fmt.Sprintf(`  "%s" [label="%s", shape=%s, fillcolor="%s"];`+"\n",
			escapeDOT(string(h)), escapeDOT(label), shape, fill))
	}

	// Render edges deterministically and collapse duplicates per (from,to,controller).
	type edgeEntry struct {
		from       StateHash
		to         StateHash
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
