package analyze

import (
	"fmt"
	"slices"
	"strings"
)

const (
	readEdgeColor  = "#1f77b4"
	writeEdgeColor = "#d62728"
	controllerFill = "#cfe2ff"
	resourceFill   = "#e0e0e0"
)

// RenderDependencyGraphDOT renders a dependency graph to deterministic Graphviz DOT.
func RenderDependencyGraphDOT(graph *Graph) string {
	if graph == nil {
		return "digraph DependencyGraph {\n}\n"
	}

	var b strings.Builder
	b.WriteString("digraph DependencyGraph {\n")
	b.WriteString("  rankdir=LR;\n")
	b.WriteString("  node [style=\"rounded,filled\", fontname=\"monospace\", fontsize=10];\n")

	nodeIDs := make([]NodeID, 0, len(graph.Nodes))
	for id := range graph.Nodes {
		nodeIDs = append(nodeIDs, id)
	}
	slices.Sort(nodeIDs)

	for _, id := range nodeIDs {
		node := graph.Nodes[id]
		label := nodeLabel(node)
		shape := "ellipse"
		fill := resourceFill
		if node.Kind == NodeController {
			shape = "box"
			fill = controllerFill
		}
		b.WriteString(fmt.Sprintf("  \"%s\" [label=\"%s\", shape=%s, fillcolor=\"%s\"];\n",
			escapeDOT(string(id)), escapeDOT(label), shape, fill))
	}

	edges := make([]Edge, len(graph.Edges))
	copy(edges, graph.Edges)
	slices.SortFunc(edges, func(a, b Edge) int {
		return strings.Compare(string(a.ID), string(b.ID))
	})

	for _, edge := range edges {
		attrs := []string{fmt.Sprintf("label=\"%s\"", escapeDOT(edgeLabel(edge)))}
		switch edge.Kind {
		case EdgeReads:
			attrs = append(attrs, fmt.Sprintf("color=\"%s\"", readEdgeColor))
			attrs = append(attrs, fmt.Sprintf("fontcolor=\"%s\"", readEdgeColor))
		case EdgeWrites:
			attrs = append(attrs, fmt.Sprintf("color=\"%s\"", writeEdgeColor))
			attrs = append(attrs, fmt.Sprintf("fontcolor=\"%s\"", writeEdgeColor))
		}
		b.WriteString(fmt.Sprintf("  \"%s\" -> \"%s\" [%s];\n",
			escapeDOT(string(edge.From)),
			escapeDOT(string(edge.To)),
			strings.Join(attrs, ", ")))
	}

	b.WriteString("}\n")
	return b.String()
}

func nodeLabel(node Node) string {
	if node.Kind == NodeController {
		return node.Controller.Name
	}
	return fmt.Sprintf("%s/%s/%s", canonicalGroup(node.Resource.Group), node.Resource.Version, node.Resource.Kind)
}

func edgeLabel(edge Edge) string {
	kind := edgeKindString(edge.Kind)
	switch edge.Kind {
	case EdgeWatches:
		return fmt.Sprintf("%s:%s", kind, watchKindString(edge.Attr.Watch.Kind))
	case EdgeReads:
		return fmt.Sprintf("%s:%s", kind, targetString(edge.Attr.Read.Target))
	case EdgeWrites:
		return fmt.Sprintf("%s:%s", kind, targetString(edge.Attr.Write.Target))
	default:
		return kind
	}
}

func escapeDOT(s string) string {
	repl := strings.NewReplacer("\"", "\\\"", "\\", "\\\\")
	return repl.Replace(s)
}
