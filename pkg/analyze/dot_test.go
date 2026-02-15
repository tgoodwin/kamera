package analyze

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestRenderDependencyGraphDOT(t *testing.T) {
	data, err := os.ReadFile(filepath.Join("testdata", "graph.json"))
	require.NoError(t, err)

	raw, err := ParseRawGraphJSON(data)
	require.NoError(t, err)

	graph, err := BuildGraphFromRaw(raw)
	require.NoError(t, err)

	dot := RenderDependencyGraphDOT(graph)

	expected := "" +
		"digraph DependencyGraph {\n" +
		"  rankdir=LR;\n" +
		"  node [style=\"rounded,filled\", fontname=\"monospace\", fontsize=10];\n" +
		"  \"c:RouteReconciler\" [label=\"RouteReconciler\", shape=box, fillcolor=\"#cfe2ff\"];\n" +
		"  \"r:core/v1/Service\" [label=\"core/v1/Service\", shape=ellipse, fillcolor=\"#e0e0e0\"];\n" +
		"  \"r:serving.knative.dev/v1/Route\" [label=\"serving.knative.dev/v1/Route\", shape=ellipse, fillcolor=\"#e0e0e0\"];\n" +
		"  \"c:RouteReconciler\" -> \"r:serving.knative.dev/v1/Route\" [label=\"reads:status\", color=\"#1f77b4\", fontcolor=\"#1f77b4\"];\n" +
		"  \"c:RouteReconciler\" -> \"r:serving.knative.dev/v1/Route\" [label=\"watches:primary\"];\n" +
		"  \"c:RouteReconciler\" -> \"r:core/v1/Service\" [label=\"writes:spec\", color=\"#d62728\", fontcolor=\"#d62728\"];\n" +
		"}\n"

	require.Equal(t, expected, dot)
}
