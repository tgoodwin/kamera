package analyze

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestBuildGraphFromRawJSON(t *testing.T) {
	data, err := os.ReadFile(filepath.Join("testdata", "graph.json"))
	require.NoError(t, err)

	raw, err := ParseRawGraphJSON(data)
	require.NoError(t, err)

	graph, err := BuildGraphFromRaw(raw)
	require.NoError(t, err)

	require.Len(t, graph.Nodes, 3)
	require.Len(t, graph.Edges, 3)

	controllerID := NodeID("c:RouteReconciler")
	controller, ok := graph.Nodes[controllerID]
	require.True(t, ok)
	require.Equal(t, NodeController, controller.Kind)
	require.Equal(t, "RouteReconciler", controller.Controller.Name)

	resourceID := NodeID("r:serving.knative.dev/v1/Route")
	resource, ok := graph.Nodes[resourceID]
	require.True(t, ok)
	require.Equal(t, NodeResource, resource.Kind)
	require.Equal(t, "serving.knative.dev", resource.Resource.Group)
	require.Equal(t, "v1", resource.Resource.Version)
	require.Equal(t, "Route", resource.Resource.Kind)

	coreServiceID := NodeID("r:core/v1/Service")
	coreService, ok := graph.Nodes[coreServiceID]
	require.True(t, ok)
	require.Equal(t, "", coreService.Resource.Group)
	require.Equal(t, "v1", coreService.Resource.Version)
	require.Equal(t, "Service", coreService.Resource.Kind)

	watchEdge := findEdge(t, graph, EdgeWatches)
	require.Equal(t, controllerID, watchEdge.From)
	require.Equal(t, resourceID, watchEdge.To)
	require.Equal(t, WatchPrimary, watchEdge.Attr.Watch.Kind)

	readEdge := findEdge(t, graph, EdgeReads)
	require.Equal(t, controllerID, readEdge.From)
	require.Equal(t, resourceID, readEdge.To)
	require.Equal(t, TargetStatus, readEdge.Attr.Read.Target)

	writeEdge := findEdge(t, graph, EdgeWrites)
	require.Equal(t, controllerID, writeEdge.From)
	require.Equal(t, coreServiceID, writeEdge.To)
	require.Equal(t, TargetSpec, writeEdge.Attr.Write.Target)
}

func TestBuildGraphRejectsInvalidEdge(t *testing.T) {
	raw := RawGraph{
		Nodes: []RawNode{
			{Kind: "controller", Name: "RouteReconciler"},
			{Kind: "resource", GVK: "serving.knative.dev/v1/Route"},
		},
		Edges: []RawEdge{
			{Kind: "owns", From: "RouteReconciler", To: "serving.knative.dev/v1/Route"},
		},
	}

	_, err := BuildGraphFromRaw(raw)
	require.Error(t, err)
}

func findEdge(t *testing.T, graph *Graph, kind EdgeKind) Edge {
	t.Helper()
	for _, edge := range graph.Edges {
		if edge.Kind == kind {
			return edge
		}
	}
	require.Fail(t, "edge kind not found", "kind=%v", kind)
	return Edge{}
}
