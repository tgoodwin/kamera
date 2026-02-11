package analyze

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestDetectHotspots(t *testing.T) {
	raw := RawGraph{
		Nodes: []RawNode{
			{Kind: "controller", Name: "ControllerA"},
			{Kind: "controller", Name: "ControllerB"},
			{Kind: "controller", Name: "ControllerC"},
			{Kind: "resource", GVK: "core/v1/Service"},
			{Kind: "resource", GVK: "apps/v1/Deployment"},
			{Kind: "resource", GVK: "core/v1/ConfigMap"},
			{Kind: "resource", GVK: "example.dev/v1/Parent"},
			{Kind: "resource", GVK: "example.dev/v1/ChildA"},
			{Kind: "resource", GVK: "example.dev/v1/ChildB"},
		},
		Edges: []RawEdge{
			{Kind: "writes", From: "ControllerA", To: "core/v1/Service", Target: "spec"},
			{Kind: "writes", From: "ControllerB", To: "core/v1/Service", Target: "spec"},
			{Kind: "reads", From: "ControllerC", To: "apps/v1/Deployment", Target: "status"},
			{Kind: "writes", From: "ControllerA", To: "apps/v1/Deployment", Target: "spec"},
			{Kind: "watches", From: "ControllerA", To: "core/v1/ConfigMap", WatchKind: "indexed"},
			{Kind: "watches", From: "ControllerB", To: "core/v1/ConfigMap", WatchKind: "primary"},
			{Kind: "writes", From: "ControllerB", To: "core/v1/ConfigMap", Target: "status"},
			{Kind: "reads", From: "ControllerA", To: "core/v1/ConfigMap", Target: "status"},
			{Kind: "writes", From: "ControllerA", To: "example.dev/v1/ChildA", Target: "spec"},
			{Kind: "writes", From: "ControllerB", To: "example.dev/v1/ChildB", Target: "spec"},
			{Kind: "owns", From: "example.dev/v1/Parent", To: "example.dev/v1/ChildA"},
			{Kind: "owns", From: "example.dev/v1/Parent", To: "example.dev/v1/ChildB"},
			{Kind: "reads", From: "ControllerB", To: "core/v1/Service", Target: "spec"},
			{Kind: "reads", From: "ControllerB", To: "apps/v1/Deployment", Target: "spec"},
		},
	}

	graph, err := BuildGraphFromRaw(raw)
	require.NoError(t, err)

	hotspots, err := DetectHotspots(graph)
	require.NoError(t, err)

	byType := groupHotspotsByType(hotspots)

	require.NotEmpty(t, byType[HotspotMultiWriter])

	missing := byType[HotspotMissingTrigger]
	require.GreaterOrEqual(t, len(missing), 2)
	require.True(t, hasMissingTriggerResource(missing, NodeID("r:apps/v1/Deployment")))
	require.True(t, hasMissingTriggerWithoutResource(missing))

	fanout := byType[HotspotDiamondPattern]
	require.NotEmpty(t, fanout)
	require.True(t, hasFanoutViaOwns(fanout))

	aggregation := byType[HotspotReducer]
	require.Len(t, aggregation, 1)
	require.True(t, containsNode(aggregation[0].Controllers, NodeID("c:ControllerB")))

	cycles := byType[HotspotFeedbackCycle]
	require.Len(t, cycles, 1)
	require.True(t, containsNode(cycles[0].Controllers, NodeID("c:ControllerB")))
	require.True(t, containsNode(cycles[0].Resources, NodeID("r:core/v1/ConfigMap")))
}

func groupHotspotsByType(hotspots []HotspotInstance) map[HotspotType][]HotspotInstance {
	out := make(map[HotspotType][]HotspotInstance)
	for _, hotspot := range hotspots {
		out[hotspot.Type] = append(out[hotspot.Type], hotspot)
	}
	return out
}

func containsNode(nodes []NodeID, target NodeID) bool {
	for _, node := range nodes {
		if node == target {
			return true
		}
	}
	return false
}

func hasMissingTriggerResource(hotspots []HotspotInstance, resource NodeID) bool {
	for _, hotspot := range hotspots {
		if hotspot.Attributes["missing_trigger_resource"] == string(resource) {
			return true
		}
	}
	return false
}

func hasMissingTriggerWithoutResource(hotspots []HotspotInstance) bool {
	for _, hotspot := range hotspots {
		if _, ok := hotspot.Attributes["missing_trigger_resource"]; !ok {
			return true
		}
	}
	return false
}

func hasFanoutViaOwns(hotspots []HotspotInstance) bool {
	for _, hotspot := range hotspots {
		if hotspot.Attributes["converges_via"] == "owns" {
			return true
		}
	}
	return false
}
