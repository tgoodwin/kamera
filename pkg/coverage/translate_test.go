package coverage

import (
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/tgoodwin/kamera/pkg/analyze"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
)

func TestTranslateHotspots(t *testing.T) {
	raw := analyze.RawGraph{
		Nodes: []analyze.RawNode{
			{Kind: "controller", Name: "WriterA"},
			{Kind: "controller", Name: "WriterB"},
			{Kind: "resource", GVK: "core/v1/Service"},
		},
		Edges: []analyze.RawEdge{
			{Kind: "reconciles", From: "WriterA", To: "core/v1/Service"},
			{Kind: "reconciles", From: "WriterB", To: "core/v1/Service"},
			{Kind: "writes", From: "WriterA", To: "core/v1/Service", Target: "status"},
			{Kind: "writes", From: "WriterB", To: "core/v1/Service", Target: "status"},
		},
	}

	graph, err := analyze.BuildGraphFromRaw(raw)
	require.NoError(t, err)

	hotspots, err := analyze.DetectHotspots(graph)
	require.NoError(t, err)
	require.NotEmpty(t, hotspots)

	var multi []analyze.HotspotInstance
	for _, hs := range hotspots {
		if hs.Type == analyze.HotspotMultiWriter {
			multi = append(multi, hs)
		}
	}
	require.NotEmpty(t, multi)

	template := &unstructured.Unstructured{Object: map[string]any{
		"apiVersion": "v1",
		"kind":       "Service",
		"metadata": map[string]any{
			"name":      "template",
			"namespace": "template-ns",
		},
		"status": map[string]any{
			"phase": "Active",
		},
	}}

	inputMap := InputMap{Mapping: map[string][]InputTemplate{
		"core/v1/Service": {{Name: "svc", Object: template}},
	}}

	inputs, err := TranslateHotspots(graph, multi, inputMap)
	require.NoError(t, err)
	require.Len(t, inputs, len(multi))

	input := inputs[0]
	require.Len(t, input.UserInputs, 1)
	inputAction := input.UserInputs[0]
	require.Equal(t, "CREATE", string(inputAction.Type))
	obj := inputAction.Object
	require.Equal(t, "hs-multi_writer-0-service", obj.GetName())
	require.Equal(t, "default", obj.GetNamespace())

	_, found, err := unstructured.NestedFieldNoCopy(obj.Object, "status")
	require.NoError(t, err)
	require.False(t, found)

	require.ElementsMatch(t, []string{"WriterA", "WriterB"}, input.Tuning.PermuteControllers)
}
