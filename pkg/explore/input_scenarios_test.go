package explore

import (
	"testing"

	"github.com/tgoodwin/kamera/pkg/analyze"
	"github.com/tgoodwin/kamera/pkg/tracecheck"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	ctrlclient "sigs.k8s.io/controller-runtime/pkg/client"
)

func TestInputsToScenariosMultiWriter(t *testing.T) {
	raw := analyze.RawGraph{
		Nodes: []analyze.RawNode{
			{Kind: "controller", Name: "WriterA"},
			{Kind: "controller", Name: "WriterB"},
			{Kind: "resource", GVK: "core/v1/Service"},
		},
		Edges: []analyze.RawEdge{
			{Kind: "reconciles", From: "WriterA", To: "core/v1/Service"},
			{Kind: "reconciles", From: "WriterB", To: "core/v1/Service"},
		},
	}
	graph, err := analyze.BuildGraphFromRaw(raw)
	if err != nil {
		t.Fatalf("build graph: %v", err)
	}

	hotspot := analyze.HotspotInstance{
		Type:        analyze.HotspotMultiWriter,
		Controllers: []analyze.NodeID{"c:WriterA", "c:WriterB"},
		Resources:   []analyze.NodeID{"r:core/v1/Service"},
		Attributes:  map[string]string{"target": "any"},
	}

	svc := &unstructured.Unstructured{}
	svc.SetGroupVersionKind(schema.GroupVersionKind{Group: "", Version: "v1", Kind: "Service"})
	svc.SetNamespace("default")
	svc.SetName("svc")

	inputs := []Input{{Objects: []ctrlclient.Object{svc}}}

	builder := tracecheck.NewExplorerBuilder(runtime.NewScheme())

	scenarios, err := InputsToScenarios(builder, hotspot, inputs, graph, nil)
	if err != nil {
		t.Fatalf("inputs to scenarios: %v", err)
	}
	if len(scenarios) != 1 {
		t.Fatalf("expected 1 scenario, got %d", len(scenarios))
	}

	cfg := scenarios[0].Config
	if len(cfg.PermutationScope) != 2 {
		t.Fatalf("expected 2 controllers in permutation scope, got %v", cfg.PermutationScope)
	}

	pending := scenarios[0].InitialState.PendingReconciles
	if len(pending) != 2 {
		t.Fatalf("expected 2 pending reconciles, got %d", len(pending))
	}
}

func TestInputsToScenariosMissingTriggerStaleReads(t *testing.T) {
	raw := analyze.RawGraph{
		Nodes: []analyze.RawNode{
			{Kind: "controller", Name: "Reader"},
			{Kind: "resource", GVK: "core/v1/ConfigMap"},
		},
		Edges: []analyze.RawEdge{
			{Kind: "reconciles", From: "Reader", To: "core/v1/ConfigMap"},
		},
	}
	graph, err := analyze.BuildGraphFromRaw(raw)
	if err != nil {
		t.Fatalf("build graph: %v", err)
	}

	hotspot := analyze.HotspotInstance{
		Type:        analyze.HotspotMissingTrigger,
		Controllers: []analyze.NodeID{"c:Reader"},
		Resources:   []analyze.NodeID{"r:core/v1/ConfigMap"},
		Attributes:  map[string]string{"missing_trigger": "true"},
	}

	cm := &unstructured.Unstructured{}
	cm.SetGroupVersionKind(schema.GroupVersionKind{Group: "", Version: "v1", Kind: "ConfigMap"})
	cm.SetNamespace("default")
	cm.SetName("cfg")

	inputs := []Input{{Objects: []ctrlclient.Object{cm}}}
	builder := tracecheck.NewExplorerBuilder(runtime.NewScheme())

	scenarios, err := InputsToScenarios(builder, hotspot, inputs, graph, nil)
	if err != nil {
		t.Fatalf("inputs to scenarios: %v", err)
	}
	if len(scenarios) != 1 {
		t.Fatalf("expected 1 scenario, got %d", len(scenarios))
	}

	stale := scenarios[0].Config.StaleReads
	expected := schema.GroupVersionKind{Group: "", Version: "v1", Kind: "ConfigMap"}
	found := false
	for _, gvk := range stale {
		if gvk == expected {
			found = true
		}
	}
	if !found {
		t.Fatalf("expected stale reads to include %v, got %v", expected, stale)
	}
}
