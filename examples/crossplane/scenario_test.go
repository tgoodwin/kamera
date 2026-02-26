package main

import (
	"testing"

	"github.com/tgoodwin/kamera/pkg/coverage"
	"github.com/tgoodwin/kamera/pkg/event"
	"github.com/tgoodwin/kamera/pkg/tracecheck"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
)

func TestScenariosFromInputsRequiresBuilder(t *testing.T) {
	_, err := scenariosFromInputs(nil, []coverage.Input{{Name: "x"}})
	if err == nil {
		t.Fatal("expected error, got nil")
	}
}

func TestScenariosFromInputsRequiresInputs(t *testing.T) {
	builder := newCrossplaneExplorerBuilder()
	_, err := scenariosFromInputs(builder, nil)
	if err == nil {
		t.Fatal("expected error, got nil")
	}
}

func TestScenariosFromInputsTranslatesStateActionsAndTuning(t *testing.T) {
	builder := newCrossplaneExplorerBuilder()

	functionObj := mustObjectAsUnstructured(t, buildFunctionRevision())
	xr := buildCompositeResource()

	input := coverage.Input{
		Name: "crossplane-translate",
		EnvironmentState: coverage.EnvironmentState{
			Objects: []*unstructured.Unstructured{functionObj},
		},
		UserInputs: []coverage.UserInput{
			{
				Type:   event.CREATE,
				Object: xr,
			},
		},
		Tuning: coverage.InputTuning{
			MaxDepth:           7,
			PermuteControllers: []string{"CompositeReconciler"},
			StaleReads: map[string][]string{
				"CompositionReconciler": {"apiextensions.crossplane.io/CompositionRevision"},
			},
			StaleLookback: map[string]int{
				"apiextensions.crossplane.io/CompositionRevision": 2,
			},
		},
	}

	scenarios, err := scenariosFromInputs(builder, []coverage.Input{input})
	if err != nil {
		t.Fatalf("scenariosFromInputs error = %v", err)
	}
	if len(scenarios) != 1 {
		t.Fatalf("expected 1 scenario, got %d", len(scenarios))
	}

	sc := scenarios[0]
	if sc.Name != input.Name {
		t.Fatalf("expected scenario name %q, got %q", input.Name, sc.Name)
	}
	if len(sc.EnvironmentState.Objects()) == 0 {
		t.Fatalf("expected seeded state objects, got empty state")
	}
	if len(sc.UserInputs) != 1 {
		t.Fatalf("expected 1 user action, got %d", len(sc.UserInputs))
	}
	if sc.UserInputs[0].ID != "user-input-0" {
		t.Fatalf("expected default action id user-input-0, got %q", sc.UserInputs[0].ID)
	}
	if sc.UserInputs[0].OpType != event.CREATE {
		t.Fatalf("expected CREATE user action, got %q", sc.UserInputs[0].OpType)
	}
	payload, ok := sc.UserInputs[0].Payload.(*unstructured.Unstructured)
	if !ok {
		t.Fatalf("expected unstructured payload, got %T", sc.UserInputs[0].Payload)
	}
	if payload == xr {
		t.Fatalf("expected payload deep copy, got original pointer")
	}
	if payload.GetKind() != "XWidget" || payload.GetName() != "example" {
		t.Fatalf("unexpected payload identity: kind=%q name=%q", payload.GetKind(), payload.GetName())
	}
	if sc.Config.MaxDepth != 7 {
		t.Fatalf("expected MaxDepth=7, got %d", sc.Config.MaxDepth)
	}
	if !sc.Config.Perturbations.PermuteOrder[tracecheck.ReconcilerID("CompositeReconciler")] {
		t.Fatalf("expected CompositeReconciler permutation to be enabled")
	}
	staleness, ok := sc.Config.Perturbations.Staleness[tracecheck.ReconcilerID("CompositionReconciler")]
	if !ok {
		t.Fatalf("expected staleness tuning for CompositionReconciler")
	}
	if staleness.StaleReadBounds["apiextensions.crossplane.io/CompositionRevision"] != 2 {
		t.Fatalf("expected lookback=2, got %d", staleness.StaleReadBounds["apiextensions.crossplane.io/CompositionRevision"])
	}
}

func TestScenariosFromInputsSeedsCreateOnlyInputs(t *testing.T) {
	builder := newCrossplaneExplorerBuilder()
	xr := buildCompositeResource()

	input := coverage.Input{
		Name: "crossplane-create-only",
		UserInputs: []coverage.UserInput{
			{
				ID:     "create-xr",
				Type:   event.CREATE,
				Object: xr,
			},
		},
	}

	scenarios, err := scenariosFromInputs(builder, []coverage.Input{input})
	if err != nil {
		t.Fatalf("scenariosFromInputs error = %v", err)
	}
	if len(scenarios) != 1 {
		t.Fatalf("expected 1 scenario, got %d", len(scenarios))
	}

	sc := scenarios[0]
	if len(sc.EnvironmentState.Objects()) == 0 {
		t.Fatalf("expected state to be seeded from create inputs")
	}
	if len(sc.UserInputs) != 1 {
		t.Fatalf("expected 1 user action, got %d", len(sc.UserInputs))
	}
	if sc.UserInputs[0].OpType != event.UPDATE {
		t.Fatalf("expected seeded CREATE to translate to UPDATE, got %q", sc.UserInputs[0].OpType)
	}
}

func mustObjectAsUnstructured(t *testing.T, obj runtime.Object) *unstructured.Unstructured {
	t.Helper()
	raw, err := runtime.DefaultUnstructuredConverter.ToUnstructured(obj)
	if err != nil {
		t.Fatalf("convert object %T to unstructured: %v", obj, err)
	}
	u := &unstructured.Unstructured{Object: raw}
	gvk := obj.GetObjectKind().GroupVersionKind()
	if gvk.Kind != "" {
		u.SetGroupVersionKind(gvk)
	}
	return u
}
