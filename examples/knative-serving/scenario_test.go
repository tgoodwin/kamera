package main

import (
	"strings"
	"testing"

	"github.com/tgoodwin/kamera/pkg/coverage"
	"github.com/tgoodwin/kamera/pkg/explore"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
)

func TestScenariosFromInputsRequiresBuilder(t *testing.T) {
	_, err := scenariosFromInputs(nil, []coverage.Input{{Name: "x"}})
	if err == nil {
		t.Fatal("expected error, got nil")
	}
}

func TestScenariosFromInputsGeneratesSingleActionVariants(t *testing.T) {
	builder := newKnativeExplorerBuilder()

	input := coverage.Input{
		Name: "knative-base",
		Objects: []*unstructured.Unstructured{
			mustServiceAsUnstructured(t),
		},
		Pending: []coverage.Pending{
			{
				ControllerID: "ServiceReconciler",
				Key: coverage.NamespacedName{
					Namespace: "default",
					Name:      "demo",
				},
			},
		},
	}

	scenarios, err := scenariosFromInputs(builder, []coverage.Input{input})
	if err != nil {
		t.Fatalf("scenariosFromInputs error = %v", err)
	}
	if len(scenarios) < 2 {
		t.Fatalf("expected at least 2 scenarios, got %d", len(scenarios))
	}

	seenImageVariant := false
	for _, sc := range scenarios {
		if len(sc.InitialState.Objects()) == 0 {
			t.Fatalf("scenario %q has empty state objects", sc.Name)
		}
		if len(sc.InitialState.PendingReconciles) == 0 {
			t.Fatalf("scenario %q has no pending reconciles", sc.Name)
		}
		if strings.Contains(sc.Name, "set-image") {
			seenImageVariant = true
		}
	}
	if !seenImageVariant {
		t.Fatalf("expected at least one image variant, got names: %v", scenarioNames(scenarios))
	}
}

func mustServiceAsUnstructured(t *testing.T) *unstructured.Unstructured {
	t.Helper()
	svc := buildBaselineService()
	obj, err := runtime.DefaultUnstructuredConverter.ToUnstructured(svc)
	if err != nil {
		t.Fatalf("convert service: %v", err)
	}
	u := &unstructured.Unstructured{Object: obj}
	u.SetAPIVersion("serving.knative.dev/v1")
	u.SetKind("Service")
	return u
}

func scenarioNames(scenarios []explore.Scenario) []string {
	names := make([]string, 0, len(scenarios))
	for _, sc := range scenarios {
		names = append(names, sc.Name)
	}
	return names
}
