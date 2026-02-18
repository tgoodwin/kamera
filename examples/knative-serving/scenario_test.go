package main

import (
	"fmt"
	"strings"
	"testing"

	"github.com/tgoodwin/kamera/pkg/coverage"
	"github.com/tgoodwin/kamera/pkg/explore"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"knative.dev/serving/pkg/apis/autoscaling"
)

func TestScenariosFromInputsRequiresBuilder(t *testing.T) {
	_, err := scenariosFromInputs(nil, []coverage.Input{{Name: "x"}})
	if err == nil {
		t.Fatal("expected error, got nil")
	}
}

func TestScenariosFromInputsUsesInputsAsFinalScenarioUnits(t *testing.T) {
	builder := newKnativeExplorerBuilder()

	inputA := coverage.Input{
		Name: "knative-a",
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
	inputB := coverage.Input{
		Name: "knative-b",
		Objects: []*unstructured.Unstructured{
			mustServiceAsUnstructured(t),
		},
	}

	scenarios, err := scenariosFromInputs(builder, []coverage.Input{inputA, inputB})
	if err != nil {
		t.Fatalf("scenariosFromInputs error = %v", err)
	}
	if len(scenarios) != 2 {
		t.Fatalf("expected exactly 2 scenarios, got %d", len(scenarios))
	}

	seen := map[string]bool{}
	for _, sc := range scenarios {
		if len(sc.InitialState.Objects()) == 0 {
			t.Fatalf("scenario %q has empty state objects", sc.Name)
		}
		if len(sc.InitialState.PendingReconciles) == 0 {
			t.Fatalf("scenario %q has no pending reconciles", sc.Name)
		}
		seen[sc.Name] = true
	}
	if !seen["knative-a"] || !seen["knative-b"] {
		t.Fatalf("expected original scenario names, got names: %v", scenarioNames(scenarios))
	}
}

func TestValidateKnativeServiceParamsRejectsMinScaleGreaterThanMaxScale(t *testing.T) {
	svc := buildBaselineService()
	anns := ensureTemplateAnnotations(svc)
	anns[autoscaling.MinScaleAnnotationKey] = "2"
	anns[autoscaling.MaxScaleAnnotationKey] = "1"

	err := validateKnativeServiceParams(svc)
	if err == nil {
		t.Fatal("expected minScale > maxScale to be invalid")
	}
	if !strings.Contains(err.Error(), "min scale") {
		t.Fatalf("expected min scale error, got %q", err.Error())
	}
}

func TestValidateKnativeServiceParamsRejectsInvalidScaleValue(t *testing.T) {
	svc := buildBaselineService()
	anns := ensureTemplateAnnotations(svc)
	anns[autoscaling.MinScaleAnnotationKey] = "not-a-number"

	err := validateKnativeServiceParams(svc)
	if err == nil {
		t.Fatal("expected invalid min scale to fail")
	}
	if !strings.Contains(err.Error(), autoscaling.MinScaleAnnotationKey) {
		t.Fatalf("expected error mentioning min scale key, got %q", err.Error())
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

func Example_validateKnativeServiceParams() {
	svc := buildBaselineService()
	anns := ensureTemplateAnnotations(svc)
	anns[autoscaling.MinScaleAnnotationKey] = "2"
	anns[autoscaling.MaxScaleAnnotationKey] = "1"

	err := validateKnativeServiceParams(svc)
	fmt.Println(err != nil)
	// Output: true
}
