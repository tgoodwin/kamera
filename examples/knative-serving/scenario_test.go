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
	seenSampledVariant := false
	for _, sc := range scenarios {
		if len(sc.InitialState.Objects()) == 0 {
			t.Fatalf("scenario %q has empty state objects", sc.Name)
		}
		if len(sc.InitialState.PendingReconciles) == 0 {
			t.Fatalf("scenario %q has no pending reconciles", sc.Name)
		}
		if strings.Contains(sc.Name, "/single/image-") {
			seenImageVariant = true
		}
		if strings.Contains(sc.Name, "/sampled-") {
			seenSampledVariant = true
		}
	}
	if !seenImageVariant {
		t.Fatalf("expected at least one image variant, got names: %v", scenarioNames(scenarios))
	}
	if !seenSampledVariant {
		t.Fatalf("expected at least one sampled variant, got names: %v", scenarioNames(scenarios))
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

func TestScenariosFromInputsUsesFuzzCasesFlagBudget(t *testing.T) {
	builder := newKnativeExplorerBuilder()
	input := coverage.Input{
		Name: "knative-budget",
		Objects: []*unstructured.Unstructured{
			mustServiceAsUnstructured(t),
		},
	}

	restore := setFuzzSamplingForTest(t, 3, 42)
	defer restore()

	scenarios, err := scenariosFromInputs(builder, []coverage.Input{input})
	if err != nil {
		t.Fatalf("scenariosFromInputs error = %v", err)
	}

	sampled := 0
	for _, sc := range scenarios {
		if strings.Contains(sc.Name, "/sampled-") {
			sampled++
		}
	}
	if sampled != 3 {
		t.Fatalf("expected 3 sampled scenarios, got %d (names=%v)", sampled, scenarioNames(scenarios))
	}
}

func setFuzzSamplingForTest(t *testing.T, cases int, seed int64) func() {
	t.Helper()
	oldCases := *fuzzCasesFlag
	oldSeed := *fuzzSeedFlag

	*fuzzCasesFlag = cases
	*fuzzSeedFlag = seed

	return func() {
		*fuzzCasesFlag = oldCases
		*fuzzSeedFlag = oldSeed
	}
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
