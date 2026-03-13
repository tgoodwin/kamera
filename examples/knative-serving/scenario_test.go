package main

import (
	"fmt"
	"strings"
	"testing"

	"github.com/tgoodwin/kamera/pkg/coverage"
	"github.com/tgoodwin/kamera/pkg/explore"
	"github.com/tgoodwin/kamera/pkg/tracecheck"
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
		EnvironmentState: coverage.EnvironmentState{
			Objects: []*unstructured.Unstructured{
				mustServiceAsUnstructured(t),
			},
		},
	}
	inputB := coverage.Input{
		Name: "knative-b",
		EnvironmentState: coverage.EnvironmentState{
			Objects: []*unstructured.Unstructured{
				mustServiceAsUnstructured(t),
			},
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
		if len(sc.EnvironmentState.Objects()) == 0 {
			t.Fatalf("scenario %q has empty state objects", sc.Name)
		}
		// scenarios with zero userInputs should remain valid and run as no-op setups
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

func TestApplyInputTuningAppliesStaleness(t *testing.T) {
	base := tracecheck.ExploreConfig{
		Perturbations: tracecheck.PerturbationConfig{
			Staleness: map[tracecheck.ReconcilerID]tracecheck.StalenessConfig{
				"ExistingController": {
					StaleReadBounds: tracecheck.LookbackLimits{
						"core/ConfigMap": tracecheck.LookbackLimit(2),
					},
					MaxRestarts: 1,
				},
			},
		},
	}
	tuning := coverage.InputTuning{
		StaleReads: map[string][]string{
			"ServiceReconciler": {"core/ConfigMap", "core/Secret"},
		},
		StaleLookback: map[string]int{
			"core/ConfigMap": 3,
			"core/Secret":    1,
		},
	}

	cfg, err := explore.ApplyInputTuning(base, tuning)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	st, ok := cfg.Perturbations.Staleness["ServiceReconciler"]
	if !ok {
		t.Fatal("expected ServiceReconciler staleness config")
	}
	if st.StaleReadBounds["core/ConfigMap"] != tracecheck.LookbackLimit(3) {
		t.Fatalf("expected core/ConfigMap lookback=3, got %d", st.StaleReadBounds["core/ConfigMap"])
	}
	if st.StaleReadBounds["core/Secret"] != tracecheck.LookbackLimit(1) {
		t.Fatalf("expected core/Secret lookback=1, got %d", st.StaleReadBounds["core/Secret"])
	}
	if existing, ok := cfg.Perturbations.Staleness["ExistingController"]; !ok || existing.MaxRestarts != 1 {
		t.Fatalf("expected existing staleness config to remain intact")
	}
}

func TestScenariosFromInputsReliesOnCoreClosedLoopPlanner(t *testing.T) {
	builder := newKnativeExplorerBuilder()
	input := coverage.Input{
		Name: "knative-a",
		EnvironmentState: coverage.EnvironmentState{
			Objects: []*unstructured.Unstructured{
				mustServiceAsUnstructured(t),
			},
		},
		Tuning: coverage.InputTuning{
			PermuteControllers: []string{"ServiceReconciler"},
			StaleReads: map[string][]string{
				"ServiceReconciler": {"core/ConfigMap"},
			},
			StaleLookback: map[string]int{"core/ConfigMap": 2},
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
	if sc.ClosedLoop != nil {
		t.Fatalf("expected no harness-specific closed-loop planner")
	}
	if sc.Context.Workflow != "batch-input" {
		t.Fatalf("expected batch-input workflow context, got %q", sc.Context.Workflow)
	}
	if got := sc.Context.Attributes["phase"]; got != "" {
		t.Fatalf("expected no phase attribute from harness context, got %q", got)
	}
}
