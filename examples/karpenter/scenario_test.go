package main

import (
	"strings"
	"testing"

	"github.com/tgoodwin/kamera/pkg/coverage"
	"github.com/tgoodwin/kamera/pkg/explore"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	v1 "sigs.k8s.io/karpenter/pkg/apis/v1"
)

func TestScenariosFromInputsRequiresBuilder(t *testing.T) {
	_, err := scenariosFromInputs(nil, []coverage.Input{{Name: "x"}})
	if err == nil {
		t.Fatal("expected error, got nil")
	}
}

func TestScenariosFromInputsGeneratesWorkflowVariants(t *testing.T) {
	builder := newKarpenterExplorerBuilder()
	input := mustKarpenterInput(t, "karpenter-base")

	restore := setFuzzSamplingForTest(t, 3, 1337)
	defer restore()

	scenarios, err := scenariosFromInputs(builder, []coverage.Input{input})
	if err != nil {
		t.Fatalf("scenariosFromInputs error = %v", err)
	}
	if len(scenarios) < 3 {
		t.Fatalf("expected at least 3 scenarios, got %d", len(scenarios))
	}

	seenPodVariant := false
	seenNoFitVariant := false
	seenSampledVariant := false
	for _, sc := range scenarios {
		if len(sc.InitialState.Objects()) == 0 {
			t.Fatalf("scenario %q has empty state objects", sc.Name)
		}
		if len(sc.InitialState.PendingReconciles) == 0 {
			t.Fatalf("scenario %q has no pending reconciles", sc.Name)
		}
		if strings.Contains(sc.Name, "/single/pod-") {
			seenPodVariant = true
		}
		if strings.Contains(sc.Name, "/single/no-fit-") {
			seenNoFitVariant = true
		}
		if strings.Contains(sc.Name, "/sampled-") {
			seenSampledVariant = true
		}
	}

	if !seenPodVariant {
		t.Fatalf("expected at least one pod variant, got names: %v", scenarioNames(scenarios))
	}
	if !seenNoFitVariant {
		t.Fatalf("expected at least one no-fit variant, got names: %v", scenarioNames(scenarios))
	}
	if !seenSampledVariant {
		t.Fatalf("expected at least one sampled variant, got names: %v", scenarioNames(scenarios))
	}
}

func TestScenariosFromInputsUsesFuzzCasesBudget(t *testing.T) {
	builder := newKarpenterExplorerBuilder()
	input := mustKarpenterInput(t, "karpenter-budget")

	restore := setFuzzSamplingForTest(t, 2, 42)
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
	if sampled != 2 {
		t.Fatalf("expected 2 sampled scenarios, got %d (names=%v)", sampled, scenarioNames(scenarios))
	}
}

func TestExpandKarpenterParameterizedInputAddsNoFitNodeSelectorVariant(t *testing.T) {
	input := mustKarpenterInput(t, "karpenter-params")
	variants, err := expandKarpenterParameterizedInput(input, 0, 99)
	if err != nil {
		t.Fatalf("expandKarpenterParameterizedInput error = %v", err)
	}

	target := "karpenter-params/single/no-fit-pod-selector-unmatched"
	for _, variant := range variants {
		if variant.Name != target {
			continue
		}
		podIdx := findKarpenterPod(variant.Objects)
		if podIdx < 0 {
			t.Fatalf("variant %q missing pod object", target)
		}
		pod, err := unstructuredToPod(variant.Objects[podIdx])
		if err != nil {
			t.Fatalf("convert pod variant: %v", err)
		}
		if pod.Spec.NodeSelector["karpenter.sh/nonexistent-capability"] != "required" {
			t.Fatalf("expected no-fit selector in %q, got selectors=%v", target, pod.Spec.NodeSelector)
		}
		return
	}

	t.Fatalf("expected variant %q, got names=%v", target, coverageInputNames(variants))
}

func mustKarpenterInput(t *testing.T, name string) coverage.Input {
	t.Helper()
	objs, err := newScenarioObjects()
	if err != nil {
		t.Fatalf("newScenarioObjects: %v", err)
	}

	objects := make([]*unstructured.Unstructured, 0, len(objs))
	for _, obj := range objs {
		u, err := objectToUnstructured(obj)
		if err != nil {
			t.Fatalf("convert object %T: %v", obj, err)
		}
		objects = append(objects, u)
	}

	return coverage.Input{
		Name:    name,
		Objects: objects,
		Pending: []coverage.Pending{
			{
				ControllerID: "state.pod",
				Key: coverage.NamespacedName{
					Namespace: "default",
					Name:      "pending",
				},
			},
			{
				ControllerID: "provisioner.trigger.pod",
				Key: coverage.NamespacedName{
					Namespace: "default",
					Name:      "pending",
				},
			},
			{
				ControllerID: "provisioner",
				Key: coverage.NamespacedName{
					Name: "singleton",
				},
			},
			{
				ControllerID: "state.nodepool",
				Key: coverage.NamespacedName{
					Name: "default",
				},
			},
		},
	}
}

func objectToUnstructured(obj runtime.Object) (*unstructured.Unstructured, error) {
	raw, err := runtime.DefaultUnstructuredConverter.ToUnstructured(obj)
	if err != nil {
		return nil, err
	}
	u := &unstructured.Unstructured{Object: raw}

	switch typed := obj.(type) {
	case *corev1.Pod:
		u.SetGroupVersionKind(corev1.SchemeGroupVersion.WithKind("Pod"))
		u.SetNamespace(typed.Namespace)
		u.SetName(typed.Name)
	case *v1.NodePool:
		u.SetGroupVersionKind(schema.GroupVersion{Group: "karpenter.sh", Version: "v1"}.WithKind("NodePool"))
		u.SetName(typed.Name)
	default:
		// retain whatever GVK conversion provided
	}
	return u, nil
}

func scenarioNames(scenarios []explore.Scenario) []string {
	names := make([]string, 0, len(scenarios))
	for _, sc := range scenarios {
		names = append(names, sc.Name)
	}
	return names
}

func coverageInputNames(inputs []coverage.Input) []string {
	names := make([]string, 0, len(inputs))
	for _, in := range inputs {
		names = append(names, in.Name)
	}
	return names
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
