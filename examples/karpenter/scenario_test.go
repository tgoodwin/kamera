package main

import (
	"context"
	"strings"
	"testing"

	"github.com/tgoodwin/kamera/pkg/coverage"
	"github.com/tgoodwin/kamera/pkg/event"
	"github.com/tgoodwin/kamera/pkg/explore"
	"github.com/tgoodwin/kamera/pkg/tracecheck"
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
		if len(sc.EnvironmentState.Objects()) == 0 {
			t.Fatalf("scenario %q has empty state objects", sc.Name)
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
		podIdx := findKarpenterPodInUserInputs(variant.UserInputs)
		if podIdx < 0 {
			t.Fatalf("variant %q missing pod user input", target)
		}
		pod, err := unstructuredToPod(variant.UserInputs[podIdx].Object)
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

func TestExpandKarpenterParameterizedInputAddsNoFitNodePoolRequirementVariant(t *testing.T) {
	input := mustKarpenterInput(t, "karpenter-params")
	variants, err := expandKarpenterParameterizedInput(input, 0, 99)
	if err != nil {
		t.Fatalf("expandKarpenterParameterizedInput error = %v", err)
	}

	target := "karpenter-params/single/no-fit-nodepool-requirement-unmatched"
	for _, variant := range variants {
		if variant.Name != target {
			continue
		}
		nodePoolIdx := findKarpenterNodePool(variant.EnvironmentState.Objects)
		if nodePoolIdx < 0 {
			t.Fatalf("variant %q missing nodepool environment object", target)
		}
		nodePool, err := unstructuredToNodePool(variant.EnvironmentState.Objects[nodePoolIdx])
		if err != nil {
			t.Fatalf("convert nodepool variant: %v", err)
		}
		for _, req := range nodePool.Spec.Template.Spec.Requirements {
			if req.Key == "karpenter.sh/nonexistent-capability" {
				return
			}
		}
		t.Fatalf("expected unmatched requirement in %q, got requirements=%v", target, nodePool.Spec.Template.Spec.Requirements)
	}

	t.Fatalf("expected variant %q, got names=%v", target, coverageInputNames(variants))
}

func TestBuildStateFromCoverageInputSeedsNodePoolPendingReconcile(t *testing.T) {
	builder := newKarpenterExplorerBuilder()
	input := mustKarpenterInput(t, "karpenter-state-seed")

	state, _, err := buildStateFromCoverageInput(builder, input)
	if err != nil {
		t.Fatalf("buildStateFromCoverageInput error = %v", err)
	}

	for _, pending := range state.PendingReconciles {
		if pending.ReconcilerID == "state.nodepool" && pending.Request.Name == "default" {
			return
		}
	}

	t.Fatalf("expected state.nodepool pending reconcile for default NodePool, got pending=%v", state.PendingReconciles)
}

func TestBuildUserActionsFromCoverageInputConvertsSeededPodCreateToUpdate(t *testing.T) {
	builder := newKarpenterExplorerBuilder()
	input := mustKarpenterInput(t, "karpenter-user-actions")

	_, seeded, err := buildStateFromCoverageInput(builder, input)
	if err != nil {
		t.Fatalf("buildStateFromCoverageInput error = %v", err)
	}
	actions, err := buildUserActionsFromCoverageInput(input, seeded)
	if err != nil {
		t.Fatalf("buildUserActionsFromCoverageInput error = %v", err)
	}
	if len(actions) != 1 {
		t.Fatalf("expected one pod create user action, got %d actions: %#v", len(actions), actions)
	}
	if actions[0].OpType != event.UPDATE {
		t.Fatalf("expected seeded pod user action op type UPDATE, got %q", actions[0].OpType)
	}
}

func TestCoverageInputProducesExternalUserStep(t *testing.T) {
	builder := newKarpenterExplorerBuilder()
	builder.WithMaxDepth(64)
	input := mustKarpenterInput(t, "karpenter-external-user")

	state, seeded, err := buildStateFromCoverageInput(builder, input)
	if err != nil {
		t.Fatalf("buildStateFromCoverageInput error = %v", err)
	}
	actions, err := buildUserActionsFromCoverageInput(input, seeded)
	if err != nil {
		t.Fatalf("buildUserActionsFromCoverageInput error = %v", err)
	}

	builder.WithUserActions(actions)
	explorer, err := builder.Build("standalone")
	if err != nil {
		t.Fatalf("build explorer: %v", err)
	}

	result := explorer.Explore(context.Background(), state)
	allStates := append(append([]tracecheck.ResultState(nil), result.ConvergedStates...), result.AbortedStates...)
	for _, st := range allStates {
		for _, path := range st.Paths {
			for _, step := range path {
				if step.ControllerID == tracecheck.UserControllerID {
					return
				}
			}
		}
	}
	t.Fatalf("expected at least one %q step in execution paths", tracecheck.UserControllerID)
}

func TestCoverageInputEventuallyCreatesNode(t *testing.T) {
	builder := newKarpenterExplorerBuilder()
	builder.WithMaxDepth(64)
	input := mustKarpenterInput(t, "karpenter-node-created")

	state, seeded, err := buildStateFromCoverageInput(builder, input)
	if err != nil {
		t.Fatalf("buildStateFromCoverageInput error = %v", err)
	}
	actions, err := buildUserActionsFromCoverageInput(input, seeded)
	if err != nil {
		t.Fatalf("buildUserActionsFromCoverageInput error = %v", err)
	}

	builder.WithUserActions(actions)
	explorer, err := builder.Build("standalone")
	if err != nil {
		t.Fatalf("build explorer: %v", err)
	}

	result := explorer.Explore(context.Background(), state)
	if len(result.ConvergedStates) == 0 {
		t.Fatalf("expected converged states, got 0")
	}

	for _, st := range result.ConvergedStates {
		for _, obj := range explorer.Objects(st) {
			if obj.GetKind() == "Node" {
				goto checkNoPodLifecycle
			}
		}
	}
	t.Fatalf("expected at least one converged state to contain a Node object")

checkNoPodLifecycle:
	for _, st := range result.ConvergedStates {
		for _, path := range st.Paths {
			for _, step := range path {
				if step.ControllerID == "PodLifecycleController" {
					t.Fatalf("expected PodLifecycleController to be disabled in karpenter harness, but observed it in path")
				}
			}
		}
	}
}

func mustKarpenterInput(t *testing.T, name string) coverage.Input {
	t.Helper()
	envObjs, err := newEnvironmentObjects()
	if err != nil {
		t.Fatalf("newEnvironmentObjects: %v", err)
	}

	objects := make([]*unstructured.Unstructured, 0, len(envObjs))
	for _, obj := range envObjs {
		u, err := objectToUnstructured(obj)
		if err != nil {
			t.Fatalf("convert object %T: %v", obj, err)
		}
		objects = append(objects, u)
	}
	podObj, err := objectToUnstructured(newPendingPod())
	if err != nil {
		t.Fatalf("convert pod object: %v", err)
	}

	return coverage.Input{
		Name: name,
		EnvironmentState: coverage.EnvironmentState{
			Objects: objects,
		},
		UserInputs: []coverage.UserInput{
			{
				ID:     "create-pending-pod",
				Type:   event.CREATE,
				Object: podObj,
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
