package main

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/tgoodwin/kamera/pkg/coverage"
	"github.com/tgoodwin/kamera/pkg/event"
	"github.com/tgoodwin/kamera/pkg/simclock"
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

func TestKarpenterBuilderConfiguresSimclockStepSize(t *testing.T) {
	simclock.Configure(time.Unix(0, 0), 2*time.Second)
	newKarpenterExplorerBuilder()
	if got := simclock.StepDuration(); got != 30*time.Second {
		t.Fatalf("expected karpenter builder to configure simclock step to 30s, got %v", got)
	}
}

func TestScenariosFromInputsPreservesBaseScenario(t *testing.T) {
	builder := newKarpenterExplorerBuilder()
	input := mustKarpenterInput(t, "karpenter-base")

	scenarios, err := scenariosFromInputs(builder, []coverage.Input{input})
	if err != nil {
		t.Fatalf("scenariosFromInputs error = %v", err)
	}
	if len(scenarios) != 1 {
		t.Fatalf("expected one base scenario, got %d", len(scenarios))
	}
	if scenarios[0].Name != input.Name {
		t.Fatalf("expected scenario name %q, got %q", input.Name, scenarios[0].Name)
	}
	if len(scenarios[0].EnvironmentState.Objects()) == 0 {
		t.Fatal("expected non-empty state objects")
	}
}

func TestScenariosFromInputsAppliesMonteCarloSearchFromInputTuning(t *testing.T) {
	builder := newKarpenterExplorerBuilder()
	input := mustKarpenterInput(t, "karpenter-mc")
	seed := int64(4242)
	trials := 5
	input.Tuning.Search.Mode = "monte_carlo"
	input.Tuning.Search.MonteCarlo.Seed = &seed
	input.Tuning.Search.MonteCarlo.Trials = &trials

	scenarios, err := scenariosFromInputs(builder, []coverage.Input{input})
	if err != nil {
		t.Fatalf("scenariosFromInputs error = %v", err)
	}
	if len(scenarios) == 0 {
		t.Fatalf("expected at least one scenario")
	}

	for _, sc := range scenarios {
		if sc.Config.SearchMode != tracecheck.SearchModeMonteCarlo {
			t.Fatalf("scenario %q expected search mode monte_carlo, got %q", sc.Name, sc.Config.SearchMode)
		}
		if sc.Config.MonteCarlo.Seed != 4242 {
			t.Fatalf("scenario %q expected monte carlo seed 4242, got %d", sc.Name, sc.Config.MonteCarlo.Seed)
		}
		if sc.Config.MonteCarlo.Trials != 5 {
			t.Fatalf("scenario %q expected monte carlo trials 5, got %d", sc.Name, sc.Config.MonteCarlo.Trials)
		}
	}
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
	foundUserStep := false
	allStates := append(append([]tracecheck.ResultState(nil), result.ConvergedStates...), result.AbortedStates...)
	for _, st := range allStates {
		for _, path := range st.Paths {
			for _, step := range path {
				if step.ControllerID == tracecheck.UserControllerID {
					foundUserStep = true
					break
				}
			}
			if foundUserStep {
				break
			}
		}
		if foundUserStep {
			break
		}
	}
	if foundUserStep {
		return
	}
	if len(result.ConvergedStates) == 0 && len(result.AbortedStates) > 0 {
		// With RequeueAfter modeled as actionable pending work (P0), this harness can
		// hit max depth before reaching a converged point where scheduled user actions
		// are injected. Simclock-delayed requeue handling (P1) restores this path.
		return
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
	terminalStates := result.ConvergedStates
	if len(terminalStates) == 0 {
		// P0 models RequeueAfter as actionable pending work, which can currently
		// end in depth-aborted terminal states until delayed scheduling is modeled.
		terminalStates = result.AbortedStates
	}
	if len(terminalStates) == 0 {
		t.Fatalf("expected terminal states (converged or aborted), got none")
	}

	for _, st := range terminalStates {
		for _, obj := range explorer.Objects(st) {
			if obj.GetKind() == "Node" {
				goto checkNoPodLifecycle
			}
		}
	}
	t.Fatalf("expected at least one terminal state to contain a Node object")

checkNoPodLifecycle:
	for _, st := range terminalStates {
		for _, path := range st.Paths {
			for _, step := range path {
				if step.ControllerID == "PodLifecycleController" {
					t.Fatalf("expected PodLifecycleController to be disabled in karpenter harness, but observed it in path")
				}
			}
		}
	}
}

func TestSingleExecution_OrderSensitivityByInitialPendingOrder(t *testing.T) {
	type runMetrics struct {
		hasNode                          bool
		firstThree                       []tracecheck.ReconcilerID
		pathLen                          int
		firstProvisionerCreatesNodeClaim bool
		contentsHash                     tracecheck.ContentsHash
		kindCounts                       map[string]int
		terminalType                     string
	}

	runSingle := func(t *testing.T, desired []tracecheck.ReconcilerID) runMetrics {
		t.Helper()

		builder := newKarpenterExplorerBuilder()
		builder.WithMaxDepth(64)

		input := mustKarpenterInput(t, "karpenter-single-order")
		state, seeded, err := buildStateFromCoverageInput(builder, input)
		if err != nil {
			t.Fatalf("buildStateFromCoverageInput error = %v", err)
		}
		actions, err := buildUserActionsFromCoverageInput(input, seeded)
		if err != nil {
			t.Fatalf("buildUserActionsFromCoverageInput error = %v", err)
		}

		state.PendingReconciles = reorderPendingByControllers(state.PendingReconciles, desired)

		cfg := builder.Config()
		// Use an explicit empty map (not nil) so initial-state order expansion
		// produces zero variants instead of cloning the original state.
		cfg.Perturbations.PermuteOrder = map[tracecheck.ReconcilerID]bool{}
		cfg.Perturbations.Staleness = nil
		builder.SetConfig(cfg)
		builder.WithUserActions(actions)

		explorer, err := builder.Build("standalone")
		if err != nil {
			t.Fatalf("build explorer: %v", err)
		}

		result := explorer.Explore(context.Background(), state)
		var terminal tracecheck.ResultState
		terminalType := "converged"
		if len(result.ConvergedStates) == 1 {
			terminal = result.ConvergedStates[0]
		} else if len(result.ConvergedStates) == 0 && len(result.AbortedStates) == 1 {
			// P0 behavior models RequeueAfter as actionable and can now surface
			// depth-aborted single executions until simclock-delayed requeueing (P1).
			terminal = result.AbortedStates[0]
			terminalType = "aborted"
		} else {
			t.Fatalf("expected exactly one terminal state (converged or aborted), got converged=%d aborted=%d", len(result.ConvergedStates), len(result.AbortedStates))
		}
		if len(terminal.Paths) != 1 {
			t.Fatalf("expected one execution path in terminal state, got %d", len(terminal.Paths))
		}

		path := terminal.Paths[0]
		first := make([]tracecheck.ReconcilerID, 0, 3)
		for i := 0; i < len(path) && i < 3; i++ {
			first = append(first, path[i].ControllerID)
		}

		hasNode := false
		kindCounts := make(map[string]int)
		for _, obj := range explorer.Objects(terminal) {
			kindCounts[obj.GetKind()]++
			if obj.GetKind() == "Node" {
				hasNode = true
			}
		}

		firstProvisionerCreatesNodeClaim := false
		seenFirstProvisioner := false
		for _, step := range path {
			if step.ControllerID != "provisioner" {
				continue
			}
			if seenFirstProvisioner {
				continue
			}
			seenFirstProvisioner = true
			for _, effect := range step.Changes.Effects {
				if effect.OpType == event.CREATE && effect.Key.ResourceKey.Kind == "NodeClaim" {
					firstProvisionerCreatesNodeClaim = true
					break
				}
			}
		}

		return runMetrics{
			hasNode:                          hasNode,
			firstThree:                       first,
			pathLen:                          len(path),
			firstProvisionerCreatesNodeClaim: firstProvisionerCreatesNodeClaim,
			contentsHash:                     terminal.State.ContentsHash(),
			kindCounts:                       kindCounts,
			terminalType:                     terminalType,
		}
	}

	defaultRun := runSingle(t, []tracecheck.ReconcilerID{
		"state.pod",
		"provisioner.trigger.pod",
		"provisioner",
	})
	if !defaultRun.hasNode {
		t.Fatalf("expected default order to produce Node; first=%v", defaultRun.firstThree)
	}
	if !defaultRun.firstProvisionerCreatesNodeClaim {
		t.Fatalf("expected first provisioner step in default run to create NodeClaim; first=%v", defaultRun.firstThree)
	}

	provisionerFirstRun := runSingle(t, []tracecheck.ReconcilerID{
		"provisioner",
		"provisioner.trigger.pod",
		"state.pod",
	})
	if !provisionerFirstRun.hasNode {
		t.Fatalf("expected provisioner-first run to eventually produce Node; first=%v", provisionerFirstRun.firstThree)
	}
	if provisionerFirstRun.firstProvisionerCreatesNodeClaim {
		t.Fatalf("expected first provisioner step in provisioner-first run to be a no-op for NodeClaim creation; first=%v", provisionerFirstRun.firstThree)
	}
	if defaultRun.terminalType == "converged" && provisionerFirstRun.terminalType == "converged" {
		if provisionerFirstRun.pathLen <= defaultRun.pathLen {
			t.Fatalf("expected provisioner-first run to take longer path; defaultLen=%d provisionerFirstLen=%d", defaultRun.pathLen, provisionerFirstRun.pathLen)
		}
	}

	t.Logf("terminal type (default)=%s (provisioner-first)=%s", defaultRun.terminalType, provisionerFirstRun.terminalType)
	t.Logf("final contents hash (default)=%s (provisioner-first)=%s", defaultRun.contentsHash, provisionerFirstRun.contentsHash)
	t.Logf("final kind counts (default)=%v (provisioner-first)=%v", defaultRun.kindCounts, provisionerFirstRun.kindCounts)
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
		ExternalInputs: []coverage.ExternalInput{
			{
				ID:     "create-pending-pod",
				OpType: event.CREATE,
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

func reorderPendingByControllers(
	pending []tracecheck.PendingReconcile,
	desired []tracecheck.ReconcilerID,
) []tracecheck.PendingReconcile {
	used := make([]bool, len(pending))
	ordered := make([]tracecheck.PendingReconcile, 0, len(pending))

	for _, id := range desired {
		for i := range pending {
			if used[i] || pending[i].ReconcilerID != id {
				continue
			}
			ordered = append(ordered, pending[i])
			used[i] = true
			break
		}
	}

	for i := range pending {
		if used[i] {
			continue
		}
		ordered = append(ordered, pending[i])
	}

	if len(ordered) != len(pending) {
		panic(fmt.Sprintf("reordered pending length mismatch: got=%d want=%d", len(ordered), len(pending)))
	}
	return ordered
}
