package main

import (
	"encoding/json"
	"fmt"
	"strings"

	"github.com/tgoodwin/kamera/pkg/coverage"
	"github.com/tgoodwin/kamera/pkg/event"
	"github.com/tgoodwin/kamera/pkg/explore"
	"github.com/tgoodwin/kamera/pkg/tag"
	"github.com/tgoodwin/kamera/pkg/tracecheck"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"

	v1 "sigs.k8s.io/karpenter/pkg/apis/v1"
	"sigs.k8s.io/karpenter/pkg/test"
	"sigs.k8s.io/karpenter/pkg/test/v1alpha1"
)

func newEnvironmentObjects() ([]client.Object, error) {
	// TestNodeClass (fake cloud provider)
	nc := test.NodeClass(v1alpha1.TestNodeClass{ObjectMeta: metav1.ObjectMeta{Name: "default", UID: types.UID("testnodeclass-uid")}})
	tag.AddSleeveObjectID(nc)

	// NodePool referencing the TestNodeClass
	np := test.NodePool(v1.NodePool{ObjectMeta: metav1.ObjectMeta{Name: "default", UID: types.UID("nodepool-uid")}})
	tag.AddSleeveObjectID(np)

	return []client.Object{nc, np}, nil
}

func newPendingPod() *corev1.Pod {
	// Provisionable Pod (PodScheduled=False, Reason=Unschedulable)
	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Name: "pending", Namespace: "default", UID: types.UID("pod-uid")},
		Spec:       corev1.PodSpec{Containers: []corev1.Container{{Name: "c", Image: "pause"}}},
		Status: corev1.PodStatus{Conditions: []corev1.PodCondition{{
			Type:   corev1.PodScheduled,
			Status: corev1.ConditionFalse,
			Reason: corev1.PodReasonUnschedulable,
		}}},
	}
	tag.AddSleeveObjectID(pod)
	return pod
}

func newInitialUserActions() []tracecheck.UserAction {
	return []tracecheck.UserAction{
		{
			ID:      "create-pending-pod",
			OpType:  event.CREATE,
			Payload: newPendingPod(),
		},
	}
}

func buildInitialKarpenterState(builder *tracecheck.ExplorerBuilder) tracecheck.StateNode {
	stateBuilder := builder.NewStateEventBuilder()
	objs, _ := newEnvironmentObjects()

	nc := objs[0]
	np := objs[1]

	poolState := stateBuilder.AddTopLevelObject(np, "state.nodepool")
	classState := stateBuilder.AddTopLevelObject(nc)

	return tracecheck.MergeStateNodes(poolState, classState)
}

func scenariosFromInputs(builder *tracecheck.ExplorerBuilder, inputs []coverage.Input) ([]explore.Scenario, error) {
	return explore.CompileInputScenarios(builder, inputs, explore.ScenarioCompileOptions{
		BuildState: buildStateFromCoverageInput,
		ExpandInput: func(input coverage.Input) ([]coverage.Input, error) {
			return expandKarpenterParameterizedInput(input, *fuzzCasesFlag, *fuzzSeedFlag)
		},
	})
}

func expandKarpenterParameterizedInput(input coverage.Input, _ int, _ int64) ([]coverage.Input, error) {
	base := cloneCoverageInput(input)
	if strings.TrimSpace(base.Name) == "" {
		base.Name = "scenario"
	}
	return []coverage.Input{base}, nil
}

func buildStateFromCoverageInput(builder *tracecheck.ExplorerBuilder, input coverage.Input) (tracecheck.StateNode, []client.Object, error) {
	if builder == nil {
		return tracecheck.StateNode{}, nil, fmt.Errorf("builder is nil")
	}

	objects := make([]client.Object, 0, len(input.EnvironmentState.Objects))
	for _, obj := range input.EnvironmentState.Objects {
		if obj == nil {
			continue
		}
		objects = append(objects, safeDeepCopyUnstructured(obj))
	}
	for _, userInput := range input.ExternalInputs {
		if userInput.OpType != event.CREATE || userInput.Object == nil || !isKarpenterPod(userInput.Object) {
			continue
		}
		if explore.InputObjectSeeded(userInput.Object, objects) {
			continue
		}
		objects = append(objects, userInput.Object.DeepCopy())
	}
	if len(objects) == 0 {
		fallback, err := newEnvironmentObjects()
		if err != nil {
			return tracecheck.StateNode{}, nil, err
		}
		objects = append(objects, fallback...)
	}
	if len(objects) == 0 {
		return tracecheck.StateNode{}, nil, fmt.Errorf("input has no objects")
	}

	state, err := buildStateFromObjects(builder, objects)
	if err != nil {
		return tracecheck.StateNode{}, nil, err
	}
	return state, objects, nil
}

func buildStateFromObjects(builder *tracecheck.ExplorerBuilder, objects []client.Object) (tracecheck.StateNode, error) {
	if builder == nil {
		return tracecheck.StateNode{}, fmt.Errorf("builder is nil")
	}
	if len(objects) == 0 {
		return tracecheck.StateNode{}, fmt.Errorf("no objects supplied")
	}

	stateBuilder := builder.NewStateEventBuilder()
	ordered := orderInitialStateObjects(objects)

	var (
		state  tracecheck.StateNode
		seeded bool
	)
	for _, obj := range ordered {
		if obj == nil {
			continue
		}
		next := stateBuilder.AddTopLevelObject(obj, initialDependentControllers(obj)...)
		if !seeded {
			state = next
			seeded = true
			continue
		}
		state = tracecheck.MergeStateNodes(state, next)
	}
	if !seeded {
		return tracecheck.StateNode{}, fmt.Errorf("no non-nil objects supplied")
	}
	return state, nil
}

func orderInitialStateObjects(objects []client.Object) []client.Object {
	ordered := make([]client.Object, 0, len(objects))

	appendKind := func(match func(client.Object) bool) {
		for _, obj := range objects {
			if obj == nil || !match(obj) {
				continue
			}
			ordered = append(ordered, obj)
		}
	}

	appendKind(isKarpenterPod)
	appendKind(isKarpenterNodePool)
	appendKind(func(obj client.Object) bool {
		return !isKarpenterPod(obj) && !isKarpenterNodePool(obj)
	})
	return ordered
}

func initialDependentControllers(obj client.Object) []tracecheck.ReconcilerID {
	if obj == nil {
		return nil
	}
	if isKarpenterPod(obj) {
		return []tracecheck.ReconcilerID{"state.pod", "provisioner.trigger.pod", "provisioner"}
	}
	if isKarpenterNodePool(obj) {
		return []tracecheck.ReconcilerID{"state.nodepool"}
	}
	if isKarpenterNodeClaim(obj) {
		return []tracecheck.ReconcilerID{"state.nodeclaim", "nodeclaim.hydration", "nodeclaim.lifecycle", "node.registrar"}
	}
	if isKarpenterNode(obj) {
		return []tracecheck.ReconcilerID{"state.node", "node.hydration"}
	}
	return nil
}

func applyInputTuning(base tracecheck.ExploreConfig, tuning coverage.InputTuning) (tracecheck.ExploreConfig, error) {
	return explore.ApplyInputTuning(base, tuning)
}

func cloneCoverageInput(input coverage.Input) coverage.Input {
	objects := make([]*unstructured.Unstructured, 0, len(input.EnvironmentState.Objects))
	for _, obj := range input.EnvironmentState.Objects {
		if obj == nil {
			objects = append(objects, nil)
			continue
		}
		objects = append(objects, obj.DeepCopy())
	}

	userInputs := cloneUserInputs(input.ExternalInputs)
	tuning := coverage.InputTuning{
		MaxDepth:              input.Tuning.MaxDepth,
		PermuteControllers:    append([]string(nil), input.Tuning.PermuteControllers...),
		StaleReads:            cloneStringSliceMap(input.Tuning.StaleReads),
		StaleLookback:         cloneIntMap(input.Tuning.StaleLookback),
		UserActionReadyDepths: cloneIntMap(input.Tuning.UserActionReadyDepths),
		StalenessIntervals:    append([]coverage.InputStalenessInterval(nil), input.Tuning.StalenessIntervals...),
		FaultInjection:        append([]coverage.InputFaultInjection(nil), input.Tuning.FaultInjection...),
		Search:                cloneInputSearchTuning(input.Tuning.Search),
	}
	if input.Tuning.PermuteDepthRange != nil {
		cpy := *input.Tuning.PermuteDepthRange
		tuning.PermuteDepthRange = &cpy
	}
	if input.Tuning.PermuteAfterEvent != nil {
		cpy := *input.Tuning.PermuteAfterEvent
		tuning.PermuteAfterEvent = &cpy
	}
	return coverage.Input{
		Name:             input.Name,
		EnvironmentState: coverage.EnvironmentState{Objects: objects},
		ExternalInputs:   userInputs,
		Tuning:           tuning,
	}
}

func cloneInputSearchTuning(search coverage.InputSearchTuning) coverage.InputSearchTuning {
	out := coverage.InputSearchTuning{
		Mode: strings.TrimSpace(search.Mode),
	}
	if search.MonteCarlo.Seed != nil {
		seed := *search.MonteCarlo.Seed
		out.MonteCarlo.Seed = &seed
	}
	if search.MonteCarlo.Trials != nil {
		trials := *search.MonteCarlo.Trials
		out.MonteCarlo.Trials = &trials
	}
	if search.MonteCarlo.TrialIndex != nil {
		trialIdx := *search.MonteCarlo.TrialIndex
		out.MonteCarlo.TrialIndex = &trialIdx
	}
	if search.MonteCarlo.ScenarioGroup != nil {
		group := *search.MonteCarlo.ScenarioGroup
		out.MonteCarlo.ScenarioGroup = &group
	}
	return out
}

func buildUserActionsFromCoverageInput(input coverage.Input, seededObjects []client.Object) ([]tracecheck.UserAction, error) {
	return explore.UserActionsFromInput(input, seededObjects)
}

func cloneUserInputs(inputs []coverage.ExternalInput) []coverage.ExternalInput {
	if len(inputs) == 0 {
		return nil
	}

	out := make([]coverage.ExternalInput, 0, len(inputs))
	for _, input := range inputs {
		cloned := coverage.ExternalInput{
			ID:     input.ID,
			OpType: input.OpType,
		}
		if input.Object != nil {
			cloned.Object = safeDeepCopyUnstructured(input.Object)
		}
		out = append(out, cloned)
	}
	return out
}

// safeDeepCopyUnstructured copies an Unstructured object via JSON round-trip.
// The standard DeepCopy panics on []uint8 values that appear when Go's JSON
// unmarshaler encounters null fields in unstructured data.
func safeDeepCopyUnstructured(obj *unstructured.Unstructured) *unstructured.Unstructured {
	if obj == nil {
		return nil
	}
	data, err := json.Marshal(obj.Object)
	if err != nil {
		// Fall back to standard deep copy if marshal fails.
		return obj.DeepCopy()
	}
	var m map[string]interface{}
	if err := json.Unmarshal(data, &m); err != nil {
		return obj.DeepCopy()
	}
	return &unstructured.Unstructured{Object: m}
}

func cloneStringSliceMap(in map[string][]string) map[string][]string {
	if len(in) == 0 {
		return nil
	}
	out := make(map[string][]string, len(in))
	for k, v := range in {
		out[k] = append([]string(nil), v...)
	}
	return out
}

func cloneIntMap(in map[string]int) map[string]int {
	if len(in) == 0 {
		return nil
	}
	out := make(map[string]int, len(in))
	for k, v := range in {
		out[k] = v
	}
	return out
}

func isKarpenterPod(obj client.Object) bool {
	if obj == nil {
		return false
	}
	gvk := obj.GetObjectKind().GroupVersionKind()
	if gvk.Kind == "Pod" {
		return gvk.Group == "" || gvk.Group == "core"
	}
	if u, ok := obj.(*unstructured.Unstructured); ok {
		return u.GetKind() == "Pod"
	}
	return false
}

func isKarpenterNodePool(obj client.Object) bool {
	if obj == nil {
		return false
	}
	gvk := obj.GetObjectKind().GroupVersionKind()
	if gvk.Kind == "NodePool" && gvk.Group == "karpenter.sh" {
		return true
	}
	if u, ok := obj.(*unstructured.Unstructured); ok {
		return u.GetKind() == "NodePool" && strings.HasPrefix(u.GetAPIVersion(), "karpenter.sh/")
	}
	return false
}

func isKarpenterNodeClaim(obj client.Object) bool {
	if obj == nil {
		return false
	}
	gvk := obj.GetObjectKind().GroupVersionKind()
	if gvk.Kind == "NodeClaim" && gvk.Group == "karpenter.sh" {
		return true
	}
	if u, ok := obj.(*unstructured.Unstructured); ok {
		return u.GetKind() == "NodeClaim" && strings.HasPrefix(u.GetAPIVersion(), "karpenter.sh/")
	}
	return false
}

func isKarpenterNode(obj client.Object) bool {
	if obj == nil {
		return false
	}
	gvk := obj.GetObjectKind().GroupVersionKind()
	if gvk.Kind == "Node" && (gvk.Group == "" || gvk.Group == "core") {
		return true
	}
	if u, ok := obj.(*unstructured.Unstructured); ok {
		return u.GetKind() == "Node"
	}
	return false
}
