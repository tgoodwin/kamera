package main

import (
	"fmt"
	"math/rand"
	"sort"
	"strings"

	"github.com/tgoodwin/kamera/pkg/coverage"
	"github.com/tgoodwin/kamera/pkg/event"
	"github.com/tgoodwin/kamera/pkg/explore"
	"github.com/tgoodwin/kamera/pkg/tag"
	"github.com/tgoodwin/kamera/pkg/tracecheck"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
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
	if builder == nil {
		return nil, fmt.Errorf("builder is nil")
	}
	if len(inputs) == 0 {
		return nil, fmt.Errorf("no inputs supplied")
	}

	baseCfg := builder.Config()
	scenarios := make([]explore.Scenario, 0, len(inputs))
	for idx, input := range inputs {
		variants, err := expandKarpenterParameterizedInput(input, *fuzzCasesFlag, *fuzzSeedFlag)
		if err != nil {
			return nil, fmt.Errorf("expand input %d (%s): %w", idx, input.Name, err)
		}

		for _, variant := range variants {
			state, seededObjects, err := buildStateFromCoverageInput(builder, variant)
			if err != nil {
				return nil, fmt.Errorf("build start state for %s: %w", variant.Name, err)
			}
			userActions, err := buildUserActionsFromCoverageInput(variant, seededObjects)
			if err != nil {
				return nil, fmt.Errorf("build user actions for %s: %w", variant.Name, err)
			}

			scenarios = append(scenarios, explore.Scenario{
				Name:             variant.Name,
				EnvironmentState: state,
				UserInputs:       userActions,
				Config:           applyInputTuning(baseCfg, variant.Tuning),
			})
		}
	}

	if len(scenarios) == 0 {
		return nil, fmt.Errorf("no scenarios produced")
	}
	return scenarios, nil
}

type karpenterParamSpec struct {
	options []karpenterParamOption
}

type karpenterParamOption struct {
	name       string
	isBaseline bool
	apply      func(*corev1.Pod, *v1.NodePool)
}

func expandKarpenterParameterizedInput(input coverage.Input, fuzzCases int, fuzzSeed int64) ([]coverage.Input, error) {
	baseName := strings.TrimSpace(input.Name)
	if baseName == "" {
		baseName = "scenario"
	}

	base := cloneCoverageInput(input)
	base.Name = baseName + "/base"

	podIdx := findKarpenterPodInUserInputs(base.UserInputs)
	nodePoolIdx := findKarpenterNodePool(base.EnvironmentState.Objects)
	if podIdx < 0 || nodePoolIdx < 0 {
		return []coverage.Input{base}, nil
	}

	templatePod, err := unstructuredToPod(base.UserInputs[podIdx].Object)
	if err != nil {
		return nil, err
	}
	nodePoolObject := base.EnvironmentState.Objects[nodePoolIdx]
	templateNodePool, err := unstructuredToNodePool(nodePoolObject)
	if err != nil {
		return nil, err
	}

	variants := []coverage.Input{base}
	singleVariants, err := expandKarpenterSingleParamVariants(input, baseName, podIdx, nodePoolIdx, templatePod, templateNodePool)
	if err != nil {
		return nil, err
	}
	variants = append(variants, singleVariants...)

	sampledVariants, err := expandKarpenterSampledParamVariants(input, baseName, podIdx, nodePoolIdx, templatePod, templateNodePool, fuzzCases, fuzzSeed)
	if err != nil {
		return nil, err
	}
	variants = append(variants, sampledVariants...)

	return variants, nil
}

func karpenterParamCatalog() []karpenterParamSpec {
	return []karpenterParamSpec{
		{
			options: []karpenterParamOption{
				{name: "baseline", isBaseline: true, apply: func(*corev1.Pod, *v1.NodePool) {}},
				{
					name: "pod-requests-500m",
					apply: func(pod *corev1.Pod, _ *v1.NodePool) {
						container := ensurePrimaryContainer(pod)
						if container.Resources.Requests == nil {
							container.Resources.Requests = corev1.ResourceList{}
						}
						container.Resources.Requests[corev1.ResourceCPU] = resource.MustParse("500m")
						container.Resources.Requests[corev1.ResourceMemory] = resource.MustParse("512Mi")
					},
				},
				{
					name: "pod-selector-arch-arm64",
					apply: func(pod *corev1.Pod, _ *v1.NodePool) {
						ensurePodNodeSelector(pod)["kubernetes.io/arch"] = "arm64"
					},
				},
			},
		},
		{
			options: []karpenterParamOption{
				{name: "baseline", isBaseline: true, apply: func(*corev1.Pod, *v1.NodePool) {}},
				{
					name: "no-fit-pod-selector-unmatched",
					apply: func(pod *corev1.Pod, _ *v1.NodePool) {
						ensurePodNodeSelector(pod)["karpenter.sh/nonexistent-capability"] = "required"
					},
				},
				{
					name: "no-fit-nodepool-requirement-unmatched",
					apply: func(_ *corev1.Pod, nodePool *v1.NodePool) {
						nodePool.Spec.Template.Spec.Requirements = append(nodePool.Spec.Template.Spec.Requirements, v1.NodeSelectorRequirementWithMinValues{
							Key:      "karpenter.sh/nonexistent-capability",
							Operator: corev1.NodeSelectorOpIn,
							Values:   []string{"required"},
						})
					},
				},
			},
		},
	}
}

func expandKarpenterSingleParamVariants(input coverage.Input, baseName string, podIdx int, nodePoolIdx int, templatePod *corev1.Pod, templateNodePool *v1.NodePool) ([]coverage.Input, error) {
	variants := make([]coverage.Input, 0)
	for _, spec := range karpenterParamCatalog() {
		for _, option := range spec.options {
			if option.isBaseline {
				continue
			}
			name := fmt.Sprintf("%s/single/%s", baseName, option.name)
			updated, err := buildKarpenterVariantInput(input, podIdx, nodePoolIdx, templatePod, templateNodePool, name, []karpenterParamOption{option})
			if err != nil {
				return nil, err
			}
			variants = append(variants, updated)
		}
	}
	return variants, nil
}

func expandKarpenterSampledParamVariants(input coverage.Input, baseName string, podIdx int, nodePoolIdx int, templatePod *corev1.Pod, templateNodePool *v1.NodePool, cases int, seed int64) ([]coverage.Input, error) {
	if cases <= 0 {
		return nil, nil
	}

	specs := karpenterParamCatalog()
	rng := rand.New(rand.NewSource(seed))
	seenAssignments := map[string]struct{}{}
	variants := make([]coverage.Input, 0, cases)

	maxAttempts := cases * 20
	for attempts := 0; attempts < maxAttempts && len(variants) < cases; attempts++ {
		selection := sampleKarpenterParamSelection(rng, specs)
		key := karpenterSelectionKey(selection)
		if key == "" {
			continue
		}
		if _, exists := seenAssignments[key]; exists {
			continue
		}
		seenAssignments[key] = struct{}{}

		choiceOptions := make([]karpenterParamOption, 0, len(selection))
		choiceNames := make([]string, 0, len(selection))
		for specIdx, optionIdx := range selection {
			option := specs[specIdx].options[optionIdx]
			choiceOptions = append(choiceOptions, option)
			choiceNames = append(choiceNames, option.name)
		}
		sort.Strings(choiceNames)
		name := fmt.Sprintf("%s/sampled-%03d/%s", baseName, len(variants)+1, strings.Join(choiceNames, "+"))

		updated, err := buildKarpenterVariantInput(input, podIdx, nodePoolIdx, templatePod, templateNodePool, name, choiceOptions)
		if err != nil {
			return nil, err
		}
		variants = append(variants, updated)
	}

	return variants, nil
}

func buildKarpenterVariantInput(input coverage.Input, podIdx int, nodePoolIdx int, templatePod *corev1.Pod, templateNodePool *v1.NodePool, name string, options []karpenterParamOption) (coverage.Input, error) {
	updated := cloneCoverageInput(input)
	updated.Name = name

	pod := templatePod.DeepCopy()
	nodePool := templateNodePool.DeepCopy()
	for _, option := range options {
		option.apply(pod, nodePool)
	}

	podObj, err := podToUnstructured(pod)
	if err != nil {
		return coverage.Input{}, fmt.Errorf("convert pod for %q: %w", name, err)
	}
	nodePoolObj, err := nodePoolToUnstructured(nodePool)
	if err != nil {
		return coverage.Input{}, fmt.Errorf("convert nodepool for %q: %w", name, err)
	}
	updated.UserInputs[podIdx].Object = podObj
	if nodePoolIdx < 0 || nodePoolIdx >= len(updated.EnvironmentState.Objects) {
		return coverage.Input{}, fmt.Errorf("nodepool environment index %d out of range", nodePoolIdx)
	}
	updated.EnvironmentState.Objects[nodePoolIdx] = nodePoolObj

	return updated, nil
}

func sampleKarpenterParamSelection(rng *rand.Rand, specs []karpenterParamSpec) map[int]int {
	selection := map[int]int{}
	if len(specs) == 0 {
		return selection
	}

	primaryIdx := rng.Intn(len(specs))
	selection[primaryIdx] = randomKarpenterNonBaselineOptionIndex(rng, specs[primaryIdx])

	if secondaryIdx, ok := randomKarpenterSpecIndexExcluding(rng, len(specs), selection); ok && rng.Intn(100) < 40 {
		selection[secondaryIdx] = randomKarpenterNonBaselineOptionIndex(rng, specs[secondaryIdx])
	}

	return selection
}

func randomKarpenterSpecIndexExcluding(rng *rand.Rand, total int, excluded map[int]int) (int, bool) {
	candidates := make([]int, 0, total-len(excluded))
	for i := 0; i < total; i++ {
		if _, blocked := excluded[i]; blocked {
			continue
		}
		candidates = append(candidates, i)
	}
	if len(candidates) == 0 {
		return 0, false
	}
	return candidates[rng.Intn(len(candidates))], true
}

func randomKarpenterNonBaselineOptionIndex(rng *rand.Rand, spec karpenterParamSpec) int {
	choices := make([]int, 0, len(spec.options))
	for idx, option := range spec.options {
		if option.isBaseline {
			continue
		}
		choices = append(choices, idx)
	}
	if len(choices) == 0 {
		return 0
	}
	return choices[rng.Intn(len(choices))]
}

func karpenterSelectionKey(selection map[int]int) string {
	if len(selection) == 0 {
		return ""
	}
	keys := make([]int, 0, len(selection))
	for k := range selection {
		keys = append(keys, k)
	}
	sort.Ints(keys)

	parts := make([]string, 0, len(keys))
	for _, k := range keys {
		parts = append(parts, fmt.Sprintf("%d=%d", k, selection[k]))
	}
	return strings.Join(parts, ",")
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
		objects = append(objects, obj.DeepCopy())
	}
	for _, userInput := range input.UserInputs {
		if userInput.Type != event.CREATE || userInput.Object == nil || !isKarpenterPod(userInput.Object) {
			continue
		}
		if isInputObjectSeeded(userInput.Object, objects) {
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
	return nil
}

func applyInputTuning(base tracecheck.ExploreConfig, tuning coverage.InputTuning) tracecheck.ExploreConfig {
	cfg := base.Clone()
	if tuning.MaxDepth > 0 {
		cfg.MaxDepth = tuning.MaxDepth
	}
	if len(tuning.PermuteControllers) > 0 {
		if cfg.Perturbations.PermuteOrder == nil {
			cfg.Perturbations.PermuteOrder = make(map[tracecheck.ReconcilerID]bool)
		}
		for _, controllerID := range tuning.PermuteControllers {
			cfg.Perturbations.PermuteOrder[tracecheck.ReconcilerID(controllerID)] = true
		}
	}
	return cfg
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

	userInputs := cloneUserInputs(input.UserInputs)
	tuning := coverage.InputTuning{
		MaxDepth:           input.Tuning.MaxDepth,
		PermuteControllers: append([]string(nil), input.Tuning.PermuteControllers...),
		StaleReads:         cloneStringSliceMap(input.Tuning.StaleReads),
		StaleLookback:      cloneIntMap(input.Tuning.StaleLookback),
	}
	return coverage.Input{
		Name:             input.Name,
		EnvironmentState: coverage.EnvironmentState{Objects: objects},
		UserInputs:       userInputs,
		Tuning:           tuning,
	}
}

func buildUserActionsFromCoverageInput(input coverage.Input, seededObjects []client.Object) ([]tracecheck.UserAction, error) {
	actions := make([]tracecheck.UserAction, 0, len(input.UserInputs))
	for idx, userInput := range input.UserInputs {
		if userInput.Object == nil {
			return nil, fmt.Errorf("user input %d (%s) missing object", idx, input.Name)
		}
		opType := userInput.Type
		if opType == event.CREATE && isInputObjectSeeded(userInput.Object, seededObjects) {
			opType = event.UPDATE
		}
		actions = append(actions, tracecheck.UserAction{
			ID:      strings.TrimSpace(userInput.ID),
			OpType:  opType,
			Payload: userInput.Object.DeepCopy(),
		})
	}
	for idx := range actions {
		if strings.TrimSpace(actions[idx].ID) == "" {
			actions[idx].ID = fmt.Sprintf("user-input-%d", idx)
		}
	}
	return actions, nil
}

func isInputObjectSeeded(object client.Object, seededObjects []client.Object) bool {
	if object == nil {
		return false
	}
	for _, seeded := range seededObjects {
		if sameObjectIdentity(seeded, object) {
			return true
		}
	}
	return false
}

func sameObjectIdentity(a, b client.Object) bool {
	if a == nil || b == nil {
		return false
	}
	aGVK := a.GetObjectKind().GroupVersionKind()
	bGVK := b.GetObjectKind().GroupVersionKind()
	if aGVK.Group != bGVK.Group || aGVK.Kind != bGVK.Kind {
		return false
	}
	return a.GetNamespace() == b.GetNamespace() && a.GetName() == b.GetName()
}

func cloneUserInputs(inputs []coverage.UserInput) []coverage.UserInput {
	if len(inputs) == 0 {
		return nil
	}

	out := make([]coverage.UserInput, 0, len(inputs))
	for _, input := range inputs {
		cloned := coverage.UserInput{
			ID:   input.ID,
			Type: input.Type,
		}
		if input.Object != nil {
			cloned.Object = input.Object.DeepCopy()
		}
		out = append(out, cloned)
	}
	return out
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

func findKarpenterPodInUserInputs(userInputs []coverage.UserInput) int {
	for idx, input := range userInputs {
		if input.Object == nil {
			continue
		}
		if isKarpenterPod(input.Object) {
			return idx
		}
	}
	return -1
}

func findKarpenterNodePool(objects []*unstructured.Unstructured) int {
	for idx, obj := range objects {
		if obj == nil {
			continue
		}
		if isKarpenterNodePool(obj) {
			return idx
		}
	}
	return -1
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

func unstructuredToPod(obj *unstructured.Unstructured) (*corev1.Pod, error) {
	if obj == nil {
		return nil, fmt.Errorf("pod object is nil")
	}
	var pod corev1.Pod
	if err := runtime.DefaultUnstructuredConverter.FromUnstructured(obj.Object, &pod); err != nil {
		return nil, fmt.Errorf("convert pod from unstructured: %w", err)
	}
	pod.SetGroupVersionKind(corev1.SchemeGroupVersion.WithKind("Pod"))
	return &pod, nil
}

func podToUnstructured(pod *corev1.Pod) (*unstructured.Unstructured, error) {
	if pod == nil {
		return nil, fmt.Errorf("pod is nil")
	}
	obj, err := runtime.DefaultUnstructuredConverter.ToUnstructured(pod)
	if err != nil {
		return nil, fmt.Errorf("convert pod to unstructured: %w", err)
	}
	u := &unstructured.Unstructured{Object: obj}
	u.SetGroupVersionKind(corev1.SchemeGroupVersion.WithKind("Pod"))
	return u, nil
}

func unstructuredToNodePool(obj *unstructured.Unstructured) (*v1.NodePool, error) {
	if obj == nil {
		return nil, fmt.Errorf("nodepool object is nil")
	}
	var nodePool v1.NodePool
	if err := runtime.DefaultUnstructuredConverter.FromUnstructured(obj.Object, &nodePool); err != nil {
		return nil, fmt.Errorf("convert nodepool from unstructured: %w", err)
	}
	nodePool.SetGroupVersionKind(schema.GroupVersion{Group: "karpenter.sh", Version: "v1"}.WithKind("NodePool"))
	return &nodePool, nil
}

func nodePoolToUnstructured(nodePool *v1.NodePool) (*unstructured.Unstructured, error) {
	if nodePool == nil {
		return nil, fmt.Errorf("nodepool is nil")
	}
	obj, err := runtime.DefaultUnstructuredConverter.ToUnstructured(nodePool)
	if err != nil {
		return nil, fmt.Errorf("convert nodepool to unstructured: %w", err)
	}
	u := &unstructured.Unstructured{Object: obj}
	u.SetGroupVersionKind(schema.GroupVersion{Group: "karpenter.sh", Version: "v1"}.WithKind("NodePool"))
	return u, nil
}

func ensurePrimaryContainer(pod *corev1.Pod) *corev1.Container {
	if len(pod.Spec.Containers) == 0 {
		pod.Spec.Containers = []corev1.Container{{Name: "c", Image: "pause"}}
	}
	return &pod.Spec.Containers[0]
}

func ensurePodNodeSelector(pod *corev1.Pod) map[string]string {
	if pod.Spec.NodeSelector == nil {
		pod.Spec.NodeSelector = map[string]string{}
	}
	return pod.Spec.NodeSelector
}
