package main

import (
	"crypto/sha256"
	"fmt"
	"math/big"
	"strconv"
	"strings"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	"github.com/kcp-dev/logicalcluster/v3"
	apisv1alpha1 "github.com/kcp-dev/sdk/apis/apis/v1alpha1"
	apisv1alpha2 "github.com/kcp-dev/sdk/apis/apis/v1alpha2"
	"github.com/kcp-dev/sdk/apis/core"
	corev1alpha1 "github.com/kcp-dev/sdk/apis/core/v1alpha1"
	tenancyv1alpha1 "github.com/kcp-dev/sdk/apis/tenancy/v1alpha1"
	conditionsv1alpha1 "github.com/kcp-dev/sdk/apis/third_party/conditions/apis/conditions/v1alpha1"
	"github.com/kcp-dev/sdk/apis/third_party/conditions/util/conditions"
	topologyv1alpha1 "github.com/kcp-dev/sdk/apis/topology/v1alpha1"
	kcpscheme "github.com/kcp-dev/sdk/client/clientset/versioned/cluster/scheme"
	"github.com/tgoodwin/kamera/pkg/event"

	"github.com/tgoodwin/kamera/pkg/coverage"
	"github.com/tgoodwin/kamera/pkg/explore"
	"github.com/tgoodwin/kamera/pkg/replay"
	"github.com/tgoodwin/kamera/pkg/tracecheck"
)

const (
	logicalClusterControllerID             tracecheck.ReconcilerID = "LogicalClusterController"
	apiBinderInitializerControllerID       tracecheck.ReconcilerID = "APIBinderInitializerController"
	defaultAPIBindingLifecycleControllerID tracecheck.ReconcilerID = "DefaultAPIBindingLifecycleController"
	apiExportEndpointSliceControllerID     tracecheck.ReconcilerID = "APIExportEndpointSliceController"
	apiExportEndpointSliceURLsControllerID tracecheck.ReconcilerID = "APIExportEndpointSliceURLsController"
	logicalClusterCleanupControllerID       tracecheck.ReconcilerID = "LogicalClusterCleanupController"
	apiExportControllerID                  tracecheck.ReconcilerID = "APIExportController"
	apiBindingAnnotationSyncControllerID   tracecheck.ReconcilerID = "APIBindingAnnotationSyncController"
	apiBindingReconcilerControllerID       tracecheck.ReconcilerID = "APIBindingReconcilerController"
	apiBindingDeletionControllerID         tracecheck.ReconcilerID = "APIBindingDeletionController"

	rootClusterPath     = "root"
	providerClusterPath = "root:provider"
	consumerClusterPath = "root:consumer"

	rootShardName     = "root"
	rootShardExternal = "https://root.example.invalid"

	workspaceTypeName = "consumer-type"
	apiExportName     = "widgets"
	partitionName     = "internal"

	traceClusterNSKey = "trace.kamera.dev/cluster"
	traceActualNSKey  = "trace.kamera.dev/actual-namespace"

	maxExportNamePrefixLength = 245
)

func newKCPExplorerBuilder() *tracecheck.ExplorerBuilder {
	scheme := runtime.NewScheme()
	mustAddToScheme(scheme)

	builder := tracecheck.NewExplorerBuilder(scheme)
	builder.WithMaxDepth(64)

	builder.WithCustomStrategy(logicalClusterControllerID, func(r replay.EffectRecorder) tracecheck.Strategy {
		return newUpstreamKCPStrategy(logicalClusterControllerID, scheme, r)
	}).For("core.kcp.io/LogicalCluster").
		Watches("core.kcp.io/LogicalCluster", enqueueTraceObject())

	builder.WithCustomStrategy(apiBinderInitializerControllerID, func(r replay.EffectRecorder) tracecheck.Strategy {
		return newUpstreamKCPStrategy(apiBinderInitializerControllerID, scheme, r)
	}).For("core.kcp.io/LogicalCluster").
		Watches("core.kcp.io/LogicalCluster", enqueueTraceObject()).
		Watches("apis.kcp.io/APIBinding", enqueueLogicalClusterForAPIBinding)

	builder.WithCustomStrategy(defaultAPIBindingLifecycleControllerID, func(r replay.EffectRecorder) tracecheck.Strategy {
		return newUpstreamKCPStrategy(defaultAPIBindingLifecycleControllerID, scheme, r)
	}).For("core.kcp.io/LogicalCluster").
		Watches("core.kcp.io/LogicalCluster", enqueueTraceObject()).
		Watches("apis.kcp.io/APIBinding", enqueueLogicalClusterForAPIBinding)

	builder.WithCustomStrategy(apiExportEndpointSliceControllerID, func(r replay.EffectRecorder) tracecheck.Strategy {
		return newUpstreamKCPStrategy(apiExportEndpointSliceControllerID, scheme, r)
	}).For("apis.kcp.io/APIExportEndpointSlice").
		Watches("apis.kcp.io/APIExportEndpointSlice", enqueueTraceObject())

	builder.WithCustomStrategy(apiExportEndpointSliceURLsControllerID, func(r replay.EffectRecorder) tracecheck.Strategy {
		return newUpstreamKCPStrategy(apiExportEndpointSliceURLsControllerID, scheme, r)
	}).For("apis.kcp.io/APIExportEndpointSlice").
		Watches("apis.kcp.io/APIExportEndpointSlice", enqueueTraceObject()).
		Watches("apis.kcp.io/APIBinding", enqueueEndpointSliceForAPIBinding)

	builder.WithCustomStrategy(logicalClusterCleanupControllerID, func(r replay.EffectRecorder) tracecheck.Strategy {
		return newUpstreamKCPStrategy(logicalClusterCleanupControllerID, scheme, r)
	}).For("core.kcp.io/LogicalCluster").
		Watches("core.kcp.io/LogicalCluster", enqueueTraceObject()).
		Watches("apis.kcp.io/APIBinding", enqueueLogicalClusterForAPIBinding)

	builder.WithCustomStrategy(apiExportControllerID, func(r replay.EffectRecorder) tracecheck.Strategy {
		return newUpstreamKCPStrategy(apiExportControllerID, scheme, r)
	}).For("apis.kcp.io/APIExport").
		Watches("apis.kcp.io/APIExport", enqueueTraceObject())

	builder.WithCustomStrategy(apiBindingAnnotationSyncControllerID, func(r replay.EffectRecorder) tracecheck.Strategy {
		return newUpstreamKCPStrategy(apiBindingAnnotationSyncControllerID, scheme, r)
	}).For("apis.kcp.io/APIBinding").
		Watches("apis.kcp.io/APIBinding", enqueueTraceObject())

	builder.WithCustomStrategy(apiBindingReconcilerControllerID, func(r replay.EffectRecorder) tracecheck.Strategy {
		return newUpstreamKCPStrategy(apiBindingReconcilerControllerID, scheme, r)
	}).For("apis.kcp.io/APIBinding").
		Watches("apis.kcp.io/APIBinding", enqueueTraceObject()).
		Watches("apis.kcp.io/APIExport", enqueueBindingsForAPIExport)

	builder.WithCustomStrategy(apiBindingDeletionControllerID, func(r replay.EffectRecorder) tracecheck.Strategy {
		return newUpstreamKCPStrategy(apiBindingDeletionControllerID, scheme, r)
	}).For("apis.kcp.io/APIBinding").
		Watches("apis.kcp.io/APIBinding", enqueueTraceObject())

	return builder
}

func buildInitialKCPState(builder *tracecheck.ExplorerBuilder) tracecheck.StateNode {
	stateBuilder := builder.NewStateEventBuilder()
	objects := []client.Object{
		buildRootLogicalClusterTyped(),
		buildProviderLogicalClusterTyped(),
		buildConsumerLogicalClusterTyped(),
		buildSchedulableShardTyped(),
		buildWorkspaceTypeTyped(),
		buildAPIExportTyped(),
		buildConsumerAPIBindingTyped(),
		buildPartitionTyped(),
		buildAPIExportEndpointSliceTyped(),
	}

	state := addSeedObject(stateBuilder, objects[0])
	for _, obj := range objects[1:] {
		state = tracecheck.MergeStateNodes(state, addSeedObject(stateBuilder, obj))
	}
	return state
}

func defaultInteractiveUserActions() []tracecheck.UserAction {
	return nil
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
		state, seededObjects, err := buildStateFromCoverageInput(builder, input)
		if err != nil {
			return nil, fmt.Errorf("build start state for input %d (%s): %w", idx, input.Name, err)
		}
		userInputs, err := buildUserActionsFromCoverageInput(input, seededObjects)
		if err != nil {
			return nil, fmt.Errorf("build user actions for input %d (%s): %w", idx, input.Name, err)
		}

		scenarios = append(scenarios, explore.Scenario{
			Name:             input.Name,
			EnvironmentState: state,
			ExternalInputs:       userInputs,
			Config:           applyInputTuning(baseCfg, input.Tuning),
		})
	}

	return scenarios, nil
}

func buildStateFromCoverageInput(builder *tracecheck.ExplorerBuilder, input coverage.Input) (tracecheck.StateNode, []client.Object, error) {
	if builder == nil {
		return tracecheck.StateNode{}, nil, fmt.Errorf("builder is nil")
	}

	objects := make([]client.Object, 0, len(input.EnvironmentState.Objects))
	for idx, obj := range input.EnvironmentState.Objects {
		if obj == nil {
			return tracecheck.StateNode{}, nil, fmt.Errorf("input environment object %d is nil", idx)
		}
		objects = append(objects, obj.DeepCopy())
	}

	if len(objects) == 0 {
		for _, action := range input.ExternalInputs {
			if action.Object == nil || action.OpType != event.CREATE {
				continue
			}
			objects = append(objects, action.Object.DeepCopy())
		}
	}

	if len(objects) == 0 {
		return tracecheck.StateNode{}, nil, fmt.Errorf("input has no seedable objects")
	}

	stateBuilder := builder.NewStateEventBuilder()
	state := addSeedObject(stateBuilder, objects[0])
	for _, obj := range objects[1:] {
		state = tracecheck.MergeStateNodes(state, addSeedObject(stateBuilder, obj))
	}
	return state, objects, nil
}

func addSeedObject(builder *tracecheck.StateEventBuilder, obj client.Object) tracecheck.StateNode {
	obj = obj.DeepCopyObject().(client.Object)
	gvk := obj.GetObjectKind().GroupVersionKind()
	cluster := traceClusterFor(obj)
	if needsClusterRequestKey(gvk) && obj.GetNamespace() == "" && cluster != "" {
		obj.SetNamespace(cluster)
	}

	switch {
	case gvk.Group == corev1alpha1.SchemeGroupVersion.Group && gvk.Kind == "LogicalCluster" && cluster == consumerClusterPath:
		state := builder.AddTopLevelObject(obj, logicalClusterControllerID)
		state = tracecheck.MergeStateNodes(state, builder.AddTopLevelObject(obj, apiBinderInitializerControllerID))
		return tracecheck.MergeStateNodes(state, builder.AddTopLevelObject(obj, defaultAPIBindingLifecycleControllerID))
	case gvk.Group == apisv1alpha1.SchemeGroupVersion.Group && gvk.Kind == "APIExportEndpointSlice":
		state := builder.AddTopLevelObject(obj, apiExportEndpointSliceControllerID)
		return tracecheck.MergeStateNodes(state, builder.AddTopLevelObject(obj, apiExportEndpointSliceURLsControllerID))
	case gvk.Group == apisv1alpha2.SchemeGroupVersion.Group && gvk.Kind == "APIBinding":
		return builder.AddTopLevelObject(obj, apiBindingAnnotationSyncControllerID)
	default:
		return builder.AddTopLevelObject(obj)
	}
}

func buildUserActionsFromCoverageInput(input coverage.Input, seededObjects []client.Object) ([]tracecheck.UserAction, error) {
	actions := make([]tracecheck.UserAction, 0, len(input.ExternalInputs))
	for idx, action := range input.ExternalInputs {
		if action.Object == nil {
			return nil, fmt.Errorf("input user input %d has nil object", idx)
		}

		id := strings.TrimSpace(action.ID)
		if id == "" {
			id = fmt.Sprintf("user-input-%d", idx)
		}

		opType := action.OpType
		if opType == "CREATE" && isInputObjectSeeded(action.Object, seededObjects) {
			opType = "UPDATE"
		}

		actions = append(actions, tracecheck.UserAction{
			ID:      id,
			OpType:  opType,
			Payload: action.Object.DeepCopy(),
		})
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
	if len(tuning.StaleReads) > 0 {
		if cfg.Perturbations.Staleness == nil {
			cfg.Perturbations.Staleness = make(map[tracecheck.ReconcilerID]tracecheck.StalenessConfig)
		}
		for controllerID, kinds := range tuning.StaleReads {
			id := tracecheck.ReconcilerID(controllerID)
			st := cfg.Perturbations.Staleness[id]
			if st.StaleReadBounds == nil {
				st.StaleReadBounds = make(tracecheck.LookbackLimits)
			}
			for _, kind := range kinds {
				trimmed := strings.TrimSpace(kind)
				if trimmed == "" {
					continue
				}
				lookback := tuning.StaleLookback[trimmed]
				if lookback <= 0 {
					lookback = 1
				}
				st.StaleReadBounds[trimmed] = tracecheck.LookbackLimit(lookback)
			}
			cfg.Perturbations.Staleness[id] = st
		}
	}
	if len(tuning.UserActionReadyDepths) > 0 {
		if cfg.Perturbations.UserActionReadyDepths == nil {
			cfg.Perturbations.UserActionReadyDepths = make(map[int]int)
		}
		for idxStr, depth := range tuning.UserActionReadyDepths {
			idx, err := strconv.Atoi(idxStr)
			if err != nil {
				continue
			}
			cfg.Perturbations.UserActionReadyDepths[idx] = depth
		}
	}
	return cfg
}

func enqueueTraceObject() tracecheck.WatchMapper {
	return func(obj *unstructured.Unstructured) []reconcile.Request {
		if obj == nil || obj.GetName() == "" {
			return nil
		}
		return []reconcile.Request{{
			NamespacedName: types.NamespacedName{
				Namespace: obj.GetNamespace(),
				Name:      obj.GetName(),
			},
		}}
	}
}

func enqueueBindingsForAPIExport(obj *unstructured.Unstructured) []reconcile.Request {
	// When an APIExport changes, we need to re-reconcile all APIBindings.
	// In the real controller this uses an index; here we just return an empty
	// list since the binding is already tracked via its own watch.
	// The binding will be re-enqueued by its own update event.
	return nil
}

func enqueueEndpointSliceForAPIBinding(obj *unstructured.Unstructured) []reconcile.Request {
	if obj == nil {
		return nil
	}
	exportRef, found, err := unstructured.NestedMap(obj.Object, "spec", "reference", "export")
	if err != nil || !found {
		return nil
	}
	name, _ := exportRef["name"].(string)
	path, _ := exportRef["path"].(string)
	if name == "" {
		return nil
	}
	if path == "" {
		path = traceClusterFor(obj)
	}
	return []reconcile.Request{{
		NamespacedName: types.NamespacedName{
			Namespace: path,
			Name:      name,
		},
	}}
}

func enqueueLogicalClusterForAPIBinding(obj *unstructured.Unstructured) []reconcile.Request {
	if obj == nil {
		return nil
	}
	cluster := traceClusterFor(obj)
	if cluster == "" {
		cluster = obj.GetNamespace()
	}
	if cluster == "" {
		return nil
	}
	return []reconcile.Request{{
		NamespacedName: types.NamespacedName{
			Namespace: cluster,
			Name:      corev1alpha1.LogicalClusterName,
		},
	}}
}

// Seed object builders

func buildRootLogicalClusterTyped() *corev1alpha1.LogicalCluster {
	lc := &corev1alpha1.LogicalCluster{
		TypeMeta: metav1.TypeMeta{
			APIVersion: corev1alpha1.SchemeGroupVersion.String(),
			Kind:       "LogicalCluster",
		},
		ObjectMeta: metav1.ObjectMeta{
			Name: corev1alpha1.LogicalClusterName,
			Annotations: map[string]string{
				logicalcluster.AnnotationKey:         rootClusterPath,
				core.LogicalClusterPathAnnotationKey: rootClusterPath,
			},
		},
		Status: corev1alpha1.LogicalClusterStatus{
			Phase: corev1alpha1.LogicalClusterPhaseReady,
			Conditions: []conditionsv1alpha1.Condition{{
				Type:   conditionsv1alpha1.ReadyCondition,
				Status: corev1.ConditionTrue,
			}},
		},
	}
	conditions.MarkTrue(lc, tenancyv1alpha1.WorkspaceInitialized)
	return lc
}

func buildProviderLogicalClusterTyped() *corev1alpha1.LogicalCluster {
	lc := &corev1alpha1.LogicalCluster{
		TypeMeta: metav1.TypeMeta{
			APIVersion: corev1alpha1.SchemeGroupVersion.String(),
			Kind:       "LogicalCluster",
		},
		ObjectMeta: metav1.ObjectMeta{
			Name: corev1alpha1.LogicalClusterName,
			Annotations: map[string]string{
				logicalcluster.AnnotationKey:         providerClusterPath,
				core.LogicalClusterPathAnnotationKey: providerClusterPath,
			},
		},
		Status: corev1alpha1.LogicalClusterStatus{
			Phase: corev1alpha1.LogicalClusterPhaseReady,
			Conditions: []conditionsv1alpha1.Condition{{
				Type:   conditionsv1alpha1.ReadyCondition,
				Status: corev1.ConditionTrue,
			}},
		},
	}
	conditions.MarkTrue(lc, tenancyv1alpha1.WorkspaceInitialized)
	return lc
}

func buildConsumerLogicalClusterTyped() *corev1alpha1.LogicalCluster {
	return &corev1alpha1.LogicalCluster{
		TypeMeta: metav1.TypeMeta{
			APIVersion: corev1alpha1.SchemeGroupVersion.String(),
			Kind:       "LogicalCluster",
		},
		ObjectMeta: metav1.ObjectMeta{
			Name: corev1alpha1.LogicalClusterName,
			Annotations: map[string]string{
				logicalcluster.AnnotationKey:                    consumerClusterPath,
				core.LogicalClusterPathAnnotationKey:            consumerClusterPath,
				tenancyv1alpha1.LogicalClusterTypeAnnotationKey: rootClusterPath + ":" + workspaceTypeName,
			},
		},
		Status: corev1alpha1.LogicalClusterStatus{
			Phase:        corev1alpha1.LogicalClusterPhaseInitializing,
			Initializers: []corev1alpha1.LogicalClusterInitializer{tenancyv1alpha1.WorkspaceAPIBindingsInitializer},
		},
	}
}

func buildSchedulableShardTyped() *corev1alpha1.Shard {
	return &corev1alpha1.Shard{
		TypeMeta: metav1.TypeMeta{
			APIVersion: corev1alpha1.SchemeGroupVersion.String(),
			Kind:       "Shard",
		},
		ObjectMeta: metav1.ObjectMeta{
			Name: rootShardName,
			Annotations: map[string]string{
				logicalcluster.AnnotationKey:         rootClusterPath,
				core.LogicalClusterPathAnnotationKey: rootClusterPath,
			},
			Labels: map[string]string{
				"region": "internal",
			},
		},
		Spec: corev1alpha1.ShardSpec{
			BaseURL:             rootShardExternal,
			ExternalURL:         rootShardExternal,
			VirtualWorkspaceURL: rootShardExternal,
		},
	}
}

func buildWorkspaceTypeTyped() *tenancyv1alpha1.WorkspaceType {
	mode := tenancyv1alpha1.APIBindingLifecycleModeMaintain
	return &tenancyv1alpha1.WorkspaceType{
		TypeMeta: metav1.TypeMeta{
			APIVersion: tenancyv1alpha1.SchemeGroupVersion.String(),
			Kind:       "WorkspaceType",
		},
		ObjectMeta: metav1.ObjectMeta{
			Name: workspaceTypeName,
			Annotations: map[string]string{
				logicalcluster.AnnotationKey:         rootClusterPath,
				core.LogicalClusterPathAnnotationKey: rootClusterPath,
			},
		},
		Spec: tenancyv1alpha1.WorkspaceTypeSpec{
			DefaultAPIBindings: []tenancyv1alpha1.APIExportReference{{
				Path:   providerClusterPath,
				Export: apiExportName,
			}},
			DefaultAPIBindingLifecycle: &mode,
		},
	}
}

func buildAPIExportTyped() *apisv1alpha2.APIExport {
	return &apisv1alpha2.APIExport{
		TypeMeta: metav1.TypeMeta{
			APIVersion: apisv1alpha2.SchemeGroupVersion.String(),
			Kind:       "APIExport",
		},
		ObjectMeta: metav1.ObjectMeta{
			Name: apiExportName,
			Annotations: map[string]string{
				logicalcluster.AnnotationKey:         providerClusterPath,
				core.LogicalClusterPathAnnotationKey: providerClusterPath,
				"extra.apis.kcp.io/visibility":       "internal",
			},
		},
		Status: apisv1alpha2.APIExportStatus{
			IdentityHash: "widgets-identity",
		},
	}
}

func buildConsumerAPIBindingTyped() *apisv1alpha2.APIBinding {
	binding := &apisv1alpha2.APIBinding{
		TypeMeta: metav1.TypeMeta{
			APIVersion: apisv1alpha2.SchemeGroupVersion.String(),
			Kind:       "APIBinding",
		},
		ObjectMeta: metav1.ObjectMeta{
			Name: buildConsumerAPIBindingName(),
			Annotations: map[string]string{
				logicalcluster.AnnotationKey: consumerClusterPath,
			},
		},
		Spec: apisv1alpha2.APIBindingSpec{
			Reference: apisv1alpha2.BindingReference{
				Export: &apisv1alpha2.ExportBindingReference{
					Path: providerClusterPath,
					Name: apiExportName,
				},
			},
		},
	}
	conditions.MarkTrue(binding, apisv1alpha2.InitialBindingCompleted)
	return binding
}

func buildPartitionTyped() *topologyv1alpha1.Partition {
	return &topologyv1alpha1.Partition{
		TypeMeta: metav1.TypeMeta{
			APIVersion: topologyv1alpha1.SchemeGroupVersion.String(),
			Kind:       "Partition",
		},
		ObjectMeta: metav1.ObjectMeta{
			Name: partitionName,
			Annotations: map[string]string{
				logicalcluster.AnnotationKey:         providerClusterPath,
				core.LogicalClusterPathAnnotationKey: providerClusterPath,
			},
		},
		Spec: topologyv1alpha1.PartitionSpec{
			Selector: &metav1.LabelSelector{
				MatchLabels: map[string]string{
					"region": "internal",
				},
			},
		},
	}
}

func buildAPIExportEndpointSliceTyped() *apisv1alpha1.APIExportEndpointSlice {
	return &apisv1alpha1.APIExportEndpointSlice{
		TypeMeta: metav1.TypeMeta{
			APIVersion: apisv1alpha1.SchemeGroupVersion.String(),
			Kind:       "APIExportEndpointSlice",
		},
		ObjectMeta: metav1.ObjectMeta{
			Name: apiExportName,
			Annotations: map[string]string{
				logicalcluster.AnnotationKey: providerClusterPath,
			},
		},
		Spec: apisv1alpha1.APIExportEndpointSliceSpec{
			APIExport: apisv1alpha1.ExportBindingReference{
				Path: providerClusterPath,
				Name: apiExportName,
			},
			Partition: partitionName,
		},
	}
}

func buildConsumerAPIBindingName() string {
	return generateAPIBindingName(logicalcluster.Name(consumerClusterPath), providerClusterPath, apiExportName)
}

func generateAPIBindingName(clusterName logicalcluster.Name, exportPath, exportName string) string {
	maxLen := len(exportName)
	if maxLen > maxExportNamePrefixLength {
		maxLen = maxExportNamePrefixLength
	}
	exportNamePrefix := exportName[:maxLen]
	hash := toBase36Sha224(clusterName.String() + "|" + exportPath + "|" + exportName)
	hash = strings.ToLower(hash[:5])
	return fmt.Sprintf("%s-%s", exportNamePrefix, hash)
}

func toBase36Sha224(input string) string {
	sum := sha256.Sum224([]byte(input))
	var i big.Int
	i.SetBytes(sum[:])
	return i.Text(36)
}

// Helpers

func mustAddToScheme(scheme *runtime.Scheme) {
	must(kcpscheme.AddToScheme(scheme))
	must(corev1.AddToScheme(scheme))
	must(corev1alpha1.AddToScheme(scheme))
	must(tenancyv1alpha1.AddToScheme(scheme))
	must(apisv1alpha1.AddToScheme(scheme))
	must(apisv1alpha2.AddToScheme(scheme))
	must(topologyv1alpha1.AddToScheme(scheme))
}

func mustToTraceObject(obj client.Object, cluster string) *unstructured.Unstructured {
	raw, err := runtime.DefaultUnstructuredConverter.ToUnstructured(obj)
	if err != nil {
		panic(err)
	}
	u := &unstructured.Unstructured{Object: raw}
	u.SetGroupVersionKind(obj.GetObjectKind().GroupVersionKind())
	if needsClusterRequestKey(u.GroupVersionKind()) {
		u.SetNamespace(cluster)
	}
	if u.GetAnnotations() == nil {
		u.SetAnnotations(map[string]string{})
	}
	u.GetAnnotations()[traceClusterNSKey] = cluster
	return u
}

func traceClusterFor(obj client.Object) string {
	if obj == nil {
		return ""
	}
	if anns := obj.GetAnnotations(); anns != nil {
		if anns[traceClusterNSKey] != "" {
			return anns[traceClusterNSKey]
		}
		if anns[logicalcluster.AnnotationKey] != "" {
			return anns[logicalcluster.AnnotationKey]
		}
	}
	if needsClusterRequestKey(obj.GetObjectKind().GroupVersionKind()) && obj.GetNamespace() != "" {
		return obj.GetNamespace()
	}
	return ""
}

func needsClusterRequestKey(gvk schema.GroupVersionKind) bool {
	switch gvk.Group {
	case tenancyv1alpha1.SchemeGroupVersion.Group,
		core.GroupName,
		apisv1alpha1.SchemeGroupVersion.Group,
		apisv1alpha2.SchemeGroupVersion.Group,
		topologyv1alpha1.SchemeGroupVersion.Group:
		return true
	default:
		return false
	}
}

func must(err error) {
	if err != nil {
		panic(err)
	}
}
