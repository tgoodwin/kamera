package main

import (
	"fmt"
	"strings"

	zookeeperv1beta1 "github.com/tgoodwin/kamera/examples/zookeeper-operator/zk"
	"github.com/tgoodwin/kamera/pkg/coverage"
	"github.com/tgoodwin/kamera/pkg/event"
	"github.com/tgoodwin/kamera/pkg/explore"
	"github.com/tgoodwin/kamera/pkg/tracecheck"
	"github.com/tgoodwin/kamera/sleevectrl/pkg/controller"

	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"
)

const (
	zkReconcilerID tracecheck.ReconcilerID = "ZookeeperClusterReconciler"

	zkGroup     = "zookeeper.pravega.io"
	zkKind      = "ZookeeperCluster"

	defaultNamespace = "default"
	defaultZkName    = "zookeeper-cluster"
)

func newScheme() *runtime.Scheme {
	sch := runtime.NewScheme()
	utilruntime.Must(clientgoscheme.AddToScheme(sch))
	utilruntime.Must(zookeeperv1beta1.AddToScheme(sch))
	return sch
}

func newZookeeperExplorerBuilder() *tracecheck.ExplorerBuilder {
	builder := tracecheck.NewExplorerBuilder(newScheme())
	builder.WithMaxDepth(100)

	// Wire ZookeeperCluster reconciler
	builder.WithReconciler(zkReconcilerID, func(c client.Client) tracecheck.Reconciler {
		return &ZookeeperClusterReconciler{
			Client:   c,
			Scheme:   newScheme(),
			ZkClient: &noopZkClient{},
		}
	}).For(zkGroup + "/" + zkKind)

	// Resource dependencies
	builder.WithResourceDep(zkGroup+"/"+zkKind, zkReconcilerID)

	// StatefulSet controller — creates Pods from StatefulSet spec
	builder.WithReconciler("StatefulSetController", func(c client.Client) tracecheck.Reconciler {
		return &controller.StatefulSetReconciler{
			Client: c,
			Scheme: newScheme(),
		}
	}).For("apps/StatefulSet")
	builder.WithResourceDepGK(schema.GroupKind{Group: "apps", Kind: "StatefulSet"}, "StatefulSetController")
	builder.WithResourceDepGK(schema.GroupKind{Group: "", Kind: "Pod"}, "StatefulSetController")
	builder.WithResourceDepGK(schema.GroupKind{Group: "", Kind: "PersistentVolumeClaim"}, "StatefulSetController")

	// PVC controller — binds PVCs to PVs
	builder.WithReconciler("PVCController", func(c client.Client) tracecheck.Reconciler {
		return &controller.PersistentVolumeClaimReconciler{
			Client: c,
			Scheme: newScheme(),
		}
	}).For("PersistentVolumeClaim")
	builder.WithResourceDepGK(schema.GroupKind{Group: "", Kind: "PersistentVolumeClaim"}, "PVCController")
	builder.WithResourceDepGK(schema.GroupKind{Group: "", Kind: "PersistentVolume"}, "PVCController")

	return builder
}

func buildDefaultState(builder *tracecheck.ExplorerBuilder) tracecheck.StateNode {
	stateBuilder := builder.NewStateEventBuilder()
	zkCluster := buildZookeeperCluster(defaultZkName, defaultNamespace, 1)
	return stateBuilder.AddTopLevelObject(zkCluster, zkReconcilerID)
}

func defaultUserActions() []tracecheck.UserAction {
	return []tracecheck.UserAction{
		{
			ID:      "delete-zk",
			OpType:  event.MARK_FOR_DELETION,
			Payload: buildZookeeperCluster(defaultZkName, defaultNamespace, 1),
		},
	}
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
		state, seededObjects, err := buildStateFromInput(builder, input)
		if err != nil {
			return nil, fmt.Errorf("build start state for input %d (%s): %w", idx, input.Name, err)
		}
		userInputs, err := buildUserActionsFromInput(input, seededObjects)
		if err != nil {
			return nil, fmt.Errorf("build user actions for input %d (%s): %w", idx, input.Name, err)
		}

		cfg, err := explore.ApplyInputTuning(baseCfg, input.Tuning)
		if err != nil {
			return nil, fmt.Errorf("apply tuning for input %d (%s): %w", idx, input.Name, err)
		}
		scenarios = append(scenarios, explore.Scenario{
			Name:             input.Name,
			EnvironmentState: state,
			ExternalInputs:   userInputs,
			Config:           cfg,
		})
	}
	return scenarios, nil
}

func buildStateFromInput(builder *tracecheck.ExplorerBuilder, input coverage.Input) (tracecheck.StateNode, []client.Object, error) {
	objects := make([]client.Object, 0, len(input.EnvironmentState.Objects))
	for idx, obj := range input.EnvironmentState.Objects {
		if obj == nil {
			return tracecheck.StateNode{}, nil, fmt.Errorf("input environment object %d is nil", idx)
		}
		objects = append(objects, obj.DeepCopy())
	}

	// If no environment objects, return an empty state (scenarios provide objects via externalInputs).
	if len(objects) == 0 {
		return tracecheck.StateNode{}, nil, nil
	}

	stateBuilder := builder.NewStateEventBuilder()
	state := addSeedObject(stateBuilder, objects[0])
	for _, obj := range objects[1:] {
		state = tracecheck.MergeStateNodes(state, addSeedObject(stateBuilder, obj))
	}
	return state, objects, nil
}

func addSeedObject(builder *tracecheck.StateEventBuilder, obj client.Object) tracecheck.StateNode {
	gvk := obj.GetObjectKind().GroupVersionKind()
	switch {
	case gvk.Group == zkGroup && gvk.Kind == zkKind:
		return builder.AddTopLevelObject(obj, zkReconcilerID)
	default:
		return builder.AddTopLevelObject(obj)
	}
}

func buildUserActionsFromInput(input coverage.Input, seededObjects []client.Object) ([]tracecheck.UserAction, error) {
	actions := make([]tracecheck.UserAction, 0, len(input.ExternalInputs))
	for idx, action := range input.ExternalInputs {
		if action.Object == nil {
			return nil, fmt.Errorf("input user input %d has nil object", idx)
		}

		id := strings.TrimSpace(action.ID)
		if id == "" {
			id = fmt.Sprintf("user-input-%d", idx)
		}

		actions = append(actions, tracecheck.UserAction{
			ID:      id,
			OpType:  action.OpType,
			Payload: action.Object.DeepCopy(),
		})
	}
	return actions, nil
}

func isObjectSeeded(object client.Object, seededObjects []client.Object) bool {
	if object == nil {
		return false
	}
	aGVK := object.GetObjectKind().GroupVersionKind()
	for _, seeded := range seededObjects {
		bGVK := seeded.GetObjectKind().GroupVersionKind()
		if aGVK.Group == bGVK.Group && aGVK.Kind == bGVK.Kind &&
			object.GetNamespace() == seeded.GetNamespace() &&
			object.GetName() == seeded.GetName() {
			return true
		}
	}
	return false
}

// buildZookeeperCluster creates an unstructured ZookeeperCluster object.
func buildZookeeperCluster(name, namespace string, replicas int) *unstructured.Unstructured {
	return &unstructured.Unstructured{
		Object: map[string]any{
			"apiVersion": zkGroup + "/v1beta1",
			"kind":       zkKind,
			"metadata": map[string]any{
				"name":      name,
				"namespace": namespace,
			},
			"spec": map[string]any{
				"replicas": int64(replicas),
				"persistence": map[string]any{
					"reclaimPolicy": "Delete",
				},
			},
		},
	}
}

// Unused but kept for reference: the reconcile.Request type used in watches.
var _ reconcile.Request
