package main

import (
	"fmt"
	"strings"

	rmq "github.com/tgoodwin/kamera/examples/rabbitmq-operator/rmq"
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
	rmqReconcilerID tracecheck.ReconcilerID = "RabbitmqClusterReconciler"

	rmqGroup = "rabbitmq.com"
	rmqKind  = "RabbitmqCluster"

	defaultNamespace = "default"
	defaultRmqName   = "rabbitmq-cluster"
)

func newScheme() *runtime.Scheme {
	sch := runtime.NewScheme()
	utilruntime.Must(clientgoscheme.AddToScheme(sch))
	utilruntime.Must(rmq.AddToScheme(sch))
	return sch
}

func newRabbitmqExplorerBuilder() *tracecheck.ExplorerBuilder {
	builder := tracecheck.NewExplorerBuilder(newScheme())
	builder.WithMaxDepth(200)

	// Wire RabbitmqCluster reconciler
	builder.WithReconciler(rmqReconcilerID, func(c client.Client) tracecheck.Reconciler {
		return &RabbitmqClusterReconciler{
			Client:   c,
			Scheme:   newScheme(),
			Recorder: &noopEventRecorder{},
		}
	}).For(rmqGroup + "/" + rmqKind)

	// Resource dependencies
	builder.WithResourceDep(rmqGroup+"/"+rmqKind, rmqReconcilerID)

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
	rmqCluster := buildRabbitmqCluster(defaultRmqName, defaultNamespace, 1, "10Gi")
	return stateBuilder.AddTopLevelObject(rmqCluster, rmqReconcilerID)
}

func defaultUserActions() []tracecheck.UserAction {
	return []tracecheck.UserAction{
		{
			ID:      "delete-rmq",
			OpType:  event.MARK_FOR_DELETION,
			Payload: buildRabbitmqCluster(defaultRmqName, defaultNamespace, 1, "10Gi"),
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
	case gvk.Group == rmqGroup && gvk.Kind == rmqKind:
		return builder.AddTopLevelObject(obj, rmqReconcilerID)
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

func buildRabbitmqCluster(name, namespace string, replicas int, storage string) *unstructured.Unstructured {
	return &unstructured.Unstructured{
		Object: map[string]any{
			"apiVersion": rmqGroup + "/v1beta1",
			"kind":       rmqKind,
			"metadata": map[string]any{
				"name":      name,
				"namespace": namespace,
			},
			"spec": map[string]any{
				"replicas": int64(replicas),
				"image":    "rabbitmq:3.8.12-management",
				"persistence": map[string]any{
					"storage": storage,
				},
			},
		},
	}
}

var _ reconcile.Request
