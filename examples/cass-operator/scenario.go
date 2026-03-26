package main

import (
	"fmt"
	"strings"

	api "github.com/tgoodwin/kamera/examples/cass-operator/cassandra/v1beta1"
	"github.com/tgoodwin/kamera/examples/cass-operator/dynamicwatch"
	"github.com/tgoodwin/kamera/examples/cass-operator/reconciliation"
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
)

const (
	cassReconcilerID tracecheck.ReconcilerID = "CassandraDatacenterReconciler"

	cassGroup = "cassandra.datastax.com"
	cassKind  = "CassandraDatacenter"

	defaultNamespace = "default"
	defaultDCName    = "cassandra-datacenter"
)

func newScheme() *runtime.Scheme {
	sch := runtime.NewScheme()
	utilruntime.Must(clientgoscheme.AddToScheme(sch))
	utilruntime.Must(api.AddToScheme(sch))
	return sch
}

func newCassExplorerBuilder() *tracecheck.ExplorerBuilder {
	builder := tracecheck.NewExplorerBuilder(newScheme())
	builder.WithMaxDepth(200)

	// Wire CassandraDatacenter reconciler (real code from cass-operator)
	builder.WithReconciler(cassReconcilerID, func(c client.Client) tracecheck.Reconciler {
		return &reconciliation.ReconcileCassandraDatacenter{
			Client:        c,
			Scheme:        newScheme(),
			Recorder:      &noopEventRecorder{},
			SecretWatches: &dynamicwatch.NoopDynamicWatches{},
		}
	}).For(cassGroup + "/" + cassKind)

	builder.WithResourceDep(cassGroup+"/"+cassKind, cassReconcilerID)

	// StatefulSet controller
	builder.WithReconciler("StatefulSetController", func(c client.Client) tracecheck.Reconciler {
		return &controller.StatefulSetReconciler{
			Client: c,
			Scheme: newScheme(),
		}
	}).For("apps/StatefulSet")
	builder.WithResourceDepGK(schema.GroupKind{Group: "apps", Kind: "StatefulSet"}, "StatefulSetController")
	builder.WithResourceDepGK(schema.GroupKind{Group: "", Kind: "Pod"}, "StatefulSetController")
	builder.WithResourceDepGK(schema.GroupKind{Group: "", Kind: "PersistentVolumeClaim"}, "StatefulSetController")

	// PVC controller
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
	dc := buildCassandraDatacenter(defaultDCName, defaultNamespace, 1)
	return stateBuilder.AddTopLevelObject(dc, cassReconcilerID)
}

func defaultUserActions() []tracecheck.UserAction {
	return []tracecheck.UserAction{
		{
			ID:      "delete-dc",
			OpType:  event.MARK_FOR_DELETION,
			Payload: buildCassandraDatacenter(defaultDCName, defaultNamespace, 1),
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
	case gvk.Group == cassGroup && gvk.Kind == cassKind:
		return builder.AddTopLevelObject(obj, cassReconcilerID)
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

func buildCassandraDatacenter(name, namespace string, size int) *unstructured.Unstructured {
	storageClass := "server-storage"
	return &unstructured.Unstructured{
		Object: map[string]any{
			"apiVersion": cassGroup + "/v1beta1",
			"kind":       cassKind,
			"metadata": map[string]any{
				"name":      name,
				"namespace": namespace,
			},
			"spec": map[string]any{
				"clusterName": "cluster1",
				"serverType":  "cassandra",
				"serverVersion": "3.11.7",
				"managementApiAuth": map[string]any{
					"insecure": map[string]any{},
				},
				"size": int64(size),
				"storageConfig": map[string]any{
					"cassandraDataVolumeClaimSpec": map[string]any{
						"storageClassName": storageClass,
						"accessModes":      []any{"ReadWriteOnce"},
						"resources": map[string]any{
							"requests": map[string]any{
								"storage": "3Gi",
							},
						},
					},
				},
				"config": map[string]any{
					"cassandra-yaml": map[string]any{
						"authenticator": "org.apache.cassandra.auth.PasswordAuthenticator",
						"authorizer":    "org.apache.cassandra.auth.CassandraAuthorizer",
						"role_manager":  "org.apache.cassandra.auth.CassandraRoleManager",
					},
					"jvm-options": map[string]any{
						"initial_heap_size": "800M",
						"max_heap_size":     "800M",
					},
				},
			},
		},
	}
}
