package main

import (
	"fmt"
	"strings"

	"github.com/tgoodwin/kamera/pkg/coverage"
	"github.com/tgoodwin/kamera/pkg/event"
	"github.com/tgoodwin/kamera/pkg/explore"
	"github.com/tgoodwin/kamera/pkg/tracecheck"
	networkingv1 "k8s.io/api/networking/v1"
	extv1 "k8s.io/apiextensions-apiserver/pkg/apis/apiextensions/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	v1alpha1 "github.com/kubernetes-sigs/kro/api/v1alpha1"
	"github.com/kubernetes-sigs/kro/pkg/graph"
	"github.com/kubernetes-sigs/kro/pkg/testutil/generator"
)

const (
	resourceGraphDefinitionControllerID tracecheck.ReconcilerID = "ResourceGraphDefinitionController"
	applicationControllerID             tracecheck.ReconcilerID = "ApplicationController"

	kroDomainName               = "kro.run"
	applicationAPIVersion       = "kro.run/v1alpha1"
	applicationKind             = "Application"
	resourceGraphDefinitionKind = "ResourceGraphDefinition"
	resourceGraphDefinitionName = "my-application"
	applicationCRDName          = "applications.kro.run"
	defaultApplicationName      = "my-app"
	defaultApplicationInstance  = "my-app-instance"
	defaultApplicationNamespace = "default"
	instanceNameLabelKey        = "kro.run/instance-name"
	instanceNamespaceLabelKey   = "kro.run/instance-namespace"
)

func newKROExplorerBuilder() *tracecheck.ExplorerBuilder {
	sch := runtime.NewScheme()
	utilruntime.Must(extv1.AddToScheme(sch))
	utilruntime.Must(networkingv1.AddToScheme(sch))
	utilruntime.Must(v1alpha1.AddToScheme(sch))

	builder := tracecheck.NewExplorerBuilder(sch)
	builder.WithMaxDepth(30)

	applicationGVR := schema.GroupVersionResource{
		Group: "kro.run", Version: "v1alpha1", Resource: "applications",
	}

	// Wire real RGD controller
	builder.WithReconciler(resourceGraphDefinitionControllerID, func(c client.Client) tracecheck.Reconciler {
		return newRGDReconciler(c)
	}).For(kroDomainName + "/" + resourceGraphDefinitionKind)

	// Wire real Instance (Application) controller
	builder.WithReconciler(applicationControllerID, func(c client.Client) tracecheck.Reconciler {
		log := ctrl.Log.WithName("application-controller")
		appGraph := mustBuildGraph(buildQuickstartApplicationRGDTyped())
		return newInstanceController(c, log, applicationGVR, appGraph)
	}).For(kroDomainName+"/"+applicationKind).
		Watches("apps/Deployment", enqueueApplicationFromManagedResource).
		Watches("Service", enqueueApplicationFromManagedResource).
		Watches("networking.k8s.io/Ingress", enqueueApplicationFromManagedResource)

	builder.WithResourceDep(kroDomainName+"/"+resourceGraphDefinitionKind, resourceGraphDefinitionControllerID)
	builder.WithResourceDep(kroDomainName+"/"+applicationKind, applicationControllerID)

	return builder
}

// mustBuildGraph builds a graph.Graph from a typed RGD
// using the core schema resolver (no API server needed).
func mustBuildGraph(rgd *v1alpha1.ResourceGraphDefinition) *graph.Graph {
	mapper := staticRESTMapper()
	graphBuilder := graph.NewBuilderFromResolver(nil, mapper)

	g, err := graphBuilder.NewResourceGraphDefinition(rgd)
	if err != nil {
		panic(fmt.Sprintf("build graph from RGD: %v", err))
	}
	return g
}

func buildInitialKROState(builder *tracecheck.ExplorerBuilder) tracecheck.StateNode {
	stateBuilder := builder.NewStateEventBuilder()
	rgd := buildQuickstartApplicationRGD()
	return stateBuilder.AddTopLevelObject(rgd, resourceGraphDefinitionControllerID)
}

func defaultInteractiveUserActions() []tracecheck.UserAction {
	return []tracecheck.UserAction{
		{
			ID:      "create-application-instance",
			OpType:  event.CREATE,
			Payload: buildQuickstartApplicationInstance(),
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
		state, seededObjects, err := buildStateFromCoverageInput(builder, input)
		if err != nil {
			return nil, fmt.Errorf("build start state for input %d (%s): %w", idx, input.Name, err)
		}
		userInputs, err := buildUserActionsFromCoverageInput(input, seededObjects)
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
			if action.OpType != event.CREATE || action.Object == nil {
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
	gvk := obj.GetObjectKind().GroupVersionKind()
	switch {
	case gvk.Group == kroDomainName && gvk.Kind == resourceGraphDefinitionKind:
		return builder.AddTopLevelObject(obj, resourceGraphDefinitionControllerID)
	case gvk.Group == kroDomainName && gvk.Kind == applicationKind:
		return builder.AddTopLevelObject(obj, applicationControllerID)
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
		if opType == event.CREATE && isInputObjectSeeded(action.Object, seededObjects) {
			opType = event.UPDATE
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

func enqueueApplicationFromManagedResource(obj *unstructured.Unstructured) []reconcile.Request {
	if obj == nil {
		return nil
	}
	labels := obj.GetLabels()
	instanceName := strings.TrimSpace(labels[instanceNameLabelKey])
	if instanceName == "" {
		return nil
	}
	namespace := labels[instanceNamespaceLabelKey]
	if namespace == "" {
		namespace = obj.GetNamespace()
	}
	return []reconcile.Request{{
		NamespacedName: client.ObjectKey{Namespace: namespace, Name: instanceName},
	}}
}

// buildQuickstartApplicationRGDTyped returns the typed RGD used for graph building.
func buildQuickstartApplicationRGDTyped() *v1alpha1.ResourceGraphDefinition {
	return generator.NewResourceGraphDefinition(resourceGraphDefinitionName,
		generator.WithSchema(
			applicationKind, "v1alpha1",
			map[string]interface{}{
				"name":     "string",
				"image":    `string | default="nginx"`,
				"replicas": "integer | default=3",
				"ingress": map[string]interface{}{
					"enabled": "boolean | default=false",
				},
			},
			nil, // no status schema — avoids needing OpenAPI schemas for CEL type inference
		),
		generator.WithResource("deployment", map[string]interface{}{
			"apiVersion": "apps/v1",
			"kind":       "Deployment",
			"metadata": map[string]interface{}{
				"name": "${schema.spec.name}",
			},
			"spec": map[string]interface{}{
				"replicas": "${schema.spec.replicas}",
				"selector": map[string]interface{}{
					"matchLabels": map[string]interface{}{
						"app": "${schema.spec.name}",
					},
				},
				"template": map[string]interface{}{
					"metadata": map[string]interface{}{
						"labels": map[string]interface{}{
							"app": "${schema.spec.name}",
						},
					},
					"spec": map[string]interface{}{
						"containers": []interface{}{
							map[string]interface{}{
								"name":  "${schema.spec.name}",
								"image": "${schema.spec.image}",
								"ports": []interface{}{
									map[string]interface{}{
										"containerPort": int64(80),
									},
								},
							},
						},
					},
				},
			},
		}, nil, nil),
		generator.WithResource("service", map[string]interface{}{
			"apiVersion": "v1",
			"kind":       "Service",
			"metadata": map[string]interface{}{
				"name": "${schema.spec.name}-svc",
			},
			"spec": map[string]interface{}{
				"selector": map[string]interface{}{
					"app": "${schema.spec.name}",
				},
				"ports": []interface{}{
					map[string]interface{}{
						"protocol":   "TCP",
						"port":       int64(80),
						"targetPort": int64(80),
					},
				},
			},
		}, nil, nil),
		generator.WithResource("ingress", map[string]interface{}{
			"apiVersion": "networking.k8s.io/v1",
			"kind":       "Ingress",
			"metadata": map[string]interface{}{
				"name": "${schema.spec.name}-ingress",
			},
			"spec": map[string]interface{}{
				"rules": []interface{}{
					map[string]interface{}{
						"http": map[string]interface{}{
							"paths": []interface{}{
								map[string]interface{}{
									"path":     "/",
									"pathType": "Prefix",
									"backend": map[string]interface{}{
										"service": map[string]interface{}{
											"name": "${schema.spec.name}-svc",
											"port": map[string]interface{}{
												"number": int64(80),
											},
										},
									},
								},
							},
						},
					},
				},
			},
		}, nil, []string{`${schema.spec.ingress.enabled}`}),
	)
}

// buildQuickstartApplicationRGD returns the RGD as unstructured for seeding state.
func buildQuickstartApplicationRGD() *unstructured.Unstructured {
	rgd := buildQuickstartApplicationRGDTyped()
	obj, err := runtime.DefaultUnstructuredConverter.ToUnstructured(rgd)
	if err != nil {
		panic(fmt.Sprintf("convert typed RGD to unstructured: %v", err))
	}
	u := &unstructured.Unstructured{Object: obj}
	u.SetAPIVersion(applicationAPIVersion)
	u.SetKind(resourceGraphDefinitionKind)
	return u
}

func buildQuickstartApplicationInstance() *unstructured.Unstructured {
	return &unstructured.Unstructured{
		Object: map[string]any{
			"apiVersion": applicationAPIVersion,
			"kind":       applicationKind,
			"metadata": map[string]any{
				"name":      defaultApplicationInstance,
				"namespace": defaultApplicationNamespace,
			},
			"spec": map[string]any{
				"name":     defaultApplicationName,
				"replicas": int64(1),
				"ingress": map[string]any{
					"enabled": true,
				},
			},
		},
	}
}

func defaultNamespace(namespace string) string {
	if strings.TrimSpace(namespace) == "" {
		return defaultApplicationNamespace
	}
	return namespace
}
