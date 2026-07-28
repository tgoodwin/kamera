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
)

const (
	resourceGraphDefinitionControllerID tracecheck.ReconcilerID = "ResourceGraphDefinitionController"
	applicationControllerID             tracecheck.ReconcilerID = "ApplicationController"

	kroDomainName               = "kro.run"
	applicationAPIVersion       = "kro.run/v1alpha1"
	applicationKind             = "Application"
	resourceGraphDefinitionKind = "ResourceGraphDefinition"
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
	builder.WithMaxDepth(60)

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
	return explore.CompileInputScenarios(builder, inputs, explore.ScenarioCompileOptions{
		BuildState: buildStateFromCoverageInput,
	})
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

	// If no environment objects, use the default RGD state.
	// This avoids requiring the full RGD with resources in JSON inputs.
	if len(objects) == 0 {
		rgd := buildQuickstartApplicationRGD()
		return buildInitialKROState(builder), []client.Object{rgd}, nil
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

func defaultNamespace(namespace string) string {
	if strings.TrimSpace(namespace) == "" {
		return defaultApplicationNamespace
	}
	return namespace
}
