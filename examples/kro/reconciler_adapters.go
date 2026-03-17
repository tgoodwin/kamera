package main

import (
	"context"
	"errors"
	"time"

	"github.com/go-logr/logr"
	"k8s.io/apiextensions-apiserver/pkg/generated/openapi"
	"k8s.io/apimachinery/pkg/api/meta"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apiserver/pkg/cel/openapi/resolver"
	"k8s.io/client-go/kubernetes/scheme"
	ctrl "sigs.k8s.io/controller-runtime"
	ctrlclient "sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	v1alpha1 "github.com/kubernetes-sigs/kro/api/v1alpha1"
	"github.com/kubernetes-sigs/kro/pkg/controller/instance"
	"github.com/kubernetes-sigs/kro/pkg/controller/resourcegraphdefinition"
	"github.com/kubernetes-sigs/kro/pkg/dynamiccontroller"
	"github.com/kubernetes-sigs/kro/pkg/graph"
	"github.com/kubernetes-sigs/kro/pkg/metadata"
	"github.com/kubernetes-sigs/kro/pkg/requeue"
	"github.com/tgoodwin/kamera/pkg/tracecheck"
)

// --- Instance Controller ---

// adaptInstanceController wraps KRO's instance.Controller (returns error only)
// into a controller-runtime Reconciler (returns Result, error).
// Translates KRO's requeue error types into Result fields.
func adaptInstanceController(ic *instance.Controller) tracecheck.Reconciler {
	return reconcile.Func(func(ctx context.Context, req reconcile.Request) (reconcile.Result, error) {
		err := ic.Reconcile(ctx, ctrl.Request(req))
		if err == nil {
			return reconcile.Result{}, nil
		}

		var reqAfter *requeue.RequeueNeededAfter
		if errors.As(err, &reqAfter) {
			return reconcile.Result{RequeueAfter: reqAfter.Duration()}, nil
		}

		var reqNeeded *requeue.RequeueNeeded
		if errors.As(err, &reqNeeded) {
			return reconcile.Result{Requeue: true}, nil
		}

		var noReq *requeue.NoRequeue
		if errors.As(err, &noReq) {
			return reconcile.Result{}, nil
		}

		return reconcile.Result{}, err
	})
}

// newInstanceController creates a real KRO instance.Controller.
func newInstanceController(
	c ctrlclient.Client,
	log logr.Logger,
	gvr schema.GroupVersionResource,
	rgd *graph.Graph,
) tracecheck.Reconciler {
	mapper := staticRESTMapper()
	clientSet := newReplayClientSet(c, mapper)
	labeler := metadata.NewKROMetaLabeler()

	ic := instance.NewController(
		log,
		instance.ReconcileConfig{
			DefaultRequeueDuration:    3 * time.Second,
			DeletionGraceTimeDuration: 30 * time.Second,
			DeletionPolicy:            "Delete",
		},
		gvr,
		rgd,
		clientSet,
		labeler,
	)
	return adaptInstanceController(ic)
}

// --- RGD Controller ---

// stubDynamicControllerRegistrar is a no-op implementation.
// Kamera handles watch/enqueue natively.
type stubDynamicControllerRegistrar struct{}

var _ resourcegraphdefinition.DynamicControllerRegistrar = (*stubDynamicControllerRegistrar)(nil)

func (s *stubDynamicControllerRegistrar) Register(
	_ context.Context,
	_ schema.GroupVersionResource,
	_ dynamiccontroller.Handler,
	_ ...schema.GroupVersionResource,
) error {
	return nil
}

func (s *stubDynamicControllerRegistrar) Deregister(
	_ context.Context,
	_ schema.GroupVersionResource,
) error {
	return nil
}

// newRGDReconciler creates a real KRO ResourceGraphDefinitionReconciler.
func newRGDReconciler(c ctrlclient.Client) tracecheck.Reconciler {
	mapper := staticRESTMapper()
	clientSet := newReplayClientSet(c, mapper)

	// Core-only schema resolver — resolves Deployment, Service, Ingress
	// from compiled-in OpenAPI definitions. No network calls.
	coreResolver := resolver.NewDefinitionsSchemaResolver(
		openapi.GetOpenAPIDefinitions,
		scheme.Scheme,
	)
	graphBuilder := graph.NewBuilderFromResolver(coreResolver, mapper)

	reconciler := resourcegraphdefinition.NewResourceGraphDefinitionReconciler(
		clientSet,
		false, // allowCRDDeletion
		&stubDynamicControllerRegistrar{},
		graphBuilder,
		1, // maxConcurrentReconciles
	)
	reconciler.Client = c

	return reconcile.AsReconciler[*v1alpha1.ResourceGraphDefinition](c, reconciler)
}

// --- Static REST Mapper ---

func staticRESTMapper() meta.RESTMapper {
	mapper := meta.NewDefaultRESTMapper([]schema.GroupVersion{
		{Group: "", Version: "v1"},
		{Group: "apps", Version: "v1"},
		{Group: "networking.k8s.io", Version: "v1"},
		{Group: "kro.run", Version: "v1alpha1"},
		{Group: "apiextensions.k8s.io", Version: "v1"},
	})
	mapper.Add(schema.GroupVersionKind{Group: "", Version: "v1", Kind: "Service"}, meta.RESTScopeNamespace)
	mapper.Add(schema.GroupVersionKind{Group: "apps", Version: "v1", Kind: "Deployment"}, meta.RESTScopeNamespace)
	mapper.Add(schema.GroupVersionKind{Group: "networking.k8s.io", Version: "v1", Kind: "Ingress"}, meta.RESTScopeNamespace)
	mapper.Add(schema.GroupVersionKind{Group: "kro.run", Version: "v1alpha1", Kind: "Application"}, meta.RESTScopeNamespace)
	mapper.Add(schema.GroupVersionKind{Group: "kro.run", Version: "v1alpha1", Kind: "ResourceGraphDefinition"}, meta.RESTScopeRoot)
	mapper.Add(schema.GroupVersionKind{Group: "apiextensions.k8s.io", Version: "v1", Kind: "CustomResourceDefinition"}, meta.RESTScopeRoot)
	return mapper
}
