package main

import (
	"github.com/crossplane/crossplane-runtime/v2/pkg/event"
	"github.com/crossplane/crossplane-runtime/v2/pkg/logging"
	"github.com/crossplane/crossplane-runtime/v2/pkg/resource/fake"
	ucomposite "github.com/crossplane/crossplane-runtime/v2/pkg/resource/unstructured/composite"
	"github.com/crossplane/crossplane/v2/apis/apiextensions/v1"
	"github.com/crossplane/crossplane/v2/internal/controller/apiextensions/composite"
	"github.com/crossplane/crossplane/v2/internal/controller/apiextensions/composition"

	"github.com/tgoodwin/kamera/pkg/tag"
	"github.com/tgoodwin/kamera/pkg/tracecheck"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

const (
	xrKind          = "XWidget"
	xrAPIVersion    = "example.org/v1"
	compositionName = "widget-composition"
)

func newCrossplaneExplorerBuilder() *tracecheck.ExplorerBuilder {
	builder := tracecheck.NewExplorerBuilder(newScheme())

	builder.WithReconciler("CompositionReconciler", func(c client.Client) tracecheck.Reconciler {
		mgr := &fake.Manager{Client: c}
		return composition.NewReconciler(mgr, composition.WithLogger(logging.NewNopLogger()))
	}).For("apiextensions.crossplane.io/Composition")

	builder.WithReconciler("CompositeReconciler", func(c client.Client) tracecheck.Reconciler {
		runner := stubFunctionRunner{}
		composer := composite.NewFunctionComposer(c, c, runner)
		return composite.NewReconciler(
			c,
			schema.GroupVersionKind{Group: "example.org", Version: "v1", Kind: xrKind},
			composite.WithComposer(composer),
			composite.WithCompositeSchema(ucomposite.SchemaLegacy),
			composite.WithLogger(logging.NewNopLogger()),
			composite.WithRecorder(event.NewNopRecorder()),
		)
	}).For(xrAPIVersion + "/" + xrKind)

	return builder
}

func buildInitialCrossplaneState(builder *tracecheck.ExplorerBuilder) tracecheck.StateNode {
	stateBuilder := builder.NewStateEventBuilder()
	composition := buildComposition()
	tag.AddSleeveObjectID(composition)
	compositionState := stateBuilder.AddTopLevelObject(composition, "CompositionReconciler")

	xr := buildCompositeResource()
	tag.AddSleeveObjectID(xr)
	xrState := stateBuilder.AddTopLevelObject(xr, "CompositeReconciler")

	return tracecheck.MergeStateNodes(compositionState, xrState)
}

func buildComposition() *v1.Composition {
	return &v1.Composition{
		ObjectMeta: metav1.ObjectMeta{
			Name: compositionName,
		},
		Spec: v1.CompositionSpec{
			CompositeTypeRef: v1.TypeReference{
				APIVersion: xrAPIVersion,
				Kind:       xrKind,
			},
			Mode: v1.CompositionModePipeline,
			Pipeline: []v1.PipelineStep{
				{
					Step: "pipeline",
					FunctionRef: v1.FunctionReference{
						Name: stubFunctionName,
					},
				},
			},
		},
	}
}

func buildCompositeResource() *unstructured.Unstructured {
	xr := &unstructured.Unstructured{}
	xr.SetAPIVersion(xrAPIVersion)
	xr.SetKind(xrKind)
	xr.SetName("example")
	xr.SetNamespace("default")
	xr.Object["spec"] = map[string]any{
		"compositionRef": map[string]any{
			"name": compositionName,
		},
	}
	return xr
}
