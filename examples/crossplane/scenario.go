package main

import (
	"github.com/crossplane/crossplane-runtime/v2/pkg/event"
	"github.com/crossplane/crossplane-runtime/v2/pkg/logging"
	"github.com/crossplane/crossplane-runtime/v2/pkg/resource/fake"
	ucomposite "github.com/crossplane/crossplane-runtime/v2/pkg/resource/unstructured/composite"
	"github.com/crossplane/crossplane/v2/apis/apiextensions/v1"
	pkgmetav1 "github.com/crossplane/crossplane/v2/apis/pkg/meta/v1"
	pkgv1 "github.com/crossplane/crossplane/v2/apis/pkg/v1"
	"github.com/crossplane/crossplane/v2/internal/controller/apiextensions/composite"
	"github.com/crossplane/crossplane/v2/internal/controller/apiextensions/composition"
	"github.com/crossplane/crossplane/v2/internal/controller/apiextensions/revision"

	"github.com/tgoodwin/kamera/pkg/tag"
	"github.com/tgoodwin/kamera/pkg/tracecheck"
	sleevelog "github.com/tgoodwin/kamera/pkg/util/logger"
	apimeta "k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
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
	log := logging.NewLogrLogger(sleevelog.GetLogger(sleevelog.Debug))
	recorder := newLogRecorder(log)

	builder.WithReconciler("CompositionReconciler", func(c client.Client) tracecheck.Reconciler {
		mgr := &fake.Manager{Client: c}
		return composition.NewReconciler(
			mgr,
			composition.WithLogger(log),
			composition.WithRecorder(recorder),
		)
	}).For("apiextensions.crossplane.io/Composition")

	builder.WithReconciler("CompositionRevisionReconciler", func(c client.Client) tracecheck.Reconciler {
		mgr := &fake.Manager{Client: c}
		return revision.NewReconciler(
			mgr,
			revision.WithLogger(log),
			revision.WithRecorder(recorder),
		)
	}).For("apiextensions.crossplane.io/CompositionRevision")

	builder.WithReconciler("CompositeReconciler", func(c client.Client) tracecheck.Reconciler {
		runner := stubFunctionRunner{}
		composer := composite.NewFunctionComposer(c, c, runner)
		return composite.NewReconciler(
			c,
			schema.GroupVersionKind{Group: "example.org", Version: "v1", Kind: xrKind},
			composite.WithComposer(composer),
			composite.WithCompositeSchema(ucomposite.SchemaLegacy),
			composite.WithLogger(log),
			composite.WithRecorder(recorder),
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

	functionRevision := buildFunctionRevision()
	tag.AddSleeveObjectID(functionRevision)
	functionState := stateBuilder.AddTopLevelObject(functionRevision)

	initialState := tracecheck.MergeStateNodes(compositionState, xrState)
	return tracecheck.MergeStateNodes(initialState, functionState)
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

func buildFunctionRevision() *pkgv1.FunctionRevision {
	return &pkgv1.FunctionRevision{
		ObjectMeta: metav1.ObjectMeta{
			Name: stubFunctionName + "-rev",
			Labels: map[string]string{
				pkgv1.LabelParentPackage: stubFunctionName,
			},
		},
		Spec: pkgv1.FunctionRevisionSpec{
			PackageRevisionSpec: pkgv1.PackageRevisionSpec{
				DesiredState: pkgv1.PackageRevisionActive,
			},
		},
		Status: pkgv1.FunctionRevisionStatus{
			PackageRevisionStatus: pkgv1.PackageRevisionStatus{
				Capabilities: []string{pkgmetav1.FunctionCapabilityComposition},
			},
		},
	}
}

type logRecorder struct {
	log         logging.Logger
	annotations map[string]string
}

func newLogRecorder(log logging.Logger) event.Recorder {
	return logRecorder{log: log, annotations: map[string]string{}}
}

func (r logRecorder) Event(obj runtime.Object, e event.Event) {
	fields := []any{
		"type", string(e.Type),
		"reason", string(e.Reason),
		"message", e.Message,
	}

	if obj != nil {
		if accessor, err := apimeta.Accessor(obj); err == nil {
			fields = append(fields, "name", accessor.GetName())
			if ns := accessor.GetNamespace(); ns != "" {
				fields = append(fields, "namespace", ns)
			}
		}

		gvk := obj.GetObjectKind().GroupVersionKind()
		if gvk.Kind != "" {
			fields = append(fields, "apiVersion", gvk.GroupVersion().String(), "kind", gvk.Kind)
		}
	}

	annotations := mergeAnnotations(r.annotations, e.Annotations)
	if len(annotations) > 0 {
		fields = append(fields, "annotations", annotations)
	}

	r.log.Info("Crossplane event", fields...)
}

func (r logRecorder) WithAnnotations(keysAndValues ...string) event.Recorder {
	next := logRecorder{
		log:         r.log,
		annotations: copyAnnotations(r.annotations),
	}
	addAnnotationPairs(next.annotations, keysAndValues)
	return next
}

func addAnnotationPairs(dst map[string]string, keysAndValues []string) {
	for i := 0; i+1 < len(keysAndValues); i += 2 {
		dst[keysAndValues[i]] = keysAndValues[i+1]
	}
}

func mergeAnnotations(base, extra map[string]string) map[string]string {
	if len(base) == 0 && len(extra) == 0 {
		return nil
	}

	out := copyAnnotations(base)
	for k, v := range extra {
		out[k] = v
	}
	return out
}

func copyAnnotations(in map[string]string) map[string]string {
	out := make(map[string]string, len(in))
	for k, v := range in {
		out[k] = v
	}
	return out
}
