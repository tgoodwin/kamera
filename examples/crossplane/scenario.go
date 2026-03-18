package main

import (
	"context"
	"fmt"
	"strings"

	xpevent "github.com/crossplane/crossplane-runtime/v2/pkg/event"
	"github.com/crossplane/crossplane-runtime/v2/pkg/logging"
	"github.com/crossplane/crossplane-runtime/v2/pkg/resource/fake"
	ucomposite "github.com/crossplane/crossplane-runtime/v2/pkg/resource/unstructured/composite"
	"github.com/crossplane/crossplane/v2/apis/apiextensions/v1"
	pkgmetav1 "github.com/crossplane/crossplane/v2/apis/pkg/meta/v1"
	pkgv1 "github.com/crossplane/crossplane/v2/apis/pkg/v1"
	"github.com/crossplane/crossplane/v2/internal/controller/apiextensions/claim"
	"github.com/crossplane/crossplane/v2/internal/controller/apiextensions/composite"
	"github.com/crossplane/crossplane/v2/internal/controller/apiextensions/composition"
	"github.com/crossplane/crossplane/v2/internal/controller/apiextensions/revision"
	xpresource "github.com/crossplane/crossplane-runtime/v2/pkg/resource"

	"github.com/tgoodwin/kamera/pkg/coverage"
	"github.com/tgoodwin/kamera/pkg/event"
	"github.com/tgoodwin/kamera/pkg/explore"
	"github.com/tgoodwin/kamera/pkg/tag"
	"github.com/tgoodwin/kamera/pkg/tracecheck"
	sleevelog "github.com/tgoodwin/kamera/pkg/util/logger"
	apimeta "k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"
)

const (
	xrKind          = "XWidget"
	claimKind       = "WidgetClaim"
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
	}).For("apiextensions.crossplane.io/CompositionRevision").
		Watches("pkg.crossplane.io/FunctionRevision", functionRevisionToCompositionRevisionMapper())

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
	}).ForGK(schema.GroupKind{Group: "example.org", Kind: xrKind})

	claimGVK := schema.GroupVersionKind{Group: "example.org", Version: "v1", Kind: claimKind}
	xrGVK := schema.GroupVersionKind{Group: "example.org", Version: "v1", Kind: xrKind}

	builder.WithReconciler("ClaimReconciler", func(c client.Client) tracecheck.Reconciler {
		return claim.NewReconciler(
			c,
			claimGVK,
			xrGVK,
			claim.WithCompositeSyncer(claim.NewClientSideCompositeSyncer(c, deterministicNameGenerator{})),
			claim.WithConnectionPropagator(claim.ConnectionPropagatorFn(
				func(_ context.Context, _ claim.LocalConnectionSecretOwner, _ claim.ConnectionSecretOwner) (bool, error) {
					return false, nil // no-op: connection secret propagation not tested
				},
			)),
			claim.WithLogger(log),
			claim.WithRecorder(recorder),
		)
	}).ForGK(schema.GroupKind{Group: "example.org", Kind: claimKind}).
		Watches("example.org/XWidget", claimXRToClaimMapper())

	return builder
}

// deterministicNameGenerator generates a fixed name for XRs created by claims.
// The real Crossplane NameGenerator uses random suffixes, which would make every
// kamera trial produce a trivially-unique terminal state. This generator uses
// a deterministic suffix derived from the claim name.
type deterministicNameGenerator struct{}

func (deterministicNameGenerator) GenerateName(_ context.Context, cd xpresource.Object) error {
	if cd.GetGenerateName() != "" {
		cd.SetName(cd.GetGenerateName() + "xr")
		cd.SetGenerateName("")
	}
	return nil
}

// claimXRToClaimMapper creates a WatchMapper that enqueues the claim when an
// XR changes. In real Crossplane, this uses the XR's claimRef. For the harness,
// we use a naming convention: the claim name matches the XR's claimRef.name.
func claimXRToClaimMapper() tracecheck.WatchMapper {
	return func(obj *unstructured.Unstructured) []reconcile.Request {
		if obj == nil {
			return nil
		}
		// Read claimRef from XR spec
		spec, ok := obj.Object["spec"].(map[string]any)
		if !ok {
			return nil
		}
		claimRef, ok := spec["claimRef"].(map[string]any)
		if !ok {
			return nil
		}
		name, _ := claimRef["name"].(string)
		namespace, _ := claimRef["namespace"].(string)
		if name == "" {
			return nil
		}
		return []reconcile.Request{
			{NamespacedName: types.NamespacedName{Name: name, Namespace: namespace}},
		}
	}
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

// functionRevisionToCompositionRevisionMapper creates a WatchMapper that
// models the Crossplane EnqueueCompositionRevisionsForFunctionRevision handler.
// When a FunctionRevision changes, the real handler lists all CompositionRevisions
// and enqueues those whose pipeline references the function. Since the Kamera
// WatchMapper doesn't have store access, this mapper uses the known
// CompositionRevision naming convention from the scenario.
func functionRevisionToCompositionRevisionMapper() tracecheck.WatchMapper {
	return func(obj *unstructured.Unstructured) []reconcile.Request {
		if obj == nil {
			return nil
		}
		labels := obj.GetLabels()
		if labels == nil {
			return nil
		}
		// Only trigger for functions that have the parent package label
		pkgName := labels[pkgv1.LabelParentPackage]
		if pkgName == "" {
			return nil
		}
		// In the real controller, this would List all CompositionRevisions
		// and find those whose pipeline references this function.
		// For the Kamera harness, we use the known CompositionRevision name
		// from the scenario. The rev-1 naming follows the convention used
		// in the workflow JSON.
		return []reconcile.Request{
			{NamespacedName: types.NamespacedName{Name: compositionName + "-rev-1"}},
		}
	}
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

func newLogRecorder(log logging.Logger) xpevent.Recorder {
	return logRecorder{log: log, annotations: map[string]string{}}
}

func (r logRecorder) Event(obj runtime.Object, e xpevent.Event) {
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

func (r logRecorder) WithAnnotations(keysAndValues ...string) xpevent.Recorder {
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
			ExternalInputs:       userInputs,
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

	// Compute initial pending reconciles for environment state objects.
	// Each object whose GVK has a registered primary reconciler gets an
	// initial pending reconcile so the reconciler processes it at startup.
	pending := initialPendingForObjects(builder, objects)

	state, err := builder.BuildStartStateFromObjects(objects, pending)
	if err != nil {
		return tracecheck.StateNode{}, nil, err
	}
	return state, objects, nil
}

// initialPendingForObjects returns initial pending reconciles for environment
// state objects that have a registered primary reconciler.
func initialPendingForObjects(builder *tracecheck.ExplorerBuilder, objects []client.Object) []tracecheck.PendingReconcile {
	var pending []tracecheck.PendingReconcile
	for _, obj := range objects {
		gvk := obj.GetObjectKind().GroupVersionKind()
		if gvk.Kind == "" {
			continue
		}
		reconcilerID := builder.ReconcilerForGVK(gvk)
		if reconcilerID == "" {
			continue
		}
		pending = append(pending, tracecheck.PendingReconcile{
			ReconcilerID: reconcilerID,
			Request: reconcile.Request{
				NamespacedName: types.NamespacedName{
					Namespace: obj.GetNamespace(),
					Name:      obj.GetName(),
				},
			},
			Source: tracecheck.SourceStateChange,
		})
	}
	return pending
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

