package main

import (
	"context"
	"fmt"
	"strings"
	"time"

	knativeharness "github.com/tgoodwin/kamera/examples/knative-serving/knative"
	"github.com/tgoodwin/kamera/pkg/coverage"
	"github.com/tgoodwin/kamera/pkg/explore"
	"github.com/tgoodwin/kamera/pkg/replay"
	"github.com/tgoodwin/kamera/pkg/simclock"
	"github.com/tgoodwin/kamera/pkg/tag"
	"github.com/tgoodwin/kamera/pkg/tracecheck"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"knative.dev/pkg/configmap"
	"knative.dev/pkg/controller"
	"knative.dev/pkg/logging"
	"knative.dev/serving/pkg/apis/autoscaling"
	"knative.dev/serving/pkg/apis/serving"
	v1 "knative.dev/serving/pkg/apis/serving/v1"
	kpareconciler "knative.dev/serving/pkg/reconciler/autoscaling/kpa"
	"knative.dev/serving/pkg/reconciler/configuration"
	revisionreconciler "knative.dev/serving/pkg/reconciler/revision"
	routecontroller "knative.dev/serving/pkg/reconciler/route"
	serverlessservicecontroller "knative.dev/serving/pkg/reconciler/serverlessservice"
	servicecontroller "knative.dev/serving/pkg/reconciler/service"
	"sigs.k8s.io/controller-runtime/pkg/client"
	logf "sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"
)

func newKnativeExplorerBuilder() *tracecheck.ExplorerBuilder {
	// Configure simclock to use 2s steps instead of 1s to speed up scale-to-zero simulation
	// (60s stable window + 30s grace period = 90s total, which is 45 steps at 2s/step)
	// Note: 2s matches the KPA ticker interval (tickInterval = 2s), so tickers work correctly
	simclock.Configure(time.Unix(0, 0), 2*time.Second)

	builder := tracecheck.NewExplorerBuilder(scheme)
	configureKnativeExplorer(builder)

	builder.WithoutOptimizations()

	return builder
}

func buildInitialKnativeState(builder *tracecheck.ExplorerBuilder) tracecheck.StateNode {
	stateBuilder := builder.NewStateEventBuilder()
	svc := buildBaselineService()
	tag.AddSleeveObjectID(svc)
	serviceState := stateBuilder.AddTopLevelObject(svc, "ServiceReconciler")
	return tracecheck.MergeStateNodes(serviceState)
}

func buildBaselineService() *v1.Service {
	return &v1.Service{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "demo",
			Namespace: "default",
		},
		Spec: v1.ServiceSpec{
			ConfigurationSpec: v1.ConfigurationSpec{
				Template: v1.RevisionTemplateSpec{
					ObjectMeta: metav1.ObjectMeta{
						Name: "kamera-test",
						Annotations: map[string]string{
							// Ensure KPA class annotation is present so the KPA reconciler processes the PA.
							autoscaling.ClassAnnotationKey: autoscaling.KPA,
							// Set InitialScale=1 to allow activation
							autoscaling.InitialScaleAnnotationKey: "1",
							// Set MinScale=0 to allow scale-to-zero
							autoscaling.MinScaleAnnotationKey: "0",
						},
					},
					Spec: v1.RevisionSpec{
						PodSpec: corev1.PodSpec{
							Containers: []corev1.Container{{
								Image: "dev.local/test", // this bypasses digest resolution
							}},
						},
					},
				},
			},
		},
	}
}

func configureKnativeExplorer(builder *tracecheck.ExplorerBuilder) {
	builder.WithCustomStrategy("RevisionReconciler", func(r replay.EffectRecorder) tracecheck.Strategy {
		factory := func(ctx context.Context, cmw configmap.Watcher) *controller.Impl {
			impl := revisionreconciler.NewController(ctx, cmw)
			overrideRevisionResolver(impl)
			return impl
		}
		strategy, err := knativeharness.NewKnativeStrategy(factory, r)
		if err != nil {
			panic(err)
		}
		strategy.SetLogger(logf.Log.WithName("RevisionReconciler"))
		return strategy
	}).For("serving.knative.dev/Revision")

	builder.WithCustomStrategy("KPA", func(r replay.EffectRecorder) tracecheck.Strategy {
		factory := func(ctx context.Context, cmw configmap.Watcher) *controller.Impl {
			// Create MultiScaler and wrap it to capture enqueues.
			// The wrapper uses the global async enqueue collector directly.
			baseMultiScaler := knativeharness.NewFakeMultiScaler(ctx.Done(), logging.FromContext(ctx))
			multiScaler := knativeharness.NewEnqueueCapturingDeciders(baseMultiScaler, "KPA")

			impl := kpareconciler.NewController(ctx, cmw, multiScaler)
			return impl
		}
		strategy, err := knativeharness.NewKnativeStrategy(factory, r, serving.RevisionUID)
		if err != nil {
			panic(fmt.Sprintf("NewKnativeStrategy() error = %v", err))
		}
		strategy.SetLogger(logf.Log.WithName("KPAReconciler"))
		return strategy
	}).For("autoscaling.internal.knative.dev/PodAutoscaler")

	builder.WithCustomStrategy("ServiceReconciler", func(r replay.EffectRecorder) tracecheck.Strategy {
		strategy, err := knativeharness.NewKnativeStrategy(servicecontroller.NewController, r)
		if err != nil {
			panic(fmt.Sprintf("NewKnativeStrategy() error = %v", err))
		}
		strategy.SetLogger(logf.Log.WithName("ServiceReconciler"))
		return strategy
	}).For("serving.knative.dev/Service")

	builder.WithCustomStrategy("RouteReconciler", func(r replay.EffectRecorder) tracecheck.Strategy {
		strategy, err := knativeharness.NewKnativeStrategy(routecontroller.NewController, r)
		if err != nil {
			panic(fmt.Sprintf("NewKnativeStrategy() error = %v", err))
		}
		strategy.SetLogger(logf.Log.WithName("RouteReconciler"))
		return strategy
	}).For("serving.knative.dev/Route").Watches(
		"serving.knative.dev/Configuration",
		func(u *unstructured.Unstructured) []reconcile.Request {
			labels := u.GetLabels()
			svcName := labels[serving.ServiceLabelKey]
			if svcName == "" {
				return nil
			}

			return []reconcile.Request{
				{NamespacedName: client.ObjectKey{Namespace: u.GetNamespace(), Name: svcName}},
			}
		})

	builder.WithCustomStrategy("ServerlessServiceReconciler", func(r replay.EffectRecorder) tracecheck.Strategy {
		strategy, err := knativeharness.NewKnativeStrategy(serverlessservicecontroller.NewController, r)
		if err != nil {
			panic(fmt.Sprintf("NewKnativeStrategy() error = %v", err))
		}
		strategy.SetLogger(logf.Log.WithName("ServerlessServiceReconciler"))
		return strategy
	}).For("networking.internal.knative.dev/ServerlessService")

	builder.WithResourceDep("networking.internal.knative.dev/ServerlessService", "ServerlessServiceReconciler", "KPA")

	builder.WithCustomStrategy("ConfigurationReconciler", func(r replay.EffectRecorder) tracecheck.Strategy {
		strategy, err := knativeharness.NewKnativeStrategy(configuration.NewController, r)
		if err != nil {
			panic(err)
		}
		strategy.SetLogger(logf.Log.WithName("ConfigurationReconciler"))
		return strategy
	}).For("serving.knative.dev/Configuration")

	builder.WithResourceDep("Configuration", "ConfigurationReconciler", "RevisionReconciler")

	builder.WithReconciler("RevisionDigestStub", func(c client.Client) tracecheck.Reconciler {
		return &revisionDigestStub{Client: c}
	}).For("serving.knative.dev/Revision")

	builder.WithReconciler("IngressStatusStub", func(c client.Client) tracecheck.Reconciler {
		return &knativeharness.IngressStatusStub{Client: c}
	}).For("networking.internal.knative.dev/Ingress")

	// builder.WithResourceDep("serving.knative.dev/Revision", "RevisionDigestStub", "RevisionReconciler", "KPA", "ServiceReconciler")
	// builder.WithResourceDep("autoscaling.internal.knative.dev/PodAutoscaler", "KPA", "ServerlessServiceReconciler")
	// builder.WithResourceDep("autoscaling.internal.knative.dev/PodAutoscaler", "RevisionReconciler")
	// builder.WithResourceDep("serving.knative.dev/Service", "ServiceReconciler")
	// builder.WithResourceDep("serving.knative.dev/Configuration", "ServiceReconciler", "RevisionReconciler", "RouteReconciler")
	// builder.WithResourceDep("serving.knative.dev/Route", "RouteReconciler", "ServiceReconciler")
	// builder.WithResourceDep("networking.internal.knative.dev/Ingress", "IngressStatusStub", "RouteReconciler", "ServerlessServiceReconciler")
	// builder.WithResourceDep("apps/Deployment", "RevisionReconciler")
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
		variants, err := expandKnativeSingleActionInput(input)
		if err != nil {
			return nil, fmt.Errorf("expand input %d (%s): %w", idx, input.Name, err)
		}

		for _, variant := range variants {
			state, err := buildStateFromCoverageInput(builder, variant)
			if err != nil {
				return nil, fmt.Errorf("build start state for %s: %w", variant.Name, err)
			}

			scenarios = append(scenarios, explore.Scenario{
				Name:         variant.Name,
				InitialState: state,
				Config:       applyInputTuning(baseCfg, variant.Tuning),
			})
		}
	}

	if len(scenarios) == 0 {
		return nil, fmt.Errorf("no scenarios produced")
	}
	return scenarios, nil
}

func defaultKnativeInputs() ([]coverage.Input, error) {
	serviceObj, err := serviceToUnstructured(buildBaselineService())
	if err != nil {
		return nil, err
	}
	return []coverage.Input{
		{
			Name:    "knative-default",
			Objects: []*unstructured.Unstructured{serviceObj},
			Pending: []coverage.Pending{
				{
					ControllerID: "ServiceReconciler",
					Key: coverage.NamespacedName{
						Namespace: "default",
						Name:      "demo",
					},
				},
			},
		},
	}, nil
}

type singleActionMutation struct {
	name  string
	apply func(*v1.Service)
}

func expandKnativeSingleActionInput(input coverage.Input) ([]coverage.Input, error) {
	baseName := strings.TrimSpace(input.Name)
	if baseName == "" {
		baseName = "scenario"
	}

	base := cloneCoverageInput(input)
	base.Name = baseName + "/base"
	serviceIdx := findKnativeService(base.Objects)
	if serviceIdx < 0 {
		return []coverage.Input{base}, nil
	}

	templateSvc, err := unstructuredToService(base.Objects[serviceIdx])
	if err != nil {
		return nil, err
	}

	variants := []coverage.Input{base}
	for _, mutation := range singleActionMutations() {
		updated := cloneCoverageInput(input)
		updated.Name = fmt.Sprintf("%s/%s", baseName, mutation.name)

		svc := templateSvc.DeepCopy()
		mutation.apply(svc)
		updated.Objects[serviceIdx], err = serviceToUnstructured(svc)
		if err != nil {
			return nil, fmt.Errorf("apply mutation %q: %w", mutation.name, err)
		}

		variants = append(variants, updated)
	}
	return variants, nil
}

func singleActionMutations() []singleActionMutation {
	return []singleActionMutation{
		{
			name: "set-image-v2",
			apply: func(svc *v1.Service) {
				ensurePrimaryContainer(svc).Image = "dev.local/test:v2"
			},
		},
		{
			name: "set-min-scale-1",
			apply: func(svc *v1.Service) {
				anns := ensureTemplateAnnotations(svc)
				anns[autoscaling.MinScaleAnnotationKey] = "1"
			},
		},
		{
			name: "set-max-scale-5",
			apply: func(svc *v1.Service) {
				anns := ensureTemplateAnnotations(svc)
				anns[autoscaling.MaxScaleAnnotationKey] = "5"
			},
		},
		{
			name: "set-concurrency-1",
			apply: func(svc *v1.Service) {
				one := int64(1)
				svc.Spec.Template.Spec.ContainerConcurrency = &one
			},
		},
	}
}

func buildStateFromCoverageInput(builder *tracecheck.ExplorerBuilder, input coverage.Input) (tracecheck.StateNode, error) {
	if len(input.Objects) == 0 {
		return tracecheck.StateNode{}, fmt.Errorf("input has no objects")
	}

	objects := make([]client.Object, 0, len(input.Objects))
	for idx, obj := range input.Objects {
		if obj == nil {
			return tracecheck.StateNode{}, fmt.Errorf("input object %d is nil", idx)
		}
		clone := obj.DeepCopy()
		tag.AddSleeveObjectID(clone)
		objects = append(objects, clone)
	}

	pending := make([]tracecheck.PendingReconcile, 0, len(input.Pending))
	for _, p := range input.Pending {
		pending = append(pending, tracecheck.PendingReconcile{
			ReconcilerID: tracecheck.ReconcilerID(p.ControllerID),
			Request: reconcile.Request{
				NamespacedName: client.ObjectKey{
					Namespace: p.Key.Namespace,
					Name:      p.Key.Name,
				},
			},
			Source: tracecheck.SourceStateChange,
		})
	}
	if len(pending) == 0 {
		for _, obj := range objects {
			if isKnativeService(obj) {
				pending = append(pending, tracecheck.PendingReconcile{
					ReconcilerID: "ServiceReconciler",
					Request: reconcile.Request{
						NamespacedName: client.ObjectKey{
							Namespace: obj.GetNamespace(),
							Name:      obj.GetName(),
						},
					},
					Source: tracecheck.SourceStateChange,
				})
			}
		}
	}
	if len(pending) == 0 {
		return tracecheck.StateNode{}, fmt.Errorf("input has no pending reconciles")
	}

	return builder.BuildStartStateFromObjects(objects, pending)
}

func applyInputTuning(base tracecheck.ExploreConfig, tuning coverage.InputTuning) tracecheck.ExploreConfig {
	cfg := base.Clone()
	if tuning.MaxDepth > 0 {
		cfg.MaxDepth = tuning.MaxDepth
	}
	if len(tuning.PermuteControllers) > 0 {
		if cfg.PermuteOrder == nil {
			cfg.PermuteOrder = make(map[tracecheck.ReconcilerID]bool)
		}
		for _, controllerID := range tuning.PermuteControllers {
			cfg.PermuteOrder[tracecheck.ReconcilerID(controllerID)] = true
		}
	}
	return cfg
}

func cloneCoverageInput(input coverage.Input) coverage.Input {
	objects := make([]*unstructured.Unstructured, 0, len(input.Objects))
	for _, obj := range input.Objects {
		if obj == nil {
			objects = append(objects, nil)
			continue
		}
		objects = append(objects, obj.DeepCopy())
	}

	pending := append([]coverage.Pending(nil), input.Pending...)
	tuning := coverage.InputTuning{
		MaxDepth:           input.Tuning.MaxDepth,
		PermuteControllers: append([]string(nil), input.Tuning.PermuteControllers...),
		StaleReads:         cloneStringSliceMap(input.Tuning.StaleReads),
		StaleLookback:      cloneIntMap(input.Tuning.StaleLookback),
	}
	return coverage.Input{
		Name:    input.Name,
		Objects: objects,
		Pending: pending,
		Tuning:  tuning,
	}
}

func cloneStringSliceMap(in map[string][]string) map[string][]string {
	if len(in) == 0 {
		return nil
	}
	out := make(map[string][]string, len(in))
	for k, v := range in {
		out[k] = append([]string(nil), v...)
	}
	return out
}

func cloneIntMap(in map[string]int) map[string]int {
	if len(in) == 0 {
		return nil
	}
	out := make(map[string]int, len(in))
	for k, v := range in {
		out[k] = v
	}
	return out
}

func findKnativeService(objects []*unstructured.Unstructured) int {
	for idx, obj := range objects {
		if obj == nil {
			continue
		}
		if isKnativeService(obj) {
			return idx
		}
	}
	return -1
}

func isKnativeService(obj client.Object) bool {
	if obj == nil {
		return false
	}
	gvk := obj.GetObjectKind().GroupVersionKind()
	if gvk.Kind != "Service" {
		return false
	}
	if gvk.Group == "serving.knative.dev" {
		return true
	}
	if u, ok := obj.(*unstructured.Unstructured); ok {
		return strings.HasPrefix(u.GetAPIVersion(), "serving.knative.dev/")
	}
	return false
}

func ensurePrimaryContainer(svc *v1.Service) *corev1.Container {
	containers := svc.Spec.Template.Spec.Containers
	if len(containers) == 0 {
		svc.Spec.Template.Spec.Containers = []corev1.Container{{Name: "user-container", Image: "dev.local/test"}}
	}
	return &svc.Spec.Template.Spec.Containers[0]
}

func ensureTemplateAnnotations(svc *v1.Service) map[string]string {
	anns := svc.Spec.Template.ObjectMeta.Annotations
	if anns == nil {
		anns = map[string]string{}
		svc.Spec.Template.ObjectMeta.Annotations = anns
	}
	return anns
}

func unstructuredToService(obj *unstructured.Unstructured) (*v1.Service, error) {
	if obj == nil {
		return nil, fmt.Errorf("service object is nil")
	}
	var svc v1.Service
	if err := runtime.DefaultUnstructuredConverter.FromUnstructured(obj.Object, &svc); err != nil {
		return nil, fmt.Errorf("convert service from unstructured: %w", err)
	}
	svc.SetGroupVersionKind(v1.SchemeGroupVersion.WithKind("Service"))
	return &svc, nil
}

func serviceToUnstructured(svc *v1.Service) (*unstructured.Unstructured, error) {
	if svc == nil {
		return nil, fmt.Errorf("service is nil")
	}
	obj, err := runtime.DefaultUnstructuredConverter.ToUnstructured(svc)
	if err != nil {
		return nil, fmt.Errorf("convert service to unstructured: %w", err)
	}
	u := &unstructured.Unstructured{Object: obj}
	u.SetGroupVersionKind(v1.SchemeGroupVersion.WithKind("Service"))
	return u, nil
}
